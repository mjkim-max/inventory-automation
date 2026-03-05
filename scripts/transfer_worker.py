from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

try:
    import tomllib  # py311+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]
try:
    import tomli  # py310 fallback
except Exception:  # pragma: no cover
    tomli = None  # type: ignore[assignment]

try:
    import gspread
except Exception:  # pragma: no cover
    gspread = None  # type: ignore[assignment]
try:
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore[assignment]


def _load_toml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_bytes()
    if tomllib is not None:
        return tomllib.loads(raw.decode("utf-8"))
    if tomli is not None:
        return tomli.loads(raw.decode("utf-8"))
    raise RuntimeError("tomllib/tomli not available. Install tomli for Python<3.11.")


def _load_secrets() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    secrets_path = Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml"
    try:
        cfg.update(_load_toml(secrets_path))
    except Exception:
        pass
    return cfg


def _connect_sheet(cfg: Dict[str, Any]):
    if gspread is None or Credentials is None:
        raise RuntimeError("gspread/google-auth not installed.")
    gs_cfg = cfg.get("google_sheets", {})
    sa = cfg.get("google_sheets_service_account", {})
    if not gs_cfg or not sa:
        raise RuntimeError("google_sheets or google_sheets_service_account missing in secrets.")
    sheet_id = gs_cfg.get("sheet_id") or gs_cfg.get("spreadsheet_id")
    if not sheet_id:
        raise RuntimeError("google_sheets.sheet_id is required.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(sa), scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)
    try:
        return ss.worksheet("TransferQueue")
    except Exception:
        ws = ss.add_worksheet(title="TransferQueue", rows=2000, cols=12)
        _ensure_transfer_queue_header(ws)
        return ws


def _ensure_transfer_queue_header(ws) -> None:
    header = [
        "date",
        "from_channel",
        "to_channel",
        "sku_name",
        "quantity",
        "status",
        "message",
        "created_at",
        "updated_at",
    ]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0] != header:
        ws.insert_row(header, index=1)


def _find_pending_rows(values: List[List[str]]) -> List[tuple[int, List[str]]]:
    pending = []
    for idx, row in enumerate(values[1:], start=2):
        status = row[5].strip() if len(row) > 5 else ""
        if status in {"", "PENDING"}:
            pending.append((idx, row))
    return pending


def main() -> None:
    cfg = _load_secrets()
    ws = _connect_sheet(cfg)
    _ensure_transfer_queue_header(ws)

    values = ws.get_all_values()
    pending = _find_pending_rows(values)
    print(f"[INFO] pending rows: {len(pending)}")

    # Backbone only: do not process unless explicitly enabled
    if os.getenv("TRANSFER_WORKER_PROCESS", "0") != "1":
        print("[INFO] TRANSFER_WORKER_PROCESS != 1; skipping processing.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row_idx, row in pending:
        ws.update(
            f"F{row_idx}:I{row_idx}",
            [["BLOCKED", "handlers not configured", now, now]],
        )


if __name__ == "__main__":
    main()
