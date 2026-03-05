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

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

SKU_NAME_TO_BARCODE = {
    "플라우드 노트 Pro / 블랙": "199284926073",
    "플라우드 노트 Pro / 실버": "199284928237",
    "플라우드 노트 / 블랙": "6977512610000",
    "플라우드 노트 / 실버": "6977512610024",
    "플라우드 노트핀S / 블랙": "0199284031340",
    "플라우드 노트핀S / 실버": "0199284909670",
    "사용설명서": "8821006473832",
}


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
        "action",
        "external_id",
    ]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0] != header:
        ws.insert_row(header, index=1)


def _col_letter(idx: int) -> str:
    # 1-based index
    s = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s


def _get(row: List[str], idx: int, default: str = "") -> str:
    if idx < 0 or idx >= len(row):
        return default
    return str(row[idx]).strip()


def _build_header_index(header: List[str]) -> Dict[str, int]:
    return {name: i for i, name in enumerate(header)}


def _update_row(ws, row_idx: int, header_idx: Dict[str, int], *,
               status: str, message: str, updated_at: str,
               action: str = "", external_id: str = "") -> None:
    status_col = header_idx.get("status", -1) + 1
    message_col = header_idx.get("message", -1) + 1
    updated_col = header_idx.get("updated_at", -1) + 1
    action_col = header_idx.get("action", -1) + 1
    external_col = header_idx.get("external_id", -1) + 1
    # Update minimal cells to avoid overwriting created_at
    if status_col > 0:
        ws.update_cell(row_idx, status_col, status)
    if message_col > 0:
        ws.update_cell(row_idx, message_col, message)
    if updated_col > 0:
        ws.update_cell(row_idx, updated_col, updated_at)
    if action_col > 0:
        ws.update_cell(row_idx, action_col, action)
    if external_col > 0 and external_id:
        ws.update_cell(row_idx, external_col, external_id)


def _poomgo_headers(token: str) -> Dict[str, str]:
    if not token:
        return {}
    if token.lower().startswith("bearer "):
        return {"Authorization": token, "Content-Type": "application/json", "accept": "application/json"}
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "accept": "application/json"}


def _poomgo_create_receiving(
    *,
    token: str,
    name: str,
    depart_at: str,
    arrive_at: str,
    schedule_form_code_key: str,
    delivery_type: str,
    pallet_count: int,
    box_count: int,
    destination_warehouse: str,
    resources: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is not installed.")
    url = "https://api.poomgo.com/open-api/receiving-sheets"
    payload = {
        "name": name,
        "depart_at": depart_at,
        "arrive_at": arrive_at,
        "schedule_form_code_key": schedule_form_code_key,
        "delivery_type": delivery_type,
        "pallet_count": pallet_count,
        "box_count": box_count,
        "destination_warehouse": destination_warehouse,
        "resources": resources,
    }
    headers = _poomgo_headers(token)
    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    if resp.status_code in {401, 403} and not token.lower().startswith("bearer "):
        headers = _poomgo_headers(f"Bearer {token}")
        resp = requests.put(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _poomgo_cancel_receiving(*, token: str, receiving_id: str) -> None:
    if requests is None:
        raise RuntimeError("requests is not installed.")
    url = f"https://api.poomgo.com/open-api/wms/receiving-sheets/{receiving_id}"
    headers = _poomgo_headers(token)
    resp = requests.delete(url, headers=headers, timeout=30)
    if resp.status_code in {401, 403} and not token.lower().startswith("bearer "):
        headers = _poomgo_headers(f"Bearer {token}")
        resp = requests.delete(url, headers=headers, timeout=30)
    resp.raise_for_status()


def main() -> None:
    cfg = _load_secrets()
    ws = _connect_sheet(cfg)
    _ensure_transfer_queue_header(ws)

    values = ws.get_all_values()
    if not values:
        print("[INFO] TransferQueue is empty.")
        return
    header = values[0]
    header_idx = _build_header_index(header)
    rows = list(enumerate(values[1:], start=2))
    print(f"[INFO] rows: {len(rows)}")

    # Backbone only: do not process unless explicitly enabled
    if os.getenv("TRANSFER_WORKER_PROCESS", "0") != "1":
        print("[INFO] TRANSFER_WORKER_PROCESS != 1; skipping processing.")
        return

    poomgo_cfg = cfg.get("poomgo", {})
    poomgo_token = poomgo_cfg.get("token") or os.getenv("POOMGO_TOKEN", "")
    recv_cfg = poomgo_cfg.get("receiving", {})
    destination_warehouse = str(
        recv_cfg.get("destination_warehouse") or os.getenv("POOMGO_DESTINATION_WAREHOUSE", "")
    ).strip()
    schedule_form_code_key = str(
        recv_cfg.get("schedule_form_code_key") or os.getenv("POOMGO_SCHEDULE_FORM_CODE_KEY", "")
    ).strip()
    delivery_type = str(
        recv_cfg.get("delivery_type") or os.getenv("POOMGO_DELIVERY_TYPE", "")
    ).strip()
    pallet_count_raw = recv_cfg.get("pallet_count") or os.getenv("POOMGO_PALLET_COUNT", "")
    box_count_raw = recv_cfg.get("box_count") or os.getenv("POOMGO_BOX_COUNT", "")

    def _to_int(val: Any) -> int:
        try:
            return int(val)
        except Exception:
            return 0

    pallet_count = _to_int(pallet_count_raw)
    box_count = _to_int(box_count_raw)

    for row_idx, row in rows:
        status = _get(row, header_idx.get("status", -1))
        action = _get(row, header_idx.get("action", -1))
        to_channel = _get(row, header_idx.get("to_channel", -1))
        from_channel = _get(row, header_idx.get("from_channel", -1))
        sku_name = _get(row, header_idx.get("sku_name", -1))
        qty_raw = _get(row, header_idx.get("quantity", -1))
        date_str = _get(row, header_idx.get("date", -1))
        external_id = _get(row, header_idx.get("external_id", -1))

        if action == "CANCEL":
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not poomgo_token or not external_id:
                _update_row(
                    ws,
                    row_idx,
                    header_idx,
                    status="CANCEL_FAILED",
                    message="missing token or external_id",
                    updated_at=now,
                    action="",
                )
                continue
            try:
                _poomgo_cancel_receiving(token=poomgo_token, receiving_id=external_id)
                _update_row(
                    ws,
                    row_idx,
                    header_idx,
                    status="CANCELLED",
                    message="cancelled",
                    updated_at=now,
                    action="",
                )
            except Exception as e:
                _update_row(
                    ws,
                    row_idx,
                    header_idx,
                    status="CANCEL_FAILED",
                    message=str(e)[:200],
                    updated_at=now,
                    action="",
                )
            continue

        if status not in {"", "PENDING"}:
            continue

        if to_channel != "품고":
            continue

        missing = []
        if not poomgo_token:
            missing.append("poomgo.token")
        if not destination_warehouse:
            missing.append("destination_warehouse")
        if not schedule_form_code_key:
            missing.append("schedule_form_code_key")
        if not delivery_type:
            missing.append("delivery_type")
        if pallet_count <= 0:
            missing.append("pallet_count")
        if box_count <= 0:
            missing.append("box_count")
        if missing:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _update_row(
                ws,
                row_idx,
                header_idx,
                status="BLOCKED",
                message="missing: " + ", ".join(missing),
                updated_at=now,
            )
            continue

        barcode = SKU_NAME_TO_BARCODE.get(sku_name)
        if not barcode:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _update_row(
                ws,
                row_idx,
                header_idx,
                status="FAILED",
                message=f"barcode not found for {sku_name}",
                updated_at=now,
            )
            continue

        try:
            qty = int(qty_raw)
        except Exception:
            qty = 0
        if qty <= 0:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _update_row(
                ws,
                row_idx,
                header_idx,
                status="FAILED",
                message="invalid quantity",
                updated_at=now,
            )
            continue

        name_prefix = date_str.replace("-", "") if date_str else datetime.now().strftime("%Y%m%d")
        name = f"{name_prefix}_{from_channel}_품고"
        depart_at = date_str or datetime.now().strftime("%Y-%m-%d")
        arrive_at = depart_at
        resources = [{"barcode": barcode, "quantity": qty}]

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            resp = _poomgo_create_receiving(
                token=poomgo_token,
                name=name,
                depart_at=depart_at,
                arrive_at=arrive_at,
                schedule_form_code_key=schedule_form_code_key,
                delivery_type=delivery_type,
                pallet_count=pallet_count,
                box_count=box_count,
                destination_warehouse=destination_warehouse,
                resources=resources,
            )
            rid = str(resp.get("id") or "").strip()
            _update_row(
                ws,
                row_idx,
                header_idx,
                status="SUCCESS",
                message=f"poomgo_id={rid}" if rid else "poomgo_created",
                updated_at=now,
                external_id=rid,
            )
        except Exception as e:
            _update_row(
                ws,
                row_idx,
                header_idx,
                status="FAILED",
                message=str(e)[:200],
                updated_at=now,
            )


if __name__ == "__main__":
    main()
