from __future__ import annotations

import json
import os
import socket
import time
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

try:
    from ezadmin_inbound_request import create_inbound_request
    _EZADMIN_IMPORT_ERROR = ""
except Exception as e:  # pragma: no cover
    create_inbound_request = None  # type: ignore[assignment]
    _EZADMIN_IMPORT_ERROR = str(e)
try:
    from ezadmin_outbound_request import create_outbound_request
    _EZADMIN_OUTBOUND_IMPORT_ERROR = ""
except Exception as e:  # pragma: no cover
    create_outbound_request = None  # type: ignore[assignment]
    _EZADMIN_OUTBOUND_IMPORT_ERROR = str(e)

SKU_NAME_TO_BARCODE = {
    "플라우드 노트 Pro / 블랙": "199284926073",
    "플라우드 노트 Pro / 실버": "199284928237",
    "플라우드 노트 / 블랙": "6977512610000",
    "플라우드 노트 / 실버": "6977512610024",
    "플라우드 노트핀S / 블랙": "0199284031340",
    "플라우드 노트핀S / 실버": "0199284909670",
    "사용설명서": "8821006473832",
    "노트프로 블랙": "199284926073",
    "노트프로 실버": "199284928237",
    "노트 블랙": "6977512610000",
    "노트 실버": "6977512610024",
    "노트핀S 블랙": "0199284031340",
    "노트핀S 실버": "0199284909670",
}

# DNS workaround: getaddrinfo fails while gethostbyname succeeds on this host.
_orig_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    try:
        return _orig_getaddrinfo(host, port, family, type, proto, flags)
    except socket.gaierror as e:
        try:
            ipv4 = socket.gethostbyname(host)
            return _orig_getaddrinfo(ipv4, port, socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
        except Exception:
            raise e


socket.getaddrinfo = _patched_getaddrinfo


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


def _connect_sheet(cfg: Dict[str, Any], retries: int = 5, backoff_sec: int = 3):
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
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client = gspread.authorize(creds)
            ss = client.open_by_key(sheet_id)
            try:
                return ss.worksheet("TransferQueue")
            except Exception:
                ws = ss.add_worksheet(title="TransferQueue", rows=2000, cols=12)
                _ensure_transfer_queue_header(ws)
                return ws
        except Exception as e:
            last_err = e
            if attempt < retries:
                print(f"[WARN] sheet connect failed (attempt {attempt}/{retries}): {e}")
                time.sleep(backoff_sec)
    raise RuntimeError(f"구글 시트 연결 실패: {last_err}")


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
    # Poomgo docs use raw KEY in Authorization (no Bearer).
    return {"Authorization": token, "Content-Type": "application/json", "accept": "application/json"}


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
    url = "https://api.poomgo.com/open-api/wms/receiving-sheets"
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
    if resp.status_code >= 400:
        raise RuntimeError(f"poomgo error {resp.status_code}: {resp.text[:200]}")
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
    if resp.status_code >= 400:
        raise RuntimeError(f"poomgo error {resp.status_code}: {resp.text[:200]}")


def _run_once() -> None:
    # Preflight DNS check (helps flaky resolver on this host)
    for i in range(1, 4):
        try:
            ip = socket.gethostbyname("oauth2.googleapis.com")
            print(f"[INFO] DNS preflight ok: {ip}")
            break
        except Exception as e:
            print(f"[WARN] DNS preflight failed ({i}/3): {e}")
            time.sleep(1)

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
    destination_warehouse_raw = str(
        recv_cfg.get("destination_warehouse") or os.getenv("POOMGO_DESTINATION_WAREHOUSE", "")
    ).strip()
    allow_null_warehouse = (
        destination_warehouse_raw.lower() in {"null", "none"}
        or os.getenv("POOMGO_ALLOW_NULL_WAREHOUSE", "0") == "1"
    )
    destination_warehouse = None if destination_warehouse_raw.lower() in {"null", "none", ""} else destination_warehouse_raw
    schedule_form_code_key = str(
        recv_cfg.get("schedule_form_code_key") or os.getenv("POOMGO_SCHEDULE_FORM_CODE_KEY", "")
    ).strip()
    delivery_type = str(
        recv_cfg.get("delivery_type") or os.getenv("POOMGO_DELIVERY_TYPE", "")
    ).strip()
    pallet_count_raw = recv_cfg.get("pallet_count")
    if pallet_count_raw is None:
        pallet_count_raw = os.getenv("POOMGO_PALLET_COUNT", "")
    box_count_raw = recv_cfg.get("box_count")
    if box_count_raw is None:
        box_count_raw = os.getenv("POOMGO_BOX_COUNT", "")

    def _to_int(val: Any):
        if val is None:
            return None
        if isinstance(val, str) and not val.strip():
            return None
        try:
            return int(val)
        except Exception:
            return None

    pallet_count = _to_int(pallet_count_raw)
    box_count = _to_int(box_count_raw)

    # Ezadmin inbound processing (grouped by date/from/to)
    ez_headless = os.getenv("EZADMIN_HEADLESS", "1") != "0"
    ez_enabled = os.getenv("EZADMIN_INBOUND_ENABLE", "0") == "1"
    ez_out_enabled = os.getenv("EZADMIN_OUTBOUND_ENABLE")
    if ez_out_enabled is None:
        ez_out_enabled = "1" if ez_enabled else "0"
    ez_out_enabled = ez_out_enabled == "1"
    if not ez_enabled:
        print("[INFO] EZADMIN_INBOUND_ENABLE != 1; skipping ezadmin inbound.")
    elif create_inbound_request is None:
        print(f"[WARN] ezadmin_inbound_request not available: {_EZADMIN_IMPORT_ERROR}")
    else:
        ez_groups: Dict[tuple, List[tuple]] = {}
        for row_idx, row in rows:
            status = _get(row, header_idx.get("status", -1))
            action = _get(row, header_idx.get("action", -1))
            to_channel = _get(row, header_idx.get("to_channel", -1))
            from_channel = _get(row, header_idx.get("from_channel", -1))
            date_str = _get(row, header_idx.get("date", -1))
            if action == "CANCEL":
                continue
            if status not in {"", "PENDING"}:
                continue
            if to_channel != "이지어드민":
                continue
            key = (date_str, from_channel, to_channel)
            ez_groups.setdefault(key, []).append((row_idx, row))

        print(f"[INFO] ezadmin groups: {len(ez_groups)}")
        for (date_str, from_channel, _to_channel), group_rows in ez_groups.items():
            items = []
            for _row_idx, row in group_rows:
                sku_name = _get(row, header_idx.get("sku_name", -1))
                barcode = SKU_NAME_TO_BARCODE.get(sku_name)
                items.append(
                    {
                        "sku_name": sku_name,
                        "barcode": barcode or "",
                        "quantity": _get(row, header_idx.get("quantity", -1)),
                    }
                )
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                resp = create_inbound_request(
                    items=items,
                    date_str=date_str or datetime.now().strftime("%Y-%m-%d"),
                    from_channel=from_channel or "신규",
                    headless=ez_headless,
                )
                msg = f"ezadmin_sheet={resp.get('display_name') or resp.get('sheet_name','')}"
                for row_idx, _ in group_rows:
                    _update_row(
                        ws,
                        row_idx,
                        header_idx,
                        status="SUCCESS",
                        message=msg,
                        updated_at=now,
                    )
            except Exception as e:
                for row_idx, _ in group_rows:
                    _update_row(
                        ws,
                        row_idx,
                        header_idx,
                        status="FAILED",
                        message=str(e)[:200],
                        updated_at=now,
                    )

    # Ezadmin outbound processing (이지어드민 -> 품고)
    if not ez_out_enabled:
        print("[INFO] EZADMIN_OUTBOUND_ENABLE != 1; skipping ezadmin outbound.")
    elif create_outbound_request is None:
        print(f"[WARN] ezadmin_outbound_request not available: {_EZADMIN_OUTBOUND_IMPORT_ERROR}")
    else:
        ez_out_groups: Dict[tuple, List[tuple]] = {}
        for row_idx, row in rows:
            status = _get(row, header_idx.get("status", -1))
            action = _get(row, header_idx.get("action", -1))
            to_channel = _get(row, header_idx.get("to_channel", -1))
            from_channel = _get(row, header_idx.get("from_channel", -1))
            date_str = _get(row, header_idx.get("date", -1))
            if action == "CANCEL":
                continue
            if status not in {"", "PENDING"}:
                continue
            if not (from_channel == "이지어드민" and to_channel == "품고"):
                continue
            key = (date_str, from_channel, to_channel)
            ez_out_groups.setdefault(key, []).append((row_idx, row))

        print(f"[INFO] ezadmin outbound groups: {len(ez_out_groups)}")
        for (date_str, from_channel, _to_channel), group_rows in ez_out_groups.items():
            items = []
            for _row_idx, row in group_rows:
                items.append(
                    {
                        "sku_name": _get(row, header_idx.get("sku_name", -1)),
                        "quantity": _get(row, header_idx.get("quantity", -1)),
                    }
                )
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                resp = create_outbound_request(
                    items=items,
                    date_str=date_str or datetime.now().strftime("%Y-%m-%d"),
                    from_channel=from_channel or "이지어드민",
                    headless=ez_headless,
                    stop_after_create=False,
                )
                msg = f"ezadmin_outbound_sheet={resp.get('display_name') or resp.get('sheet_name','')}"
                for row_idx, _ in group_rows:
                    _update_row(
                        ws,
                        row_idx,
                        header_idx,
                        status="EZADMIN_DONE",
                        message=msg,
                        updated_at=now,
                    )
            except Exception as e:
                for row_idx, _ in group_rows:
                    _update_row(
                        ws,
                        row_idx,
                        header_idx,
                        status="FAILED",
                        message=str(e)[:200],
                        updated_at=now,
                    )

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

        if status not in {"", "PENDING", "EZADMIN_DONE"}:
            continue

        if to_channel != "품고":
            continue

        missing = []
        if not poomgo_token:
            missing.append("poomgo.token")
        if not destination_warehouse and not allow_null_warehouse:
            missing.append("destination_warehouse")
        if not schedule_form_code_key:
            missing.append("schedule_form_code_key")
        if not delivery_type:
            missing.append("delivery_type")
        if pallet_count is None:
            missing.append("pallet_count")
        if box_count is None:
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
    loop = os.getenv("TRANSFER_WORKER_LOOP", "0") == "1"
    try:
        interval = int(os.getenv("TRANSFER_WORKER_INTERVAL", "60"))
    except Exception:
        interval = 60
    interval = max(10, interval)
    if not loop:
        _run_once()
    else:
        print(f"[INFO] loop enabled: interval={interval}s")
        while True:
            try:
                _run_once()
            except Exception as e:
                print(f"[ERROR] worker failed: {e}")
            time.sleep(interval)
