from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List

import streamlit as st
import streamlit.components.v1 as components
import gspread
from google.oauth2.service_account import Credentials
import json
import re
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

_KST = ZoneInfo("Asia/Seoul") if ZoneInfo else None
_UTC = ZoneInfo("UTC") if ZoneInfo else None


def _now_kst() -> datetime:
    if _KST:
        return datetime.now(_KST)
    return datetime.now()


def _parse_kst(ts: str) -> datetime | None:
    if not ts:
        return None
    dt = None
    try:
        dt = datetime.fromisoformat(ts.strip())
    except Exception:
        try:
            dt = datetime.strptime(ts.strip(), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    if _KST:
        if dt.tzinfo is None and _UTC:
            dt = dt.replace(tzinfo=_UTC).astimezone(_KST)
        else:
            dt = dt.astimezone(_KST)
    return dt


SHEET_COLUMNS = {
    "date": "A",
    "notepro_black": {"poomgo": "B", "ezadmin": "C", "coupang": "D"},
    "notepro_silver": {"poomgo": "F", "ezadmin": "G", "coupang": "H"},
    "note_black": {"poomgo": "J", "ezadmin": "K", "coupang": "L"},
    "note_silver": {"poomgo": "N", "ezadmin": "O", "coupang": "P"},
    "notepin_black": {"poomgo": "R", "ezadmin": "S", "coupang": "T"},
    "notepin_silver": {"poomgo": "V", "ezadmin": "W", "coupang": "X"},
    "manual": {"poomgo": "Z", "ezadmin": "", "coupang": ""},
}

SKU_LABELS = {
    "notepro_black": "노트프로 블랙",
    "notepro_silver": "노트프로 실버",
    "note_black": "노트 블랙",
    "note_silver": "노트 실버",
    "notepin_black": "노트핀S 블랙",
    "notepin_silver": "노트핀S 실버",
    "manual": "사용설명서",
}


def _col_to_index(col: str) -> int:
    col = col.upper().strip()
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx


def _connect_sheet(readonly: bool = True):
    cfg = st.secrets.get("google_sheets", {})
    sa = st.secrets.get("google_sheets_service_account", {})
    if not cfg or not sa:
        raise RuntimeError("google_sheets / google_sheets_service_account missing in secrets.")
    sheet_id = cfg.get("sheet_id") or cfg.get("spreadsheet_id")
    worksheet = cfg.get("worksheet", "daily_inventory")
    if not sheet_id:
        raise RuntimeError("google_sheets.sheet_id missing.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    if not readonly:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(sa), scopes=scopes)
    client = gspread.authorize(creds)
    try:
        ss = client.open_by_key(sheet_id)
    except Exception:
        # If a full URL was provided, extract the ID and retry
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", str(sheet_id))
        if m:
            ss = client.open_by_key(m.group(1))
        else:
            raise
    return ss.worksheet(worksheet)


def _connect_sales_sheet(readonly: bool = True):
    cfg = st.secrets.get("google_sheets", {})
    sa = st.secrets.get("google_sheets_service_account", {})
    if not cfg or not sa:
        raise RuntimeError("google_sheets / google_sheets_service_account missing in secrets.")
    sheet_id = cfg.get("sheet_id") or cfg.get("spreadsheet_id")
    worksheet = cfg.get("sales_worksheet", "sales_snapshot")
    if not sheet_id:
        raise RuntimeError("google_sheets.sheet_id missing.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    if not readonly:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(sa), scopes=scopes)
    client = gspread.authorize(creds)
    try:
        ss = client.open_by_key(sheet_id)
    except Exception:
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", str(sheet_id))
        if m:
            ss = client.open_by_key(m.group(1))
        else:
            raise
    return ss.worksheet(worksheet)


@st.cache_data(ttl=30, show_spinner=False)
def _get_sheet_values_cached(worksheet_name: str) -> List[List[str]]:
    ws = _connect_sheet(readonly=True)
    if worksheet_name != ws.title:
        ws = ws.spreadsheet.worksheet(worksheet_name)
    return ws.get_all_values()


@st.cache_data(ttl=30, show_spinner=False)
def _get_sales_values_cached() -> List[List[str]]:
    ws = _connect_sales_sheet(readonly=True)
    return ws.get_all_values()


def _get_latest_sales_snapshot():
    values = _get_sales_values_cached()
    if len(values) < 2:
        return {}
    latest_row = None
    latest_dt = None
    for i, row in enumerate(values):
        if i == 0:
            continue
        if len(row) < 3:
            continue
        raw_ts = row[1].strip()
        if not raw_ts:
            continue
        try:
            dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
            latest_row = row
    if not latest_row:
        return {}
    payload_json = latest_row[2].strip() if len(latest_row) > 2 else ""
    payload = {}
    if payload_json:
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = {}
    now_kst = _now_kst()
    return {
        "fetched_at": latest_row[1].strip(),
        "date": payload.get("date", "-"),
        "cafe24_sales_qty": payload.get("cafe24_sales_qty", "-"),
        "coupang_sales_qty": payload.get("coupang_sales_qty", "-"),
        "smartstore_sales_qty": payload.get("smartstore_sales_qty", "-"),
        "cafe24_items": payload.get("cafe24_items", {}),
        "coupang_items": payload.get("coupang_items", {}),
        "smartstore_items": payload.get("smartstore_items", {}),
        "view_time": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _load_sales_history(days: int = 90) -> Dict[str, Dict[str, int]]:
    values = _get_sales_values_cached()
    if len(values) < 2:
        return {}
    # Keep latest snapshot per date (by fetched_at timestamp)
    latest_by_date: Dict[str, Dict[str, str]] = {}
    for i, row in enumerate(values):
        if i == 0 or len(row) < 3:
            continue
        fetched_at = row[1].strip()
        payload_json = row[2].strip()
        if not payload_json:
            continue
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        date_val = str(payload.get("date", "")).strip()
        if not date_val:
            continue
        # Normalize to YYYY-MM-DD
        try:
            date_val = datetime.fromisoformat(date_val).strftime("%Y-%m-%d")
        except Exception:
            date_val = date_val.split(" ")[0]
        prev = latest_by_date.get(date_val)
        if not prev or fetched_at >= prev.get("fetched_at", ""):
            latest_by_date[date_val] = {
                "fetched_at": fetched_at,
                "payload_json": payload_json,
            }

    cutoff = (_now_kst() - timedelta(days=days)).strftime("%Y-%m-%d")
    per_date: Dict[str, Dict[str, int]] = {}
    label_map = {
        "P00000CL000E": "플라우드 노트 / 블랙",
        "P00000CL000I": "플라우드 노트 / 실버",
        "P00000DN000M": "플라우드 노트 Pro / 블랙",
        "P00000DN000N": "플라우드 노트 Pro / 실버",
        "P00000CT000U": "플라우드 노트핀S / 블랙",
        "P00000CT000V": "플라우드 노트핀S / 실버",
    }
    coupang_map = {
        "94199205555": "플라우드 노트 Pro / 블랙",
        "94199205552": "플라우드 노트 Pro / 실버",
        "90737907302": "플라우드 노트 / 블랙",
        "90737907295": "플라우드 노트 / 실버",
        "91942294087": "플라우드 노트핀S / 블랙",
        "91942294096": "플라우드 노트핀S / 실버",
    }
    for date_val, info in latest_by_date.items():
        if date_val < cutoff:
            continue
        try:
            payload = json.loads(info["payload_json"])
        except Exception:
            continue
        cafe_items = payload.get("cafe24_items", {}) or {}
        smart_items = payload.get("smartstore_items", {}) or {}
        coupang_items = payload.get("coupang_items", {}) or {}
        day = per_date.setdefault(date_val, {})
        # Cafe24 by code -> name
        for code, name in label_map.items():
            day[name] = day.get(name, 0) + _safe_int(cafe_items.get(code, 0))
        # Smartstore by name
        for name in label_map.values():
            day[name] = day.get(name, 0) + _safe_int(smart_items.get(name, 0))
        # Coupang by id -> name
        for cid, name in coupang_map.items():
            day[name] = day.get(name, 0) + _safe_int(coupang_items.get(cid, 0))
    return per_date


def _ensure_add_inventory_header(ws) -> None:
    header = ["date", "from_channel", "channel", "sku_name", "quantity"]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0] != header:
        ws.insert_row(header, index=1)


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
        "sheet_name",
    ]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0][: len(header)] != header:
        ws.update("A1", [header])


def _get_latest_date_row(values: List[List[str]]) -> int:
    date_col = _col_to_index(SHEET_COLUMNS["date"]) - 1
    latest_idx = -1
    latest_dt = None
    for i, row in enumerate(values):
        if i < 2:
            continue
        if date_col >= len(row):
            continue
        raw = row[date_col].strip()
        if not raw:
            continue
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
        except Exception:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
            latest_idx = i
    return latest_idx


def _row_value(row: List[str], col: str) -> str:
    if not col:
        return ""
    idx = _col_to_index(col) - 1
    if idx < len(row):
        return row[idx]
    return ""


def _build_row_summary(row: List[str]) -> Dict[str, Dict[str, str]]:
    summary: Dict[str, Dict[str, str]] = {}
    for key, cols in SHEET_COLUMNS.items():
        if key == "date":
            continue
        summary[key] = {
            "poomgo": _row_value(row, cols["poomgo"]),
            "ezadmin": _row_value(row, cols["ezadmin"]),
            "coupang": _row_value(row, cols["coupang"]),
        }
    return summary


def _safe_int(value: str) -> int:
    raw = str(value or "").replace(",", "").strip()
    if not raw:
        return 0
    if not raw.lstrip("-").isdigit():
        return 0
    return int(raw)


def _parse_date(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None


def _load_intake_rows(sheet) -> List[Dict[str, str]]:
    try:
        ws = sheet.spreadsheet.worksheet("Add_inventory")
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return []
        header = values[0]
        rows = values[1:]
        result = []
        for r in rows:
            if not r or r == header:
                continue
            if len(r) >= 1 and str(r[0]).strip().lower() == "date":
                continue
            result.append(dict(zip(header, r)))
        return result
    except Exception:
        return []


def _get_stock_series(values: List[List[str]], col: str) -> List[Tuple[datetime, int]]:
    if not col:
        return []
    date_col = _col_to_index(SHEET_COLUMNS["date"]) - 1
    stock_col = _col_to_index(col) - 1
    series: List[Tuple[datetime, int]] = []
    for row in values:
        if date_col >= len(row):
            continue
        dt = _parse_date(row[date_col].strip())
        if not dt:
            continue
        val = row[stock_col] if stock_col < len(row) else ""
        series.append((dt, _safe_int(val)))
    series.sort(key=lambda x: x[0])
    return series


def _calc_avg_outflow(
    series: List[Tuple[datetime, int]],
    intake_rows: List[Dict[str, str]],
    channel_label: str,
    sku_label: str,
) -> Tuple[Optional[float], Optional[int]]:
    if not series:
        return None, None
    today_dt, today_stock = series[-1]
    target_dt = today_dt - timedelta(days=30)
    # Pick the date closest to target_dt (within past dates only)
    candidates = [(dt, val) for dt, val in series if dt <= today_dt]
    if not candidates:
        return None, None
    past_dt, past_stock = min(
        candidates,
        key=lambda x: abs((x[0] - target_dt).days),
    )
    days = (today_dt - past_dt).days
    if days <= 0:
        return None, None

    intake_sum = 0
    outbound_sum = 0
    for r in intake_rows:
        to_ch = str(r.get("channel", ""))
        from_ch = str(r.get("from_channel", ""))
        if str(r.get("sku_name", "")) != sku_label:
            continue
        dt = _parse_date(str(r.get("date", "")))
        if not dt:
            continue
        if past_dt < dt <= today_dt:
            qty = _safe_int(r.get("quantity", "0"))
            if to_ch == channel_label:
                intake_sum += qty
            if from_ch == channel_label:
                outbound_sum += qty

    avg = (past_stock - today_stock + intake_sum - outbound_sum) / days
    if avg < 0:
        avg = 0.0
    return avg, days


def main() -> None:
    st.set_page_config(page_title="재고 대시보드", layout="wide")
    # Auto-refresh shortly after each top-of-hour update (align with hourly sheet writes)
    components.html(
        """
<script>
(function(){
  const now = new Date();
  const next = new Date(now);
  next.setHours(now.getHours() + 1, 2, 0, 0); // HH:02 to allow sheet update to finish
  const delay = Math.max(1000, next.getTime() - now.getTime());
  setTimeout(() => { window.location.reload(); }, delay);
})();
</script>
""",
        height=0,
        width=0,
    )
    header_left, header_right = st.columns([3, 1])
    with header_left:
        st.title("재고 대시보드")
    with header_right:
        st.write("")
        st.write("")
        # 재고 최신화 버튼 제거 (로컬 스케줄러로만 동작)

    try:
        ws = _connect_sheet(readonly=True)
    except Exception as e:
        st.error(f"구글 시트 연결 실패: {e}")
        st.stop()
    values = _get_sheet_values_cached(ws.title)
    if not values:
        st.warning("시트 데이터가 비어 있습니다.")
        return

    latest_idx = _get_latest_date_row(values)
    if latest_idx == -1:
        st.warning("유효한 날짜 행이 없습니다.")
        return

    latest_row = values[latest_idx]
    latest_date = _row_value(latest_row, SHEET_COLUMNS["date"])

    now_kst = _now_kst()
    latest_label = f"{latest_date} {now_kst.strftime('%H:%M')}"
    st.subheader(f"최근 데이터: {latest_label}")

    def _channel_status(row: List[str], channel_key: str) -> str:
        for key, cols in SHEET_COLUMNS.items():
            if key == "date":
                continue
            val = _row_value(row, cols[channel_key])
            if str(val).strip():
                return "수집완료"
        return "수집실패"

    status_line = (
        f"품고 : {_channel_status(latest_row, 'poomgo')}   ㅣ   "
        f"이지어드민 : {_channel_status(latest_row, 'ezadmin')}   ㅣ   "
        f"쿠팡 : {_channel_status(latest_row, 'coupang')}"
    )
    st.caption(status_line)
    summary = _build_row_summary(latest_row)
    intake_rows = _load_intake_rows(ws)

    def build_channel_table(channel_key: str, channel_label: str) -> List[Dict[str, str]]:
        rows = []
        for key, label in SKU_LABELS.items():
            col = SHEET_COLUMNS[key][channel_key]
            series = _get_stock_series(values, col)
            avg, _days = _calc_avg_outflow(series, intake_rows, channel_label, label)
            stock = summary.get(key, {}).get(channel_key, "")
            avg_val = f"{avg:.2f}" if avg is not None else "-"
            if avg and avg > 0:
                days_left = int(_safe_int(stock) / avg) if _safe_int(stock) > 0 else 0
                days_text = str(days_left)
            else:
                days_text = "-"
            rows.append(
                {
                    "품목명": label,
                    "재고수량": stock,
                    "일평균 출고량": avg_val,
                    "출고 가능 일 수": days_text,
                }
            )
        return rows

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.subheader("품고")
        st.dataframe(build_channel_table("poomgo", "품고"), use_container_width=True, hide_index=True)
    with col_b:
        st.subheader("이지어드민")
        st.dataframe(build_channel_table("ezadmin", "이지어드민"), use_container_width=True, hide_index=True)
    with col_c:
        st.subheader("쿠팡")
        st.dataframe(build_channel_table("coupang", "쿠팡"), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("합계")
    total_rows = []
    for key, label in SKU_LABELS.items():
        stock_sum = (
            _safe_int(summary.get(key, {}).get("poomgo", "0"))
            + _safe_int(summary.get(key, {}).get("ezadmin", "0"))
            + _safe_int(summary.get(key, {}).get("coupang", "0"))
        )
        # Aggregate avg outflow as sum of channel averages
        avg_sum = 0.0
        for ch_key, ch_label in [("poomgo", "품고"), ("ezadmin", "이지어드민"), ("coupang", "쿠팡")]:
            col = SHEET_COLUMNS[key][ch_key]
            series = _get_stock_series(values, col)
            avg, _days = _calc_avg_outflow(series, intake_rows, ch_label, label)
            if avg is not None:
                avg_sum += avg
        avg_val = f"{avg_sum:.2f}" if avg_sum > 0 else "-"
        if avg_sum > 0 and stock_sum > 0:
            days_left = int(stock_sum / avg_sum)
            days_text = str(days_left)
        else:
            days_text = "-"
        total_rows.append(
            {
                "품목명": label,
                "재고수량": f"{stock_sum:,}",
                "일평균 출고량": avg_val,
                "출고 가능 일 수": days_text,
            }
        )
    st.dataframe(total_rows, use_container_width=True, hide_index=True)

    st.divider()
    left, divider, right = st.columns([0.49, 0.02, 0.49])

    with divider:
        st.markdown(
            "<div style='height: 100%; min-height: 600px; border-left: 1px solid #e0e0e0;'></div>",
            unsafe_allow_html=True,
        )

    with left:
        st.subheader("입고 등록")
        st.caption("입고되는 재고를 입력해주세요.")
        with st.form("add_inventory_form"):
            transfer_mode = st.radio(
                "입고 형태",
                ["신규 → 이지어드민", "이지어드민 → 품고"],
                horizontal=True,
            )
            if transfer_mode == "신규 → 이지어드민":
                from_channel, channel = "신규", "이지어드민"
            else:
                from_channel, channel = "이지어드민", "품고"
            date_value = st.date_input("날짜")

            base_rows = [{"품목명": v, "입고수량": 0} for v in SKU_LABELS.values()]
            intake_df = st.data_editor(
                base_rows,
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
            )
            submitted = st.form_submit_button("저장")
            if submitted:
                try:
                    write_ws = _connect_sheet(readonly=False)
                    ss = write_ws.spreadsheet
                    add_ws = ss.worksheet("Add_inventory")
                except Exception:
                    try:
                        add_ws = ss.add_worksheet(title="Add_inventory", rows=1000, cols=10)
                    except Exception as e:
                        st.error(f"시트 연결 실패: {e}")
                        st.stop()
                _ensure_add_inventory_header(add_ws)
                try:
                    queue_ws = ss.worksheet("TransferQueue")
                except Exception:
                    try:
                        queue_ws = ss.add_worksheet(title="TransferQueue", rows=2000, cols=12)
                    except Exception as e:
                        st.error(f"TransferQueue 생성 실패: {e}")
                        st.stop()
                _ensure_transfer_queue_header(queue_ws)
                appended = 0
                queued = 0
                now_ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
                for row in intake_df:
                    sku_name = str(row.get("품목명", "")).strip()
                    qty = row.get("입고수량", 0)
                    try:
                        qty_int = int(qty)
                    except Exception:
                        qty_int = 0
                    if sku_name and qty_int > 0:
                        try:
                            add_ws.append_row(
                                [date_value.strftime("%Y-%m-%d"), from_channel, channel, sku_name, qty_int],
                                value_input_option="USER_ENTERED",
                            )
                        except Exception as e:
                            st.error(f"입고 저장 실패: {e}")
                            continue
                        try:
                            queue_ws.append_row(
                                [
                                    date_value.strftime("%Y-%m-%d"),
                                    from_channel,
                                    channel,
                                    sku_name,
                                    qty_int,
                                    "PENDING",
                                    "",
                                    now_ts,
                                    "",
                                    "",
                                    "",
                                ],
                                value_input_option="USER_ENTERED",
                            )
                        except Exception as e:
                            st.error(f"TransferQueue 저장 실패: {e}")
                            continue
                        appended += 1
                        queued += 1
                if appended:
                    st.success(f"입고 {appended}건이 저장되었습니다. (큐 {queued}건 등록)")
                else:
                    st.info("입고수량이 0인 항목은 저장되지 않습니다.")

    with right:
        st.subheader("최근 입고내역")
        intake_rows = []
        intake_groups = []
        tq_status_map = {}
        tq_status_simple = {}
        filter_date = st.date_input("날짜 필터", value=None)
        try:
            intake_values = _get_sheet_values_cached("Add_inventory")
            if intake_values and len(intake_values) > 1:
                header = intake_values[0]
                header_lower = {h.lower(): h for h in header}
                def _val(r, key):
                    k = header_lower.get(key, key)
                    return r.get(k, "")
                data_rows = []
                for i, r in enumerate(intake_values[1:], start=2):
                    if not r:
                        continue
                    if r == header:
                        continue
                    if len(r) >= 1 and str(r[0]).strip().lower() == "date":
                        continue
                    row_dict = dict(zip(header, r))
                    row_date = str(_val(row_dict, "date"))
                    if filter_date:
                        if row_date != filter_date.strftime("%Y-%m-%d"):
                            continue
                    data_rows.append((i, row_dict))

                # Load transfer queue status for matching rows (best-effort)
                try:
                    tq_values = _get_sheet_values_cached("TransferQueue")
                    tq_header = tq_values[0] if tq_values else []
                    tq_idx = {name: j for j, name in enumerate(tq_header)}
                    for row_i, row in enumerate(tq_values[1:], start=2):
                        if not row:
                            continue
                        date = row[tq_idx.get("date", -1)] if tq_idx.get("date", -1) >= 0 else ""
                        from_ch = row[tq_idx.get("from_channel", -1)] if tq_idx.get("from_channel", -1) >= 0 else ""
                        to_ch = row[tq_idx.get("to_channel", -1)] if tq_idx.get("to_channel", -1) >= 0 else ""
                        sku = row[tq_idx.get("sku_name", -1)] if tq_idx.get("sku_name", -1) >= 0 else ""
                        qty = row[tq_idx.get("quantity", -1)] if tq_idx.get("quantity", -1) >= 0 else ""
                        status = row[tq_idx.get("status", -1)] if tq_idx.get("status", -1) >= 0 else ""
                        updated = row[tq_idx.get("updated_at", -1)] if tq_idx.get("updated_at", -1) >= 0 else ""
                        created = row[tq_idx.get("created_at", -1)] if tq_idx.get("created_at", -1) >= 0 else ""
                        key = (str(date), str(from_ch), str(to_ch), str(created), str(sku), str(qty))
                        prev = tq_status_map.get(key)
                        if not prev or row_i >= int(prev.get("row_i", 0)):
                            tq_status_map[key] = {"status": status, "updated": updated, "row_i": row_i}
                        key_simple = (str(date), str(from_ch), str(to_ch), str(sku), str(qty))
                        prev_simple = tq_status_simple.get(key_simple)
                        if not prev_simple or row_i >= int(prev_simple.get("row_i", 0)):
                            tq_status_simple[key_simple] = {"status": status, "updated": updated, "row_i": row_i}
                except Exception:
                    tq_status_map = {}
                    tq_status_simple = {}

                # Group by (date, from_channel, channel)
                # Load created_at per (date/from/to/sku/qty) to group by created_at when possible
                created_at_map = {}
                try:
                    tq_values = _get_sheet_values_cached("TransferQueue")
                    tq_header = tq_values[0] if tq_values else []
                    tq_idx = {name: j for j, name in enumerate(tq_header)}
                    for row in tq_values[1:]:
                        if not row:
                            continue
                        date = row[tq_idx.get("date", -1)] if tq_idx.get("date", -1) >= 0 else ""
                        from_ch = row[tq_idx.get("from_channel", -1)] if tq_idx.get("from_channel", -1) >= 0 else ""
                        to_ch = row[tq_idx.get("to_channel", -1)] if tq_idx.get("to_channel", -1) >= 0 else ""
                        sku = row[tq_idx.get("sku_name", -1)] if tq_idx.get("sku_name", -1) >= 0 else ""
                        qty = row[tq_idx.get("quantity", -1)] if tq_idx.get("quantity", -1) >= 0 else ""
                        created = row[tq_idx.get("created_at", -1)] if tq_idx.get("created_at", -1) >= 0 else ""
                        key = (str(date), str(from_ch), str(to_ch), str(sku), str(qty))
                        if created:
                            created_at_map[key] = created
                except Exception:
                    created_at_map = {}

                groups = {}
                for row_idx, row_dict in data_rows:
                    date_val = str(_val(row_dict, "date"))
                    from_val = str(_val(row_dict, "from_channel"))
                    to_val = str(_val(row_dict, "channel"))
                    sku_val = str(_val(row_dict, "sku_name"))
                    qty_val = str(_val(row_dict, "quantity"))
                    created_val = created_at_map.get((date_val, from_val, to_val, sku_val, qty_val), "")
                    gkey = (
                        date_val,
                        from_val,
                        to_val,
                        created_val,
                    )
                    groups.setdefault(gkey, []).append((row_idx, row_dict))

                # Order by latest row index desc, show up to 10 groups
                grouped = sorted(
                    groups.items(),
                    key=lambda kv: max(r[0] for r in kv[1]),
                    reverse=True,
                )[:10]
                intake_groups = grouped
            else:
                st.info("입고내역이 없습니다.")
        except Exception:
            st.info("입고내역을 불러올 수 없습니다.")

        if intake_groups:
            st.caption("날짜 / 출고 / 입고 기준으로 그룹화되어 있습니다.")
            for (date_val, from_ch, to_ch, created_val), rows in intake_groups:
                time_only = ""
                if created_val:
                    parsed_kst = _parse_kst(created_val)
                    if parsed_kst:
                        time_only = parsed_kst.strftime("%H:%M:%S")
                    else:
                        parts = created_val.split(" ")
                        time_only = parts[1] if len(parts) > 1 else created_val
                time_label = f" {time_only}" if time_only else ""
                header_label = f"{date_val}{time_label}  {from_ch} → {to_ch}"
                delete_key = f"delete_{date_val}_{from_ch}_{to_ch}_{created_val}"
                with st.expander(header_label, expanded=False):
                    # Build rows with status
                    display_rows = []
                    for _row_idx, row_dict in rows:
                        sku = _val(row_dict, "sku_name")
                        qty = _val(row_dict, "quantity")
                        key = (str(date_val), str(from_ch), str(to_ch), str(created_val), str(sku), str(qty))
                        status = tq_status_map.get(key, {}).get("status", "")
                        if not status:
                            key_simple = (str(date_val), str(from_ch), str(to_ch), str(sku), str(qty))
                            status = tq_status_simple.get(key_simple, {}).get("status", "")
                        status_label = status if status else "-"
                        if status == "CANCEL_PENDING":
                            status_label = "취소중"
                        elif status == "CANCELLED":
                            status_label = "취소완료"
                        elif status == "CANCEL_FAILED":
                            status_label = "취소실패"
                        display_rows.append(
                            {
                                "품목명": sku,
                                "수량": qty,
                                "상태": status_label,
                            }
                        )
                    st.dataframe(display_rows, use_container_width=True, hide_index=True)
                    if st.button("등록 삭제", key=delete_key):
                        deleted = False
                        # Clear cached reads to avoid stale rows after deletion
                        try:
                            _get_sheet_values_cached.clear()
                            _get_sales_values_cached.clear()
                        except Exception:
                            pass
                        try:
                            add_ws = _connect_sheet(readonly=False).spreadsheet.worksheet("Add_inventory")
                            # Queue cancel for matching transfer rows (if any)
                            try:
                                tq_ws = _connect_sheet(readonly=False).spreadsheet.worksheet("TransferQueue")
                                tq_values = _get_sheet_values_cached("TransferQueue")
                                tq_header = tq_values[0] if tq_values else []
                                tq_idx = {name: j for j, name in enumerate(tq_header)}
                                now_ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
                                for i, tq_row in enumerate(tq_values[1:], start=2):
                                    if not tq_row:
                                        continue
                                    t_date = tq_row[tq_idx.get("date", -1)] if tq_idx.get("date", -1) >= 0 else ""
                                    t_from = tq_row[tq_idx.get("from_channel", -1)] if tq_idx.get("from_channel", -1) >= 0 else ""
                                    t_to = tq_row[tq_idx.get("to_channel", -1)] if tq_idx.get("to_channel", -1) >= 0 else ""
                                    if (
                                        str(t_date) == str(date_val)
                                        and str(t_from) == str(from_ch)
                                        and str(t_to) == str(to_ch)
                                    ):
                                        action_col = tq_idx.get("action", -1) + 1
                                        status_col = tq_idx.get("status", -1) + 1
                                        updated_col = tq_idx.get("updated_at", -1) + 1
                                        if action_col > 0:
                                            tq_ws.update_cell(i, action_col, "CANCEL")
                                        if status_col > 0:
                                            tq_ws.update_cell(i, status_col, "CANCEL_PENDING")
                                        if updated_col > 0:
                                            tq_ws.update_cell(i, updated_col, now_ts)
                            except Exception:
                                pass
                            # delete from bottom to top to keep indices stable
                            for row_idx, _ in sorted(rows, key=lambda x: x[0], reverse=True):
                                add_ws.delete_rows(row_idx)
                            st.success("삭제되었습니다.")
                            deleted = True
                        except Exception:
                            st.error("삭제에 실패했습니다.")
                        if deleted:
                            st.rerun()


    st.divider()
    st.subheader("발주 추천")
    try:
        sales_hist = _load_sales_history(days=90)
    except Exception:
        sales_hist = {}

    lead_time_days = 14
    cover_days = 35
    safety_factor = 0.3
    growth_cap = (0.7, 1.3)
    growth_weight = 0.5

    def _avg_daily(sales_by_date: Dict[str, Dict[str, int]], sku_name: str, days_window: int) -> float:
        if not sales_by_date:
            return 0.0
        cutoff = (_now_kst() - timedelta(days=days_window)).strftime("%Y-%m-%d")
        total = 0
        days = 0
        for d, items in sales_by_date.items():
            if d < cutoff:
                continue
            total += _safe_int(items.get(sku_name, 0))
            days += 1
        if days == 0:
            return 0.0
        return total / days

    def _fallback_avg_from_stock(
        values: List[List[str]],
        intake_rows: List[Dict[str, str]],
        sku_key: str,
    ) -> float:
        cols = SHEET_COLUMNS.get(sku_key, {})
        if not cols:
            return 0.0
        date_col = _col_to_index(SHEET_COLUMNS["date"]) - 1
        series: List[Tuple[datetime, int]] = []
        for row in values:
            if date_col >= len(row):
                continue
            dt = _parse_date(row[date_col].strip())
            if not dt:
                continue
            total = 0
            for ch in ("poomgo", "ezadmin", "coupang"):
                col = cols.get(ch, "")
                if col:
                    total += _safe_int(_row_value(row, col))
            series.append((dt, total))
        if len(series) < 2:
            return 0.0
        series.sort(key=lambda x: x[0])
        past_dt, past_stock = series[0]
        today_dt, today_stock = series[-1]
        days = (today_dt - past_dt).days - 1
        if days <= 0:
            return 0.0
        intake_sum = 0
        outbound_sum = 0
        sku_label = SKU_LABELS.get(sku_key, "")
        in_scope = {"품고", "이지어드민", "쿠팡"}
        for r in intake_rows:
            if str(r.get("sku_name", "")) != sku_label:
                continue
            to_ch = str(r.get("channel", ""))
            from_ch = str(r.get("from_channel", ""))
            dt = _parse_date(str(r.get("date", "")))
            if not dt:
                continue
            if past_dt < dt <= today_dt:
                qty = _safe_int(r.get("quantity", "0"))
                if to_ch in in_scope:
                    intake_sum += qty
                if from_ch in in_scope:
                    outbound_sum += qty
        avg = (past_stock - today_stock + intake_sum - outbound_sum) / days
        if avg < 0:
            avg = 0.0
        return avg

    order_rows = []
    for key, label in SKU_LABELS.items():
        # Skip manual for order recommendation
        if label == "사용설명서":
            continue
        # Use stock-flow based average (inventory movement) for recommendations
        fallback_avg = _fallback_avg_from_stock(values, intake_rows, key)
        avg_90 = fallback_avg
        avg_30 = fallback_avg
        r = (avg_30 / avg_90) if avg_90 > 0 else 1.0
        g = 1 + growth_weight * (r - 1)
        g = max(growth_cap[0], min(growth_cap[1], g))
        forecast = avg_90 * g
        cover_demand = forecast * cover_days
        safety_stock = cover_demand * safety_factor

        stock_total = 0
        if key in summary:
            stock_total += _safe_int(summary[key].get("poomgo", "0"))
            stock_total += _safe_int(summary[key].get("ezadmin", "0"))
            stock_total += _safe_int(summary[key].get("coupang", "0"))

        recommend = int(round(cover_demand + safety_stock - stock_total))
        if recommend < 0:
            recommend = 0
        order_rows.append(
            {
                "품목명": label,
                "최근90일 일평균": f"{avg_90:.2f}",
                "최근30일 일평균": f"{avg_30:.2f}",
                "성장계수": f"{g:.2f}",
                "현재재고(합계)": stock_total,
                "추천발주수량": recommend,
            }
        )

    st.caption(
        f"리드타임 {lead_time_days}일, 커버 {cover_days}일, 안전재고 {int(safety_factor*100)}% 반영"
    )
    st.dataframe(order_rows, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("판매수량")
    try:
        st.session_state["sales_snapshot"] = _get_latest_sales_snapshot()
    except Exception:
        st.session_state["sales_snapshot"] = {}

    snap = st.session_state.get("sales_snapshot", {})
    snap_date = snap.get("date", "-") if isinstance(snap, dict) else "-"
    now_kst = _now_kst()
    if snap_date and snap_date != "-":
        # Normalize to YYYY-MM-DD if payload already includes time
        try:
            parsed = datetime.fromisoformat(str(snap_date).strip())
            snap_date = parsed.strftime("%Y-%m-%d")
        except Exception:
            snap_date = str(snap_date).split(" ")[0]
        sales_label = f"{snap_date} {now_kst.strftime('%H:%M')}"
    else:
        sales_label = "-"
    st.subheader(f"최근 데이터: {sales_label}")

    def _sales_status(value) -> str:
        if value is None:
            return "수집실패"
        if isinstance(value, str) and not value.strip():
            return "수집실패"
        if value == "-":
            return "수집실패"
        return "수집완료"

    sales_status_line = (
        f"CAFE24 : {_sales_status(snap.get('cafe24_sales_qty', '-'))}   ㅣ   "
        f"스마트스토어 : {_sales_status(snap.get('smartstore_sales_qty', '-'))}   ㅣ   "
        f"쿠팡 : {_sales_status(snap.get('coupang_sales_qty', '-'))}"
    )
    st.caption(sales_status_line)
    label_map = {
        "P00000CL000E": "플라우드 노트 / 블랙",
        "P00000CL000I": "플라우드 노트 / 실버",
        "P00000DN000M": "플라우드 노트 Pro / 블랙",
        "P00000DN000N": "플라우드 노트 Pro / 실버",
        "P00000CT000U": "플라우드 노트핀S / 블랙",
        "P00000CT000V": "플라우드 노트핀S / 실버",
    }
    coupang_map = {
        "94199205555": "플라우드 노트 Pro / 블랙",
        "94199205552": "플라우드 노트 Pro / 실버",
        "90737907302": "플라우드 노트 / 블랙",
        "90737907295": "플라우드 노트 / 실버",
        "91942294087": "플라우드 노트핀S / 블랙",
        "91942294096": "플라우드 노트핀S / 실버",
    }
    coupang_by_name = {v: k for k, v in coupang_map.items()}
    items = snap.get("cafe24_items", {}) if isinstance(snap, dict) else {}
    coupang_items = snap.get("coupang_items", {}) if isinstance(snap, dict) else {}
    smart_items = snap.get("smartstore_items", {}) if isinstance(snap, dict) else {}
    rows = []
    total_cafe24 = 0
    total_coupang = 0
    total_smart = 0
    for code, name in label_map.items():
        cafe_qty = _safe_int(items.get(code, 0))
        coupang_key = coupang_by_name.get(name, "")
        coupang_qty = _safe_int(coupang_items.get(coupang_key, 0)) if coupang_key else 0
        smart_qty = _safe_int(smart_items.get(name, 0))
        total_cafe24 += cafe_qty
        total_coupang += coupang_qty
        total_smart += smart_qty
        rows.append(
            {
                "품목명": name,
                "CAFE24": cafe_qty,
                "스마트스토어": smart_qty,
                "쿠팡": coupang_qty,
            }
        )
    rows.append(
        {
            "품목명": "합계",
            "CAFE24": total_cafe24,
            "스마트스토어": total_smart,
            "쿠팡": total_coupang,
        }
    )
    st.dataframe(rows, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
