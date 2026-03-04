from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import json
import subprocess
import sys
from pathlib import Path


SHEET_COLUMNS = {
    "date": "A",
    "notepro_black": {"poomgo": "B", "ezadmin": "C", "coupang": "D"},
    "notepro_silver": {"poomgo": "F", "ezadmin": "G", "coupang": "H"},
    "note_black": {"poomgo": "J", "ezadmin": "K", "coupang": "L"},
    "note_silver": {"poomgo": "N", "ezadmin": "O", "coupang": "P"},
    "notepin_black": {"poomgo": "R", "ezadmin": "S", "coupang": "T"},
    "notepin_silver": {"poomgo": "V", "ezadmin": "W", "coupang": "X"},
}

SKU_LABELS = {
    "notepro_black": "노트프로 블랙",
    "notepro_silver": "노트프로 실버",
    "note_black": "노트 블랙",
    "note_silver": "노트 실버",
    "notepin_black": "노트핀S 블랙",
    "notepin_silver": "노트핀S 실버",
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
    ss = client.open_by_key(sheet_id)
    return ss.worksheet(worksheet)


def _ensure_add_inventory_header(ws) -> None:
    header = ["date", "from_channel", "channel", "sku_name", "quantity"]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0] != header:
        ws.insert_row(header, index=1)


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
    for r in intake_rows:
        if str(r.get("channel", "")) != channel_label:
            continue
        if str(r.get("sku_name", "")) != sku_label:
            continue
        dt = _parse_date(str(r.get("date", "")))
        if not dt:
            continue
        if past_dt < dt <= today_dt:
            intake_sum += _safe_int(r.get("quantity", "0"))

    avg = (past_stock - today_stock - intake_sum) / days
    if avg < 0:
        avg = 0.0
    return avg, days


def main() -> None:
    st.set_page_config(page_title="재고 대시보드", layout="wide")
    header_left, header_right = st.columns([3, 1])
    with header_left:
        st.title("재고 대시보드")
    with header_right:
        st.write("")
        st.write("")
        # 재고 최신화 버튼 제거 (로컬 스케줄러로만 동작)

    ws = _connect_sheet(readonly=True)
    values = ws.get_all_values()
    if not values:
        st.warning("시트 데이터가 비어 있습니다.")
        return

    latest_idx = _get_latest_date_row(values)
    if latest_idx == -1:
        st.warning("유효한 날짜 행이 없습니다.")
        return

    latest_row = values[latest_idx]
    latest_date = _row_value(latest_row, SHEET_COLUMNS["date"])

    try:
        from zoneinfo import ZoneInfo
        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now_kst = datetime.now()
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
            col1, col2 = st.columns(2)
            from_channel = col1.selectbox("출고", ["신규", "품고", "이지어드민", "쿠팡"])
            channel = col2.selectbox("입고", ["품고", "이지어드민", "쿠팡"])
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
                    add_ws = ss.add_worksheet(title="Add_inventory", rows=1000, cols=10)
                _ensure_add_inventory_header(add_ws)
                appended = 0
                for row in intake_df:
                    sku_name = str(row.get("품목명", "")).strip()
                    qty = row.get("입고수량", 0)
                    try:
                        qty_int = int(qty)
                    except Exception:
                        qty_int = 0
                    if sku_name and qty_int > 0:
                        add_ws.append_row(
                            [date_value.strftime("%Y-%m-%d"), from_channel, channel, sku_name, qty_int],
                            value_input_option="USER_ENTERED",
                        )
                        appended += 1
                if appended:
                    st.success(f"입고 {appended}건이 저장되었습니다.")
                else:
                    st.info("입고수량이 0인 항목은 저장되지 않습니다.")

    with right:
        st.subheader("최근 입고내역")
        intake_rows = []
        try:
            add_ws = _connect_sheet(readonly=True).spreadsheet.worksheet("Add_inventory")
            intake_values = add_ws.get_all_values()
            if intake_values and len(intake_values) > 1:
                header = intake_values[0]
                rows = intake_values[1:]
                rows = list(reversed(rows))[:10]
                filtered = []
                for r in rows:
                    if not r:
                        continue
                    if r == header:
                        continue
                    if len(r) >= 1 and str(r[0]).strip().lower() == "date":
                        continue
                    filtered.append(r)
                intake_rows = [dict(zip(header, r)) for r in filtered]
                st.dataframe(intake_rows, use_container_width=True, hide_index=True)
            else:
                st.info("입고내역이 없습니다.")
        except Exception:
            st.info("입고내역을 불러올 수 없습니다.")

        if intake_rows:
            st.caption("삭제할 항목을 선택하세요.")
            del_col1, del_col2 = st.columns(2)
            key_options = [
                f"{r.get('date','')} | {r.get('from_channel','')} | {r.get('channel','')} | {r.get('sku_name','')} | {r.get('quantity','')}"
                for r in intake_rows
            ]
            target = del_col1.selectbox("삭제 대상", key_options)
            if del_col2.button("삭제"):
                try:
                    add_ws = _connect_sheet(readonly=False).spreadsheet.worksheet("Add_inventory")
                    all_values = add_ws.get_all_values()
                    # Find matching row
                    for idx, row in enumerate(all_values[1:], start=2):
                        key = (
                            f"{row[0] if len(row)>0 else ''} | "
                            f"{row[1] if len(row)>1 else ''} | "
                            f"{row[2] if len(row)>2 else ''} | "
                            f"{row[3] if len(row)>3 else ''} | "
                            f"{row[4] if len(row)>4 else ''}"
                        )
                        if key == target:
                            add_ws.delete_rows(idx)
                            st.success("삭제되었습니다.")
                            st.experimental_rerun()
                            break
                except Exception:
                    st.error("삭제에 실패했습니다.")

    st.divider()
    st.subheader("판매수량")
    if "sales_snapshot" not in st.session_state:
        st.session_state["sales_snapshot"] = {}

    _, col_s2 = st.columns([1, 1])
    with col_s2:
        if st.button("판매수량 최신화하기"):
            with st.spinner("판매수량 최신화 중..."):
                try:
                    script_path = Path(__file__).resolve().parent / "scripts" / "sales_snapshot.py"
                    venv_py = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
                    py = str(venv_py) if venv_py.exists() else sys.executable
                    result = subprocess.run(
                        [py, str(script_path)],
                        check=True,
                        timeout=300,
                        capture_output=True,
                        text=True,
                    )
                    try:
                        out = result.stdout.strip()
                        warn_lines = [ln for ln in out.splitlines() if ln.strip().startswith("[WARN]")]
                        for wl in warn_lines:
                            st.warning(wl)
                        # Use last JSON line in stdout (ignore WARN lines)
                        json_line = ""
                        for line in out.splitlines()[::-1]:
                            if line.strip().startswith("{") and line.strip().endswith("}"):
                                json_line = line.strip()
                                break
                        if not json_line:
                            json_line = out
                        payload = json.loads(json_line)
                        try:
                            from zoneinfo import ZoneInfo
                            now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
                        except Exception:
                            now_kst = datetime.now()
                        st.session_state["sales_snapshot"] = {
                            "fetched_at": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
                            "date": payload.get("date", "-"),
                            "cafe24_sales_qty": payload.get("cafe24_sales_qty", "-"),
                            "coupang_sales_qty": payload.get("coupang_sales_qty", "-"),
                            "smartstore_sales_qty": payload.get("smartstore_sales_qty", "-"),
                            "cafe24_items": payload.get("cafe24_items", {}),
                            "coupang_items": payload.get("coupang_items", {}),
                            "smartstore_items": payload.get("smartstore_items", {}),
                        }
                        st.success("판매수량 최신화 완료")
                    except Exception as e:
                        st.error(f"판매수량 파싱 실패: {e}")
                        st.code(result.stdout.strip() or "(stdout empty)")
                except Exception as e:
                    st.error(f"판매수량 최신화 실패: {e}")
                    if isinstance(e, subprocess.CalledProcessError):
                        if e.stdout:
                            st.code(e.stdout.strip())
                        if e.stderr:
                            st.code(e.stderr.strip())

    snap = st.session_state.get("sales_snapshot", {})
    snap_date = snap.get("date", "-") if isinstance(snap, dict) else "-"
    try:
        from zoneinfo import ZoneInfo
        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now_kst = datetime.now()
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
