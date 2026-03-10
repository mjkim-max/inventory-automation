from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import date as date_cls, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import time

try:
    import tomllib  # py311+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]
try:
    import tomli  # py310 fallback
except Exception:  # pragma: no cover
    tomli = None  # type: ignore[assignment]

import requests

try:
    import gspread
except Exception:  # pragma: no cover
    gspread = None  # type: ignore[assignment]
try:
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore[assignment]

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]
try:
    import bcrypt
except Exception:  # pragma: no cover
    bcrypt = None  # type: ignore[assignment]
try:
    import pybase64
except Exception:  # pragma: no cover
    pybase64 = None  # type: ignore[assignment]
try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover
    sync_playwright = None  # type: ignore[assignment]

try:
    from coupang_auth import ensure_logged_in as coupang_ensure_logged_in
except Exception:  # pragma: no cover
    coupang_ensure_logged_in = None  # type: ignore[assignment]


COUPANG_CANONICAL_ITEM_MAP: Dict[str, str] = {
    "94199205555": "플라우드 노트 Pro / 블랙",
    "94199205552": "플라우드 노트 Pro / 실버",
    "90737907302": "플라우드 노트 / 블랙",
    "90737907295": "플라우드 노트 / 실버",
    "91942294087": "플라우드 노트핀S / 블랙",
    "91942294096": "플라우드 노트핀S / 실버",
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


def _get_cfg_value(cfg: Dict[str, Any], *keys: str, env: Optional[str] = None, default: str = "") -> str:
    cur: Any = cfg
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            cur = None
            break
        cur = cur[key]
    if isinstance(cur, str) and cur.strip():
        return cur.strip()
    if env:
        val = os.getenv(env, "").strip()
        if val:
            return val
    return default


def _connect_sheet(cfg: Dict[str, Any], *, readonly: bool = False):
    if gspread is None or Credentials is None:
        raise RuntimeError("gspread/google-auth not installed.")
    gs_cfg = cfg.get("google_sheets", {})
    sa = cfg.get("google_sheets_service_account", {})
    if not gs_cfg or not sa:
        raise RuntimeError("google_sheets or google_sheets_service_account missing in secrets.")
    sheet_id = gs_cfg.get("sheet_id") or gs_cfg.get("spreadsheet_id")
    worksheet = gs_cfg.get("sales_worksheet", "sales_snapshot")
    if not sheet_id:
        raise RuntimeError("google_sheets.sheet_id is required.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    if not readonly:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(sa), scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sheet_id)
    try:
        return ss.worksheet(worksheet)
    except Exception:
        if readonly:
            raise
        ws = ss.add_worksheet(title=worksheet, rows=2000, cols=10)
        _ensure_sales_header(ws)
        return ws


def _ensure_sales_header(ws) -> None:
    header = ["date", "fetched_at", "payload_json"]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0] != header:
        ws.insert_row(header, index=1)


def _upsert_sales_row(ws, *, date_str: str, fetched_at: str, payload_json: str) -> None:
    values = ws.get_all_values()
    if not values:
        _ensure_sales_header(ws)
        values = ws.get_all_values()
    # Find existing row for date (skip header)
    for i, row in enumerate(values[1:], start=2):
        if not row:
            continue
        if row[0].strip() == date_str:
            ws.update(f"A{i}:C{i}", [[date_str, fetched_at, payload_json]])
            return
    ws.append_row([date_str, fetched_at, payload_json], value_input_option="USER_ENTERED")


def _resolve_target_date_kst() -> str:
    explicit = os.getenv("SALES_TARGET_DATE", "").strip()
    if explicit:
        try:
            return datetime.strptime(explicit, "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        offset_days = int(os.getenv("SALES_TARGET_OFFSET_DAYS", "0").strip() or "0")
    except Exception:
        offset_days = 0
    if ZoneInfo:
        now = datetime.now(ZoneInfo("Asia/Seoul"))
    else:
        now = datetime.now()
    target = (now - timedelta(days=offset_days)).date()
    return target.strftime("%Y-%m-%d")


def _day_range_kst(target_date: str) -> tuple[str, str]:
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
    except Exception:
        if ZoneInfo:
            dt = datetime.now(ZoneInfo("Asia/Seoul"))
        else:
            dt = datetime.now()
    if ZoneInfo:
        dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = dt.replace(hour=23, minute=59, second=59, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def _fetch_make_token(make_webhook: str) -> str:
    resp = requests.post(make_webhook, timeout=30)
    resp.raise_for_status()
    text = resp.text.strip()
    try:
        data = resp.json()
        token = data.get("access_token") or data.get("token") or ""
        if token:
            return token
    except Exception:
        data = None
    # Fallback: extract token from plain text response
    if text:
        if "access_token" in text or "token" in text:
            import re
            m = re.search(r"access_token[\"':=\\s]+([A-Za-z0-9\\-_.]+)", text)
            if m:
                return m.group(1)
        # If response is just the token string
        if len(text) > 20 and "{" not in text:
            return text.strip()
    raise RuntimeError(f"Make webhook response missing access_token. Raw: {text[:200]}")


def _cafe24_orders(
    base_url: str,
    token: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/orders"
    headers = {"Authorization": f"Bearer {token}"}
    orders: List[Dict[str, Any]] = []
    offset = 0
    limit = 100
    paid_status = "N10,N20,N21,N22,N30,N40,N50"
    while True:
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "date_type": "pay_date",
            "embed": "items",
            "shop_no": 1,
            "order_status": paid_status,
            "limit": limit,
            "offset": offset,
        }
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("orders") or []
        orders.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return orders


def _cafe24_sales_qty(orders: List[Dict[str, Any]]) -> int:
    qty = 0
    for order in orders:
        status = str(order.get("order_status", "")).upper()
        # Exclude cancellation/return/exchange/refund status types (C/R/E/U)
        if status.startswith(("C", "R", "E", "U")):
            continue
        items = order.get("items") or []
        for item in items:
            try:
                q = int(item.get("quantity", 0))
            except Exception:
                q = 0
            qty += q
    return qty


def _cafe24_sales_by_variant(orders: List[Dict[str, Any]]) -> Dict[str, int]:
    mapping = {
        "P00000CL000E": "플라우드 노트 / 블랙",
        "P00000CL000I": "플라우드 노트 / 실버",
        "P00000DN000M": "플라우드 노트 Pro / 블랙",
        "P00000DN000N": "플라우드 노트 Pro / 실버",
        "P00000CT000U": "플라우드 노트핀S / 블랙",
        "P00000CT000V": "플라우드 노트핀S / 실버",
    }
    result = {k: 0 for k in mapping.keys()}
    for order in orders:
        status = str(order.get("order_status", "")).upper()
        if status.startswith(("C", "R", "E", "U")):
            continue
        items = order.get("items") or []
        for item in items:
            code = str(item.get("variant_code") or item.get("option_code") or "").strip()
            if code in result:
                try:
                    q = int(item.get("quantity", 0))
                except Exception:
                    q = 0
                result[code] += q
    return result


def _coupang_auth(access_key: str, secret_key: str, method: str, path: str, query: str) -> str:
    import time, hmac, hashlib
    signed_date = time.strftime("%y%m%dT%H%M%SZ", time.gmtime())
    message = f"{signed_date}{method}{path}{query}"
    signature = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"CEA algorithm=HmacSHA256, access-key={access_key}, signed-date={signed_date}, signature={signature}"


def _coupang_label_from_item_name(name: str) -> str:
    text = (name or "").strip().lower()
    if not text:
        return ""
    is_black = ("black" in text) or ("블랙" in text)
    is_silver = ("silver" in text) or ("실버" in text)
    if ("노트핀" in text) or ("notepin" in text):
        if is_black:
            return "플라우드 노트핀S / 블랙"
        if is_silver:
            return "플라우드 노트핀S / 실버"
    if ("pro" in text) or ("프로" in text):
        if is_black:
            return "플라우드 노트 Pro / 블랙"
        if is_silver:
            return "플라우드 노트 Pro / 실버"
    if ("노트" in text) or ("note" in text):
        if is_black:
            return "플라우드 노트 / 블랙"
        if is_silver:
            return "플라우드 노트 / 실버"
    return ""


def _coupang_growth_ui_sales_qty(
    *,
    dashboard_url: str,
    profile_dir: str,
    option_ids: List[str],
    headless: bool,
    target_date: str,
) -> tuple[int, Dict[str, int]]:
    if sync_playwright is None:
        raise RuntimeError("playwright is not installed.")
    if not dashboard_url:
        raise RuntimeError("COUPANG_GROWTH_URL 설정이 필요합니다.")

    alias_to_canonical = {
        **{k: k for k in COUPANG_CANONICAL_ITEM_MAP.keys()},
        # Current option id alias observed from API
        "94199205553": "94199205555",
    }
    item_qty = {k: 0 for k in COUPANG_CANONICAL_ITEM_MAP.keys()}
    canonical_key_by_label = {label: key for key, label in COUPANG_CANONICAL_ITEM_MAP.items()}
    option_filter = set(option_ids)

    parsed = urllib.parse.urlparse(dashboard_url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query["start_date"] = [target_date]
    query["end_date"] = [target_date]
    live_url = urllib.parse.urlunparse(
        parsed._replace(query=urllib.parse.urlencode(query, doseq=True))
    )

    launch_kwargs: Dict[str, Any] = {
        "user_data_dir": profile_dir,
        "headless": headless,
    }
    profile_name = os.getenv("COUPANG_GROWTH_PROFILE_NAME", "Profile 1").strip()
    launch_args: List[str] = []
    if profile_name:
        launch_args.append(f"--profile-directory={profile_name}")
    if os.getenv("COUPANG_GROWTH_USE_CHROME", "1").strip().lower() in {"1", "true", "yes", "y"}:
        launch_kwargs["channel"] = "chrome"
        launch_args.extend([
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ])
    if launch_args:
        launch_kwargs["args"] = launch_args

    dom_option_qty: Dict[str, int] = {}
    api_option_qty: Dict[str, int] = {k: 0 for k in COUPANG_CANONICAL_ITEM_MAP.keys()}
    api_total_units: Optional[int] = None

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(**launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()
        api_vendor_summary: Dict[str, Any] = {}
        api_vi_detail: Dict[str, Any] = {}

        def _on_response(resp) -> None:
            nonlocal api_vendor_summary, api_vi_detail
            url = resp.url or ""
            if "/tenants/rfm-ss/api/business-insight/" not in url:
                return
            if resp.status != 200:
                return
            try:
                txt = resp.text()
            except Exception:
                return
            if not txt or txt.lstrip().startswith("<!DOCTYPE"):
                return
            try:
                data = json.loads(txt)
            except Exception:
                return
            if "/business-insight/vendor-summary" in url and isinstance(data, dict):
                api_vendor_summary = data
            elif "/business-insight/vi-detail-search" in url and isinstance(data, dict):
                api_vi_detail = data

        page.on("response", _on_response)
        page.goto(live_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        final_url = page.url
        if coupang_ensure_logged_in is None:
            context.close()
            raise RuntimeError("coupang_auth 모듈 로드 실패")
        coupang_ensure_logged_in(page, target_url=live_url, timeout_sec=90)
        # Login redirect 후 판매분석 API 응답을 안정적으로 재수집
        page.reload(wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(1000)
        final_url = page.url

        # Preferred path: parse backend JSON response captured from page requests.
        try:
            summary_metrics = (
                api_vendor_summary.get("summaryMetrics", {})
                if isinstance(api_vendor_summary, dict)
                else {}
            )
            total_units_raw = summary_metrics.get("totalUnitsSold")
            if total_units_raw is not None:
                api_total_units = int(round(float(total_units_raw)))
        except Exception:
            api_total_units = None

        vi_items = []
        if isinstance(api_vi_detail, dict):
            vi_items = api_vi_detail.get("vendorItems") or []
        if isinstance(vi_items, list):
            for vi in vi_items:
                if not isinstance(vi, dict):
                    continue
                details = vi.get("vendorItemDetails") or {}
                metrics = vi.get("businessInsightsMetricsResponse") or {}
                raw_id = str(details.get("vendorItemId") or vi.get("vendorItemId") or "").strip()
                item_name = str(
                    details.get("itemName")
                    or details.get("productName")
                    or vi.get("itemName")
                    or ""
                )
                qty_raw = metrics.get("totalUnitsSold")
                try:
                    qty = int(round(float(qty_raw)))
                except Exception:
                    qty = 0
                if qty <= 0:
                    continue
                key = alias_to_canonical.get(raw_id, "")
                if not key:
                    label = _coupang_label_from_item_name(item_name)
                    key = canonical_key_by_label.get(label, "")
                if not key:
                    continue
                if option_filter and key not in option_filter:
                    continue
                api_option_qty[key] = api_option_qty.get(key, 0) + qty

        # Prefer DOM extraction over regex to avoid capturing percentages (e.g., 142.86%).
        dom_option_ids = sorted(option_filter or alias_to_canonical.keys())
        try:
            dom_map = page.evaluate(
                """
                ({ optionIds }) => {
                  const wanted = new Set(optionIds || []);
                  const seen = new Set();
                  const orderedIds = [];
                  for (const a of Array.from(document.querySelectorAll('a[href*="vendorItemId="]'))) {
                    const href = a.getAttribute('href') || '';
                    const m = href.match(/vendorItemId=(\\d+)/);
                    const text = (a.textContent || '').replace(/\\s+/g, ' ').trim();
                    if (!m || !text.includes('옵션 ID')) continue;
                    const id = m[1];
                    if (!wanted.has(id) || seen.has(id)) continue;
                    seen.add(id);
                    orderedIds.push(id);
                  }

                  const parseIntText = (text) => {
                    const raw = (text || '').replace(/\\s+/g, ' ').trim();
                    if (!raw || raw.includes('%')) return null;
                    const m = raw.match(/([0-9][0-9,]*)/);
                    if (!m) return null;
                    const n = parseInt(m[1].replace(/,/g, ''), 10);
                    return Number.isFinite(n) ? n : null;
                  };

                  const values = [];
                  for (const el of Array.from(document.querySelectorAll('div,span,p'))) {
                    const label = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                    if (label !== '판매량') continue;
                    const wrap = el.closest('div');
                    const prev = wrap && wrap.previousElementSibling;
                    const n = parseIntText(prev ? prev.textContent : '');
                    if (n === null) continue;
                    values.push(n);
                  }

                  // Values appear duplicated (desktop/mobile) and may include top-summary first.
                  const dedup = [];
                  for (const n of values) {
                    if (dedup.length === 0 || dedup[dedup.length - 1] !== n) dedup.push(n);
                  }
                  const picks = dedup.slice(-orderedIds.length);
                  const result = {};
                  for (let i = 0; i < picks.length; i += 1) {
                    result[orderedIds[i]] = picks[i];
                  }
                  return result;
                }
                """,
                {"optionIds": dom_option_ids},
            )
        except Exception:
            dom_map = {}
        if isinstance(dom_map, dict):
            for raw_id, qty in dom_map.items():
                try:
                    n = int(qty)
                except Exception:
                    continue
                dom_option_qty[str(raw_id)] = max(0, n)

        body = page.inner_text("body")
        context.close()

    if "xauth.coupang.com" in final_url or "wing.coupang.com/login" in final_url:
        raise RuntimeError("쿠팡 Wing 로그인 세션이 없습니다. .venv/bin/python scripts/coupang_growth_login.py 로 1회 로그인 후 다시 실행하세요.")
    if "Access Denied" in body and "xauth.coupang.com" in body:
        raise RuntimeError("쿠팡 Wing 접근이 차단되었습니다(Access Denied). Growth 모드는 COUPANG_GROWTH_HEADLESS=0 + 로그인 세션이 필요합니다.")
    if "판매자 로그인" in body or "판매자가 아니신가요?" in body or ("로그인" in body and "쿠팡" in body):
        raise RuntimeError("쿠팡 Wing 로그인 세션이 없습니다. .venv/bin/python scripts/coupang_growth_login.py 로 1회 로그인 후 다시 실행하세요.")

    total = 0
    api_sum = sum(v for v in api_option_qty.values() if v > 0)
    if api_sum > 0:
        for key, qty in api_option_qty.items():
            item_qty[key] += qty
            total += qty
        return total, item_qty
    if api_total_units is not None and api_total_units > 0:
        # Fallback if backend detail list is temporarily empty
        return api_total_units, item_qty

    if dom_option_qty:
        for raw_id, qty in dom_option_qty.items():
            if option_filter and raw_id not in option_filter:
                continue
            key = alias_to_canonical.get(raw_id, "")
            if not key:
                continue
            item_qty[key] += qty
            total += qty
        if total > 0:
            return total, item_qty

    # Fallback: body regex parsing (skip values that look like percentages).
    matches: List[tuple[str, str]] = []
    for m in re.finditer(r"옵션 ID[:：]\s*(\d+)[\s\S]{0,800}?판매량\s*([0-9][0-9,]*)", body):
        next_char = body[m.end(2): m.end(2) + 1]
        if next_char in {".", "%"}:
            continue
        matches.append((m.group(1), m.group(2)))

    if not matches:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "coupang_growth_body.txt").write_text(body, encoding="utf-8")
        raise RuntimeError("그로스 화면에서 옵션 판매량을 찾지 못했습니다. debug/coupang_growth_body.txt 확인 필요.")
    for raw_id, raw_qty in matches:
        if option_filter and raw_id not in option_filter:
            continue
        key = alias_to_canonical.get(raw_id, "")
        if not key:
            continue
        try:
            qty = int(raw_qty.replace(",", ""))
        except Exception:
            qty = 0
        item_qty[key] += qty
        total += qty
    return total, item_qty


def _coupang_sales_qty(
    vendor_id: str,
    access_key: str,
    secret_key: str,
    *,
    target_date: str,
) -> tuple[int, Dict[str, int]]:
    # Rocket Growth sales: use RG Order API (paid date, yyyymmdd).
    host = "https://api-gateway.coupang.com"
    path = f"/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/orders"
    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    except Exception:
        if ZoneInfo:
            target_dt = datetime.now(ZoneInfo("Asia/Seoul"))
        else:
            target_dt = datetime.now()
    today_yyyymmdd = target_dt.strftime("%Y%m%d")
    today_iso = target_dt.strftime("%Y-%m-%d")
    lookback_days = 1
    try:
        lookback_days = max(0, int(os.getenv("COUPANG_PAID_LOOKBACK_DAYS", "1")))
    except Exception:
        lookback_days = 1
    paid_date_from = (target_dt - timedelta(days=lookback_days)).strftime("%Y%m%d")
    paid_date_to = today_yyyymmdd
    total_qty = 0
    canonical_item_map = COUPANG_CANONICAL_ITEM_MAP
    alias_to_canonical = {
        # Legacy vendorItemId
        "94199205555": "94199205555",
        "94199205552": "94199205552",
        "90737907302": "90737907302",
        "90737907295": "90737907295",
        "91942294087": "91942294087",
        "91942294096": "91942294096",
        # Current vendorItemId observed from live responses
        "94199205553": "94199205555",
    }
    canonical_key_by_label = {label: key for key, label in canonical_item_map.items()}
    item_qty = {k: 0 for k in canonical_item_map.keys()}
    orders: List[Dict[str, Any]] = []
    responses = []
    next_token = ""
    while True:
        query = f"paidDateFrom={paid_date_from}&paidDateTo={paid_date_to}"
        if next_token:
            query = f"{query}&nextToken={next_token}"
        auth = _coupang_auth(access_key, secret_key, "GET", path, query)
        headers = {
            "Authorization": auth,
            "X-Requested-By": vendor_id,
            "X-MARKET": "KR",
            "Content-Type": "application/json;charset=UTF-8",
        }
        url = f"{host}{path}?{query}"
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code >= 400:
            body = (resp.text or "").strip().replace("\n", " ")
            raise RuntimeError(f"Coupang RG Order API {resp.status_code}: {body[:300]}")
        data = resp.json()
        responses.append(data)
        data_orders = data.get("data") or []
        if isinstance(data_orders, list):
            orders.extend(data_orders)
        next_token = str(data.get("nextToken", "") or "").strip()
        if not next_token:
            break

    unknown_vendor_items = set()
    seen = set()

    def _paid_day_iso(order: Dict[str, Any]) -> str:
        raw = order.get("paidAt")
        if raw is None:
            return ""
        try:
            if isinstance(raw, (int, float)):
                sec = float(raw)
                if sec > 1e12:
                    sec /= 1000.0
                dt = datetime.fromtimestamp(sec, tz=ZoneInfo("Asia/Seoul") if ZoneInfo else None)
                return dt.strftime("%Y-%m-%d")
            txt = str(raw).strip()
            if not txt:
                return ""
            if txt.isdigit():
                sec = float(txt)
                if sec > 1e12:
                    sec /= 1000.0
                dt = datetime.fromtimestamp(sec, tz=ZoneInfo("Asia/Seoul") if ZoneInfo else None)
                return dt.strftime("%Y-%m-%d")
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if dt.tzinfo is not None and ZoneInfo:
                dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""

    for order in orders:
        if _paid_day_iso(order) != today_iso:
            continue
        order_id = str(order.get("orderId", "")).strip()
        items = order.get("orderItems") or order.get("items") or []
        for idx, item in enumerate(items):
            try:
                q = int(item.get("salesQuantity", item.get("shippingCount", item.get("quantity", 0))))
            except Exception:
                q = 0
            vendor_item_id = (
                item.get("vendorItemId")
                or item.get("sellerProductItemId")
                or item.get("vendor_item_id")
            )
            key = str(vendor_item_id).strip() if vendor_item_id is not None else ""
            dedupe_key = (order_id, idx, key, q)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            total_qty += q
            if key in alias_to_canonical:
                item_qty[alias_to_canonical[key]] += q
                continue
            item_name = str(
                item.get("vendorItemName")
                or item.get("vendorItemPackageName")
                or item.get("sellerProductItemName")
                or item.get("sellerProductName")
                or ""
            )
            label = _coupang_label_from_item_name(item_name)
            canonical_key = canonical_key_by_label.get(label, "")
            if canonical_key:
                item_qty[canonical_key] += q
            elif key:
                unknown_vendor_items.add(key)
    if total_qty == 0:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "coupang_ordersheets.json").write_text(
            json.dumps(
                {
                    "paidDateFrom": paid_date_from,
                    "paidDateTo": paid_date_to,
                    "today": today_iso,
                    "responses": responses,
                    "unknown_vendor_items": sorted(unknown_vendor_items),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return total_qty, item_qty


def _smartstore_token(
    client_id: str,
    client_secret: str,
    *,
    token_type: str = "SELF",
    account_id: str = "",
) -> str:
    if bcrypt is None or pybase64 is None:
        raise RuntimeError("bcrypt/pybase64 is required for Smartstore auth.")
    # Naver Commerce API client credentials flow
    timestamp = str(int(datetime.now().timestamp() * 1000))
    message = f"{client_id}_{timestamp}"
    hashed = bcrypt.hashpw(message.encode("utf-8"), client_secret.encode("utf-8"))
    secret_sign = pybase64.standard_b64encode(hashed).decode("utf-8")
    url = "https://api.commerce.naver.com/external/v1/oauth2/token"
    params = {
        "client_id": client_id,
        "timestamp": timestamp,
        "client_secret_sign": secret_sign,
        "grant_type": "client_credentials",
        "type": token_type,
    }
    if token_type == "SELLER":
        if not account_id:
            raise RuntimeError("Smartstore account_id is required for SELLER token.")
        params["account_id"] = account_id
    resp = requests.post(url, data=params, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Smartstore token error {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    token = data.get("access_token") or data.get("accessToken")
    if not token:
        raise RuntimeError(f"Smartstore token missing in response: {data}")
    return token


def _smartstore_day_range_kst(target_date: str) -> tuple[str, str]:
    try:
        now = datetime.strptime(target_date, "%Y-%m-%d")
    except Exception:
        if ZoneInfo:
            now = datetime.now(ZoneInfo("Asia/Seoul"))
        else:
            now = datetime.now()
    if ZoneInfo:
        now = now.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    # API expects yyyy-MM-dd'T'HH:mm:ss.SSSXXX (milliseconds with offset)
    try:
        return start.isoformat(timespec="milliseconds"), end.isoformat(timespec="milliseconds")
    except Exception:
        def _fmt(dt: datetime) -> str:
            base = dt.strftime("%Y-%m-%dT%H:%M:%S")
            ms = f"{int(dt.microsecond / 1000):03d}"
            tz = dt.strftime("%z")
            tz = tz[:3] + ":" + tz[3:] if tz else "+09:00"
            return f"{base}.{ms}{tz}"
        return _fmt(start), _fmt(end)


def _smartstore_fetch_product_orders(token: str, *, target_date: str) -> List[Dict[str, Any]]:
    # Fetch by pay date range (today only) to avoid pulling historical changes
    base = "https://api.commerce.naver.com/external"
    url = f"{base}/v1/pay-order/seller/product-orders"
    pay_from, pay_to = _smartstore_day_range_kst(target_date)
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": pay_from,
        "to": pay_to,
        "rangeType": "PAYED_DATETIME",
    }

    # Retry on rate limit with capped backoff
    resp = None
    for attempt in range(1, 4):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else (5 * attempt)
            wait = min(wait, 30)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    if resp is None or resp.status_code == 429:
        raise RuntimeError("Smartstore rate limited (429). Try again later.")

    data = resp.json()
    # Save debug payload (even if empty)
    try:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "smartstore_orders.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass

    orders = data.get("data") or data.get("productOrders") or []
    if isinstance(orders, dict):
        # Newer API shape: {data: {contents: [{content: {...}}], pagination: {...}}}
        if isinstance(orders.get("contents"), list):
            normalized: List[Dict[str, Any]] = []
            for item in orders["contents"]:
                if isinstance(item, dict) and isinstance(item.get("content"), dict):
                    normalized.append(item["content"])
                elif isinstance(item, dict):
                    normalized.append(item)
            return normalized
        orders = orders.get("productOrders") or orders.get("items") or []
    if isinstance(orders, list):
        # Some responses wrap each row as {"content": {...}}
        if orders and isinstance(orders[0], dict) and "content" in orders[0]:
            normalized = []
            for item in orders:
                if isinstance(item, dict) and isinstance(item.get("content"), dict):
                    normalized.append(item["content"])
                elif isinstance(item, dict):
                    normalized.append(item)
            return normalized
    if isinstance(orders, str):
        return []
    return orders or []


def _smartstore_sales_by_variant(
    product_orders: List[Dict[str, Any]],
    *,
    target_date: str,
) -> tuple[int, Dict[str, int]]:
    # Map Smartstore product/item ids to labels
    smart_map = {
        # optionCode based mapping (preferred)
        "56234258616": "플라우드 노트핀S / 블랙",
        "56234258618": "플라우드 노트핀S / 실버",
        "55736008596": "플라우드 노트 Pro / 블랙",
        "53769211633": "플라우드 노트 Pro / 실버",
        "48485810018": "플라우드 노트 / 블랙",
        "48485810022": "플라우드 노트 / 실버",
        # productId fallback (if optionCode missing)
        "11380104480": "플라우드 노트핀S",
        "12696749368": "플라우드 노트 Pro",
        "10195303069": "플라우드 노트",
    }
    result: Dict[str, int] = {v: 0 for v in smart_map.values()}
    total_qty = 0
    include_status = {"PAYED", "DELIVERING", "DELIVERED", "PURCHASE_DECIDED"}
    today_str = target_date

    for order in product_orders:
        if not isinstance(order, dict):
            continue
        # Handle wrapper shape: {"order": {...}, "productOrder": {...}, ...}
        po = order.get("productOrder") if isinstance(order.get("productOrder"), dict) else order
        od = order.get("order") if isinstance(order.get("order"), dict) else {}

        status = str(po.get("productOrderStatus", "")).upper()
        if status and status not in include_status:
            continue
        claim = str(po.get("claimStatus", "")).upper()
        if claim and claim not in {"NONE", "NA"}:
            continue
        pay_dt = str(od.get("paymentDate") or po.get("paymentDate") or "").strip()
        if not pay_dt:
            pay_dt = str(po.get("decisionDate") or po.get("lastChangedDate") or "").strip()
        if pay_dt and pay_dt[:10] != today_str:
            continue
        qty = 0
        try:
            qty = int(po.get("quantity", 0))
        except Exception:
            qty = 0
        total_qty += qty

        # Try to match on multiple fields
        option_code = str(po.get("optionCode", "")).strip()
        if option_code and option_code in smart_map:
            label = smart_map[option_code]
            result[label] = result.get(label, 0) + qty
            continue
        candidates = [
            str(po.get("productId", "")).strip(),
            str(po.get("itemNo", "")).strip(),
            str(po.get("optionManageCode", "")).strip(),
            str(po.get("sellerProductItemId", "")).strip(),
        ]
        for cid in candidates:
            if cid in smart_map:
                label = smart_map[cid]
                result[label] = result.get(label, 0) + qty
                break
    return total_qty, result


def main() -> None:
    cfg = _load_secrets()
    make_webhook = _get_cfg_value(cfg, "cafe24", "make_webhook", env="CAFE24_MAKE_WEBHOOK")
    cafe24_base = _get_cfg_value(cfg, "cafe24", "base_url", env="CAFE24_BASE_URL")
    coupang_vendor_id = _get_cfg_value(cfg, "coupang", "vendor_id", env="COUPANG_VENDOR_ID")
    coupang_access = _get_cfg_value(cfg, "coupang", "access_key", env="COUPANG_ACCESS_KEY")
    coupang_secret = _get_cfg_value(cfg, "coupang", "secret_key", env="COUPANG_SECRET_KEY")

    if not make_webhook or not cafe24_base:
        raise RuntimeError("cafe24.make_webhook / cafe24.base_url 설정이 필요합니다.")

    target_date = _resolve_target_date_kst()
    start_date, end_date = _day_range_kst(target_date)

    cafe24_token = _fetch_make_token(make_webhook)
    orders = _cafe24_orders(cafe24_base, cafe24_token, start_date, end_date)
    cafe24_qty = _cafe24_sales_qty(orders)
    cafe24_by_variant = _cafe24_sales_by_variant(orders)

    coupang_qty = None
    coupang_items: Dict[str, int] = {}
    coupang_source = _get_cfg_value(cfg, "coupang", "sales_source", env="COUPANG_SALES_SOURCE", default="rg_api")
    coupang_source = coupang_source.strip().lower() or "rg_api"
    if coupang_vendor_id and coupang_access and coupang_secret:
        try:
            if coupang_source == "growth_ui":
                growth_url = _get_cfg_value(cfg, "coupang", "growth_url", env="COUPANG_GROWTH_URL")
                option_ids_raw = _get_cfg_value(
                    cfg,
                    "coupang",
                    "growth_option_ids",
                    env="COUPANG_GROWTH_OPTION_IDS",
                    default=",".join(COUPANG_CANONICAL_ITEM_MAP.keys()),
                )
                option_ids = [x.strip() for x in option_ids_raw.split(",") if x.strip()]
                profile_dir = _get_cfg_value(
                    cfg,
                    "coupang",
                    "growth_profile_dir",
                    env="COUPANG_GROWTH_PROFILE_DIR",
                    default="/Users/mune/Desktop/Cursor/sales_check_auto/profile",
                )
                headless = os.getenv("COUPANG_GROWTH_HEADLESS", "0").strip() in {"1", "true", "yes", "y"}
                coupang_qty, coupang_items = _coupang_growth_ui_sales_qty(
                    dashboard_url=growth_url,
                    profile_dir=profile_dir,
                    option_ids=option_ids,
                    headless=headless,
                    target_date=target_date,
                )
            else:
                coupang_qty, coupang_items = _coupang_sales_qty(
                    coupang_vendor_id,
                    coupang_access,
                    coupang_secret,
                    target_date=target_date,
                )
        except Exception as e:
            # If Coupang is not accessible (e.g., IP whitelist), skip and continue
            print(f"[WARN] 쿠팡 매출 조회 실패: {e}")
            coupang_qty = None
            coupang_items = {}

    payload = {
        "date": target_date,
        "cafe24_sales_qty": cafe24_qty,
        "cafe24_items": cafe24_by_variant,
        "coupang_sales_qty": coupang_qty,
        "coupang_items": coupang_items,
        "coupang_sales_source": coupang_source,
        "smartstore_sales_qty": None,
        "smartstore_items": {},
    }

    smart_id = _get_cfg_value(cfg, "smartstore", "client_id", env="SMARTSTORE_CLIENT_ID")
    smart_secret = _get_cfg_value(cfg, "smartstore", "client_secret", env="SMARTSTORE_CLIENT_SECRET")
    smart_account = _get_cfg_value(cfg, "smartstore", "account_id", env="SMARTSTORE_ACCOUNT_ID")
    smart_type = _get_cfg_value(cfg, "smartstore", "type", env="SMARTSTORE_TOKEN_TYPE", default="SELF").upper()
    if smart_id and smart_secret:
        try:
            token = _smartstore_token(
                smart_id,
                smart_secret,
                token_type=smart_type,
                account_id=smart_account,
            )
            orders = _smartstore_fetch_product_orders(token, target_date=target_date)
            smart_qty, smart_items = _smartstore_sales_by_variant(orders, target_date=target_date)
            payload["smartstore_sales_qty"] = smart_qty
            payload["smartstore_items"] = smart_items
        except Exception as e:
            print(f"[WARN] 스마트스토어 매출 조회 실패: {e}")
    else:
        print("[WARN] 스마트스토어 인증정보 없음 (smartstore.client_id / client_secret)")
    try:
        if ZoneInfo:
            now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        else:
            now_kst = datetime.now()
        fetched_at = now_kst.strftime("%Y-%m-%d %H:%M:%S")
        ws = _connect_sheet(cfg, readonly=False)
        _ensure_sales_header(ws)
        payload_json = json.dumps(payload, ensure_ascii=False)
        date_str = target_date
        _upsert_sales_row(ws, date_str=date_str, fetched_at=fetched_at, payload_json=payload_json)
        print(f"[INFO] Sales snapshot saved: {fetched_at}")
    except Exception as e:
        print(f"[WARN] Sales snapshot sheet update failed: {e}")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
