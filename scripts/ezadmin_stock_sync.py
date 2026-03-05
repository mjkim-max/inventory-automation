from __future__ import annotations

import json
import os
import re
import tempfile
import time
import hmac
import hashlib
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    import pyperclip
except Exception:  # pragma: no cover
    pyperclip = None  # type: ignore[assignment]
try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


LOGIN_URL_DEFAULT = "https://www.ezadmin.co.kr/index.html"
INVENTORY_URL_DEFAULT = (
    "https://ga92.ezadmin.co.kr/template35.htm?template=I100&time="
)

COLUMNS = ["date", "source", "sku", "normal_stock"]

# Sheet layout (one row per day)
DATE_COLUMN = "A"
DATA_START_ROW = 3
SKU_COLUMN_MAP = {
    "00355": "C",  # 노트프로 블랙 (이지어드민)
    "00356": "G",  # 노트프로 실버 (이지어드민)
    "00358": "K",  # 노트 블랙 (이지어드민)
    "00359": "O",  # 노트 실버 (이지어드민)
    "00362": "S",  # 노트핀S 블랙 (이지어드민)
    "00363": "W",  # 노트핀S 실버 (이지어드민)
}

# Poomgo columns (품고)
POOMGO_COLUMN_MAP = {
    "00355": "B",  # 노트프로 블랙
    "00356": "F",  # 노트프로 실버
    "00358": "J",  # 노트 블랙
    "00359": "N",  # 노트 실버
    "00362": "R",  # 노트핀S 블랙
    "00363": "V",  # 노트핀S 실버
    "8821006473832": "Z",  # 사용설명서
}

POOMGO_NAME_MAP = {
    "플라우드 노트 Pro / 블랙": "00355",
    "플라우드 노트 Pro / 실버": "00356",
    "플라우드 노트 / 블랙": "00358",
    "플라우드 노트 / 실버": "00359",
    "플라우드 노트핀S / 블랙": "00362",
    "플라우드 노트핀S / 실버": "00363",
}

# Barcode -> internal SKU mapping (more stable than names)
POOMGO_CODE_MAP = {
    "199284926073": "00355",
    "199284928237": "00356",
    "6977512610000": "00358",
    "6977512610024": "00359",
    "0199284031340": "00362",
    "0199284909670": "00363",
    "8821006473832": "8821006473832",
}

# Coupang columns (쿠팡)
COUPANG_COLUMN_MAP = {
    "00355": "D",  # 노트프로 블랙
    "00356": "H",  # 노트프로 실버
    "00358": "L",  # 노트 블랙
    "00359": "P",  # 노트 실버
    "00362": "T",  # 노트핀S 블랙
    "00363": "X",  # 노트핀S 실버
}

COUPANG_VENDOR_ITEM_MAP = {
    "00355": "94199205555",
    "00356": "94199205552",
    "00358": "90737907302",
    "00359": "90737907295",
    "00362": "91942294087",
    "00363": "91942294096",
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


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _build_inventory_url(base_url: str) -> str:
    if "time=" in base_url:
        return base_url
    now = datetime.now().strftime("%a %b %d %Y %H:%M:%S GMT+0900 (KST)")
    return f"{base_url}{now}"


def _connect_sheet(cfg: Dict[str, Any], *, retries: int = 3, backoff_sec: int = 5):
    if gspread is None or Credentials is None:
        raise RuntimeError("gspread/google-auth not installed.")
    gs_cfg = cfg.get("google_sheets", {})
    sa = cfg.get("google_sheets_service_account", {})
    if not gs_cfg or not sa:
        raise RuntimeError("google_sheets or google_sheets_service_account missing in secrets.")
    sheet_id = gs_cfg.get("sheet_id") or gs_cfg.get("spreadsheet_id")
    worksheet = gs_cfg.get("worksheet", "ezadmin_stock")
    if not sheet_id:
        raise RuntimeError("google_sheets.sheet_id is required.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(sa), scopes=scopes)
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            client = gspread.authorize(creds)
            ss = client.open_by_key(sheet_id)
            return ss.worksheet(worksheet)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_sec)
    raise RuntimeError(f"구글 시트 연결 실패: {last_err}")


def _ensure_header(ws) -> None:
    values = ws.get_all_values()
    if not values:
        ws.append_row(COLUMNS)
        return
    if values[0] != COLUMNS:
        ws.insert_row(COLUMNS, index=1)


def _parse_int(value: str) -> int:
    return int(re.sub(r"[^\d]", "", value or "0") or 0)


def _select_first(scope, selectors: List[str]):
    for sel in selectors:
        loc = scope.locator(sel)
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            continue
    return None


def _find_in_frames(page, selectors: List[str]):
    # Try main page first
    loc = _select_first(page, selectors)
    if loc is not None:
        return loc
    # Then search iframes
    for frame in page.frames:
        loc = _select_first(frame, selectors)
        if loc is not None:
            return loc
    return None


def _find_table(page) -> Tuple[Any, List[str]]:
    tables = page.locator("table")
    for i in range(tables.count()):
        t = tables.nth(i)
        headers = [h.strip() for h in t.locator("th").all_text_contents()]
        if "상품코드" in headers and "정상재고" in headers:
            return t, headers
    for i in range(tables.count()):
        t = tables.nth(i)
        row = t.locator("tr").first
        cells = [c.strip() for c in row.locator("td,th").all_text_contents()]
        if "상품코드" in cells and "정상재고" in cells:
            return t, cells
    raise RuntimeError("재고 테이블을 찾지 못했습니다. 셀렉터를 확인하세요.")


def fetch_ezadmin_stock(
    *,
    domain: str,
    username: str,
    password: str,
    login_url: str,
    inventory_url: str,
    headless: bool,
) -> List[Dict[str, Any]]:
    inventory_url = _build_inventory_url(inventory_url)
    with sync_playwright() as p:
        # Prefer bundled Playwright Chromium for stability.
        # Use system Chrome only when explicitly requested via EZADMIN_CHROME_CHANNEL.
        channel = os.getenv("EZADMIN_CHROME_CHANNEL", "").strip()
        browser_pref = os.getenv("EZADMIN_BROWSER", "chromium").strip().lower()
        allow_fallback = _get_bool_env("EZADMIN_ALLOW_FALLBACK", default=True)
        profile_dir = os.getenv("EZADMIN_PROFILE_DIR", "").strip()
        if not profile_dir:
            profile_dir = str((Path(__file__).resolve().parents[1] / ".playwright" / "ezadmin-profile"))
        Path(profile_dir).mkdir(parents=True, exist_ok=True)

        launch_args = [
            "--disable-crash-reporter",
            "--disable-breakpad",
            "--disable-features=Crashpad",
            "--no-crashpad",
            "--no-sandbox",
            f"--user-data-dir={profile_dir}",
        ]
        tmp_home = tempfile.mkdtemp(prefix="ezadmin_chrome_home_")
        launch_env = os.environ.copy()
        launch_env["HOME"] = tmp_home
        browser = None
        context = None
        launch_err = None
        if browser_pref == "firefox":
            browser = p.firefox.launch(headless=headless)
            context = browser.new_context()
        elif browser_pref == "webkit":
            browser = p.webkit.launch(headless=headless)
            context = browser.new_context()
        else:
            try:
                if channel:
                    context = p.chromium.launch_persistent_context(
                        user_data_dir=profile_dir,
                        headless=headless,
                        channel=channel,
                        args=launch_args,
                        env=launch_env,
                    )
                else:
                    context = p.chromium.launch_persistent_context(
                        user_data_dir=profile_dir,
                        headless=headless,
                        args=launch_args,
                        env=launch_env,
                    )
            except Exception as e:
                launch_err = e
                if allow_fallback:
                    print("Chromium 실행 실패, Firefox로 대체 실행합니다.")
                    browser = p.firefox.launch(headless=headless)
                    context = browser.new_context()
        if context is None:
            if launch_err:
                raise launch_err
            raise RuntimeError("브라우저를 실행하지 못했습니다.")
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(login_url, wait_until="domcontentloaded")

        # Open login modal by clicking the top-right profile icon.
        profile_btn = _find_in_frames(
            page,
            [
                "li.login a",
                "li.login",
                "a.mlogin",
                "button[aria-label*='로그인']",
                "button[aria-label*='account']",
                "button:has(svg)",
                "a:has(svg)",
                "a[role='button']",
            ],
        )
        if profile_btn:
            try:
                profile_btn.first.click()
                page.wait_for_timeout(800)
            except Exception:
                pass
        # Fallback: open login popup via JS if available
        try:
            page.evaluate("if (typeof login_popup === 'function') { login_popup(); }")
        except Exception:
            pass

        # Wait for login inputs to appear
        try:
            page.wait_for_selector("#login-domain, #login-id, #login-pwd", timeout=8000)
        except Exception:
            pass

        # Find login inputs in main page or iframe.
        domain_input = _find_in_frames(page, ["#login-domain"])
        user_input = _find_in_frames(page, ["#login-id"])
        pwd_input = _find_in_frames(page, ["#login-pwd", "input[type='password']"])
        if not pwd_input:
            debug_dir = Path(__file__).resolve().parents[1] / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            try:
                page.screenshot(path=str(debug_dir / "ezadmin_login_fields_not_found.png"), full_page=True)
                (debug_dir / "ezadmin_login_fields_not_found.html").write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            raise RuntimeError("비밀번호 입력칸을 찾지 못했습니다.")
        if domain_input:
            domain_input.first.fill(domain)
        if user_input:
            user_input.first.fill(username)
        if not domain_input or not user_input:
            # Fallback: fill by order within the same container
            container = pwd_input.first.locator("xpath=ancestor::form[1]")
            if container.count() == 0:
                container = pwd_input.first.locator("xpath=ancestor::div[1]")
            text_inputs = container.locator("input[type='text']:visible, input[type='email']:visible, input[type='tel']:visible")
            if text_inputs.count() >= 2:
                text_inputs.nth(0).fill(domain)
                text_inputs.nth(1).fill(username)
            elif text_inputs.count() == 1:
                text_inputs.nth(0).fill(username)
            else:
                # Fallback: find visible text inputs anywhere (including frames)
                any_text_inputs = _find_in_frames(page, ["input[type='text']:visible"])
                if any_text_inputs and any_text_inputs.count() >= 2:
                    any_text_inputs.nth(0).fill(domain)
                    any_text_inputs.nth(1).fill(username)
                elif any_text_inputs and any_text_inputs.count() == 1:
                    any_text_inputs.nth(0).fill(username)
                else:
                    debug_dir = Path(__file__).resolve().parents[1] / "debug"
                    debug_dir.mkdir(parents=True, exist_ok=True)
                    try:
                        page.screenshot(path=str(debug_dir / "ezadmin_login_fields_not_found.png"), full_page=True)
                        (debug_dir / "ezadmin_login_fields_not_found.html").write_text(page.content(), encoding="utf-8")
                    except Exception:
                        pass
                    raise RuntimeError("도메인/아이디 입력칸을 찾지 못했습니다.")
        pwd_input.first.fill(password)

        login_btn = _select_first(
            page,
            [
                "button:has-text('로그인')",
                "input[type='button'][value*='로그인']",
                "input[type='submit'][value*='로그인']",
                "text=로그인",
            ],
        )
        if not login_btn:
            raise RuntimeError("로그인 버튼을 찾지 못했습니다.")
        login_btn.first.click()

        # Allow login to complete; if still on login page, capture debug.
        try:
            page.wait_for_selector("#login-domain, #login-id, #login-pwd", state="hidden", timeout=8000)
        except Exception:
            pass
        page.wait_for_timeout(1500)
        if page.locator("input[type='password']").count() > 0 and "ezadmin.co.kr/index" in page.url:
            debug_dir = Path(__file__).resolve().parents[1] / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            try:
                page.screenshot(path=str(debug_dir / "ezadmin_login_failed.png"), full_page=True)
                (debug_dir / "ezadmin_login_failed.html").write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            raise RuntimeError("로그인 실패로 보입니다. debug/ezadmin_login_failed.png 확인 요망.")

        page.goto(inventory_url, wait_until="domcontentloaded")
        try:
            page.wait_for_selector("#grid1", timeout=15000)
        except Exception:
            pass

        search_btn = _find_in_frames(
            page,
            [
                "div#search.table_search_button",
                "button:has-text('조회')",
                "button:has-text('검색')",
                "input[type='button'][value*='조회']",
                "input[type='button'][value*='검색']",
                "a:has-text('조회')",
                "a:has-text('검색')",
                "text=조회",
                "text=검색",
            ],
        )
        if not search_btn:
            debug_dir = Path(__file__).resolve().parents[1] / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            try:
                page.screenshot(path=str(debug_dir / "ezadmin_search_not_found.png"), full_page=True)
                (debug_dir / "ezadmin_search_not_found.html").write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            raise RuntimeError("조회/검색 버튼을 찾지 못했습니다. debug 폴더를 확인하세요.")
        search_btn.first.click()

        # Wait for jqGrid loading to finish
        try:
            page.wait_for_timeout(800)
            page.wait_for_selector("#load_grid1", state="visible", timeout=5000)
            page.wait_for_selector("#load_grid1", state="hidden", timeout=20000)
            page.wait_for_selector("table#grid1 tbody tr.ui-widget-content", timeout=15000)
        except Exception:
            pass

        # Prefer jqGrid table structure
        rows = []
        grid = _find_in_frames(page, ["table#grid1"])
        if grid:
            body_rows = grid.locator("tbody tr.ui-widget-content")
            for i in range(body_rows.count()):
                row = body_rows.nth(i)
                sku = row.locator("td.product_id a").inner_text().strip()
                stock_raw = row.locator("td.stock span").inner_text().strip()
                if not sku:
                    continue
                rows.append({"sku": sku, "normal_stock": _parse_int(stock_raw)})
        else:
            table, headers = _find_table(page)
            header_index = {h: i for i, h in enumerate(headers)}
            sku_idx = header_index.get("상품코드")
            stock_idx = header_index.get("정상재고")
            if sku_idx is None or stock_idx is None:
                raise RuntimeError("상품코드/정상재고 컬럼 인덱스를 찾지 못했습니다.")

            body_rows = table.locator("tbody tr")
            if body_rows.count() == 0:
                body_rows = table.locator("tr").filter(has=page.locator("td"))
            for i in range(body_rows.count()):
                cells = body_rows.nth(i).locator("td")
                if cells.count() == 0:
                    continue
                sku = cells.nth(sku_idx).inner_text().strip()
                stock_raw = cells.nth(stock_idx).inner_text().strip()
                if not sku:
                    continue
                rows.append(
                    {
                        "sku": sku,
                        "normal_stock": _parse_int(stock_raw),
                    }
                )
        if context:
            context.close()
        if browser:
            browser.close()
        return rows


def _rows_to_clipboard(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    lines = ["sku\tnormal_stock"]
    for r in rows:
        lines.append(f"{r['sku']}\t{r['normal_stock']}")
    payload = "\n".join(lines)
    if pyperclip is not None:
        pyperclip.copy(payload)
    return payload


def _col_to_index(col: str) -> int:
    col = col.upper().strip()
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx


def _find_or_create_date_row(ws, date_str: str, target_cols: List[str], *, overwrite: bool = True) -> int:
    all_values = ws.get_all_values()
    col_idx = _col_to_index(DATE_COLUMN)
    target_idx = [_col_to_index(c) for c in target_cols]

    # Helper to check if any target cell has data
    def row_has_data(row: List[str]) -> bool:
        for idx in target_idx:
            if idx - 1 < len(row) and str(row[idx - 1]).strip():
                return True
        return False

    # Find existing row for date
    for i in range(DATA_START_ROW - 1, len(all_values)):
        row = all_values[i]
        if len(row) >= col_idx and row[col_idx - 1].strip() == date_str:
            if overwrite:
                return i + 1
            if not row_has_data(row):
                return i + 1

    # Find first empty row starting from DATA_START_ROW
    for i in range(DATA_START_ROW - 1, len(all_values)):
        row = all_values[i]
        date_cell = row[col_idx - 1] if len(row) >= col_idx else ""
        if not str(date_cell).strip():
            ws.update_cell(i + 1, col_idx, date_str)
            return i + 1

    # Append new row at the end
    new_row_idx = len(all_values) + 1
    ws.update_cell(new_row_idx, col_idx, date_str)
    return new_row_idx


def _get_nested_field(item: Dict[str, Any], path: str) -> Any:
    cur: Any = item
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _parse_qty(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except Exception:
            return None
    return None


def _fetch_poomgo_items(*, token: str) -> List[Dict[str, Any]]:
    if requests is None:
        raise RuntimeError("requests is not installed.")
    url = "https://api.poomgo.com/open-api/wms/resources/quantity-at"
    payload = {"page": 1, "pageSize": 50, "executeAt": datetime.now().isoformat()}
    headers = {"Authorization": token}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code in {401, 403} and not token.lower().startswith("bearer "):
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = (
        data.get("collection")
        or data.get("rows")
        or data.get("data")
        or data.get("items")
        or []
    )
    if isinstance(items, dict):
        items = items.get("collection") or items.get("items") or []
    if not isinstance(items, list):
        return []
    return items


def _poomgo_items_to_stock(
    items: List[Dict[str, Any]],
    *,
    quantity_field: str = "",
    container_type_allowlist: Optional[List[str]] = None,
    pathname_allowlist: Optional[List[str]] = None,
) -> Dict[str, int]:
    allowlist = {c.strip() for c in (container_type_allowlist or []) if c.strip()}
    path_allow = [p.strip() for p in (pathname_allowlist or []) if p.strip()]
    result: Dict[str, int] = {}
    fallback_fields = [
        "available_quantity",
        "availableQuantity",
        "available_stock",
        "availableStock",
        "orderable_quantity",
        "orderableQuantity",
        "total_quantity",
        "totalQuantity",
        "quantity",
        "result_quantity",
    ]
    for item in items:
        name = str(item.get("name", "")).strip()
        if allowlist:
            ct = str(item.get("container_type_code_key", "")).strip()
            if ct not in allowlist:
                continue
        if path_allow:
            pathname = str(item.get("pathname", "")).strip()
            if not any(p in pathname for p in path_allow):
                continue
        qty_val = None
        if quantity_field:
            qty_val = _get_nested_field(item, quantity_field)
        if qty_val is None:
            for key in fallback_fields:
                if key in item:
                    qty_val = item.get(key)
                    break
        qty = _parse_qty(qty_val)
        sku = None
        code = str(item.get("code", "")).strip()
        if code in POOMGO_CODE_MAP:
            sku = POOMGO_CODE_MAP[code]
        elif name in POOMGO_NAME_MAP:
            sku = POOMGO_NAME_MAP[name]
        if sku and qty is not None:
            result[sku] = result.get(sku, 0) + qty
    return result


def fetch_poomgo_stock(
    *,
    token: str,
    quantity_field: str = "",
    container_type_allowlist: Optional[List[str]] = None,
    pathname_allowlist: Optional[List[str]] = None,
) -> Dict[str, int]:
    items = _fetch_poomgo_items(token=token)
    if not items:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        try:
            (debug_dir / "poomgo_response.json").write_text(
                json.dumps({"rows": items}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass
    return _poomgo_items_to_stock(
        items,
        quantity_field=quantity_field,
        container_type_allowlist=container_type_allowlist,
        pathname_allowlist=pathname_allowlist,
    )


def _coupang_auth(access_key: str, secret_key: str, method: str, path: str, query: str) -> str:
    # Coupang HMAC signature uses GMT time
    signed_date = time.strftime("%y%m%dT%H%M%SZ", time.gmtime())
    message = f"{signed_date}{method}{path}{query}"
    signature = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"CEA algorithm=HmacSHA256, access-key={access_key}, signed-date={signed_date}, signature={signature}"


def fetch_coupang_stock(*, vendor_id: str, access_key: str, secret_key: str) -> Dict[str, int]:
    if requests is None:
        raise RuntimeError("requests is not installed.")
    host = "https://api-gateway.coupang.com"
    path = f"/v2/providers/rg_open_api/apis/api/v1/vendors/{vendor_id}/rg/inventory/summaries"
    result: Dict[str, int] = {}
    last_response = None

    for sku, vendor_item_id in COUPANG_VENDOR_ITEM_MAP.items():
        query = urllib.parse.urlencode({"vendorItemId": vendor_item_id})
        auth = _coupang_auth(access_key, secret_key, "GET", path, query)
        headers = {
            "Authorization": auth,
            "X-Requested-By": vendor_id,
            "X-MARKET": "KR",
            "Content-Type": "application/json;charset=UTF-8",
        }
        url = f"{host}{path}?{query}"
        for attempt in range(1, 4):
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                last_response = data
                items = data.get("data") or []
                qty = None
                for item in items:
                    if str(item.get("vendorItemId")) == vendor_item_id:
                        inv = item.get("inventoryDetails") or {}
                        qty = (
                            inv.get("totalOrderableQuantity")
                            if isinstance(inv, dict)
                            else None
                        )
                        if qty is None:
                            qty = item.get("availableStock", item.get("onHandStock", item.get("quantity")))
                        break
                if qty is not None:
                    try:
                        result[sku] = int(float(qty))
                    except Exception:
                        pass
                break
            except Exception:
                if attempt < 3:
                    time.sleep(2 * attempt)
                else:
                    continue
    if not result:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        try:
            (debug_dir / "coupang_last_response.json").write_text(
                json.dumps(last_response or {}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass
    return result


def main() -> None:
    cfg = _load_secrets()
    ezadmin_enabled = _get_bool_env("EZADMIN_STOCK_ENABLE", default=True)
    poomgo_enabled = _get_bool_env("POOMGO_STOCK_ENABLE", default=True)
    coupang_enabled = _get_bool_env("COUPANG_STOCK_ENABLE", default=True)

    rows: List[Dict[str, Any]] = []
    if ezadmin_enabled:
        domain = _get_cfg_value(cfg, "ezadmin", "domain", env="EZADMIN_DOMAIN")
        username = _get_cfg_value(cfg, "ezadmin", "username", env="EZADMIN_USERNAME")
        password = _get_cfg_value(cfg, "ezadmin", "password", env="EZADMIN_PASSWORD")
        if not domain or not username or not password:
            raise RuntimeError("EZADMIN_DOMAIN/USERNAME/PASSWORD 설정이 필요합니다.")

        login_url = _get_cfg_value(cfg, "ezadmin", "login_url", env="EZADMIN_LOGIN_URL", default=LOGIN_URL_DEFAULT)
        inventory_url = _get_cfg_value(
            cfg,
            "ezadmin",
            "inventory_url",
            env="EZADMIN_INVENTORY_URL",
            default=INVENTORY_URL_DEFAULT,
        )
        headless = _get_bool_env("EZADMIN_HEADLESS", default=False)

        retry_count = int(os.getenv("EZADMIN_RETRY_COUNT", "3"))
        retry_delay = int(os.getenv("EZADMIN_RETRY_DELAY_SEC", "30"))
        last_err: Optional[Exception] = None
        for attempt in range(1, retry_count + 1):
            try:
                rows = fetch_ezadmin_stock(
                    domain=domain,
                    username=username,
                    password=password,
                    login_url=login_url,
                    inventory_url=inventory_url,
                    headless=headless,
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"[WARN] ezadmin stock fetch failed (attempt {attempt}/{retry_count}): {e}")
                if attempt < retry_count:
                    time.sleep(retry_delay)
        if last_err:
            raise last_err

        # Filter to required SKUs
        allowed = set(SKU_COLUMN_MAP.keys())
        rows = [r for r in rows if r["sku"] in allowed]

        payload = _rows_to_clipboard(rows)
        if payload:
            print("클립보드 복사 완료 (sku, normal_stock)")

    ws = _connect_sheet(cfg)
    today = datetime.now().strftime("%Y-%m-%d")
    row_idx = _find_or_create_date_row(
        ws,
        today,
        list(SKU_COLUMN_MAP.values()) + list(POOMGO_COLUMN_MAP.values()) + list(COUPANG_COLUMN_MAP.values()),
        overwrite=True,
    )

    # Build updates for ezadmin SKU columns
    updates = []
    if ezadmin_enabled:
        for r in rows:
            col = SKU_COLUMN_MAP.get(r["sku"])
            if not col:
                continue
            updates.append(
                {
                    "range": f"{col}{row_idx}",
                    "values": [[r["normal_stock"]]],
                }
            )

    # Fetch Poomgo and map to columns
    poomgo_token = _get_cfg_value(cfg, "poomgo", "token", env="POOMGO_TOKEN")
    poomgo_qty_field = _get_cfg_value(
        cfg,
        "poomgo",
        "quantity_field",
        env="POOMGO_QTY_FIELD",
        default="",
    )
    poomgo_ct_allow = _get_cfg_value(
        cfg,
        "poomgo",
        "container_type_allowlist",
        env="POOMGO_CONTAINER_TYPE_ALLOWLIST",
        default="",
    )
    poomgo_path_allow = _get_cfg_value(
        cfg,
        "poomgo",
        "pathname_allowlist",
        env="POOMGO_PATHNAME_ALLOWLIST",
        default="",
    )
    poomgo_ct_allowlist = [c.strip() for c in poomgo_ct_allow.split(",") if c.strip()]
    poomgo_path_allowlist = [p.strip() for p in poomgo_path_allow.split(",") if p.strip()]
    if poomgo_enabled and poomgo_token:
        poomgo_items = _fetch_poomgo_items(token=poomgo_token)
        poomgo = _poomgo_items_to_stock(
            poomgo_items,
            quantity_field=poomgo_qty_field,
            container_type_allowlist=poomgo_ct_allowlist,
            pathname_allowlist=poomgo_path_allowlist,
        )
        for sku, qty in poomgo.items():
            col = POOMGO_COLUMN_MAP.get(sku)
            if not col:
                continue
            updates.append({"range": f"{col}{row_idx}", "values": [[qty]]})

        # Debug sheet: compare total vs picking-only for quick validation
        try:
            poomgo_all = _poomgo_items_to_stock(
                poomgo_items,
                quantity_field=poomgo_qty_field,
                container_type_allowlist=poomgo_ct_allowlist,
                pathname_allowlist=[],
            )
            poomgo_picking = _poomgo_items_to_stock(
                poomgo_items,
                quantity_field=poomgo_qty_field,
                container_type_allowlist=poomgo_ct_allowlist,
                pathname_allowlist=["피킹 공간"],
            )
            debug_ws = ws.spreadsheet.worksheet("Poomgo_debug")
        except Exception:
            try:
                debug_ws = ws.spreadsheet.add_worksheet(title="Poomgo_debug", rows=2000, cols=10)
                debug_ws.append_row(["date", "sku", "poomgo_picking", "poomgo_all"])
            except Exception:
                debug_ws = None
        if debug_ws is not None:
            debug_rows = []
            for sku in sorted(POOMGO_COLUMN_MAP.keys()):
                debug_rows.append(
                    [
                        today,
                        sku,
                        poomgo_picking.get(sku, 0),
                        poomgo_all.get(sku, 0),
                    ]
                )
            if debug_rows:
                debug_ws.append_rows(debug_rows, value_input_option="USER_ENTERED")

    # Fetch Coupang and map to columns
    coupang_access = _get_cfg_value(cfg, "coupang", "access_key", env="COUPANG_ACCESS_KEY")
    coupang_secret = _get_cfg_value(cfg, "coupang", "secret_key", env="COUPANG_SECRET_KEY")
    coupang_vendor_id = _get_cfg_value(cfg, "coupang", "vendor_id", env="COUPANG_VENDOR_ID")
    if coupang_enabled and coupang_access and coupang_secret and coupang_vendor_id:
        try:
            coupang = fetch_coupang_stock(
                vendor_id=coupang_vendor_id,
                access_key=coupang_access,
                secret_key=coupang_secret,
            )
            for sku, qty in coupang.items():
                col = COUPANG_COLUMN_MAP.get(sku)
                if not col:
                    continue
                updates.append({"range": f"{col}{row_idx}", "values": [[qty]]})
        except Exception as e:
            print(f"[WARN] 쿠팡 재고 조회 실패: {e}")
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
    print(f"업데이트 완료: {len(updates)} cells")


if __name__ == "__main__":
    main()
