from __future__ import annotations

import os
import re
import tempfile
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

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


LOGIN_URL_DEFAULT = "https://www.ezadmin.co.kr/index.html"
OUTBOUND_LIST_URL_DEFAULT = "https://ga92.ezadmin.co.kr/template40.htm?template=IM50"

SKU_TO_EZADMIN_NAME = {
    "노트프로 블랙": "플라우드 노트 Pro / 블랙",
    "노트프로 실버": "플라우드 노트 Pro / 실버",
    "노트 블랙": "플라우드 노트 / 블랙",
    "노트 실버": "플라우드 노트 / 실버",
    "노트핀S 블랙": "플라우드 노트핀S / 블랙",
    "노트핀S 실버": "플라우드 노트핀S / 실버",
    "사용설명서": "V3C 사용설명서",
}
SKU_TO_EZADMIN_CODE = {
    "노트프로 블랙": "00355",
    "노트프로 실버": "00356",
    "노트 블랙": "00358",
    "노트 실버": "00359",
    "노트핀S 블랙": "00362",
    "노트핀S 실버": "00363",
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
    loc = _select_first(page, selectors)
    if loc is not None:
        return loc
    for frame in page.frames:
        loc = _select_first(frame, selectors)
        if loc is not None:
            return loc
    return None


def _debug_dump(page, label: str) -> None:
    try:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if page and not page.is_closed():
            page.screenshot(path=str(debug_dir / f"ezadmin_{label}_{ts}.png"), full_page=True)
            (debug_dir / f"ezadmin_{label}_{ts}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass


def _open_popup_or_same(page, click_locator, context, wait_url_contains: Optional[str] = None, timeout_ms: int = 15000):
    before_pages = set(context.pages)
    try:
        with page.expect_popup(timeout=5000) as popup_info:
            click_locator.first.click()
        popup = popup_info.value
        popup.wait_for_timeout(500)
        return popup
    except PlaywrightTimeoutError:
        click_locator.first.click()
        # Fallback: wait for new page in context
        elapsed = 0
        while elapsed < timeout_ms:
            for p in context.pages:
                if p not in before_pages:
                    try:
                        if wait_url_contains:
                            p.wait_for_url(f"**{wait_url_contains}**", timeout=5000)
                    except Exception:
                        pass
                    return p
            page.wait_for_timeout(250)
            elapsed += 250
        if wait_url_contains:
            try:
                page.wait_for_url(f"**{wait_url_contains}**", timeout=5000)
            except Exception:
                pass
        return page


def _fill_labeled_input(scope, label_text: str, value: str) -> bool:
    # Try table label -> input
    loc = scope.locator(
        f"xpath=//*[contains(normalize-space(.), '{label_text}')]/following::input[1]"
    )
    if loc.count() > 0:
        loc.first.fill(value)
        return True
    return False


def _select_option_contains(select_loc, text: str) -> bool:
    try:
        opts = select_loc.locator("option")
        for i in range(opts.count()):
            txt = opts.nth(i).text_content() or ""
            if text in txt:
                select_loc.select_option(value=opts.nth(i).get_attribute("value"))
                return True
    except Exception:
        pass
    return False


def _login_ezadmin(page, *, domain: str, username: str, password: str, login_url: str) -> None:
    page.goto(login_url, wait_until="domcontentloaded")
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
    try:
        page.evaluate("if (typeof login_popup === 'function') { login_popup(); }")
    except Exception:
        pass
    try:
        page.wait_for_selector("#login-domain, #login-id, #login-pwd", timeout=8000)
    except Exception:
        pass
    domain_input = _find_in_frames(page, ["#login-domain"])
    user_input = _find_in_frames(page, ["#login-id"])
    pwd_input = _find_in_frames(page, ["#login-pwd", "input[type='password']"])
    if not pwd_input:
        raise RuntimeError("비밀번호 입력칸을 찾지 못했습니다.")
    if domain_input:
        domain_input.first.fill(domain)
    if user_input:
        user_input.first.fill(username)
    if not domain_input or not user_input:
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
            any_text_inputs = _find_in_frames(page, ["input[type='text']:visible"])
            if any_text_inputs and any_text_inputs.count() >= 2:
                any_text_inputs.nth(0).fill(domain)
                any_text_inputs.nth(1).fill(username)
            elif any_text_inputs and any_text_inputs.count() == 1:
                any_text_inputs.nth(0).fill(username)
            else:
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
    try:
        page.wait_for_selector("#login-domain, #login-id, #login-pwd", state="hidden", timeout=8000)
    except Exception:
        pass
    page.wait_for_timeout(1200)
    if page.locator("input[type='password']").count() > 0 and "ezadmin.co.kr/index" in page.url:
        raise RuntimeError("로그인 실패로 보입니다.")


def _build_sheet_name(date_str: str, from_channel: str) -> str:
    ymd = date_str.replace("-", "")
    return f"{ymd}_{from_channel}_품고_신규"


def _collect_sheet_names(page) -> List[str]:
    tables = page.locator("table")
    for i in range(tables.count()):
        t = tables.nth(i)
        headers = [h.strip() for h in t.locator("th").all_text_contents()]
        if "전표명" in headers:
            name_idx = headers.index("전표명")
            rows = t.locator("tbody tr")
            names = []
            for r in range(rows.count()):
                cells = rows.nth(r).locator("td")
                if cells.count() > name_idx:
                    txt = cells.nth(name_idx).inner_text().strip()
                    if txt:
                        names.append(txt)
            return names
    return []


def _next_sheet_name(base_name: str, supplier_name: str, existing_names: List[str]) -> str:
    max_suffix = 0
    for name in existing_names:
        name = name.strip()
        m = re.search(rf"^{re.escape(base_name)}_(\\d+)", name)
        if m:
            suffix = int(m.group(1))
            if suffix > max_suffix:
                max_suffix = suffix
    return f"{base_name}_{max_suffix + 1}"


def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for item in items:
        sku_name = str(item.get("sku_name", "")).strip()
        barcode = str(item.get("barcode", "")).strip()
        qty = int(item.get("quantity", 0) or 0)
        if not sku_name or qty <= 0:
            continue
        ez_name = SKU_TO_EZADMIN_NAME.get(sku_name, sku_name)
        ez_code = SKU_TO_EZADMIN_CODE.get(sku_name, "").strip()
        normalized.append(
            {"sku_name": sku_name, "ez_name": ez_name, "ez_code": ez_code, "barcode": barcode, "quantity": qty}
        )
    return normalized


def create_outbound_request(
    *,
    items: List[Dict[str, Any]],
    date_str: str,
    from_channel: str,
    supplier_name: str = "주식회사뮨",
    headless: bool = True,
    stop_after_create: bool = True,
) -> Dict[str, Any]:
    cfg = _load_secrets()
    domain = _get_cfg_value(cfg, "ezadmin", "domain", env="EZADMIN_DOMAIN")
    username = _get_cfg_value(cfg, "ezadmin", "username", env="EZADMIN_USERNAME")
    password = _get_cfg_value(cfg, "ezadmin", "password", env="EZADMIN_PASSWORD")
    login_url = _get_cfg_value(cfg, "ezadmin", "login_url", env="EZADMIN_LOGIN_URL", default=LOGIN_URL_DEFAULT)
    outbound_url = _get_cfg_value(cfg, "ezadmin", "outbound_url", env="EZADMIN_OUTBOUND_URL", default=OUTBOUND_LIST_URL_DEFAULT)
    parsed = urllib.parse.urlparse(outbound_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    outbound_create_url = f"{base}/popup35.htm?template=IM51"

    if not domain or not username or not password:
        raise RuntimeError("ezadmin credentials missing (domain/username/password).")

    normalized_items = _normalize_items(items)
    if not normalized_items:
        raise RuntimeError("no valid items to register.")

    base_name = _build_sheet_name(date_str, from_channel)

    with sync_playwright() as p:
        channel = os.getenv("EZADMIN_CHROME_CHANNEL", "").strip()
        launch_args = [
            "--disable-crash-reporter",
            "--disable-features=Crashpad",
            "--no-crashpad",
            "--no-sandbox",
            "--disable-popup-blocking",
        ]
        tmp_home = tempfile.mkdtemp(prefix="ezadmin_chrome_home_")
        launch_env = os.environ.copy()
        launch_env["HOME"] = tmp_home
        if channel:
            browser = p.chromium.launch(headless=headless, channel=channel, args=launch_args, env=launch_env)
        else:
            browser = p.chromium.launch(headless=headless, args=launch_args, env=launch_env)
        context = browser.new_context()
        page = context.new_page()
        try:
            _login_ezadmin(page, domain=domain, username=username, password=password, login_url=login_url)

            page.goto(outbound_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)

            # Pre-search by base name to gather existing sheets for suffixing
            search_select = _find_in_frames(
                page,
                [
                    "select[name*='search']",
                    "select#search_kind",
                    "select[name*='key']",
                ],
            )
            if search_select:
                _select_option_contains(search_select.first, "전표명")
            search_input = _find_in_frames(
                page,
                [
                    "input#search_word",
                    "input[name*='keyword']",
                    "input[name*='search']:not([type='hidden'])",
                    "input[type='text'][name*='search']",
                ],
            )
            if search_input:
                search_input.first.fill(base_name)
            search_btn = _find_in_frames(page, ["div#search", "div.table_search_button", "button:has-text('검색')", "text=검색"])
            if search_btn:
                search_btn.first.click()
                page.wait_for_timeout(1200)

            # Compute next sheet name based on existing list (avoid duplicates)
            existing_names = _collect_sheet_names(page)
            sheet_name = _next_sheet_name(base_name, supplier_name, existing_names)
            display_name = f"{sheet_name}_{supplier_name}"

            # Create sheet (open popup URL directly to avoid popup blocking)
            create_popup = context.new_page()
            create_popup.goto(outbound_create_url, wait_until="domcontentloaded")
            create_popup.wait_for_timeout(800)

            # Supplier select
            supplier_select = _find_in_frames(create_popup, ["select", "select[name*='cust']", "select[name*='supplier']"])
            if supplier_select:
                try:
                    supplier_select.first.select_option(label=supplier_name)
                except Exception:
                    opts = supplier_select.first.locator("option")
                    for i in range(opts.count()):
                        txt = opts.nth(i).text_content() or ""
                        if supplier_name in txt:
                            supplier_select.first.select_option(value=opts.nth(i).get_attribute("value"))
                            break

            # Sheet name input
            if not _fill_labeled_input(create_popup, "전표이름", sheet_name):
                name_input = _find_in_frames(
                    create_popup,
                    ["input[name*='sheet']", "input[name*='subject']", "input[name*='title']", "input[type='text']"],
                )
                if name_input:
                    name_input.first.fill(sheet_name)

            create_btn = _find_in_frames(
                create_popup,
                [
                    "span.ez_btn.ez_btn_grey.pull-right",
                    "span.ez_btn.ez_btn_grey:has-text('전표생성')",
                    "span.ez_btn.ez_btn_grey a:has-text('전표생성')",
                    "button:has-text('전표생성')",
                    "text=전표생성",
                ],
            )
            if not create_btn:
                raise RuntimeError("전표생성 확인 버튼을 찾지 못했습니다.")
            create_btn.first.click()
            create_popup.wait_for_timeout(800)

            # Search created sheet by name to ensure it is visible
            if search_input:
                search_input.first.fill(display_name)
            if search_btn:
                search_btn.first.click()
                page.wait_for_timeout(1200)

            # Find created sheet
            page.wait_for_timeout(1200)
            sheet_link = _find_in_frames(page, [f"a:has-text('{display_name}')", f"a:has-text('{sheet_name}')"])
            if not sheet_link:
                raise RuntimeError("생성된 전표 링크를 찾지 못했습니다.")
            if stop_after_create:
                return {"sheet_name": sheet_name, "display_name": display_name}

            # Open detail (open href directly)
            href = sheet_link.first.get_attribute("href") or ""
            onclick = sheet_link.first.get_attribute("onclick") or ""
            sheet_id_from_href = None
            try:
                raw = href or onclick or ""
                if raw:
                    q = urllib.parse.urlparse(raw).query
                    qs = urllib.parse.parse_qs(q)
                    sheet_id_from_href = (qs.get("sheet") or qs.get("seq") or [None])[0]
                if not sheet_id_from_href and raw:
                    m = re.search(r"(sheet|seq)=?(\\d+)", raw)
                    if m:
                        sheet_id_from_href = m.group(2)
                if not sheet_id_from_href and raw:
                    m = re.search(r"(\\d{1,10})", raw)
                    if m:
                        sheet_id_from_href = m.group(1)
                if not sheet_id_from_href:
                    # Try to read "전표번호" column from the same row
                    row = sheet_link.first.locator("xpath=ancestor::tr[1]")
                    table = row.locator("xpath=ancestor::table[1]")
                    headers = [h.strip() for h in table.locator("th").all_text_contents()]
                    if "전표번호" in headers:
                        col_idx = headers.index("전표번호")
                        cells = row.locator("td")
                        if cells.count() > col_idx:
                            text = cells.nth(col_idx).inner_text()
                            m = re.search(r"(\\d+)", text or "")
                            if m:
                                sheet_id_from_href = m.group(1)
            except Exception:
                sheet_id_from_href = None
            # Try clicking first to follow the site's flow
            detail_popup = _open_popup_or_same(page, sheet_link, context, wait_url_contains="template=IM53")
            if detail_popup is page and sheet_id_from_href:
                detail_url = f"{base}/popup35.htm?template=IM53&sheet={sheet_id_from_href}"
                detail_popup = context.new_page()
                detail_popup.goto(detail_url, wait_until="domcontentloaded")
            elif detail_popup is page and href:
                detail_url = urllib.parse.urljoin(base + "/", href)
                detail_popup = context.new_page()
                detail_popup.goto(detail_url, wait_until="domcontentloaded")
            detail_popup.wait_for_timeout(800)

            # Open product add page directly using sheet id
            sheet_id = sheet_id_from_href
            try:
                q = urllib.parse.urlparse(detail_popup.url).query
                qs = urllib.parse.parse_qs(q)
                sheet_id = (qs.get("sheet") or qs.get("seq") or [None])[0]
            except Exception:
                sheet_id = None
            if not sheet_id:
                # fallback: click add button
                add_btn = _find_in_frames(
                    detail_popup,
                    [
                        "span:has-text('상품추가')",
                        "button:has-text('상품추가')",
                        "a:has-text('상품추가')",
                        "input[type='button'][value*='상품추가']",
                        "input[type='submit'][value*='상품추가']",
                        "text=상품추가",
                    ],
                )
                if not add_btn:
                    _debug_dump(detail_popup, "detail_no_addbtn")
                    raise RuntimeError("상품추가 버튼을 찾지 못했습니다.")
                product_popup = _open_popup_or_same(detail_popup, add_btn, context, wait_url_contains="template=IM54")
            else:
                product_url = f"{base}/popup35.htm?template=IM54&seq={sheet_id}"
                product_popup = context.new_page()
                product_popup.goto(product_url, wait_until="domcontentloaded")
            product_popup.wait_for_timeout(800)

            # Search
            search_btn = _find_in_frames(product_popup, ["div#search", "div.table_search_button", "button:has-text('검색')", "text=검색"])
            if not search_btn:
                raise RuntimeError("검색 버튼을 찾지 못했습니다.")
            search_btn.first.click()
            product_popup.wait_for_timeout(1200)

            # Wait for jqGrid rows to load
            try:
                product_popup.wait_for_function(
                    "document.querySelectorAll('table#grid1 tbody tr').length > 0",
                    timeout=20000,
                )
            except Exception:
                pass

            name_to_qty = {}
            code_to_qty = {}
            for item in normalized_items:
                name_to_qty[item["ez_name"]] = int(item["quantity"])
                if item.get("ez_code"):
                    code_to_qty[item["ez_code"]] = int(item["quantity"])

            product_popup.evaluate(
                """
                ({ nameToQty, codeToQty }) => {
                  const rows = document.querySelectorAll('table#grid1 tbody tr');
                  const headers = Array.from(document.querySelectorAll('.ui-jqgrid-htable th'));
                  const findHeaderId = (label) => {
                    const h = headers.find((el) => (el.textContent || '').trim().includes(label));
                    return h ? h.id : null;
                  };
                  const nameHeaderId = findHeaderId('상품명');
                  const codeHeaderId = findHeaderId('상품코드');
                  const qtyHeaderId = findHeaderId('출고수량');
                  rows.forEach((row) => {
                    const codeCell = codeHeaderId ? row.querySelector(`td[aria-describedby='${codeHeaderId}']`) : null;
                    const codeText = codeCell ? codeCell.textContent.trim() : '';
                    const nameCell = nameHeaderId ? row.querySelector(`td[aria-describedby='${nameHeaderId}']`) : null;
                    const nameText = nameCell ? nameCell.textContent.trim() : '';
                    let qty = null;
                    if (codeText && codeToQty[codeText] != null) {
                      qty = codeToQty[codeText];
                    }
                    for (const [name, val] of Object.entries(nameToQty)) {
                      if (name && nameText.includes(name)) {
                        qty = val;
                        break;
                      }
                    }
                    if (qty == null) return;
                    const input = qtyHeaderId
                      ? row.querySelector(`td[aria-describedby='${qtyHeaderId}'] input`)
                      : row.querySelector('td.stockout_qty input');
                    if (input) {
                      input.value = String(qty);
                      input.dispatchEvent(new Event('input', { bubbles: true }));
                      input.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                  });
                }
                """,
                {"nameToQty": name_to_qty, "codeToQty": code_to_qty},
            )

            # Click insert all
            insert_all = _find_in_frames(
                product_popup,
                [
                    "a[onclick*='insert_all']",
                    "span.ez_btn_mini.ez_btn_grey a:has-text('전체추가')",
                    "a:has-text('전체추가')",
                    "text=전체추가",
                ],
            )
            if not insert_all:
                raise RuntimeError("전체추가 버튼을 찾지 못했습니다.")
            insert_all.first.click()
            product_popup.wait_for_timeout(800)

            return {"sheet_name": sheet_name, "display_name": display_name}
        except Exception:
            _debug_dump(page, "outbound_error")
            raise


if __name__ == "__main__":
    # Manual test: reads env for a single SKU
    sku = os.getenv("EZADMIN_TEST_SKU", "노트프로 블랙")
    qty = int(os.getenv("EZADMIN_TEST_QTY", "1"))
    date_str = os.getenv("EZADMIN_TEST_DATE", datetime.now().strftime("%Y-%m-%d"))
    from_channel = os.getenv("EZADMIN_TEST_FROM", "이지어드민")
    headless = os.getenv("EZADMIN_HEADLESS", "1") != "0"
    stop_after_create = os.getenv("EZADMIN_OUTBOUND_STOP_AFTER_CREATE", "1") != "0"
    resp = create_outbound_request(
        items=[{"sku_name": sku, "quantity": qty}],
        date_str=date_str,
        from_channel=from_channel,
        headless=headless,
        stop_after_create=stop_after_create,
    )
    print(resp)
