from __future__ import annotations

import os
import re
import tempfile
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
INBOUND_LIST_URL_DEFAULT = "https://ga92.ezadmin.co.kr/template40.htm?template=IM00"

SKU_TO_EZADMIN_NAME = {
    "노트프로 블랙": "플라우드 노트 Pro / 블랙",
    "노트프로 실버": "플라우드 노트 Pro / 실버",
    "노트 블랙": "플라우드 노트 / 블랙",
    "노트 실버": "플라우드 노트 / 실버",
    "노트핀S 블랙": "플라우드 노트핀S / 블랙",
    "노트핀S 실버": "플라우드 노트핀S / 실버",
    "사용설명서": "V3C 사용설명서",
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


def _fill_labeled_input(scope, label_text: str, value: str) -> bool:
    # Try table label -> input
    loc = scope.locator(
        f\"xpath=//*[contains(normalize-space(.), '{label_text}')]/following::input[1]\"
    )
    if loc.count() > 0:
        loc.first.fill(value)
        return True
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
    return f"{ymd}_{from_channel}_이지어드민_입고"


def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for item in items:
        sku_name = str(item.get("sku_name", "")).strip()
        qty = int(item.get("quantity", 0) or 0)
        if not sku_name or qty <= 0:
            continue
        ez_name = SKU_TO_EZADMIN_NAME.get(sku_name, sku_name)
        normalized.append({"sku_name": sku_name, "ez_name": ez_name, "quantity": qty})
    return normalized


def create_inbound_request(
    *,
    items: List[Dict[str, Any]],
    date_str: str,
    from_channel: str,
    supplier_name: str = "주식회사뮨",
    headless: bool = True,
) -> Dict[str, Any]:
    cfg = _load_secrets()
    domain = _get_cfg_value(cfg, "ezadmin", "domain", env="EZADMIN_DOMAIN")
    username = _get_cfg_value(cfg, "ezadmin", "username", env="EZADMIN_USERNAME")
    password = _get_cfg_value(cfg, "ezadmin", "password", env="EZADMIN_PASSWORD")
    login_url = _get_cfg_value(cfg, "ezadmin", "login_url", env="EZADMIN_LOGIN_URL", default=LOGIN_URL_DEFAULT)
    inbound_url = _get_cfg_value(cfg, "ezadmin", "inbound_url", env="EZADMIN_INBOUND_URL", default=INBOUND_LIST_URL_DEFAULT)

    if not domain or not username or not password:
        raise RuntimeError("ezadmin credentials missing (domain/username/password).")

    normalized_items = _normalize_items(items)
    if not normalized_items:
        raise RuntimeError("no valid items to register.")

    sheet_name = _build_sheet_name(date_str, from_channel)
    display_name = f"{sheet_name}_{supplier_name}"

    with sync_playwright() as p:
        channel = os.getenv("EZADMIN_CHROME_CHANNEL", "").strip()
        launch_args = [
            "--disable-crash-reporter",
            "--disable-features=Crashpad",
            "--no-crashpad",
            "--no-sandbox",
        ]
        tmp_home = tempfile.mkdtemp(prefix="ezadmin_chrome_home_")
        launch_env = os.environ.copy()
        launch_env["HOME"] = tmp_home
        if channel:
            browser = p.chromium.launch(headless=headless, channel=channel, args=launch_args, env=launch_env)
        else:
            browser = p.chromium.launch(headless=headless, args=launch_args, env=launch_env)
        page = browser.new_page()
        _login_ezadmin(page, domain=domain, username=username, password=password, login_url=login_url)

        page.goto(inbound_url, wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        # Create sheet (popup)
        new_btn = _find_in_frames(page, ["#new_sheet", "button#new_sheet", "button:has-text('전표생성')", "text=전표생성"])
        if not new_btn:
            raise RuntimeError("전표생성 버튼을 찾지 못했습니다.")
        with page.expect_popup() as popup_info:
            new_btn.first.click()
        create_popup = popup_info.value
        create_popup.wait_for_timeout(800)

        # Supplier select
        supplier_select = _find_in_frames(create_popup, ["select", "select[name*='cust']", "select[name*='supplier']"])
        if supplier_select:
            try:
                supplier_select.first.select_option(label=supplier_name)
            except Exception:
                # fallback: choose option containing supplier_name
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

        create_btn = _find_in_frames(create_popup, ["span:has-text('전표생성')", "button:has-text('전표생성')", "text=전표생성"])
        if not create_btn:
            raise RuntimeError("전표생성 확인 버튼을 찾지 못했습니다.")
        create_btn.first.click()
        create_popup.wait_for_timeout(800)
        try:
            create_popup.close()
        except Exception:
            pass

        # Find created sheet and open detail (popup)
        page.wait_for_timeout(1200)
        sheet_link = _find_in_frames(page, [f"a:has-text('{display_name}')", f"a:has-text('{sheet_name}')"])
        if not sheet_link:
            raise RuntimeError("생성된 전표 링크를 찾지 못했습니다.")
        with page.expect_popup() as detail_popup_info:
            sheet_link.first.click()
        detail_popup = detail_popup_info.value
        detail_popup.wait_for_timeout(800)

        # Open product add popup
        add_btn = _find_in_frames(detail_popup, ["span:has-text('상품추가')", "text=상품추가", "a:has-text('상품추가')"])
        if not add_btn:
            raise RuntimeError("상품추가 버튼을 찾지 못했습니다.")
        with detail_popup.expect_popup() as product_popup_info:
            add_btn.first.click()
        product_popup = product_popup_info.value
        product_popup.wait_for_timeout(800)

        # Search
        search_btn = _find_in_frames(product_popup, ["div#search", "div.table_search_button", "button:has-text('검색')", "text=검색"])
        if not search_btn:
            raise RuntimeError("검색 버튼을 찾지 못했습니다.")
        search_btn.first.click()
        product_popup.wait_for_timeout(1200)

        # Find table with headers
        table = product_popup.locator("table").first
        headers = [h.strip() for h in table.locator("th").all_text_contents()]
        if "상품명" not in headers or "입고수량" not in headers:
            # fallback: search all tables
            tables = product_popup.locator("table")
            found = False
            for i in range(tables.count()):
                t = tables.nth(i)
                hds = [h.strip() for h in t.locator("th").all_text_contents()]
                if "상품명" in hds and "입고수량" in hds:
                    table = t
                    headers = hds
                    found = True
                    break
            if not found:
                raise RuntimeError("상품명/입고수량 컬럼을 찾지 못했습니다.")

        name_idx = headers.index("상품명")
        qty_idx = headers.index("입고수량")
        body_rows = table.locator("tbody tr")
        for i in range(body_rows.count()):
            row = body_rows.nth(i)
            cells = row.locator("td")
            if cells.count() <= max(name_idx, qty_idx):
                continue
            name_text = cells.nth(name_idx).inner_text().strip()
            for item in normalized_items:
                if item["ez_name"] in name_text:
                    qty_cell = cells.nth(qty_idx)
                    inp = qty_cell.locator("input")
                    if inp.count() > 0:
                        inp.first.fill(str(item["quantity"]))

        # Click insert all
        insert_all = _find_in_frames(product_popup, ["a:has-text('전체추가')", "text=전체추가"])
        if not insert_all:
            raise RuntimeError("전체추가 버튼을 찾지 못했습니다.")
        insert_all.first.click()
        product_popup.wait_for_timeout(800)

        return {"sheet_name": sheet_name, "display_name": display_name}


if __name__ == "__main__":
    # Manual test: reads env for a single SKU
    sku = os.getenv("EZADMIN_TEST_SKU", "노트프로 블랙")
    qty = int(os.getenv("EZADMIN_TEST_QTY", "1"))
    date_str = os.getenv("EZADMIN_TEST_DATE", datetime.now().strftime("%Y-%m-%d"))
    from_channel = os.getenv("EZADMIN_TEST_FROM", "신규")
    headless = os.getenv("EZADMIN_HEADLESS", "1") != "0"
    resp = create_inbound_request(
        items=[{"sku_name": sku, "quantity": qty}],
        date_str=date_str,
        from_channel=from_channel,
        headless=headless,
    )
    print(resp)
