from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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


INBOUND_LIST_URL_DEFAULT = "https://wing.coupang.com/tenants/rfm-inbound/inbound/list"

# Internal SKU label -> Coupang option id
SKU_TO_OPTION_ID = {
    "노트프로 블랙": "94199205555",
    "노트프로 실버": "94199205552",
    "노트 블랙": "90737907302",
    "노트 실버": "90737907295",
    "노트핀S 블랙": "91942294087",
    "노트핀S 실버": "91942294096",
    "플라우드 노트 Pro / 블랙": "94199205555",
    "플라우드 노트 Pro / 실버": "94199205552",
    "플라우드 노트 / 블랙": "90737907302",
    "플라우드 노트 / 실버": "90737907295",
    "플라우드 노트핀S / 블랙": "91942294087",
    "플라우드 노트핀S / 실버": "91942294096",
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


def _debug_dump(page, label: str) -> None:
    try:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if page and not page.is_closed():
            page.screenshot(path=str(debug_dir / f"coupang_inbound_{label}_{ts}.png"), full_page=True)
            (debug_dir / f"coupang_inbound_{label}_{ts}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass


def _build_request_name(date_str: str, from_channel: str) -> str:
    try:
        ts = datetime.now().strftime("%Y%m%d%H%M")
    except Exception:
        ts = date_str.replace("-", "")
    return f"{ts}_{from_channel}_쿠팡_입고"


def _normalize_items(items: List[Dict[str, Any]]) -> Dict[str, int]:
    option_qty: Dict[str, int] = {}
    for item in items:
        qty_raw = item.get("quantity", 0)
        try:
            qty = int(qty_raw or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue

        option_id = str(item.get("option_id", "")).strip()
        if not option_id:
            sku_name = str(item.get("sku_name", "")).strip()
            option_id = SKU_TO_OPTION_ID.get(sku_name, "")
        if not option_id:
            continue
        option_qty[option_id] = option_qty.get(option_id, 0) + qty
    return option_qty


def create_coupang_inbound_request(
    *,
    items: List[Dict[str, Any]],
    date_str: str,
    from_channel: str,
    headless: bool = False,
) -> Dict[str, Any]:
    cfg = _load_secrets()
    inbound_url = _get_cfg_value(
        cfg,
        "coupang",
        "inbound_url",
        env="COUPANG_INBOUND_URL",
        default=INBOUND_LIST_URL_DEFAULT,
    )
    profile_dir = _get_cfg_value(
        cfg,
        "coupang",
        "growth_profile_dir",
        env="COUPANG_GROWTH_PROFILE_DIR",
        default="/Users/mune/Desktop/Cursor/sales_check_auto/profile",
    )
    profile_name = os.getenv("COUPANG_GROWTH_PROFILE_NAME", "Profile 1").strip()

    option_qty = _normalize_items(items)
    if not option_qty:
        raise RuntimeError("쿠팡 옵션ID 매핑 가능한 아이템이 없습니다.")

    request_name = _build_request_name(date_str, from_channel)
    page = None
    with sync_playwright() as p:
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ]
        if profile_name:
            launch_args.insert(0, f"--profile-directory={profile_name}")
        context = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            channel="chrome",
            headless=headless,
            args=launch_args,
        )
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(inbound_url, wait_until="domcontentloaded", timeout=90000)
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass

            if "xauth.coupang.com" in page.url or "wing.coupang.com/login" in page.url:
                raise RuntimeError(
                    "쿠팡 Wing 로그인 세션이 없습니다. scripts/coupang_growth_login.py 로 로그인 후 재시도하세요."
                )
            body = page.inner_text("body")
            if "판매자 로그인" in body or "판매자가 아니신가요?" in body:
                raise RuntimeError(
                    "쿠팡 Wing 로그인 세션이 없습니다. scripts/coupang_growth_login.py 로 로그인 후 재시도하세요."
                )

            page.get_by_role("button", name="새로운 입고 생성").first.click(timeout=15000)
            page.wait_for_timeout(1200)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            # Step 1: product selection by option id
            try:
                page.get_by_role("button", name="전체 옵션보기").first.click(timeout=5000)
                page.wait_for_timeout(300)
            except Exception:
                pass
            selected = page.evaluate(
                """
                (optionQtyMap) => {
                  const selectedIds = [];
                  const missingIds = [];
                  const ids = Object.keys(optionQtyMap || {});
                  const clickCheckbox = (box) => {
                    try {
                      if (!box.checked) box.click();
                      if (!box.checked) {
                        box.checked = true;
                        box.dispatchEvent(new Event('input', { bubbles: true }));
                        box.dispatchEvent(new Event('change', { bubbles: true }));
                      }
                    } catch (e) {
                      return false;
                    }
                    return !!box.checked;
                  };

                  for (const optionId of ids) {
                    const targets = Array.from(document.querySelectorAll('tr,div')).filter((el) => {
                      const t = (el.textContent || '').replace(/\\s+/g, ' ');
                      return t.includes(optionId);
                    });
                    let ok = false;
                    for (const t of targets) {
                      const row = t.closest('tr') || t.closest('[role=row]') || t;
                      const checkbox = row.querySelector('input[type=checkbox]');
                      if (!checkbox) continue;
                      if (clickCheckbox(checkbox)) {
                        selectedIds.push(optionId);
                        ok = true;
                        break;
                      }
                    }
                    if (!ok) missingIds.push(optionId);
                  }
                  return { selectedIds, missingIds };
                }
                """,
                option_qty,
            )
            missing = selected.get("missingIds") if isinstance(selected, dict) else None
            if missing:
                raise RuntimeError(f"상품 선택 실패(옵션ID 미발견): {', '.join(map(str, missing))}")

            next_btn = page.locator("button.next:has-text('다음')").first
            if next_btn.count() == 0:
                next_btn = page.get_by_role("button", name="다음").first
            next_btn.click(timeout=15000)
            page.wait_for_timeout(1000)

            # Step 2: inbound info
            try:
                page.locator("#shipping-classification-domestic").first.check(timeout=10000)
            except Exception:
                page.get_by_text("국내배송", exact=False).first.click(timeout=10000)
            page.wait_for_timeout(300)

            filled = page.evaluate(
                """
                (optionQtyMap) => {
                  const filledIds = [];
                  const missingIds = [];
                  const fillInput = (input, qty) => {
                    input.focus();
                    input.value = '';
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.value = String(qty);
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    input.blur();
                  };

                  for (const [optionId, qty] of Object.entries(optionQtyMap || {})) {
                    const targets = Array.from(document.querySelectorAll('tr,div')).filter((el) => {
                      const t = (el.textContent || '').replace(/\\s+/g, ' ');
                      return t.includes(optionId);
                    });
                    let ok = false;
                    for (const t of targets) {
                      const row = t.closest('tr') || t.closest('[role=row]') || t;
                      const input = row.querySelector('input[type=text], input[type=number]');
                      if (!input) continue;
                      fillInput(input, qty);
                      filledIds.push(optionId);
                      ok = true;
                      break;
                    }
                    if (!ok) missingIds.push(optionId);
                  }
                  return { filledIds, missingIds };
                }
                """,
                option_qty,
            )
            missing_qty = filled.get("missingIds") if isinstance(filled, dict) else None
            if missing_qty:
                raise RuntimeError(f"수량 입력 실패(옵션ID 미발견): {', '.join(map(str, missing_qty))}")

            next_btn2 = page.locator("button.next:has-text('다음')").first
            if next_btn2.count() == 0:
                next_btn2 = page.get_by_role("button", name="다음").first
            next_btn2.click(timeout=15000)
            page.wait_for_timeout(1000)

            return {
                "request_name": request_name,
                "selected_option_ids": sorted(option_qty.keys()),
                "filled_option_qty": option_qty,
            }
        except Exception:
            _debug_dump(page, "error")
            raise
        finally:
            context.close()


if __name__ == "__main__":
    sku = os.getenv("COUPANG_TEST_SKU", "노트프로 블랙")
    qty = int(os.getenv("COUPANG_TEST_QTY", "1"))
    date_str = os.getenv("COUPANG_TEST_DATE", datetime.now().strftime("%Y-%m-%d"))
    from_channel = os.getenv("COUPANG_TEST_FROM", "신규")
    headless = os.getenv("COUPANG_HEADLESS", "0") == "1"
    resp = create_coupang_inbound_request(
        items=[{"sku_name": sku, "quantity": qty}],
        date_str=date_str,
        from_channel=from_channel,
        headless=headless,
    )
    print(resp)
