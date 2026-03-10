from __future__ import annotations

import os
import urllib.parse
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from playwright.sync_api import sync_playwright, Error as PlaywrightError
from coupang_auth import ensure_credentials_available, ensure_logged_in


def _today_kst() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def _with_today_range(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    today = _today_kst()
    q["start_date"] = [today]
    q["end_date"] = [today]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(q, doseq=True)))


def main() -> int:
    dashboard_url = os.getenv(
        "COUPANG_GROWTH_URL",
        "https://wing.coupang.com/tenants/business-insight/sales-analysis",
    ).strip()
    if not dashboard_url:
        raise RuntimeError("COUPANG_GROWTH_URL is required")
    profile_dir = os.getenv(
        "COUPANG_GROWTH_PROFILE_DIR",
        "/Users/mune/Desktop/Cursor/sales_check_auto/profile",
    ).strip()
    if not profile_dir:
        raise RuntimeError("COUPANG_GROWTH_PROFILE_DIR is required")

    profile_name = os.getenv("COUPANG_GROWTH_PROFILE_NAME", "Profile 1").strip()
    keep_open = os.getenv("COUPANG_LOGIN_KEEP_OPEN", "1").strip().lower() in {"1", "true", "yes", "y"}

    # If env creds are provided, they are encrypted and persisted locally.
    try:
        ensure_credentials_available()
    except Exception:
        pass

    live_url = _with_today_range(dashboard_url)
    Path(profile_dir).mkdir(parents=True, exist_ok=True)

    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
    ]
    if profile_name:
        launch_args.insert(0, f"--profile-directory={profile_name}")

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            channel="chrome",
            headless=False,
            args=launch_args,
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(live_url, wait_until="domcontentloaded", timeout=90000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        ensure_logged_in(page, target_url=live_url, timeout_sec=90)
        print("Coupang Growth login window opened.")
        print("1) Login if prompted")
        print("2) Verify sales-analysis page loads")
        print("3) Close the browser window to finish")
        if not keep_open:
            return 0
        try:
            page.wait_for_timeout(60 * 60 * 1000)
        except PlaywrightError:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
