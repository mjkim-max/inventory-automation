from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

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

from playwright.sync_api import sync_playwright

try:
    from coupang_auth import ensure_logged_in as coupang_ensure_logged_in
except Exception:  # pragma: no cover
    coupang_ensure_logged_in = None  # type: ignore[assignment]


INBOUND_LIST_URL_DEFAULT = "https://wing.coupang.com/tenants/rfm-inbound/inbound/list"
INBOUND_DETAIL_PATH = "/tenants/rfm-inbound/lightning/summary?id={inbound_id}"

OPTION_ID_TO_SKU = {
    "94199205555": "노트프로 블랙",
    "94199205552": "노트프로 실버",
    "90737907302": "노트 블랙",
    "90737907295": "노트 실버",
    "91942294087": "노트핀S 블랙",
    "91942294096": "노트핀S 실버",
}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def _get_cfg_value(cfg: Dict[str, Any], *keys: str, env: str = "", default: str = "") -> str:
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


def _connect_add_inventory_sheet(cfg: Dict[str, Any]):
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
        ws = ss.worksheet("Add_inventory")
    except Exception:
        ws = ss.add_worksheet(title="Add_inventory", rows=2000, cols=10)
    return ws


def _ensure_add_inventory_header(ws) -> None:
    header = ["date", "from_channel", "channel", "sku_name", "quantity"]
    values = ws.get_all_values()
    if not values:
        ws.append_row(header)
        return
    if values[0][: len(header)] != header:
        ws.update("A1", [header])


def _db_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    path = root / "data" / "coupang_inbound_archive.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path()))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inbound_archive (
          inbound_id TEXT PRIMARY KEY,
          detail_url TEXT,
          expected_date TEXT,
          status TEXT NOT NULL DEFAULT 'NEW',
          error TEXT,
          first_seen_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          synced_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inbound_items (
          inbound_id TEXT NOT NULL,
          option_id TEXT NOT NULL,
          sku_name TEXT,
          quantity_created INTEGER NOT NULL,
          PRIMARY KEY (inbound_id, option_id),
          FOREIGN KEY(inbound_id) REFERENCES inbound_archive(inbound_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()
    return conn


def _upsert_discovered_ids(conn: sqlite3.Connection, records: List[Tuple[str, str]]) -> None:
    now = _now_str()
    for inbound_id, detail_url in records:
        conn.execute(
            """
            INSERT INTO inbound_archive (inbound_id, detail_url, first_seen_at, updated_at, status)
            VALUES (?, ?, ?, ?, 'NEW')
            ON CONFLICT(inbound_id) DO UPDATE SET
              detail_url=excluded.detail_url,
              updated_at=excluded.updated_at
            """,
            (inbound_id, detail_url, now, now),
        )
    conn.commit()


def _select_targets(conn: sqlite3.Connection, limit: int) -> List[Tuple[str, str]]:
    cur = conn.execute(
        """
        SELECT inbound_id, detail_url
        FROM inbound_archive
        WHERE status IN ('NEW', 'FAILED')
        ORDER BY first_seen_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [(str(r[0]), str(r[1] or "")) for r in cur.fetchall()]


def _mark_failed(conn: sqlite3.Connection, inbound_id: str, error: str) -> None:
    conn.execute(
        """
        UPDATE inbound_archive
        SET status='FAILED', error=?, updated_at=?
        WHERE inbound_id=?
        """,
        (error[:500], _now_str(), inbound_id),
    )
    conn.commit()


def _mark_synced(conn: sqlite3.Connection, inbound_id: str, expected_date: str) -> None:
    now = _now_str()
    conn.execute(
        """
        UPDATE inbound_archive
        SET expected_date=?, status='SYNCED', error=NULL, synced_at=?, updated_at=?
        WHERE inbound_id=?
        """,
        (expected_date, now, now, inbound_id),
    )
    conn.commit()


def _save_items(conn: sqlite3.Connection, inbound_id: str, items: Dict[str, int]) -> None:
    for option_id, qty in items.items():
        sku_name = OPTION_ID_TO_SKU.get(option_id, "")
        conn.execute(
            """
            INSERT INTO inbound_items (inbound_id, option_id, sku_name, quantity_created)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(inbound_id, option_id) DO UPDATE SET
              sku_name=excluded.sku_name,
              quantity_created=excluded.quantity_created
            """,
            (inbound_id, option_id, sku_name, int(qty)),
        )
    conn.commit()


def _collect_inbound_links(page) -> List[Tuple[str, str]]:
    def _upsert(target: Dict[str, str], inbound_id: str, href: str) -> None:
        inb = inbound_id.strip()
        if not inb:
            return
        target.setdefault(inb, href.strip() or INBOUND_DETAIL_PATH.format(inbound_id=inb))

    result_map: Dict[str, str] = {}

    # 1) Prefer explicit summary links if present.
    data = page.evaluate(
        """
        () => {
          const rows = [];
          const links = Array.from(document.querySelectorAll("a[href*='summary?id=']"));
          for (const a of links) {
            const href = a.getAttribute("href") || "";
            const m = href.match(/id=(\\d+)/);
            if (!m) continue;
            rows.push([m[1], href]);
          }
          return rows;
        }
        """
    )
    for row in data or []:
        if isinstance(row, list) and len(row) >= 2:
            _upsert(result_map, str(row[0]), str(row[1]))

    # 2) Fallback: parse from rendered HTML for route fragments.
    try:
        html = page.content()
    except Exception:
        html = ""
    for m in re.finditer(r"/tenants/rfm-inbound/lightning/summary\\?id=(\\d+)", html):
        inbound_id = m.group(1)
        _upsert(result_map, inbound_id, INBOUND_DETAIL_PATH.format(inbound_id=inbound_id))

    # 3) Fallback: extract '입고 ID' cards from visible text.
    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    for m in re.finditer(r"입고\\s*ID\\s*([0-9]{10,})", body):
        inbound_id = m.group(1)
        _upsert(result_map, inbound_id, INBOUND_DETAIL_PATH.format(inbound_id=inbound_id))

    return [(inbound_id, href) for inbound_id, href in result_map.items()]


def _wait_until_inbound_list_visible(page, timeout_ms: int = 30000) -> None:
    elapsed = 0
    step_ms = 500
    while elapsed < timeout_ms:
        try:
            body = page.inner_text("body")
            if "입고 관리" in body and "입고 ID" in body:
                return
        except Exception:
            pass
        page.wait_for_timeout(step_ms)
        elapsed += step_ms


def _parse_detail(body: str, qty_field: str) -> Tuple[str, str, Dict[str, int]]:
    inbound_id_match = re.search(r"입고 ID\s*([0-9]{10,})", body)
    inbound_id = inbound_id_match.group(1) if inbound_id_match else ""

    eta_match = re.search(r"물류센터 도착예정일\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", body)
    expected_date = eta_match.group(1) if eta_match else datetime.now().strftime("%Y-%m-%d")
    # Prefer 판매개시 일자를 재고 반영 기준일로 사용
    sale_match = re.search(
        r"판매개시\s*([0-9]{4}-[0-9]{2}-[0-9]{2})(?:\s*([0-9]{2}:[0-9]{2}(?::[0-9]{2})?))?",
        body,
    )
    inventory_date = sale_match.group(1) if sale_match else expected_date

    option_qty: Dict[str, int] = {}
    qty_label = re.escape(qty_field)
    pattern = rf"옵션ID:\s*(\d+)[\s\S]{{0,240}}?{qty_label}\s*([0-9][0-9,]*)"
    for m in re.finditer(pattern, body):
        option_id = m.group(1)
        qty_str = m.group(2)
        try:
            qty = int(qty_str.replace(",", ""))
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        prev = option_qty.get(option_id, 0)
        option_qty[option_id] = max(prev, qty)

    return inbound_id, inventory_date, option_qty


def _should_retry_non_headless(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "Access Denied" in msg
        or "로그인 입력 필드를 찾지 못했습니다." in msg
        or "자동 로그인 후에도 로그인 페이지로 되돌아왔습니다." in msg
    )


def _sync_coupang_inbound_with_browser(
    *,
    cfg: Dict[str, Any],
    conn: sqlite3.Connection,
    ws,
    inbound_list_url: str,
    profile_dir: str,
    profile_name: str,
    max_process: int,
    qty_field: str,
    headless: bool,
) -> Dict[str, Any]:
    inserted_rows = 0
    synced_ids = 0
    failed_ids: List[str] = []
    discovered_count = 0

    with sync_playwright() as p:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ]
        if profile_name:
            args.insert(0, f"--profile-directory={profile_name}")
        context = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            channel="chrome",
            headless=headless,
            args=args,
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()

            page.goto(inbound_list_url, wait_until="domcontentloaded", timeout=90000)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            if coupang_ensure_logged_in is None:
                raise RuntimeError("coupang_auth 모듈 로드 실패")
            coupang_ensure_logged_in(page, target_url=inbound_list_url, timeout_sec=90)

            page.goto(inbound_list_url, wait_until="domcontentloaded", timeout=90000)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            _wait_until_inbound_list_visible(page, timeout_ms=30000)

            links = _collect_inbound_links(page)
            discovered_count = len(links)
            if not links:
                return {
                    "discovered": 0,
                    "processed": 0,
                    "inserted_rows": 0,
                    "failed_ids": [],
                }

            _upsert_discovered_ids(conn, links)
            targets = _select_targets(conn, max_process)

            base = "https://wing.coupang.com"
            for inbound_id, detail_href in targets:
                detail_url = detail_href if detail_href.startswith("http") else f"{base}{detail_href}"
                try:
                    page.goto(detail_url, wait_until="domcontentloaded", timeout=90000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    body = page.inner_text("body")

                    parsed_id, expected_date, option_qty = _parse_detail(body, qty_field=qty_field)
                    if not parsed_id:
                        parsed_id = inbound_id
                    if not option_qty:
                        raise RuntimeError(f"옵션별 '{qty_field}' 수량을 찾지 못했습니다.")

                    _save_items(conn, parsed_id, option_qty)

                    rows_to_append: List[List[Any]] = []
                    for option_id, qty in option_qty.items():
                        sku_name = OPTION_ID_TO_SKU.get(option_id, "")
                        if not sku_name:
                            continue
                        rows_to_append.append([expected_date, "신규", "쿠팡", sku_name, int(qty)])
                    if not rows_to_append:
                        raise RuntimeError("시트 반영 가능한 옵션ID 매핑이 없습니다.")

                    for r in rows_to_append:
                        ws.append_row(r, value_input_option="USER_ENTERED")
                        inserted_rows += 1

                    _mark_synced(conn, parsed_id, expected_date)
                    synced_ids += 1
                except Exception as e:
                    _mark_failed(conn, inbound_id, str(e))
                    failed_ids.append(inbound_id)
        finally:
            context.close()

    return {
        "discovered": discovered_count,
        "processed": synced_ids + len(failed_ids),
        "synced_ids": synced_ids,
        "inserted_rows": inserted_rows,
        "failed_ids": failed_ids,
    }


def sync_coupang_inbound_to_sheet() -> Dict[str, Any]:
    cfg = _load_secrets()
    inbound_list_url = _get_cfg_value(
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
    try:
        max_process = max(1, int(os.getenv("COUPANG_INBOUND_MAX_PROCESS", "10")))
    except Exception:
        max_process = 10
    qty_field = os.getenv("COUPANG_INBOUND_QTY_FIELD", "판매개시").strip()
    if qty_field not in {"판매개시", "입고생성"}:
        qty_field = "판매개시"
    headless = os.getenv("COUPANG_INBOUND_HEADLESS", "0") == "1"

    conn = _open_db()
    ws = _connect_add_inventory_sheet(cfg)
    _ensure_add_inventory_header(ws)

    try:
        return _sync_coupang_inbound_with_browser(
            cfg=cfg,
            conn=conn,
            ws=ws,
            inbound_list_url=inbound_list_url,
            profile_dir=profile_dir,
            profile_name=profile_name,
            max_process=max_process,
            qty_field=qty_field,
            headless=headless,
        )
    except Exception as exc:
        if headless and _should_retry_non_headless(exc):
            print(f"[WARN] headless inbound sync failed, retrying with visible browser: {exc}")
            return _sync_coupang_inbound_with_browser(
                cfg=cfg,
                conn=conn,
                ws=ws,
                inbound_list_url=inbound_list_url,
                profile_dir=profile_dir,
                profile_name=profile_name,
                max_process=max_process,
                qty_field=qty_field,
                headless=False,
            )
        raise


def main() -> None:
    result = sync_coupang_inbound_to_sheet()
    print(result)


if __name__ == "__main__":
    main()
