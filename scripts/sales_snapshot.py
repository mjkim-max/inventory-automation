from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
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

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


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


def _today_range_kst() -> tuple[str, str]:
    if ZoneInfo:
        now = datetime.now(ZoneInfo("Asia/Seoul"))
    else:
        now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def _fetch_make_token(make_webhook: str) -> str:
    resp = requests.post(make_webhook, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token") or data.get("token") or ""
    if not token:
        raise RuntimeError("Make webhook response missing access_token")
    return token


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


def _coupang_auth(access_key: str, secret_key: str, method: str, path: str, query: str) -> str:
    import time, hmac, hashlib
    signed_date = time.strftime("%y%m%dT%H%M%SZ", time.gmtime())
    message = f"{signed_date}{method}{path}{query}"
    signature = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"CEA algorithm=HmacSHA256, access-key={access_key}, signed-date={signed_date}, signature={signature}"


def _coupang_sales_qty(vendor_id: str, access_key: str, secret_key: str) -> int:
    # Use PO list query (by Minute) with status=ACCEPT (payment completed)
    host = "https://api-gateway.coupang.com"
    path = f"/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets"
    if ZoneInfo:
        now = datetime.now(ZoneInfo("Asia/Seoul"))
    else:
        now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    created_from = start.strftime("%Y-%m-%dT%H:%M") + "%2B09:00"
    created_to = end.strftime("%Y-%m-%dT%H:%M") + "%2B09:00"
    total_qty = 0
    last_response = None
    query = (
        f"createdAtFrom={created_from}&createdAtTo={created_to}"
        f"&searchType=timeFrame&status=ACCEPT"
    )
    auth = _coupang_auth(access_key, secret_key, "GET", path, query)
    headers = {"Authorization": auth, "X-Requested-By": vendor_id}
    url = f"{host}{path}?{query}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    last_response = data
    orders = data.get("data") or []
    for order in orders:
        items = order.get("orderItems") or order.get("items") or []
        for item in items:
            try:
                q = int(item.get("quantity", 0))
            except Exception:
                q = 0
            total_qty += q
    if total_qty == 0:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "coupang_ordersheets.json").write_text(
            json.dumps(last_response or {}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return total_qty


def main() -> None:
    cfg = _load_secrets()
    make_webhook = _get_cfg_value(cfg, "cafe24", "make_webhook", env="CAFE24_MAKE_WEBHOOK")
    cafe24_base = _get_cfg_value(cfg, "cafe24", "base_url", env="CAFE24_BASE_URL")
    coupang_vendor_id = _get_cfg_value(cfg, "coupang", "vendor_id", env="COUPANG_VENDOR_ID")
    coupang_access = _get_cfg_value(cfg, "coupang", "access_key", env="COUPANG_ACCESS_KEY")
    coupang_secret = _get_cfg_value(cfg, "coupang", "secret_key", env="COUPANG_SECRET_KEY")

    if not make_webhook or not cafe24_base:
        raise RuntimeError("cafe24.make_webhook / cafe24.base_url 설정이 필요합니다.")

    start_date, end_date = _today_range_kst()

    cafe24_token = _fetch_make_token(make_webhook)
    orders = _cafe24_orders(cafe24_base, cafe24_token, start_date, end_date)
    cafe24_qty = _cafe24_sales_qty(orders)

    coupang_qty = None
    if coupang_vendor_id and coupang_access and coupang_secret:
        coupang_qty = _coupang_sales_qty(coupang_vendor_id, coupang_access, coupang_secret)

    payload = {
        "date": start_date,
        "cafe24_sales_qty": cafe24_qty,
        "coupang_sales_qty": coupang_qty,
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
