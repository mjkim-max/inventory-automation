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


def _coupang_sales_qty(vendor_id: str, access_key: str, secret_key: str) -> tuple[int, Dict[str, int]]:
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
    item_map = {
        "94199205555": "플라우드 노트 Pro / 블랙",
        "94199205552": "플라우드 노트 Pro / 실버",
        "90737907302": "플라우드 노트 / 블랙",
        "90737907295": "플라우드 노트 / 실버",
        "91942294087": "플라우드 노트핀S / 블랙",
        "91942294096": "플라우드 노트핀S / 실버",
    }
    item_qty = {k: 0 for k in item_map.keys()}
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
            vendor_item_id = (
                item.get("vendorItemId")
                or item.get("sellerProductItemId")
                or item.get("vendor_item_id")
            )
            if vendor_item_id is not None:
                key = str(vendor_item_id).strip()
                if key in item_qty:
                    item_qty[key] += q
    if total_qty == 0:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "coupang_ordersheets.json").write_text(
            json.dumps(last_response or {}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return total_qty, item_qty


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
    cafe24_by_variant = _cafe24_sales_by_variant(orders)

    coupang_qty = None
    coupang_items: Dict[str, int] = {}
    if coupang_vendor_id and coupang_access and coupang_secret:
        try:
            coupang_qty, coupang_items = _coupang_sales_qty(
                coupang_vendor_id, coupang_access, coupang_secret
            )
        except Exception as e:
            # If Coupang is not accessible (e.g., IP whitelist), skip and continue
            print(f"[WARN] 쿠팡 매출 조회 실패: {e}")
            coupang_qty = None
            coupang_items = {}

    payload = {
        "date": start_date,
        "cafe24_sales_qty": cafe24_qty,
        "cafe24_items": cafe24_by_variant,
        "coupang_sales_qty": coupang_qty,
        "coupang_items": coupang_items,
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
