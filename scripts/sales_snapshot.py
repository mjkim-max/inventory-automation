from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
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


def _smartstore_today_range_kst() -> tuple[str, str]:
    if ZoneInfo:
        now = datetime.now(ZoneInfo("Asia/Seoul"))
    else:
        now = datetime.now()
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


def _smartstore_fetch_product_orders(token: str) -> List[Dict[str, Any]]:
    base = "https://api.commerce.naver.com/external"
    url = f"{base}/v1/pay-order/seller/product-orders/last-changed-statuses"
    last_from, last_to = _smartstore_today_range_kst()
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "lastChangedFrom": last_from,
        "lastChangedTo": last_to,
        "limitCount": 50,
    }
    product_order_ids: List[str] = []
    more_from = None
    more_sequence = None
    max_pages = 10
    while True:
        if max_pages <= 0:
            break
        if more_from:
            params["lastChangedFrom"] = more_from
        if more_sequence:
            params["moreSequence"] = more_sequence
        # Retry on rate limit with capped backoff
        resp = None
        for attempt in range(1, 4):
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else (2 * attempt)
                wait = min(wait, 8)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        if resp is None or resp.status_code == 429:
            raise RuntimeError("Smartstore rate limited (429). Try again later.")
        data = resp.json()
        items = data.get("data") or data.get("productOrders") or []
        if isinstance(items, dict):
            items = items.get("productOrderIds") or items.get("items") or []
        for item in items:
            if isinstance(item, str):
                product_order_ids.append(item)
                continue
            if not isinstance(item, dict):
                continue
            pid = item.get("productOrderId") or item.get("product_order_id")
            if pid:
                product_order_ids.append(str(pid))
        more = data.get("more") or {}
        more_from = more.get("moreFrom")
        more_sequence = more.get("moreSequence")
        if not more_from or not more_sequence:
            break
        max_pages -= 1
        time.sleep(0.3)

    if not product_order_ids:
        return []

    query_url = f"{base}/v1/pay-order/seller/product-orders/query"
    payload = {"productOrderIds": product_order_ids}
    resp = requests.post(query_url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    orders = data.get("data") or data.get("productOrders") or []
    if isinstance(orders, dict):
        orders = orders.get("productOrders") or orders.get("items") or []
    if isinstance(orders, str):
        return []
    # Save debug payload
    try:
        debug_dir = Path(__file__).resolve().parents[1] / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / "smartstore_orders.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass
    return orders or []


def _smartstore_sales_by_variant(
    product_orders: List[Dict[str, Any]],
) -> tuple[int, Dict[str, int]]:
    # Map Smartstore product/item ids to labels
    smart_map = {
        "11380104480": "플라우드 노트핀S",
        "56234258616": "플라우드 노트핀S / 블랙",
        "56234258618": "플라우드 노트핀S / 실버",
        "12696749368": "플라우드 노트 Pro",
        "55736008596": "플라우드 노트 Pro / 블랙",
        "53769211633": "플라우드 노트 Pro / 실버",
        "10195303069": "플라우드 노트",
        "48485810018": "플라우드 노트 / 블랙",
        "48485810022": "플라우드 노트 / 실버",
    }
    result: Dict[str, int] = {v: 0 for v in smart_map.values()}
    total_qty = 0
    include_status = {"PAYED", "DELIVERING", "DELIVERED", "PURCHASE_DECIDED"}

    for order in product_orders:
        if not isinstance(order, dict):
            continue
        status = str(order.get("productOrderStatus", "")).upper()
        if status and status not in include_status:
            continue
        claim = str(order.get("claimStatus", "")).upper()
        if claim and claim not in {"NONE", "NA"}:
            continue
        qty = 0
        try:
            qty = int(order.get("quantity", 0))
        except Exception:
            qty = 0
        total_qty += qty

        # Try to match on multiple fields
        candidates = [
            str(order.get("productId", "")).strip(),
            str(order.get("itemNo", "")).strip(),
            str(order.get("optionCode", "")).strip(),
            str(order.get("optionManageCode", "")).strip(),
            str(order.get("sellerProductItemId", "")).strip(),
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
            orders = _smartstore_fetch_product_orders(token)
            smart_qty, smart_items = _smartstore_sales_by_variant(orders)
            payload["smartstore_sales_qty"] = smart_qty
            payload["smartstore_items"] = smart_items
        except Exception as e:
            print(f"[WARN] 스마트스토어 매출 조회 실패: {e}")
    else:
        print("[WARN] 스마트스토어 인증정보 없음 (smartstore.client_id / client_secret)")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
