from __future__ import annotations

import base64
import getpass
import json
import os
import platform
import uuid
from pathlib import Path
from typing import Tuple

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _cred_path() -> Path:
    p = _workspace_root() / "data" / "coupang_login.enc.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _machine_secret() -> bytes:
    # Stable per-machine material + optional override pepper
    parts = [
        platform.node(),
        getpass.getuser(),
        str(uuid.getnode()),
        str(os.getuid() if hasattr(os, "getuid") else 0),
        str(Path.home()),
        os.getenv("COUPANG_LOGIN_PEPPER", ""),
    ]
    return "|".join(parts).encode("utf-8")


def _derive_key(salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=390000,
    )
    key = kdf.derive(_machine_secret())
    return base64.urlsafe_b64encode(key)


def save_encrypted_credentials(login_id: str, login_pw: str) -> Path:
    if not login_id or not login_pw:
        raise RuntimeError("쿠팡 로그인 ID/PW가 비어 있습니다.")
    salt = os.urandom(16)
    key = _derive_key(salt)
    token = Fernet(key).encrypt(
        json.dumps({"id": login_id, "pw": login_pw}, ensure_ascii=False).encode("utf-8")
    )
    payload = {
        "v": 1,
        "salt_b64": base64.b64encode(salt).decode("ascii"),
        "token": token.decode("ascii"),
    }
    path = _cred_path()
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass
    return path


def load_encrypted_credentials() -> Tuple[str, str]:
    path = _cred_path()
    if not path.exists():
        raise RuntimeError(f"쿠팡 암호화 로그인 파일 없음: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    salt = base64.b64decode(payload["salt_b64"])
    token = str(payload["token"]).encode("ascii")
    key = _derive_key(salt)
    plain = Fernet(key).decrypt(token)
    data = json.loads(plain.decode("utf-8"))
    login_id = str(data.get("id", "")).strip()
    login_pw = str(data.get("pw", "")).strip()
    if not login_id or not login_pw:
        raise RuntimeError("암호화 로그인 파일에 ID/PW가 없습니다.")
    return login_id, login_pw


def ensure_credentials_available() -> Tuple[str, str]:
    env_id = os.getenv("COUPANG_LOGIN_ID", "").strip()
    env_pw = os.getenv("COUPANG_LOGIN_PW", "").strip()
    if env_id and env_pw:
        # Always refresh encrypted file from env when explicitly provided.
        save_encrypted_credentials(env_id, env_pw)
        return env_id, env_pw
    return load_encrypted_credentials()


def is_login_page(page) -> bool:
    try:
        cur = page.url or ""
    except Exception:
        cur = ""
    if "xauth.coupang.com" in cur or "wing.coupang.com/login" in cur:
        return True
    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    return ("판매자 로그인" in body) or ("판매자가 아니신가요?" in body)


def submit_login(page, login_id: str, login_pw: str) -> None:
    user = page.locator("#username")
    pw = page.locator("#password")
    if user.count() == 0 or pw.count() == 0:
        raise RuntimeError("쿠팡 로그인 입력 필드를 찾지 못했습니다.")
    user.first.click()
    user.first.fill(login_id)
    pw.first.click()
    pw.first.fill(login_pw)
    btn = page.locator("#kc-login")
    if btn.count() == 0:
        raise RuntimeError("쿠팡 로그인 버튼(#kc-login)을 찾지 못했습니다.")
    btn.first.click()


def ensure_logged_in(page, *, target_url: str, timeout_sec: int = 90) -> None:
    if not is_login_page(page):
        return
    login_id, login_pw = ensure_credentials_available()
    submit_login(page, login_id, login_pw)
    for _ in range(timeout_sec * 2):
        page.wait_for_timeout(500)
        if not is_login_page(page):
            break
    else:
        raise RuntimeError("쿠팡 자동 로그인 실패(타임아웃).")

    # 로그인 후 대상 페이지로 재진입해서 세션 적용 확인
    page.goto(target_url, wait_until="domcontentloaded", timeout=90000)
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    if is_login_page(page):
        raise RuntimeError("쿠팡 자동 로그인 후에도 로그인 페이지로 되돌아왔습니다.")

