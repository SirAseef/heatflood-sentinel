"""Minimal JWT (HS256) auth with roles viewer < analyst < admin (FR-API-02). Stdlib only."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

ROLES = {"viewer": 0, "analyst": 1, "admin": 2}
SECRET = os.environ.get("JWT_SECRET", "dev-only-change-me")
# demo users; passwords from env in any shared deployment (ADMIN_BOOTSTRAP_PASSWORD)
USERS = {"viewer": ("viewer", os.environ.get("VIEWER_PASSWORD", "viewer")),
         "analyst": ("analyst", os.environ.get("ANALYST_PASSWORD", "analyst")),
         "admin": ("admin", os.environ.get("ADMIN_BOOTSTRAP_PASSWORD", "admin"))}


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def issue(sub: str, role: str, ttl_s: int = 900) -> str:
    head = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64(json.dumps({"sub": sub, "role": role, "exp": int(time.time()) + ttl_s}).encode())
    sig = _b64(hmac.new(SECRET.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
    return f"{head}.{body}.{sig}"


def verify(token: str) -> dict:
    try:
        head, body, sig = token.split(".")
    except ValueError as exc:
        raise HTTPException(401, "malformed token") from exc
    exp_sig = _b64(hmac.new(SECRET.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, exp_sig):
        raise HTTPException(401, "bad signature")
    claims = json.loads(_unb64(body))
    if claims["exp"] < time.time():
        raise HTTPException(401, "token expired")
    return claims


bearer = HTTPBearer(auto_error=False)


def require(role: str):
    def dep(cred: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict:
        if cred is None:
            raise HTTPException(401, "authentication required")
        claims = verify(cred.credentials)
        if ROLES[claims["role"]] < ROLES[role]:
            raise HTTPException(403, f"requires role {role}")
        return claims
    return dep
