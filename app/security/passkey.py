from __future__ import annotations

from fastapi import HTTPException, Request

from app.config import settings


COOKIE_NAME = "admin_passkey"
HEADER_NAME = "X-Admin-Passkey"


def passkey_enabled() -> bool:
    key = getattr(settings, "ADMIN_PASSKEY", None)
    return bool(key and str(key).strip())


def is_authorized(request: Request) -> bool:
    if not passkey_enabled():
        return True

    expected = str(settings.ADMIN_PASSKEY)
    provided = request.headers.get(HEADER_NAME) or request.cookies.get(COOKIE_NAME)
    return bool(provided) and provided == expected


def require_authorized(request: Request) -> None:
    if not is_authorized(request):
        raise HTTPException(status_code=401, detail="Passkey required")
