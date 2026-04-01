import logging
from datetime import datetime, timedelta, timezone

import httpx

from app.config import settings
from app.database.mongo import db

logger = logging.getLogger(__name__)

ETSY_TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token"
TOKEN_COLLECTION = "etsy_oauth_tokens"
TOKEN_DOC_ID = "primary"


def _ensure_etsy_oauth_is_configured() -> None:
    if not settings.ETSY_CLIENT_ID or not settings.ETSY_CLIENT_SECRET:
        raise ValueError(
            "Etsy OAuth is not configured. Set ETSY_CLIENT_ID and ETSY_CLIENT_SECRET in .env"
        )


def _ensure_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def exchange_code_for_tokens(code: str) -> dict:
    """Exchange Etsy authorization code for access/refresh token and store them."""
    _ensure_etsy_oauth_is_configured()

    payload = {
        "grant_type": "authorization_code",
        "client_id": settings.ETSY_CLIENT_ID,
        "code": code,
    }

    if settings.ETSY_REDIRECT_URI:
        payload["redirect_uri"] = settings.ETSY_REDIRECT_URI
    if settings.ETSY_CODE_VERIFIER:
        payload["code_verifier"] = settings.ETSY_CODE_VERIFIER

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            ETSY_TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            auth=(settings.ETSY_CLIENT_ID, settings.ETSY_CLIENT_SECRET),
        )

    if response.status_code != 200:
        logger.error("Etsy token exchange failed (%s): %s", response.status_code, response.text)
        raise ValueError(f"Etsy token exchange failed: {response.text}")

    token_data = response.json()
    await _save_tokens(token_data)
    return token_data


async def get_token_status() -> dict:
    """Return current Etsy token health for /auth/etsy/status endpoint."""
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})

    if not doc:
        return {
            "status": "no_db_token",
            "message": "No Etsy token stored in database. Complete Etsy OAuth authorization.",
        }

    now = datetime.now(timezone.utc)
    expires_at = _ensure_aware(doc.get("expires_at"))

    if expires_at:
        is_expired = now >= expires_at
        seconds_left = int((expires_at - now).total_seconds()) if not is_expired else 0
        status = "expired" if is_expired else "valid"
    else:
        seconds_left = None
        status = "unknown"

    updated_at = _ensure_aware(doc.get("updated_at"))

    return {
        "status": status,
        "has_refresh_token": bool(doc.get("refresh_token")),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "seconds_until_expiry": seconds_left,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


async def _save_tokens(token_data: dict) -> None:
    now = datetime.now(timezone.utc)
    expires_in = token_data.get("expires_in", 3600)

    doc = {
        "_id": TOKEN_DOC_ID,
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "token_type": token_data.get("token_type", "Bearer"),
        "scope": token_data.get("scope"),
        "expires_in": expires_in,
        "expires_at": now + timedelta(seconds=expires_in),
        "updated_at": now,
    }

    await db[TOKEN_COLLECTION].replace_one({"_id": TOKEN_DOC_ID}, doc, upsert=True)
    logger.info("Etsy tokens saved to MongoDB (expires in %ds)", expires_in)
