import logging
import base64
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx

from app.config import settings
from app.database.mongo import db

logger = logging.getLogger(__name__)

ETSY_TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token"
ETSY_AUTH_URL = "https://www.etsy.com/oauth/connect"
TOKEN_COLLECTION = "etsy_oauth_tokens"
TOKEN_DOC_ID = "primary"


def _ensure_etsy_oauth_is_configured() -> None:
    if not settings.ETSY_CLIENT_ID or not settings.ETSY_CLIENT_SECRET:
        raise ValueError(
            "Etsy OAuth is not configured. Set ETSY_CLIENT_ID and ETSY_CLIENT_SECRET in .env"
        )


def _pkce_challenge_s256(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def get_authorization_url(state: str | None = None) -> str:
    """Build Etsy OAuth authorization URL for one-time app authorization."""
    _ensure_etsy_oauth_is_configured()

    if not settings.ETSY_REDIRECT_URI:
        raise ValueError("ETSY_REDIRECT_URI is required for Etsy OAuth login")

    scopes = str(settings.ETSY_SCOPES or "").strip()
    if not scopes:
        raise ValueError("ETSY_SCOPES is empty; configure at least one Etsy scope")

    oauth_state = state or secrets.token_urlsafe(16)
    params: dict[str, str] = {
        "response_type": "code",
        "redirect_uri": settings.ETSY_REDIRECT_URI,
        "scope": scopes,
        "client_id": settings.ETSY_CLIENT_ID,
        "state": oauth_state,
    }

    if settings.ETSY_CODE_VERIFIER:
        params["code_challenge_method"] = "S256"
        params["code_challenge"] = _pkce_challenge_s256(settings.ETSY_CODE_VERIFIER)

    return f"{ETSY_AUTH_URL}?{urlencode(params)}"


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
            data=payload,
        )

    if response.status_code != 200:
        logger.error("Etsy token exchange failed (%s): %s", response.status_code, response.text)
        raise ValueError(f"Etsy token exchange failed: {response.text}")

    token_data = response.json()
    await _save_tokens(token_data)
    return token_data


async def refresh_access_token() -> str:
    """Use the stored Etsy refresh token to obtain a new access token."""
    _ensure_etsy_oauth_is_configured()

    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})
    if not doc or not doc.get("refresh_token"):
        raise ValueError(
            "No Etsy refresh token in database. Re-authorize and include offline_access scope."
        )

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": doc["refresh_token"],
        "client_id": settings.ETSY_CLIENT_ID,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            ETSY_TOKEN_URL,
            data=payload,
        )

    if response.status_code != 200:
        logger.error("Etsy token refresh failed (%s): %s", response.status_code, response.text)
        raise ValueError(f"Etsy token refresh failed: {response.text}")

    token_data = response.json()

    # Etsy may omit refresh_token on refresh; keep the current one.
    if "refresh_token" not in token_data:
        token_data["refresh_token"] = doc["refresh_token"]

    await _save_tokens(token_data)
    logger.info("Etsy access token refreshed successfully")
    return token_data["access_token"]


async def get_valid_token(*, force_refresh: bool = False, allow_env_fallback: bool = True) -> str:
    """Return a valid Etsy access token, refreshing from Mongo-stored refresh token when needed."""
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})

    if doc and doc.get("access_token"):
        expires_at = _ensure_aware(doc.get("expires_at"))

        if not force_refresh:
            if expires_at and datetime.now(timezone.utc) < expires_at - timedelta(minutes=5):
                return doc["access_token"]

            # If expiry is unknown, continue using stored token.
            if not expires_at:
                return doc["access_token"]

            # No refresh token available yet; use token while it is still valid.
            if datetime.now(timezone.utc) < expires_at and not doc.get("refresh_token"):
                return doc["access_token"]

        if doc.get("refresh_token"):
            logger.info("Etsy access token expired or near expiry - refreshing automatically")
            return await refresh_access_token()

    if allow_env_fallback and settings.ETSY_TOKEN:
        return settings.ETSY_TOKEN

    raise ValueError(
        "No valid Etsy token available. Authorize Etsy and ensure a refresh token is stored."
    )


async def get_token_status() -> dict:
    """Return current Etsy token health for /auth/etsy/status endpoint."""
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})

    if not doc:
        has_env_fallback = bool(settings.ETSY_TOKEN)
        return {
            "status": "no_db_token",
            "message": "No Etsy token stored in database. Complete Etsy OAuth authorization.",
            "env_fallback_active": has_env_fallback,
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
    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError("Etsy token response missing access_token")

    existing = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})
    refresh_token = token_data.get("refresh_token") or ((existing or {}).get("refresh_token"))

    doc = {
        "_id": TOKEN_DOC_ID,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_data.get("token_type", "Bearer"),
        "scope": token_data.get("scope"),
        "expires_in": expires_in,
        "expires_at": now + timedelta(seconds=expires_in),
        "updated_at": now,
    }

    await db[TOKEN_COLLECTION].replace_one({"_id": TOKEN_DOC_ID}, doc, upsert=True)
    logger.info("Etsy tokens saved to MongoDB (expires in %ds)", expires_in)
