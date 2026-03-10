import base64
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx

from app.config import settings
from app.database.mongo import db

logger = logging.getLogger(__name__)

EBAY_AUTH_URL = "https://auth.ebay.com/oauth2/authorize"
EBAY_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"

# Scopes needed for Trading API (GetMyeBaySelling, GetItem).
# Keep this list aligned with the scopes actually granted to your
# eBay application in the developer portal; requesting scopes that
# are not enabled for the app will cause "invalid_scope" errors on
# token exchange/refresh.
#
# For this service we need the generic API scope. If your eBay
# application does not support "offline_access", requesting it will
# trigger "invalid_scope". Start with the base scope only; once
# confirmed working you can add "offline_access" back *only if* it is
# enabled for your app in the eBay developer portal.
EBAY_SCOPES = [
    "https://api.ebay.com/oauth/api_scope",
]

TOKEN_COLLECTION = "ebay_oauth_tokens"
TOKEN_DOC_ID = "primary"


def _ensure_aware(dt: datetime | None) -> datetime | None:
    """Ensure a datetime from MongoDB is timezone-aware (UTC).

    Older documents may have been stored as naive UTC datetimes; comparing these
    with timezone-aware values raises TypeError. MongoDB datetimes are always
    effectively UTC, so it's safe to attach timezone.utc when tzinfo is missing.
    """

    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _basic_auth_header() -> str:
    credentials = f"{settings.EBAY_APP_ID}:{settings.EBAY_CERT_ID}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def get_authorization_url() -> str:
    """Build the eBay OAuth authorization URL to redirect the user to."""
    params = {
        "client_id": settings.EBAY_APP_ID,
        "redirect_uri": settings.EBAY_RUNAME,
        "response_type": "code",
        "scope": " ".join(EBAY_SCOPES),
    }
    return f"{EBAY_AUTH_URL}?{urlencode(params)}"


async def exchange_code_for_tokens(code: str) -> dict:
    """Exchange the authorization code for access + refresh tokens and store them."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            EBAY_TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.EBAY_RUNAME,
            },
        )

    if response.status_code != 200:
        logger.error("Token exchange failed (%s): %s", response.status_code, response.text)
        raise ValueError(f"eBay token exchange failed: {response.text}")

    token_data = response.json()
    await _save_tokens(token_data)
    return token_data


async def refresh_access_token() -> str:
    """Use the stored refresh token to get a new access token."""
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})
    if not doc or not doc.get("refresh_token"):
        raise ValueError(
            "No refresh token in database. Re-authorize at /auth/ebay/login"
        )

    async with httpx.AsyncClient() as client:
        # Per OAuth 2.0 and eBay docs, the scope parameter is optional on
        # refresh and, if omitted, the refresh token's original scopes are
        # reused. Passing a mismatched scope string can trigger
        # "invalid_scope" even when the refresh token itself is valid, so
        # we deliberately do NOT send scope here.
        response = await client.post(
            EBAY_TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": doc["refresh_token"],
            },
        )

    if response.status_code != 200:
        logger.error("Token refresh failed (%s): %s", response.status_code, response.text)
        raise ValueError(f"eBay token refresh failed: {response.text}")

    token_data = response.json()

    # eBay doesn't always issue a new refresh token on refresh - preserve the old one
    if "refresh_token" not in token_data:
        token_data["refresh_token"] = doc["refresh_token"]
        token_data["refresh_token_expires_in"] = doc.get("refresh_token_expires_in")

    await _save_tokens(token_data)
    logger.info("eBay access token refreshed successfully")
    return token_data["access_token"]


async def get_valid_token() -> str:
    """
    Return a valid access token. Refreshes automatically if expired.
    Falls back to EBAY_OAUTH_TOKEN env var when no DB token exists yet.
    """
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})

    if doc and doc.get("access_token"):
        expires_at = _ensure_aware(doc.get("expires_at"))
        # Treat token as valid if it doesn't expire for at least 5 more minutes
        if expires_at and datetime.now(timezone.utc) < expires_at - timedelta(minutes=5):
            return doc["access_token"]

        if doc.get("refresh_token"):
            logger.info("eBay access token expired - refreshing automatically")
            return await refresh_access_token()

    # No DB token yet - fall back to env var (manual token from .env)
    if settings.EBAY_OAUTH_TOKEN:
        return settings.EBAY_OAUTH_TOKEN

    raise ValueError(
        "No valid eBay token available. Authorize the app at /auth/ebay/login"
    )


async def get_token_status() -> dict:
    """Return current token health for the /auth/ebay/status endpoint."""
    doc = await db[TOKEN_COLLECTION].find_one({"_id": TOKEN_DOC_ID})

    if not doc:
        has_env_fallback = bool(settings.EBAY_OAUTH_TOKEN)
        return {
            "status": "no_db_token",
            "message": "No token stored in database. Visit /auth/ebay/login to authorize.",
            "env_fallback_active": has_env_fallback,
        }

    now = datetime.now(timezone.utc)
    expires_at = _ensure_aware(doc.get("expires_at"))

    if expires_at:
        is_expired = now >= expires_at
        seconds_left = int((expires_at - now).total_seconds()) if not is_expired else 0
        status = "expired" if is_expired else "valid"
    else:
        is_expired = None
        seconds_left = None
        status = "unknown"

    updated_at = _ensure_aware(doc.get("updated_at"))

    result = {
        "status": status,
        "has_refresh_token": bool(doc.get("refresh_token")),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "seconds_until_expiry": seconds_left,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }

    if doc.get("refresh_token_expires_at"):
        rta = _ensure_aware(doc["refresh_token_expires_at"])
        result["refresh_token_expires_at"] = rta.isoformat()
        result["refresh_token_valid"] = now < rta if rta else None

    return result


async def _save_tokens(token_data: dict) -> None:
    """Persist token data to MongoDB."""
    now = datetime.now(timezone.utc)
    expires_in = token_data.get("expires_in", 7200)

    doc = {
        "_id": TOKEN_DOC_ID,
        "access_token": token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "token_type": token_data.get("token_type", "User Access Token"),
        "expires_in": expires_in,
        "expires_at": now + timedelta(seconds=expires_in),
        "updated_at": now,
    }

    refresh_expires_in = token_data.get("refresh_token_expires_in")
    if refresh_expires_in:
        doc["refresh_token_expires_at"] = now + timedelta(seconds=refresh_expires_in)

    await db[TOKEN_COLLECTION].replace_one({"_id": TOKEN_DOC_ID}, doc, upsert=True)
    logger.info("eBay tokens saved to MongoDB (expires in %ds)", expires_in)
