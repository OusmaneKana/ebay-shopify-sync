from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.config import settings
from app.security.passkey import COOKIE_NAME, passkey_enabled

from app.services.ebay_auth_service import (
    exchange_code_for_tokens,
    get_authorization_url,
    get_token_status,
)
from app.services.etsy_auth_service import (
    exchange_code_for_tokens as exchange_etsy_code_for_tokens,
    get_authorization_url as get_etsy_authorization_url,
    get_token_status as get_etsy_token_status,
    refresh_access_token as refresh_etsy_access_token,
)

router = APIRouter()


@router.get("/passkey/status")
async def passkey_status(request: Request):
    """Lightweight status endpoint for the UI."""
    if not passkey_enabled():
        return {"enabled": False, "authorized": True}
    cookie_val = request.cookies.get(COOKIE_NAME)
    return {
        "enabled": True,
        "authorized": bool(cookie_val) and cookie_val == str(settings.ADMIN_PASSKEY),
    }


@router.post("/passkey/login")
async def passkey_login(payload: dict, request: Request):
    if not passkey_enabled():
        return {"ok": True, "message": "Passkey disabled"}

    provided = (payload or {}).get("passkey")
    if not provided or str(provided) != str(settings.ADMIN_PASSKEY):
        return JSONResponse({"ok": False, "error": "Invalid passkey"}, status_code=401)

    resp = JSONResponse({"ok": True})
    # Minimal: cookie value == passkey. HttpOnly keeps it out of JS.
    resp.set_cookie(
        key=COOKIE_NAME,
        value=str(provided),
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    return resp


@router.post("/passkey/logout")
async def passkey_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(COOKIE_NAME)
    return resp


@router.get("/ebay/login")
async def ebay_login():
    """Redirect the browser to eBay's OAuth authorization page."""
    url = get_authorization_url()
    return RedirectResponse(url=url)


@router.get("/ebay/callback")
async def ebay_callback(
    request: Request,
    code: str = None,
    error: str = None,
    error_description: str = None,
):
    """
    eBay redirects here after the user authorizes (or denies) the app.
    Exchanges the authorization code for access + refresh tokens and stores them.
    """
    if error:
        return JSONResponse(
            {"ok": False, "error": error, "description": error_description},
            status_code=400,
        )

    if not code:
        return JSONResponse(
            {"ok": False, "error": "Missing authorization code in callback"},
            status_code=400,
        )

    try:
        token_data = await exchange_code_for_tokens(code)
        return JSONResponse({
            "ok": True,
            "message": "eBay authorized successfully. Tokens saved to database.",
            "expires_in_seconds": token_data.get("expires_in"),
            "has_refresh_token": "refresh_token" in token_data,
        })
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@router.get("/ebay/status")
async def ebay_token_status():
    """Check the current eBay token health (valid / expired / missing)."""
    return await get_token_status()


@router.get("/etsy/callback")
async def etsy_callback(
    code: str = None,
    error: str = None,
    error_description: str = None,
    state: str = None,
):
    """
    Etsy redirects here after the user authorizes (or denies) the app.
    Exchanges the authorization code for access + refresh tokens and stores them.
    """
    if error:
        return JSONResponse(
            {"ok": False, "error": error, "description": error_description, "state": state},
            status_code=400,
        )

    if not code:
        return JSONResponse(
            {"ok": False, "error": "Missing authorization code in callback", "state": state},
            status_code=400,
        )

    try:
        token_data = await exchange_etsy_code_for_tokens(code)
        return JSONResponse({
            "ok": True,
            "message": "Etsy authorized successfully. Tokens saved to database.",
            "expires_in_seconds": token_data.get("expires_in"),
            "has_refresh_token": "refresh_token" in token_data,
            "scope": token_data.get("scope"),
            "state": state,
        })
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e), "state": state}, status_code=400)


@router.get("/etsy/login")
async def etsy_login(state: str | None = None):
    """Redirect the browser to Etsy OAuth authorization."""
    try:
        url = get_etsy_authorization_url(state=state)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    return RedirectResponse(url=url)


@router.get("/etsy/status")
async def etsy_token_status():
    """Check the current Etsy token health (valid / expired / missing)."""
    return await get_etsy_token_status()


@router.post("/etsy/refresh")
async def etsy_force_refresh():
    """Force an immediate Etsy token refresh using the stored refresh token."""
    try:
        new_token = await refresh_etsy_access_token()
        status = await get_etsy_token_status()
        return {
            "ok": True,
            "message": "Etsy access token refreshed successfully.",
            "token_preview": new_token[:12] + "...",
            **status,
        }
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
