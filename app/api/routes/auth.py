from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.services.ebay_auth_service import (
    exchange_code_for_tokens,
    get_authorization_url,
    get_token_status,
)

router = APIRouter()


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
