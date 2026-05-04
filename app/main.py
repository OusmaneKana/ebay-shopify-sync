import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from app.api.router import api_router
from app.services.scheduler import start_scheduler
from app.security.passkey import is_authorized, passkey_enabled
from app.database.mongo import close_mongo_client
from app.services.etsy_auth_service import get_token_status as get_etsy_token_status

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configure logging with both console and file handlers
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Console handler (INFO level)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(console_formatter)

# File handler (DEBUG level) with rotation to prevent unbounded growth
file_handler = RotatingFileHandler(
    'logs/app.log', maxBytes=10 * 1024 * 1024, backupCount=5
)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)

# Error file handler - errors and warnings only, also rotated
error_handler = RotatingFileHandler(
    'logs/errors.log', maxBytes=5 * 1024 * 1024, backupCount=3
)
error_handler.setLevel(logging.WARNING)
error_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
error_handler.setFormatter(error_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.addHandler(error_handler)

app = FastAPI(title="eBay → Shopify Sync Middleware")

#!!! Uncomment the following lines to enable the scheduler on startup

# @app.on_event("startup")
# async def startup_event():
#     start_scheduler()

_startup_logger = logging.getLogger(__name__)

@app.on_event("startup")
async def check_etsy_token_health():
    try:
        status = await get_etsy_token_status()
        token_status = status.get("status")
        has_refresh = status.get("has_refresh_token", False)
        if token_status == "no_db_token":
            env_fallback = status.get("env_fallback_active", False)
            if env_fallback:
                _startup_logger.warning(
                    "Etsy: no DB token found - using ETSY_TOKEN env fallback. "
                    "Visit /auth/etsy/login to authorize and store a refresh token."
                )
            else:
                _startup_logger.warning(
                    "Etsy: no token configured. Visit /auth/etsy/login to authorize."
                )
        elif token_status == "expired" and not has_refresh:
            _startup_logger.warning(
                "Etsy: access token is expired and no refresh token is stored. "
                "Visit /auth/etsy/login to re-authorize."
            )
        elif not has_refresh:
            _startup_logger.warning(
                "Etsy: token is valid but no refresh token stored. "
                "Re-authorize via /auth/etsy/login with offline_access scope to enable auto-refresh."
            )
        else:
            seconds_left = status.get("seconds_until_expiry")
            _startup_logger.info(
                "Etsy token OK (status=%s, has_refresh_token=%s, expires_in=%ss)",
                token_status, has_refresh, seconds_left,
            )
    except Exception as exc:
        _startup_logger.warning("Etsy token health check failed at startup: %s", exc)

app.include_router(api_router)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("shutdown")
async def shutdown_event():
    close_mongo_client()

@app.get("/", response_class=FileResponse)
async def root():
    return FileResponse("static/home.html")

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/admin", response_class=FileResponse)
async def admin_panel(request: Request):
    if passkey_enabled() and not is_authorized(request):
        return RedirectResponse(url="/login?next=/admin")
    return FileResponse("static/index.html")


@app.get("/reporting", response_class=FileResponse)
async def reporting_page(request: Request):
    return FileResponse("static/reporting.html")


@app.get("/etsy-review", response_class=FileResponse)
async def etsy_review_page(request: Request):
    return FileResponse("static/etsy_match_review.html")


@app.get("/etsy-publish-prep", response_class=FileResponse)
async def etsy_publish_prep_page(request: Request):
    if passkey_enabled() and not is_authorized(request):
        return RedirectResponse(url="/login?next=/etsy-publish-prep")
    return FileResponse("static/etsy_publish_prep.html")


@app.get("/channel-compare", response_class=FileResponse)
async def channel_compare_page(request: Request):
    return FileResponse("static/channel_compare.html")


@app.get("/multichannel-dashboard", response_class=FileResponse)
async def multichannel_dashboard_page(request: Request):
    if passkey_enabled() and not is_authorized(request):
        return RedirectResponse(url="/login?next=/multichannel-dashboard")
    return FileResponse("static/multichannel_dashboard.html")


@app.get("/inventory-command-center", response_class=FileResponse)
async def inventory_command_center_page(request: Request):
    if passkey_enabled() and not is_authorized(request):
        return RedirectResponse(url="/login?next=/inventory-command-center")
    return FileResponse("static/inventory_command_center.html")


@app.get("/item-detail-timeline", response_class=FileResponse)
async def item_detail_timeline_page(request: Request):
    if passkey_enabled() and not is_authorized(request):
        return RedirectResponse(url="/login?next=/item-detail-timeline")
    return FileResponse("static/item_detail_timeline.html")


@app.get("/login", response_class=FileResponse)
async def login_page():
    return FileResponse("static/login.html")
