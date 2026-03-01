import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.api.router import api_router
from app.services.scheduler import start_scheduler

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

app.include_router(api_router)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return {"status": "running", "message": "eBay → Shopify Sync Middleware"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/admin", response_class=FileResponse)
async def admin_panel():
    return FileResponse("static/index.html")
