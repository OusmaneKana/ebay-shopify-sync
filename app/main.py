import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.api.router import api_router
from app.services.scheduler import start_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

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
