from fastapi import FastAPI
from app.api.router import api_router
from app.services.scheduler import start_scheduler

app = FastAPI(title="eBay → Shopify Sync Middleware")

@app.on_event("startup")
async def startup_event():
    start_scheduler()

app.include_router(api_router)

@app.get("/")
def root():
    return {"status": "running", "message": "eBay → Shopify Sync Middleware"}
