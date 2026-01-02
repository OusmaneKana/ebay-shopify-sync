from fastapi import APIRouter
from app.api.routes import health, sync, products
from app.api.routes import webhooks

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(sync.router, prefix="/sync", tags=["Sync"])
api_router.include_router(sync.dev_router, prefix="/sync/dev", tags=["Sync Dev"])
api_router.include_router(sync.prod_router, prefix="/sync/prod", tags=["Sync Prod"])
api_router.include_router(products.router, prefix="/products", tags=["Products"])
api_router.include_router(webhooks.router, prefix="/webhooks", tags=["Webhooks"])
