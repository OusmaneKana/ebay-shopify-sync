from fastapi import APIRouter
from app.api.routes import health, sync, products

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(sync.router, prefix="/sync", tags=["Sync"])
api_router.include_router(products.router, prefix="/products", tags=["Products"])
