from fastapi import APIRouter
from app.services.sync_manager import full_sync
from app.services.product_service import sync_ebay_raw_to_mongo
from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify
from app.shopify.purge_all_shopify_products import purge_all_shopify_products
from app.shopify.client import ShopifyClient
from app.config import settings

router = APIRouter()

# Dev environment routes
dev_router = APIRouter()

@dev_router.post("/sync-ebay-raw")
async def sync_ebay_raw_dev():
    result = await sync_ebay_raw_to_mongo()
    return {"message": "eBay raw sync completed (DEV)", "result": result}

@dev_router.post("/normalize-raw")
async def normalize_raw_dev():
    result = await normalize_from_raw()
    return {"message": "Normalization completed (DEV)", "result": result}

@dev_router.post("/sync-shopify")
async def sync_shopify_dev(limit: int = None):
    # Use dev Shopify client
    client = ShopifyClient()
    result = await sync_to_shopify(client, limit)
    return {"message": "Shopify sync completed (DEV)", "result": result}

@dev_router.post("/purge-shopify")
async def purge_shopify_dev():
    client = ShopifyClient()
    deleted = purge_all_shopify_products(client)
    return {"message": "Shopify products purged (DEV)", "deleted": deleted}

# Prod environment routes
prod_router = APIRouter()

@prod_router.post("/sync-ebay-raw")
async def sync_ebay_raw_prod():
    result = await sync_ebay_raw_to_mongo()
    return {"message": "eBay raw sync completed (PROD)", "result": result}

@prod_router.post("/normalize-raw")
async def normalize_raw_prod():
    result = await normalize_from_raw()
    return {"message": "Normalization completed (PROD)", "result": result}

@prod_router.post("/sync-shopify")
async def sync_shopify_prod(limit: int = None):
    # Use prod Shopify client
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD
    )
    result = await sync_to_shopify(client, limit)
    return {"message": "Shopify sync completed (PROD)", "result": result}

@prod_router.post("/purge-shopify")
async def purge_shopify_prod():
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD
    )
    deleted = purge_all_shopify_products(client)
    return {"message": "Shopify products purged (PROD)", "deleted": deleted}

# Legacy routes (for backward compatibility)
@router.post("/run")
async def run_sync():
    result = await full_sync()
    return {"message": "Sync completed", "result": result}

@router.post("/sync-ebay-raw")
async def sync_ebay_raw():
    result = await sync_ebay_raw_to_mongo()
    return {"message": "eBay raw sync completed", "result": result}

@router.post("/normalize-raw")
async def normalize_raw():
    result = await normalize_from_raw()
    return {"message": "Normalization completed", "result": result}

@router.post("/sync-shopify")
async def sync_shopify(limit: int = None):
    result = await sync_to_shopify(None, limit)
    return {"message": "Shopify sync completed", "result": result}

@router.post("/purge-shopify")
async def purge_shopify():
    deleted = purge_all_shopify_products()
    return {"message": "Shopify products purged", "deleted": deleted}
