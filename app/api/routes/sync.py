from fastapi import APIRouter
from app.services.sync_manager import full_sync
from app.services.product_service import sync_ebay_raw_to_mongo
from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify
from app.shopify.purge_all_shopify_products import purge_all_shopify_products

router = APIRouter()

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
async def sync_shopify():
    result = await sync_to_shopify()
    return {"message": "Shopify sync completed", "result": result}

@router.post("/purge-shopify")
async def purge_shopify():
    deleted = purge_all_shopify_products()
    return {"message": "Shopify products purged", "deleted": deleted}
