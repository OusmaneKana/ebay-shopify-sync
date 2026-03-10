import time

from fastapi import APIRouter, Body

from app.services.sync_manager import full_sync
from app.services.product_service import sync_ebay_raw_to_mongo
from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify, sync_new_products_to_shopify, full_shopify_sync
from app.shopify.purge_all_shopify_products import purge_all_shopify_products
from app.shopify.client import ShopifyClient
from app.config import settings
from scripts.update_shopify_inventory_only import update_shopify_inventory_only

router = APIRouter()

# Dev environment routes
dev_router = APIRouter()

@dev_router.post("/sync-ebay-raw")
async def sync_ebay_raw_dev():
    start = time.perf_counter()
    result = await sync_ebay_raw_to_mongo()
    elapsed = time.perf_counter() - start
    return {
        "message": "eBay raw sync completed (DEV)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@dev_router.post("/normalize-raw")
async def normalize_raw_dev():
    start = time.perf_counter()
    result = await normalize_from_raw()
    elapsed = time.perf_counter() - start
    return {
        "message": "Normalization completed (DEV)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@dev_router.post("/sync-shopify")
async def sync_shopify_dev(
    options: dict | None = Body(None),
):
    """Dev: configurable full Shopify sync.

    Body (all optional, default True):
      {"new_products": bool, "zero_inventory": bool, "other_updates": bool}
    """

    start = time.perf_counter()
    client = ShopifyClient()

    opts = options or {}
    do_new = bool(opts.get("new_products", True))
    do_zero = bool(opts.get("zero_inventory", True))
    do_other = bool(opts.get("other_updates", True))

    result = await full_shopify_sync(
        env="dev",
        shopify_client=client,
        do_new_products=do_new,
        do_zero_inventory=do_zero,
        do_other_updates=do_other,
    )
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify sync completed (DEV)",
        "result": result,
        "elapsed_seconds": elapsed,
    }


@dev_router.post("/sync-shopify-new")
async def sync_shopify_new_dev( limit: int | None = None):
    """Dev: create Shopify products only for normalized docs without shopify_id."""
    start = time.perf_counter()
    client = ShopifyClient()
    result = await sync_new_products_to_shopify(client, limit=limit)
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify NEW-products sync completed (DEV)",
        "result": result,
        "elapsed_seconds": elapsed,
    }


@dev_router.post("/sync-shopify-inventory")
async def sync_shopify_inventory_dev(only_zero: bool = False, limit: int | None = None, dry_run: bool = False):
    """Dev: force Shopify inventory to match product_normalized.quantity only."""
    start = time.perf_counter()
    result = await update_shopify_inventory_only(
        limit=limit,
        env="dev",
        only_zero=only_zero,
        dry_run=dry_run,
    )
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify inventory-only sync completed (DEV)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@dev_router.post("/purge-shopify")
async def purge_shopify_dev():
    start = time.perf_counter()
    client = ShopifyClient()
    deleted = await purge_all_shopify_products(client)
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify products purged (DEV)",
        "deleted": deleted,
        "elapsed_seconds": elapsed,
    }


@dev_router.post("/shopify-health")
async def shopify_health_dev():
    start = time.perf_counter()
    client = ShopifyClient()
    try:
        resp = await client.get("shop.json")
        last = getattr(client, "last_response", None)
        ok = last is not None and last.status == 200
        elapsed = time.perf_counter() - start
        return {
            "ok": ok,
            "status_code": last.status if last is not None else None,
            "shop": resp,
            "elapsed_seconds": elapsed,
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {"ok": False, "error": str(e), "elapsed_seconds": elapsed}

# Prod environment routes
prod_router = APIRouter()

@prod_router.post("/sync-ebay-raw")
async def sync_ebay_raw_prod():
    start = time.perf_counter()
    result = await sync_ebay_raw_to_mongo()
    elapsed = time.perf_counter() - start
    return {
        "message": "eBay raw sync completed (PROD)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@prod_router.post("/normalize-raw")
async def normalize_raw_prod():
    start = time.perf_counter()
    result = await normalize_from_raw()
    elapsed = time.perf_counter() - start
    return {
        "message": "Normalization completed (PROD)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@prod_router.post("/sync-shopify")
async def sync_shopify_prod(
    options: dict | None = Body(None),
):
    """Prod: configurable full Shopify sync.

    Body (all optional, default True):
      {"new_products": bool, "zero_inventory": bool, "other_updates": bool}
    """

    start = time.perf_counter()
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD
    )

    opts = options or {}
    do_new = bool(opts.get("new_products", True))
    do_zero = bool(opts.get("zero_inventory", True))
    do_other = bool(opts.get("other_updates", True))

    result = await full_shopify_sync(
        env="prod",
        shopify_client=client,
        do_new_products=do_new,
        do_zero_inventory=do_zero,
        do_other_updates=do_other,
    )
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify sync completed (PROD)",
        "result": result,
        "elapsed_seconds": elapsed,
    }


@prod_router.post("/sync-shopify-new")
async def sync_shopify_new_prod(limit: int | None = None):
    """Prod: create Shopify products only for normalized docs without shopify_id."""
    start = time.perf_counter()
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD,
    )
    result = await sync_new_products_to_shopify(client, limit=limit)
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify NEW-products sync completed (PROD)",
        "result": result,
        "elapsed_seconds": elapsed,
    }


@prod_router.post("/sync-shopify-inventory")
async def sync_shopify_inventory_prod(only_zero: bool = False, limit: int | None = None, dry_run: bool = False):
    """Prod: force Shopify inventory to match product_normalized.quantity only."""
    start = time.perf_counter()
    result = await update_shopify_inventory_only(
        limit=limit,
        env="prod",
        only_zero=only_zero,
        dry_run=dry_run,
    )
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify inventory-only sync completed (PROD)",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@prod_router.post("/purge-shopify")
async def purge_shopify_prod():
    start = time.perf_counter()
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD
    )
    deleted = await purge_all_shopify_products(client)
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify products purged (PROD)",
        "deleted": deleted,
        "elapsed_seconds": elapsed,
    }


@prod_router.post("/shopify-health")
async def shopify_health_prod():
    start = time.perf_counter()
    client = ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD
    )
    try:
        resp = await client.get("shop.json")
        last = getattr(client, "last_response", None)
        ok = last is not None and last.status == 200
        elapsed = time.perf_counter() - start
        return {
            "ok": ok,
            "status_code": last.status if last is not None else None,
            "shop": resp,
            "elapsed_seconds": elapsed,
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {"ok": False, "error": str(e), "elapsed_seconds": elapsed}

# Legacy routes (for backward compatibility)
@router.post("/run")
async def run_sync():
    start = time.perf_counter()
    result = await full_sync()
    elapsed = time.perf_counter() - start
    return {"message": "Sync completed", "result": result, "elapsed_seconds": elapsed}

@router.post("/sync-ebay-raw")
async def sync_ebay_raw():
    start = time.perf_counter()
    result = await sync_ebay_raw_to_mongo()
    elapsed = time.perf_counter() - start
    return {
        "message": "eBay raw sync completed",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@router.post("/normalize-raw")
async def normalize_raw():
    start = time.perf_counter()
    result = await normalize_from_raw()
    elapsed = time.perf_counter() - start
    return {
        "message": "Normalization completed",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@router.post("/sync-shopify")
async def sync_shopify():
    start = time.perf_counter()
    result = await sync_to_shopify(None)
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify sync completed",
        "result": result,
        "elapsed_seconds": elapsed,
    }

@router.post("/purge-shopify")
async def purge_shopify():
    start = time.perf_counter()
    deleted = purge_all_shopify_products()
    elapsed = time.perf_counter() - start
    return {
        "message": "Shopify products purged",
        "deleted": deleted,
        "elapsed_seconds": elapsed,
    }


@router.post("/shopify-health")
async def shopify_health():
    # legacy/neutral route — uses default client settings
    start = time.perf_counter()
    client = ShopifyClient()
    try:
        resp = client.get("shop.json")
        last = getattr(client, "last_response", None)
        ok = last is not None and last.status_code == 200
        elapsed = time.perf_counter() - start
        return {
            "ok": ok,
            "status_code": last.status_code if last is not None else None,
            "shop": resp,
            "elapsed_seconds": elapsed,
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {"ok": False, "error": str(e), "elapsed_seconds": elapsed}
