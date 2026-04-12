import time

from fastapi import APIRouter, Body, Depends, HTTPException, Request

from app.services.sync_manager import full_sync
from app.services.product_service import sync_ebay_raw_to_mongo
from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify, sync_new_products_to_shopify, full_shopify_sync
from app.shopify.purge_all_shopify_products import purge_all_shopify_products
from app.shopify.client import ShopifyClient
from app.config import settings
from scripts.update_shopify_inventory_only import update_shopify_inventory_only
from app.security.passkey import require_authorized
from app.services.job_tracker import get_job, start_job

router = APIRouter()

# Dev environment routes
dev_router = APIRouter(dependencies=[Depends(require_authorized)])


def _parse_shopify_sync_options(options: dict | None) -> tuple[bool, bool, bool, bool]:
    """Parse Shopify sync options payload.

    Matches the existing /sync-shopify body contract:
            {"new_products": bool, "zero_inventory": bool, "allow_zero_inventory_updates": bool, "other_updates": bool}
    """

    opts = options or {}
    do_new = bool(opts.get("new_products", True))
    do_zero = bool(opts.get("zero_inventory", False))
    allow_zero = bool(opts.get("allow_zero_inventory_updates", False))
    do_other = bool(opts.get("other_updates", True))
    return do_new, do_zero, allow_zero, do_other


def _shopify_client_for_env(env: str) -> ShopifyClient:
    if env == "prod":
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    return ShopifyClient()


async def _run_all_steps(*, env: str, shopify_options: dict | None) -> dict:
    """Run eBay sync -> normalization -> Shopify sync sequentially."""

    t0 = time.perf_counter()

    # Step 1: eBay → Mongo (raw)
    t_ebay = time.perf_counter()
    ebay_result = await sync_ebay_raw_to_mongo()
    ebay_elapsed = time.perf_counter() - t_ebay

    # Step 2: raw → normalized
    t_norm = time.perf_counter()
    norm_result = await normalize_from_raw()
    norm_elapsed = time.perf_counter() - t_norm

    # Step 3: normalized → Shopify
    t_shopify = time.perf_counter()
    client = _shopify_client_for_env(env)
    do_new, do_zero, allow_zero, do_other = _parse_shopify_sync_options(shopify_options)
    shopify_result = await full_shopify_sync(
        env=env,
        shopify_client=client,
        do_new_products=do_new,
        do_zero_inventory=do_zero,
        allow_zero_inventory_updates=allow_zero,
        do_other_updates=do_other,
    )
    shopify_elapsed = time.perf_counter() - t_shopify

    total_elapsed = time.perf_counter() - t0

    return {
        "env": env,
        "steps": {
            "ebay_raw": ebay_result,
            "normalize": norm_result,
            "shopify": shopify_result,
        },
        "elapsed_seconds": {
            "ebay_raw": ebay_elapsed,
            "normalize": norm_elapsed,
            "shopify": shopify_elapsed,
            "total": total_elapsed,
        },
    }


async def _maybe_background(
    *,
    request: Request,
    name: str,
    fn,
    background: bool,
) -> dict:
    if not background:
        return await fn()

    job = await start_job(name=name, fn=fn)
    return {
        "message": f"{name} started",
        "job_id": job["id"],
        "job_status_url": str(request.url_for("sync_job_status", job_id=job["id"])),
        "status": job["status"],
    }

@dev_router.post("/sync-ebay-raw")
async def sync_ebay_raw_dev(request: Request, background: bool = False):
    async def _run() -> dict:
        start = time.perf_counter()
        result = await sync_ebay_raw_to_mongo()
        elapsed = time.perf_counter() - start
        return {
            "message": "eBay raw sync completed (DEV)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV eBay raw sync",
        fn=_run,
        background=background,
    )

@dev_router.post("/normalize-raw")
async def normalize_raw_dev(request: Request, background: bool = False):
    async def _run() -> dict:
        start = time.perf_counter()
        result = await normalize_from_raw()
        elapsed = time.perf_counter() - start
        return {
            "message": "Normalization completed (DEV)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV normalization",
        fn=_run,
        background=background,
    )

@dev_router.post("/sync-shopify")
async def sync_shopify_dev(
    request: Request,
    options: dict | None = Body(None),
    background: bool = False,
):
    """Dev: configurable full Shopify sync.

        Body (all optional):
            {"new_products": true, "zero_inventory": false, "allow_zero_inventory_updates": false, "other_updates": true}
    """

    async def _run() -> dict:
        start = time.perf_counter()
        client = ShopifyClient()

        opts = options or {}
        do_new = bool(opts.get("new_products", True))
        do_zero = bool(opts.get("zero_inventory", False))
        allow_zero = bool(opts.get("allow_zero_inventory_updates", False))
        do_other = bool(opts.get("other_updates", True))

        result = await full_shopify_sync(
            env="dev",
            shopify_client=client,
            do_new_products=do_new,
            do_zero_inventory=do_zero,
            allow_zero_inventory_updates=allow_zero,
            do_other_updates=do_other,
        )
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify sync completed (DEV)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV Shopify sync",
        fn=_run,
        background=background,
    )


@dev_router.post("/run-all")
async def run_all_dev(
    request: Request,
    shopify_options: dict | None = Body(None),
    background: bool = False,
):
    """Dev: run eBay sync, normalization, then Shopify sync in one call."""

    async def _run() -> dict:
        result = await _run_all_steps(env="dev", shopify_options=shopify_options)
        return {
            "message": "Full pipeline completed (DEV)",
            "result": result,
        }

    return await _maybe_background(
        request=request,
        name="DEV full pipeline",
        fn=_run,
        background=background,
    )


@dev_router.post("/sync-shopify-new")
async def sync_shopify_new_dev(request: Request, limit: int | None = None, background: bool = False):
    """Dev: create Shopify products only for normalized docs without shopify_id."""
    async def _run() -> dict:
        start = time.perf_counter()
        client = ShopifyClient()
        result = await sync_new_products_to_shopify(client, limit=limit)
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify NEW-products sync completed (DEV)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV Shopify new-products sync",
        fn=_run,
        background=background,
    )


@dev_router.post("/sync-shopify-inventory")
async def sync_shopify_inventory_dev(
    request: Request,
    only_zero: bool = False,
    allow_zero_updates: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
    background: bool = False,
):
    """Dev: force Shopify inventory to match product_normalized.quantity only."""
    async def _run() -> dict:
        start = time.perf_counter()
        result = await update_shopify_inventory_only(
            limit=limit,
            env="dev",
            only_zero=only_zero,
            allow_zero_updates=allow_zero_updates,
            dry_run=dry_run,
        )
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify inventory-only sync completed (DEV)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV inventory-only sync",
        fn=_run,
        background=background,
    )

@dev_router.post("/purge-shopify")
async def purge_shopify_dev(request: Request, background: bool = False):
    async def _run() -> dict:
        start = time.perf_counter()
        client = ShopifyClient()
        deleted = await purge_all_shopify_products(client)
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify products purged (DEV)",
            "deleted": deleted,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="DEV purge Shopify",
        fn=_run,
        background=background,
    )


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
prod_router = APIRouter(dependencies=[Depends(require_authorized)])

@prod_router.post("/sync-ebay-raw")
async def sync_ebay_raw_prod(request: Request, background: bool = False):
    async def _run() -> dict:
        start = time.perf_counter()
        result = await sync_ebay_raw_to_mongo()
        elapsed = time.perf_counter() - start
        return {
            "message": "eBay raw sync completed (PROD)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="PROD eBay raw sync",
        fn=_run,
        background=background,
    )

@prod_router.post("/normalize-raw")
async def normalize_raw_prod(request: Request, background: bool = False):
    async def _run() -> dict:
        start = time.perf_counter()
        result = await normalize_from_raw()
        elapsed = time.perf_counter() - start
        return {
            "message": "Normalization completed (PROD)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="PROD normalization",
        fn=_run,
        background=background,
    )

@prod_router.post("/sync-shopify")
async def sync_shopify_prod(
    request: Request,
    options: dict | None = Body(None),
    background: bool = False,
):
    """Prod: configurable full Shopify sync.

        Body (all optional):
            {"new_products": true, "zero_inventory": false, "allow_zero_inventory_updates": false, "other_updates": true}
    """

    async def _run() -> dict:
        start = time.perf_counter()
        client = ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD
        )

        opts = options or {}
        do_new = bool(opts.get("new_products", True))
        do_zero = bool(opts.get("zero_inventory", False))
        allow_zero = bool(opts.get("allow_zero_inventory_updates", False))
        do_other = bool(opts.get("other_updates", True))

        result = await full_shopify_sync(
            env="prod",
            shopify_client=client,
            do_new_products=do_new,
            do_zero_inventory=do_zero,
            allow_zero_inventory_updates=allow_zero,
            do_other_updates=do_other,
        )
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify sync completed (PROD)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="PROD Shopify sync",
        fn=_run,
        background=background,
    )


@prod_router.post("/run-all")
async def run_all_prod(
    request: Request,
    shopify_options: dict | None = Body(None),
    background: bool = False,
):
    """Prod: run eBay sync, normalization, then Shopify sync in one call."""

    async def _run() -> dict:
        result = await _run_all_steps(env="prod", shopify_options=shopify_options)
        return {
            "message": "Full pipeline completed (PROD)",
            "result": result,
        }

    return await _maybe_background(
        request=request,
        name="PROD full pipeline",
        fn=_run,
        background=background,
    )


@prod_router.post("/sync-shopify-new")
async def sync_shopify_new_prod(request: Request, limit: int | None = None, background: bool = False):
    """Prod: create Shopify products only for normalized docs without shopify_id."""
    async def _run() -> dict:
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

    return await _maybe_background(
        request=request,
        name="PROD Shopify new-products sync",
        fn=_run,
        background=background,
    )


@prod_router.post("/sync-shopify-inventory")
async def sync_shopify_inventory_prod(
    request: Request,
    only_zero: bool = False,
    allow_zero_updates: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
    background: bool = False,
):
    """Prod: force Shopify inventory to match product_normalized.quantity only."""
    async def _run() -> dict:
        start = time.perf_counter()
        result = await update_shopify_inventory_only(
            limit=limit,
            env="prod",
            only_zero=only_zero,
            allow_zero_updates=allow_zero_updates,
            dry_run=dry_run,
        )
        elapsed = time.perf_counter() - start
        return {
            "message": "Shopify inventory-only sync completed (PROD)",
            "result": result,
            "elapsed_seconds": elapsed,
        }

    return await _maybe_background(
        request=request,
        name="PROD inventory-only sync",
        fn=_run,
        background=background,
    )

@prod_router.post("/purge-shopify")
async def purge_shopify_prod(request: Request, background: bool = False):
    async def _run() -> dict:
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

    return await _maybe_background(
        request=request,
        name="PROD purge Shopify",
        fn=_run,
        background=background,
    )


@router.get("/jobs/{job_id}", name="sync_job_status")
async def sync_job_status(job_id: str, request: Request):
    require_authorized(request)
    job = await get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


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
