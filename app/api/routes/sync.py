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
from app.services.multichannel_sync_service import (
    enqueue_reconcile_jobs_for_sku,
    get_inventory_command_center,
    get_item_timeline,
    get_sync_dashboard,
    get_conflict_policy,
    ingest_sale_event,
    replay_unresolved_etsy_receipt_events,
    replay_unprocessed_ebay_fixed_price_transactions,
    replay_failed_jobs,
    run_worker_batch,
    set_conflict_policy,
)

router = APIRouter()

prod_router = APIRouter(dependencies=[Depends(require_authorized)])


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


def _shopify_client_for_prod() -> ShopifyClient:
    return ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD,
    )


async def _run_all_steps(*, shopify_options: dict | None) -> dict:
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
    client = _shopify_client_for_prod()
    do_new, do_zero, allow_zero, do_other = _parse_shopify_sync_options(shopify_options)
    shopify_result = await full_shopify_sync(
        env="prod",
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

# Production routes

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


@prod_router.get("/multichannel/dashboard")
async def multichannel_dashboard_prod(limit_recent_jobs: int = 50):
    """Design-mode dashboard data for multichannel inventory orchestration (PROD)."""
    return await get_sync_dashboard(limit_recent_jobs=limit_recent_jobs)


@prod_router.get("/multichannel/command-center")
async def multichannel_command_center_prod(
    status: str = "all",
    drift_only: bool = False,
    search: str | None = None,
    limit: int = 100,
    skip: int = 0,
):
    """Inventory command-center rows (PROD)."""
    return await get_inventory_command_center(
        status=status,
        drift_only=drift_only,
        search=search,
        limit=limit,
        skip=skip,
    )


@prod_router.get("/multichannel/timeline/{sku}")
async def multichannel_timeline_prod(sku: str, limit: int = 100):
    """Per-SKU multichannel timeline (PROD)."""
    return await get_item_timeline(sku=sku, limit=limit)


@prod_router.post("/multichannel/ingest-sale")
async def multichannel_ingest_sale_prod(payload: dict = Body(...)):
    """Ingest a sale event into canonical inventory and enqueue channel jobs (PROD)."""
    return await ingest_sale_event(
        source_channel=str(payload.get("source_channel") or "manual"),
        payload=payload.get("payload") or payload,
        quantity_sold=int(payload.get("quantity_sold") or 1),
        explicit_sku=payload.get("sku"),
        explicit_event_id=payload.get("event_id"),
        enqueue_jobs_flag=bool(payload.get("enqueue_jobs", True)),
    )


@prod_router.post("/multichannel/run-worker")
async def multichannel_run_worker_prod(limit: int = 25):
    """Run a batch of queued multichannel sync jobs (PROD)."""
    return await run_worker_batch(limit=limit)


@prod_router.post("/multichannel/replay-failed")
async def multichannel_replay_failed_prod(payload: dict = Body(None)):
    """Replay failed multichannel jobs by filters (PROD)."""
    body = payload or {}
    return await replay_failed_jobs(
        target_channel=body.get("target_channel"),
        sku=body.get("sku"),
        error_contains=body.get("error_contains"),
        limit=int(body.get("limit") or 200),
    )


@prod_router.post("/multichannel/replay-unresolved-etsy-receipts")
async def multichannel_replay_unresolved_etsy_receipts_prod(payload: dict = Body(None)):
    """Replay unresolved Etsy sale events that reference receipt resource URLs (PROD)."""
    body = payload or {}
    return await replay_unresolved_etsy_receipt_events(
        limit=int(body.get("limit") or 200),
        event_id=body.get("event_id"),
    )


@prod_router.post("/multichannel/replay-unprocessed-ebay-transactions")
async def multichannel_replay_unprocessed_ebay_transactions_prod(payload: dict = Body(None)):
    """Re-process stored eBay FixedPriceTransaction notifications that were never ingested (PROD)."""
    body = payload or {}
    return await replay_unprocessed_ebay_fixed_price_transactions(
        limit=int(body.get("limit") or 200),
        event_id=body.get("event_id"),
    )


@prod_router.post("/multichannel/reconcile/{sku}")
async def multichannel_reconcile_sku_prod(sku: str, payload: dict = Body(None)):
    """Queue reconciliation jobs for one SKU (PROD)."""
    body = payload or {}
    channels = body.get("channels")
    return await enqueue_reconcile_jobs_for_sku(
        sku=sku,
        target_channels=channels if isinstance(channels, list) else None,
        reason=str(body.get("reason") or "manual_reconcile"),
    )


@prod_router.get("/multichannel/policy/{sku}")
async def multichannel_get_policy_prod(sku: str):
    """Get per-SKU conflict policy (PROD)."""
    return await get_conflict_policy(sku)


@prod_router.put("/multichannel/policy/{sku}")
async def multichannel_set_policy_prod(sku: str, payload: dict = Body(...)):
    """Set per-SKU conflict policy (PROD)."""
    return await set_conflict_policy(
        sku=sku,
        priority_channel=payload.get("priority_channel"),
        max_delta_guard=payload.get("max_delta_guard"),
        strict_priority=bool(payload.get("strict_priority", False)),
        note=payload.get("note"),
    )

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
