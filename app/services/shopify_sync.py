import logging
import asyncio
from app.database.mongo import db
from app.shopify.create_product import create_shopify_product
from app.shopify.update_product import update_shopify_product
from app.shopify.update_inventory import set_inventory_quantity_by_variant, set_inventory_from_mongo
from scripts.update_shopify_inventory_only import update_shopify_inventory_only
from app.services.shopify_exclusions import is_shopify_excluded_doc, BLOCKED_SHOPIFY_TAGS

logger = logging.getLogger(__name__)


async def sync_to_shopify(shopify_client=None, *, allow_create: bool = True, adjust_inventory: bool = True):
    """Sync normalized products to Shopify.

    Parameters:
        - allow_create: when False, products without shopify_id are skipped
          instead of being created.
        - adjust_inventory: when False, the per-product inventory sync step
          is skipped during updates.
    """

    logger.info("▶ Syncing normalized products to Shopify (no limit) ...")

    batch_size = 500
    last_id = None
    total_processed = 0
    created = 0
    updated = 0
    skipped = 0

    # Concurrency control for per-product work (ShopifyClient still rate-limits requests)
    max_concurrency = 10
    sem = asyncio.Semaphore(max_concurrency)

    async def process_doc(doc):
        nonlocal created, updated, skipped, total_processed

        async with sem:

            # Hard exclusion: never create/update excluded items in Shopify.
            if is_shopify_excluded_doc(doc):
                skipped += 1
                total_processed += 1
                logger.info(
                    "Skipped Shopify sync for excluded item %s (blocked tags=%s)",
                    doc.get("_id", "unknown"),
                    sorted(BLOCKED_SHOPIFY_TAGS),
                )
                return

            shopify_id = doc.get("shopify_id")
            # Prefer explicit content_hash if present; fall back to legacy 'hash'
            hash_now = doc.get("content_hash") or doc.get("hash")
            hash_prev = doc.get("last_synced_hash")

            # Create new product if no Shopify ID yet (when allowed)
            if not shopify_id:
                if not allow_create:
                    skipped += 1
                    total_processed += 1
                    logger.info(
                        "Skipped create for eBay item %s because allow_create=False",
                        doc.get("_id", "unknown"),
                    )
                    return

                try:
                    await create_shopify_product(doc, shopify_client)
                    created += 1
                    logger.info(
                        "Created new Shopify product for eBay item %s",
                        doc.get("_id", "unknown"),
                    )
                except Exception as e:
                    logger.error(
                        "Failed to create Shopify product for eBay item %s: %s",
                        doc.get("_id", "unknown"),
                        e,
                    )
                finally:
                    total_processed += 1
                return

            # Skip if hash unchanged
            if hash_now == hash_prev:
                skipped += 1
                total_processed += 1
                logger.info(
                    f"Skipped Shopify product {shopify_id} for eBay item {doc.get('_id', 'unknown')} (hash unchanged)"
                )
                return

            # Update existing product
            try:
                await update_shopify_product(doc, doc, shopify_client)

                # Ensure Shopify inventory quantity matches normalized quantity
                if adjust_inventory:
                    try:
                        quantity = doc.get("quantity")
                        sku = doc.get("_id")
                        shopify_id = doc.get("shopify_id")
                        
                        # PREFERRED: Use inventory_item_id + location_id if available (no variant fetch needed)
                        inventory_item_id = doc.get("inventory_item_id")
                        location_id = doc.get("location_id")
                        
                        if inventory_item_id and location_id and quantity is not None:
                            logger.debug(
                                "[SYNC] Syncing inventory (optimized) | sku=%s | shopify_id=%s | inventory_item=%s | location=%s | qty=%s",
                                sku,
                                shopify_id,
                                inventory_item_id,
                                location_id,
                                quantity,
                            )
                            ok = await set_inventory_from_mongo(
                                inventory_item_id,
                                location_id,
                                int(quantity),
                                shopify_client,
                                sku,
                            )
                            if not ok:
                                logger.error(
                                    "[SYNC] ✗ Failed to sync inventory | sku=%s | shopify_id=%s | inventory_item=%s",
                                    sku,
                                    shopify_id,
                                    inventory_item_id,
                                )
                        # FALLBACK: Use variant_id method (requires variant fetch)
                        elif doc.get("shopify_variant_id") and quantity is not None:
                            variant_id = doc.get("shopify_variant_id")
                            logger.debug(
                                "[SYNC] Syncing inventory (fallback - no item_id) | sku=%s | shopify_id=%s | variant_id=%s | qty=%s",
                                sku,
                                shopify_id,
                                variant_id,
                                quantity,
                            )
                            await set_inventory_quantity_by_variant(
                                int(variant_id), int(quantity), shopify_client
                            )
                        else:
                            if not inventory_item_id or not location_id:
                                logger.warning(
                                    "[SYNC] Cannot sync inventory - missing inventory_item_id or location_id | sku=%s | shopify_id=%s | item_id=%s | location=%s",
                                    sku,
                                    shopify_id,
                                    inventory_item_id,
                                    location_id,
                                )
                            if quantity is None:
                                logger.warning(
                                    "[SYNC] Cannot sync inventory - missing quantity | sku=%s | shopify_id=%s",
                                    sku,
                                    shopify_id,
                                )
                    except Exception as e:
                        logger.error(
                            "[SYNC] ✗ Exception syncing inventory | sku=%s | shopify_id=%s | error=%s",
                            doc.get("_id"),
                            doc.get("shopify_id"),
                            e,
                        )
                await db.product_normalized.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"last_synced_hash": hash_now}},
                )
                updated += 1
                logger.info(
                    f"Updated Shopify product {shopify_id} for eBay item {doc.get('item_id', 'unknown')}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to update Shopify product {shopify_id} for eBay item {doc.get('item_id', 'unknown')}: {e}"
                )
            finally:
                total_processed += 1

    while True:
        # Exclude blocked-tag items at query-time to reduce work.
        query = {"tags": {"$nin": list(BLOCKED_SHOPIFY_TAGS)}}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        # Always fetch full batches; overall dataset is bounded by Mongo query
        this_batch_size = batch_size

        # Projection can be narrowed further if desired
        projection = None

        cursor = db.product_normalized.find(query, projection).limit(this_batch_size).sort("_id", 1)

        batch_docs = []
        async for doc in cursor:
            batch_docs.append(doc)
            last_id = doc["_id"]

        if not batch_docs:
            break

        # Kick off concurrent processing for this batch
        tasks = [asyncio.create_task(process_doc(doc)) for doc in batch_docs]
        if tasks:
            await asyncio.gather(*tasks)

    logger.info("✔ Shopify Sync Done %s created, %s updated, %s skipped", created, updated, skipped)
    return {"created": created, "updated": updated, "skipped": skipped}


async def sync_new_products_to_shopify(shopify_client=None, limit: int | None = None):
    """Create Shopify products only for normalized docs that don't yet have a shopify_id.

    This is useful when you want to backfill newly normalized items without
    touching existing Shopify products.
    """

    logger.info("▶ Syncing NEW normalized products to Shopify (no existing shopify_id) ...")

    total_processed = 0
    created = 0

    max_concurrency = 10
    sem = asyncio.Semaphore(max_concurrency)

    async def process_doc(doc):
        nonlocal created, total_processed

        async with sem:
            if is_shopify_excluded_doc(doc):
                logger.info(
                    "Skipped Shopify create for excluded item %s (blocked tags=%s)",
                    doc.get("_id", "unknown"),
                    sorted(BLOCKED_SHOPIFY_TAGS),
                )
                return
            try:
                await create_shopify_product(doc, shopify_client)
                created += 1
                logger.info("Created new Shopify product for eBay item %s", doc.get("_id", "unknown"))
            except Exception as e:
                logger.error(
                    "Failed to create Shopify product for eBay item %s: %s",
                    doc.get("_id", "unknown"),
                    e,
                )
            finally:
                total_processed += 1

    # Only create products for docs that are not excluded and don't yet have shopify_id.
    query = {
        "shopify_id": {"$exists": False},
        "tags": {"$nin": list(BLOCKED_SHOPIFY_TAGS)},
    }
    cursor = db.product_normalized.find(query).sort("_id", 1)
    if limit is not None:
        cursor = cursor.limit(limit)

    batch_docs = [doc async for doc in cursor]
    if not batch_docs:
        logger.info("No NEW normalized products without shopify_id found.")
        return {"created": 0, "processed": 0}

    tasks = [asyncio.create_task(process_doc(doc)) for doc in batch_docs]
    await asyncio.gather(*tasks)

    logger.info("✔ Shopify NEW products sync done → %s created (processed=%s)", created, total_processed)
    return {"created": created, "processed": total_processed}


async def full_shopify_sync(
    env: str,
    shopify_client=None,
    *,
    do_new_products: bool = True,
    do_zero_inventory: bool = True,
    do_other_updates: bool = True,
) -> dict:
    """Run a configurable full Shopify sync in prioritized phases.

    Phases (always run in this order when enabled):
      1) Create products that don't exist yet (no shopify_id).
      2) Sync inventories ONLY for zero-quantity items.
      3) Run the general sync_to_shopify pass for other updates
         (without creating new products or touching inventory again).
    """

    summary: dict = {}

    # Step 1: create missing products
    if do_new_products:
        new_result = await sync_new_products_to_shopify(shopify_client)
    else:
        new_result = {"skipped": True}

    summary["phase_new_products"] = new_result

    # Step 2: align inventory levels for zero-quantity variants in this env
    if do_zero_inventory:
        inv_result = await update_shopify_inventory_only(
            limit=None,
            env=env,
            only_zero=True,
            dry_run=False,
        )
    else:
        inv_result = {"skipped": True}

    summary["phase_zero_inventory"] = inv_result

    # Step 3: run the general sync (updates only: no create, no inventory)
    if do_other_updates:
        main_result = await sync_to_shopify(
            shopify_client,
            allow_create=False,
            adjust_inventory=False,
        )
    else:
        main_result = {"skipped": True}

    summary["phase_other_updates"] = main_result

    return summary
