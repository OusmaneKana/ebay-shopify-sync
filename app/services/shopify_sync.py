import logging
import asyncio
from app.database.mongo import db
from app.shopify.create_product import create_shopify_product
from app.shopify.update_product import update_shopify_product
from app.shopify.update_inventory import set_inventory_quantity_by_variant

logger = logging.getLogger(__name__)


async def sync_to_shopify(shopify_client=None):
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

            shopify_id = doc.get("shopify_id")
            hash_now = doc.get("hash")
            hash_prev = doc.get("last_synced_hash")

            # Create new product if no Shopify ID yet
            if not shopify_id:
                try:
                    await create_shopify_product(doc, shopify_client)
                    created += 1
                    logger.info(f"Created new Shopify product for eBay item {doc.get('_id', 'unknown')}")
                except Exception as e:
                    logger.error(f"Failed to create Shopify product for eBay item {doc.get('_id', 'unknown')}: {e}")
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
                try:
                    quantity = doc.get("quantity")
                    variant_id = doc.get("shopify_variant_id")
                    if variant_id is not None and quantity is not None:
                        await set_inventory_quantity_by_variant(int(variant_id), int(quantity), shopify_client)
                except Exception as e:
                    logger.error(
                        "Failed to set inventory for Shopify variant %s: %s",
                        doc.get("shopify_variant_id"),
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
        query = {}
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

    logger.info(f"✔ Shopify Sync Done → {created} created, {updated} updated, {skipped} skipped")
    return {"created": created, "updated": updated, "skipped": skipped}
