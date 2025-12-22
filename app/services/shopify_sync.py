import logging
from app.database.mongo import db
from app.shopify.create_product import create_shopify_product
from app.shopify.update_product import update_shopify_product

logger = logging.getLogger(__name__)


async def sync_to_shopify(shopify_client=None, limit=None):
    logger.info("▶ Syncing normalized products to Shopify..." + (f" (limit: {limit})" if limit else ""))

    cursor = db.product_normalized.find({})
    if limit:
        cursor = cursor.limit(limit)

    created = 0
    updated = 0
    skipped = 0

    async for doc in cursor:
        shopify_id = doc.get("shopify_id")
        hash_now = doc.get("hash")
        hash_prev = doc.get("last_synced_hash")

        if not shopify_id:
            try:
                await create_shopify_product(doc, shopify_client)
                created += 1
                logger.info(f"Created new Shopify product for eBay item {doc.get('item_id', 'unknown')}")
            except Exception as e:
                logger.error(f"Failed to create Shopify product for eBay item {doc.get('item_id', 'unknown')}: {e}")
            continue

        if hash_now == hash_prev:
            skipped += 1
            continue

        try:
            await update_shopify_product(doc, doc, shopify_client)
            await db.product_normalized.update_one(
                {"_id": doc["_id"]},
                {"$set": {"last_synced_hash": hash_now}}
            )
            updated += 1
            logger.info(f"Updated Shopify product {shopify_id} for eBay item {doc.get('item_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to update Shopify product {shopify_id} for eBay item {doc.get('item_id', 'unknown')}: {e}")

    logger.info(f"✔ Shopify Sync Done → {created} created, {updated} updated, {skipped} skipped")
    return {"created": created, "updated": updated, "skipped": skipped}
