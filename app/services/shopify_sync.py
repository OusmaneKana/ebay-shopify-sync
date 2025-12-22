from app.database.mongo import db
from app.shopify.create_product import create_shopify_product
from app.shopify.update_product import update_shopify_product


async def sync_to_shopify(shopify_client=None, limit=None):
    print("▶ Syncing normalized products to Shopify..." + (f" (limit: {limit})" if limit else ""))

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
            await create_shopify_product(doc, shopify_client)
            created += 1
            continue

        if hash_now == hash_prev:
            skipped += 1
            continue

        await update_shopify_product(doc, doc, shopify_client)
        await db.product_normalized.update_one(
            {"_id": doc["_id"]},
            {"$set": {"last_synced_hash": hash_now}}
        )
        updated += 1

    print(f"✔ Shopify Sync Done → {created} created, {updated} updated, {skipped} skipped")
    return {"created": created, "updated": updated, "skipped": skipped}
