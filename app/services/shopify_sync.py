from app.database.mongo import db
from app.shopify.create_product import create_shopify_product
from app.shopify.update_product import update_shopify_product

async def sync_to_shopify():
    print("▶ Syncing normalized products to Shopify...")

    cursor = db.product_normalized.find({})
    created = 0
    updated = 0
    skipped = 0

    async for doc in cursor:
        shopify_id = doc.get("shopify_id")
        hash_now = doc["hash"]
        hash_prev = doc.get("last_synced_hash")

        # New product → CREATE
        if not shopify_id:
            await create_shopify_product(doc)
            await db.product_normalized.update_one(
                {"_id": doc["_id"]},
                {"$set": {"last_synced_hash": hash_now}}
            )
            created += 1
            continue

        # Same content → skip
        if hash_now == hash_prev:
            skipped += 1
            continue

        # Changed → UPDATE
        old_doc = doc
        await update_shopify_product(old_doc, doc)
        await db.product_normalized.update_one(
            {"_id": doc["_id"]},
            {"$set": {"last_synced_hash": hash_now}}
        )
        updated += 1

    print(f"✔ Shopify Sync Done → {created} created, {updated} updated, {skipped} skipped")

    return {
        "created": created,
        "updated": updated,
        "skipped": skipped
    }
