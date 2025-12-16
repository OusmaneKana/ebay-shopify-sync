import asyncio
from app.database.mongo import db


async def reset_shopify_links():
    """
    Clear Shopify IDs + last_synced_hash from all normalized products,
    so the sync will recreate products as new in Shopify.
    """
    result = await db.product_normalized.update_many(
        {},
        {
            "$unset": {
                "shopify_id": "",
                "shopify_variant_id": "",
                "last_synced_hash": "",
            }
        }
    )
    print(f"✔ Cleared Shopify links for {result.modified_count} products")


if __name__ == "__main__":
    asyncio.run(reset_shopify_links())
