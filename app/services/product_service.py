from app.database.mongo import db
from app.ebay.fetch_products import fetch_all_ebay_products

from app.database.mongo import db
from app.ebay.fetch_products import fetch_all_ebay_products

async def sync_ebay_raw_to_mongo():
    items = await fetch_all_ebay_products()
    if not items:
        return {"inserted_or_updated": 0}

    count = 0

    for item in items:
        sku = item.get("sku")
        if not sku:
            continue

        # if your fetch_products already puts the full ebay item in item["raw"]
        raw_doc = item.get("raw", item)

        await db.product_raw.update_one(
            {"_id": sku},
            {
                "$set": {
                    "sku": sku,
                    "raw": raw_doc,
                }
            },
            upsert=True,
        )
        count += 1

    return {"inserted_or_updated": count}
