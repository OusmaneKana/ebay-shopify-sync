from app.database.mongo import db
from app.ebay.fetch_products import fetch_all_ebay_products
from datetime import datetime, timezone


def _parse_ebay_datetime(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # eBay commonly returns ISO 8601 timestamps ending with 'Z'
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1] + "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


async def sync_ebay_raw_to_mongo():
    """Sync active eBay listings into product_raw.

    - Upserts all currently active items returned by GetMyeBaySelling.
    - For any existing SKU in product_raw that is *not* returned in this run,
      set raw.QuantityAvailable to 0 so downstream normalization/Shopify
      can treat it as sold/ended.
    """

    items = await fetch_all_ebay_products()
    if not items:
        # If the fetch returned no items at all, avoid blindly zeroing the
        # entire catalog (could be an API error). Keep previous quantities.
        return {"inserted_or_updated": 0, "set_to_zero": 0}

    count = 0
    current_skus: set[str] = set()

    for item in items:
        sku = item.get("sku")
        if not sku:
            continue

        current_skus.add(sku)

        # if your fetch_products already puts the full ebay item in item["raw"]
        raw_doc = item.get("raw", item)

        posted_at = _parse_ebay_datetime(raw_doc.get("ListingStartTime"))

        await db.product_raw.update_one(
            {"_id": sku},
            {
                "$set": {
                    "sku": sku,
                    "raw": raw_doc,
                    "ebay_posted_at": posted_at,
                }
            },
            upsert=True,
        )
        count += 1

    # Any SKU in product_raw that is not in the latest active set is no longer
    # returned by GetMyeBaySelling (likely sold or ended). Mark it as having
    # zero available quantity so normalization will propagate quantity=0.
    zeroed = 0
    if current_skus:
        result = await db.product_raw.update_many(
            {"_id": {"$nin": list(current_skus)}},
            {"$set": {"raw.QuantityAvailable": 0}},
        )
        zeroed = getattr(result, "modified_count", 0)

    return {"inserted_or_updated": count, "set_to_zero": zeroed}
