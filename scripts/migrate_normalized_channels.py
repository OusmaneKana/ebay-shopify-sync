import asyncio
import os
import sys
from datetime import datetime, timezone

from pymongo import UpdateOne

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from app.database.mongo import db, close_mongo_client


BATCH_SIZE = 500


def _build_channels(doc: dict) -> dict:
    channels = dict(doc.get("channels") or {})

    shopify = dict(channels.get("shopify") or {})
    for key in [
        "shopify_id",
        "shopify_variant_id",
        "inventory_item_id",
        "location_id",
        "last_synced_hash",
    ]:
        value = doc.get(key)
        if value is not None and shopify.get(key) is None:
            shopify[key] = value
    if shopify:
        channels["shopify"] = shopify

    ebay = dict(channels.get("ebay") or {})
    if doc.get("ebay_posted_at") is not None and ebay.get("posted_at") is None:
        ebay["posted_at"] = doc.get("ebay_posted_at")

    if ebay.get("category") is None and isinstance(doc.get("ebay_category"), dict):
        ebay["category"] = doc.get("ebay_category")

    if ebay:
        channels["ebay"] = ebay

    return channels


async def main() -> None:
    total_scanned = 0
    total_modified = 0
    bulk_ops: list[UpdateOne] = []

    projection = {
        "channels": 1,
        "shopify_id": 1,
        "shopify_variant_id": 1,
        "inventory_item_id": 1,
        "location_id": 1,
        "last_synced_hash": 1,
        "ebay_posted_at": 1,
        "ebay_category": 1,
    }

    cursor = db.product_normalized.find({}, projection)
    async for doc in cursor:
        total_scanned += 1
        channels_before = doc.get("channels") or {}
        channels_after = _build_channels(doc)

        if channels_after != channels_before:
            bulk_ops.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "channels": channels_after,
                            "channel_migrated_at": datetime.now(timezone.utc),
                        }
                    },
                )
            )

        if len(bulk_ops) >= BATCH_SIZE:
            result = await db.product_normalized.bulk_write(bulk_ops, ordered=False)
            total_modified += result.modified_count
            bulk_ops = []

    if bulk_ops:
        result = await db.product_normalized.bulk_write(bulk_ops, ordered=False)
        total_modified += result.modified_count

    print(
        {
            "collection": "product_normalized",
            "scanned": total_scanned,
            "modified": total_modified,
        }
    )

    close_mongo_client()


if __name__ == "__main__":
    asyncio.run(main())
