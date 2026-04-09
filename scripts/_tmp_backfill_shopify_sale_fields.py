"""Backfill sale-control fields in product_normalized from discount tags.

For docs with a discount_* tag and Shopify linkage:
- sale_active = True
- discount_percent = smallest discount tag value
- compare_at_price = round(price / (1 - discount_percent/100), 2)
- sale_start / sale_end kept as-is unless absent (set to None only when missing)

Fields are dual-written to top-level and channels.shopify.* for rollout safety.
"""

from __future__ import annotations

import re
from typing import Any

from pymongo import MongoClient

from app.config import settings
from app.services.channel_utils import set_shopify_fields_set

DISCOUNT_RE = re.compile(r"^discount_(\d+)$", re.IGNORECASE)


def extract_discount_percent(tags: list[Any]) -> int | None:
    values: list[int] = []
    for t in tags or []:
        if not isinstance(t, str):
            continue
        m = DISCOUNT_RE.match(t.strip())
        if m:
            values.append(int(m.group(1)))
    if not values:
        return None
    return min(values)


def main() -> None:
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]
    col = db.product_normalized

    query = {
        "tags": {"$elemMatch": {"$regex": r"^discount_", "$options": "i"}},
        "$or": [
            {"shopify_id": {"$exists": True, "$ne": None}},
            {"channels.shopify.shopify_id": {"$exists": True, "$ne": None}},
        ],
    }
    proj = {
        "_id": 1,
        "price": 1,
        "tags": 1,
        "sale_start": 1,
        "sale_end": 1,
        "channels.shopify.sale_start": 1,
        "channels.shopify.sale_end": 1,
    }

    scanned = 0
    updated = 0
    skipped_no_price = 0
    skipped_bad_discount = 0

    for doc in col.find(query, proj):
        scanned += 1

        discount_percent = extract_discount_percent(doc.get("tags") or [])
        if discount_percent is None or not (0 < discount_percent < 100):
            skipped_bad_discount += 1
            continue

        raw_price = doc.get("price")
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            skipped_no_price += 1
            continue

        if price <= 0:
            skipped_no_price += 1
            continue

        compare_at = round(price / (1.0 - (discount_percent / 100.0)), 2)
        if compare_at <= price:
            skipped_bad_discount += 1
            continue

        existing_shopify = (doc.get("channels") or {}).get("shopify") or {}
        sale_start = existing_shopify.get("sale_start", doc.get("sale_start"))
        sale_end = existing_shopify.get("sale_end", doc.get("sale_end"))

        update_data = {
            "sale_active": True,
            "discount_percent": discount_percent,
            "compare_at_price": compare_at,
            "sale_start": sale_start,
            "sale_end": sale_end,
        }

        col.update_one({"_id": doc["_id"]}, {"$set": set_shopify_fields_set(update_data)})
        updated += 1

    print(f"mongo_db={settings.MONGO_DB}")
    print(f"scanned={scanned}")
    print(f"updated={updated}")
    print(f"skipped_no_price={skipped_no_price}")
    print(f"skipped_bad_discount={skipped_bad_discount}")


if __name__ == "__main__":
    main()
