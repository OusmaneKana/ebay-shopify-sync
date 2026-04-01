import argparse
import asyncio
from collections import Counter
from typing import Any, Dict, List

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.shopify.inventory_manager import get_inventory_levels


def make_shopify_client(env: str) -> ShopifyClient:
    if env == "prod":
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    return ShopifyClient()


def chunked(values: List[int], size: int) -> List[List[int]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


async def fetch_docs(limit: int | None) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {
        "inventory_item_id": {"$exists": True, "$ne": None},
        "location_id": {"$exists": True, "$ne": None},
        "quantity": {"$exists": True},
    }
    projection = {
        "_id": 1,
        "quantity": 1,
        "inventory_item_id": 1,
        "location_id": 1,
        "shopify_variant_id": 1,
        "shopify_id": 1,
    }

    cursor = db.product_normalized.find(query, projection)
    docs: List[Dict[str, Any]] = []
    async for doc in cursor:
        docs.append(doc)
        if limit is not None and len(docs) >= limit:
            break
    return docs


async def audit(env: str, limit: int | None, chunk_size: int) -> None:
    docs = await fetch_docs(limit)
    total_docs = len(docs)

    if total_docs == 0:
        print("TOTAL_DOCS 0")
        print("NO_DATA")
        return

    item_ids = sorted({int(d["inventory_item_id"]) for d in docs if d.get("inventory_item_id") is not None})
    client = make_shopify_client(env)

    levels_by_item: Dict[int, List[Dict[str, Any]]] = {}
    chunks = chunked(item_ids, chunk_size)

    for idx, ids_chunk in enumerate(chunks, start=1):
        partial = await get_inventory_levels(ids_chunk, shopify_client=client)
        levels_by_item.update(partial)
        if idx % 10 == 0 or idx == len(chunks):
            print(f"FETCH_PROGRESS {idx}/{len(chunks)}")

    missing_level = 0
    invalid_qty = 0
    matched = 0
    mismatched = 0

    delta_counter: Counter[int] = Counter()
    mismatches: List[Dict[str, Any]] = []

    for doc in docs:
        sku = str(doc.get("_id"))
        raw_qty = doc.get("quantity")
        item_id = int(doc.get("inventory_item_id"))
        loc_id = int(doc.get("location_id"))

        try:
            mongo_qty = int(raw_qty)
        except (TypeError, ValueError):
            invalid_qty += 1
            continue

        levels = levels_by_item.get(item_id, [])
        level = next((lv for lv in levels if int(lv.get("location_id", -1)) == loc_id), None)
        if level is None:
            missing_level += 1
            mismatches.append(
                {
                    "sku": sku,
                    "inventory_item_id": item_id,
                    "location_id": loc_id,
                    "mongo_qty": mongo_qty,
                    "shopify_qty": None,
                    "reason": "missing_inventory_level",
                }
            )
            continue

        shopify_qty = int(level.get("available", 0) or 0)

        if shopify_qty == mongo_qty:
            matched += 1
        else:
            mismatched += 1
            delta = shopify_qty - mongo_qty
            delta_counter[delta] += 1
            mismatches.append(
                {
                    "sku": sku,
                    "inventory_item_id": item_id,
                    "location_id": loc_id,
                    "mongo_qty": mongo_qty,
                    "shopify_qty": shopify_qty,
                    "delta": delta,
                    "reason": "qty_mismatch",
                }
            )

    print("ENV", env)
    print("TOTAL_DOCS", total_docs)
    print("UNIQUE_ITEM_IDS", len(item_ids))
    print("MATCHED", matched)
    print("MISMATCHED", mismatched)
    print("MISSING_LEVEL", missing_level)
    print("INVALID_QTY", invalid_qty)
    print("TOTAL_DISCREPANCIES", mismatched + missing_level)

    if delta_counter:
        top = delta_counter.most_common(10)
        print("TOP_DELTAS", top)

    if mismatches:
        print("SAMPLE_DISCREPANCIES_START")
        for row in mismatches[:50]:
            print(row)
        print("SAMPLE_DISCREPANCIES_END")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit Shopify live inventory vs Mongo product_normalized.quantity")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=50)
    args = parser.parse_args()

    asyncio.run(audit(env=args.env, limit=args.limit, chunk_size=args.chunk_size))
