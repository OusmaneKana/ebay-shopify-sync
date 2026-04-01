import argparse
import asyncio
from typing import Any, Dict, List

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.shopify.inventory_manager import get_inventory_levels, set_inventory_quantity_by_item_id


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
    }

    cursor = db.product_normalized.find(query, projection)
    docs: List[Dict[str, Any]] = []
    async for doc in cursor:
        docs.append(doc)
        if limit is not None and len(docs) >= limit:
            break
    return docs


async def load_levels(client: ShopifyClient, docs: List[Dict[str, Any]], chunk_size: int) -> Dict[int, List[Dict[str, Any]]]:
    item_ids = sorted({int(d["inventory_item_id"]) for d in docs if d.get("inventory_item_id") is not None})
    levels_by_item: Dict[int, List[Dict[str, Any]]] = {}
    chunks = chunked(item_ids, chunk_size)
    for idx, ids_chunk in enumerate(chunks, start=1):
        partial = await get_inventory_levels(ids_chunk, shopify_client=client)
        levels_by_item.update(partial)
        if idx % 10 == 0 or idx == len(chunks):
            print(f"FETCH_PROGRESS {idx}/{len(chunks)}")
    return levels_by_item


def build_mismatches(docs: List[Dict[str, Any]], levels_by_item: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    mismatches: List[Dict[str, Any]] = []
    for doc in docs:
        sku = str(doc.get("_id"))
        item_id = int(doc.get("inventory_item_id"))
        loc_id = int(doc.get("location_id"))

        try:
            mongo_qty = int(doc.get("quantity"))
        except (TypeError, ValueError):
            continue

        levels = levels_by_item.get(item_id, [])
        level = next((lv for lv in levels if int(lv.get("location_id", -1)) == loc_id), None)
        if level is None:
            continue

        shopify_qty = int(level.get("available", 0) or 0)
        if shopify_qty != mongo_qty:
            mismatches.append(
                {
                    "sku": sku,
                    "inventory_item_id": item_id,
                    "location_id": loc_id,
                    "mongo_qty": mongo_qty,
                    "shopify_qty": shopify_qty,
                }
            )
    return mismatches


async def apply_fix(client: ShopifyClient, mismatches: List[Dict[str, Any]], max_concurrency: int) -> Dict[str, int]:
    sem = asyncio.Semaphore(max_concurrency)
    success = 0
    failed = 0

    async def _fix_one(row: Dict[str, Any]) -> None:
        nonlocal success, failed
        async with sem:
            ok = await set_inventory_quantity_by_item_id(
                inventory_item_id=int(row["inventory_item_id"]),
                location_id=int(row["location_id"]),
                quantity=int(row["mongo_qty"]),
                shopify_client=client,
            )
            if ok:
                success += 1
            else:
                failed += 1

    tasks = [asyncio.create_task(_fix_one(row)) for row in mismatches]
    if tasks:
        await asyncio.gather(*tasks)

    return {"success": success, "failed": failed}


async def main(env: str, limit: int | None, chunk_size: int, max_concurrency: int) -> None:
    client = make_shopify_client(env)

    docs = await fetch_docs(limit)
    print("TOTAL_DOCS", len(docs))

    levels_before = await load_levels(client, docs, chunk_size)
    mismatches_before = build_mismatches(docs, levels_before)
    print("MISMATCHES_BEFORE", len(mismatches_before))

    if not mismatches_before:
        print("NOTHING_TO_FIX")
        return

    fix_result = await apply_fix(client, mismatches_before, max_concurrency)
    print("FIX_SUCCESS", fix_result["success"])
    print("FIX_FAILED", fix_result["failed"])

    levels_after = await load_levels(client, docs, chunk_size)
    mismatches_after = build_mismatches(docs, levels_after)
    print("MISMATCHES_AFTER", len(mismatches_after))

    if mismatches_after:
        print("SAMPLE_REMAINING_START")
        for row in mismatches_after[:30]:
            print(row)
        print("SAMPLE_REMAINING_END")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix Shopify inventory discrepancies from Mongo normalized quantities")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=50)
    parser.add_argument("--max-concurrency", type=int, default=10)
    args = parser.parse_args()

    asyncio.run(main(args.env, args.limit, args.chunk_size, args.max_concurrency))
