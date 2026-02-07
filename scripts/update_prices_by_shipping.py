"""
Script to update prices and tags in normalized products based on shipping cost logic.

Usage: python -m scripts.update_prices_by_shipping [--limit N]
"""

import asyncio
import logging
import sys
import time
from argparse import ArgumentParser
from app.database.mongo import db

logger = logging.getLogger(__name__)

# Configure logging for the script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def update_prices_and_tags(limit=None):
    """
    Update prices and tags in normalized products based on shipping cost logic.
    """
    start_time = time.time()
    logger.info("▶ Starting price and tag update based on shipping cost...")

    batch_size = 100
    last_id = None
    updated_count = 0
    skipped_count = 0
    error_count = 0

    while True:
        query = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_normalized.find(query).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for doc in cursor:
            batch_docs.append(doc)
            last_id = doc["_id"]

        if not batch_docs:
            break

        for doc in batch_docs:
            if limit and updated_count >= limit:
                logger.info(f"Reached limit of {limit}")
                break

            try:
                sku = doc.get("_id")
                price = doc.get("price")
                # Ensure price is a float for arithmetic
                try:
                    price = float(price)
                except (TypeError, ValueError):
                    price = 0.0
                tags = set(doc.get("tags", []))
                shipping = doc.get("shipping", [])
                shipping_cost = None

                for opt in shipping:
                    if opt.get("type") == "domestic":
                        try:
                            shipping_cost = float(opt.get("cost", 0))
                            break
                        except (ValueError, TypeError):
                            continue

                new_price = price
                updated = False

                if shipping_cost == 8.0:
                    new_price = price + 10
                    tags.add("free_shipping")
                    updated = True
                elif shipping_cost == 14.0:
                    new_price = price + 15
                    tags.add("free_shipping")
                    updated = True
                elif shipping_cost == 18.0:
                    new_price = price + 20
                    tags.add("free_shipping")
                    updated = True

                if updated:
                    await db.product_normalized.update_one(
                        {"_id": sku},
                        {"$set": {"price": new_price, "tags": sorted(tags)}}
                    )
                    logger.info(f"✔ Updated SKU {sku}: price={new_price}, tags={sorted(tags)}")
                    updated_count += 1
                else:
                    skipped_count += 1

            except Exception as e:
                logger.error(f"Failed to update SKU {doc.get('_id')}: {e}", exc_info=True)
                error_count += 1

        if limit and updated_count >= limit:
            break

    elapsed_time = time.time() - start_time
    logger.info(f"\n✔ Price/tag update complete:")
    logger.info(f"  Updated: {updated_count}")
    logger.info(f"  Skipped: {skipped_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info(f"  Time: {elapsed_time:.2f}s")

    return {
        "updated": updated_count,
        "skipped": skipped_count,
        "errors": error_count,
    }

if __name__ == "__main__":
    parser = ArgumentParser(description="Update prices and tags based on shipping cost")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of products to update")
    args = parser.parse_args()

    result = asyncio.run(update_prices_and_tags(limit=args.limit))
    sys.exit(0 if result["errors"] == 0 else 1)
