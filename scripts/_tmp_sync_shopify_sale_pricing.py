"""Sync Shopify variant price + compare_at_price from normalized sale-control fields."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any, Dict

from app.config import settings
from app.database.mongo import db
from app.services.channel_utils import get_shopify_field
from app.services.shopify_sale_pricing import resolve_shopify_variant_pricing
from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)


def make_client(env: str) -> ShopifyClient:
    if env == "prod":
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    return ShopifyClient()


async def sync_sale_pricing(env: str = "prod") -> Dict[str, Any]:
    client = make_client(env)

    query = {
        "$or": [
            {"shopify_variant_id": {"$exists": True, "$ne": None}},
            {"channels.shopify.shopify_variant_id": {"$exists": True, "$ne": None}},
        ],
        "$or": [
            {"sale_active": {"$exists": True}},
            {"channels.shopify.sale_active": {"$exists": True}},
            {"compare_at_price": {"$exists": True}},
            {"channels.shopify.compare_at_price": {"$exists": True}},
            {"tags": {"$elemMatch": {"$regex": r"^discount_", "$options": "i"}},},
        ],
    }

    projection = {
        "_id": 1,
        "price": 1,
        "compare_at_price": 1,
        "discount_percent": 1,
        "sale_active": 1,
        "sale_start": 1,
        "sale_end": 1,
        "channels.shopify": 1,
        "shopify_variant_id": 1,
    }

    cursor = db.product_normalized.find(query, projection)

    processed = 0
    updated = 0
    failed = 0
    skipped = 0

    async for doc in cursor:
        variant_id = get_shopify_field(doc, "shopify_variant_id")
        if not variant_id:
            skipped += 1
            continue

        try:
            vid = int(variant_id)
        except (TypeError, ValueError):
            skipped += 1
            continue

        pricing = resolve_shopify_variant_pricing(doc)
        payload = {
            "variant": {
                "id": vid,
                "price": pricing["price"],
                "compare_at_price": pricing["compare_at_price"],
            }
        }

        processed += 1
        try:
            await client.put(f"variants/{vid}.json", payload)
            updated += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.error("Failed variant %s (sku=%s): %s", vid, doc.get("_id"), exc)

    return {
        "env": env,
        "processed": processed,
        "updated": updated,
        "failed": failed,
        "skipped": skipped,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Shopify sale pricing fields from normalized DB")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    summary = asyncio.run(sync_sale_pricing(env=args.env))

    print(f"env={summary['env']}")
    print(f"processed={summary['processed']}")
    print(f"updated={summary['updated']}")
    print(f"failed={summary['failed']}")
    print(f"skipped={summary['skipped']}")


if __name__ == "__main__":
    main()
