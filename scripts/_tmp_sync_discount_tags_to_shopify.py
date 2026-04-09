import argparse
import asyncio
import logging
from typing import Any, Dict, Set

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)


def _make_shopify_client(env: str) -> ShopifyClient:
    if env == "prod":
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    return ShopifyClient()


async def sync_discount_tags_only(env: str = "dev") -> Dict[str, Any]:
    shopify_client = _make_shopify_client(env)

    query = {
        "shopify_id": {"$exists": True, "$ne": None},
        "tags": {"$elemMatch": {"$regex": r"^discount_", "$options": "i"}},
    }
    projection = {
        "_id": 1,
        "shopify_id": 1,
        "category": 1,
        "tags": 1,
    }

    cursor = db.product_normalized.find(query, projection)

    product_map: Dict[int, Dict[str, Any]] = {}
    async for doc in cursor:
        pid_raw = doc.get("shopify_id")
        try:
            pid = int(pid_raw)
        except (TypeError, ValueError):
            continue

        category = doc.get("category")
        tags = doc.get("tags") or []

        if pid not in product_map:
            product_map[pid] = {
                "category": category,
                "tags": set(tags),
            }
        else:
            if category and not product_map[pid].get("category"):
                product_map[pid]["category"] = category
            product_map[pid]["tags"].update(tags)

    processed = 0
    updated = 0
    failed = 0

    for pid, pdata in product_map.items():
        processed += 1
        category = pdata.get("category")
        tags_set: Set[str] = pdata.get("tags", set())

        tag_list = []
        if category:
            tag_list.append(category)
        tag_list.extend(tags_set)
        tags_str = ", ".join(sorted(set(tag_list)))

        try:
            await shopify_client.put(
                f"products/{pid}.json",
                {
                    "product": {
                        "id": pid,
                        "tags": tags_str,
                    }
                },
            )
            updated += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.error("Failed product %s: %s", pid, exc)

    return {
        "env": env,
        "products_with_discount_tags": len(product_map),
        "processed": processed,
        "updated": updated,
        "failed": failed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync discount tags only to Shopify product tags")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    summary = asyncio.run(sync_discount_tags_only(env=args.env))

    print(f"env={summary['env']}")
    print(f"products_with_discount_tags={summary['products_with_discount_tags']}")
    print(f"processed={summary['processed']}")
    print(f"updated={summary['updated']}")
    print(f"failed={summary['failed']}")


if __name__ == "__main__":
    main()
