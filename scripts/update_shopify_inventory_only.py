import os
import sys
import asyncio
import argparse
import logging
from typing import Any, Dict, Optional


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.shopify.update_inventory import set_inventory_quantity_by_variant


logger = logging.getLogger(__name__)


def _make_shopify_client(env: str) -> ShopifyClient:
    """Create a Shopify client for the given environment (dev or prod)."""
    if env == "prod":
        logger.info("Using Shopify PROD credentials")
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    logger.info("Using Shopify DEV credentials")
    return ShopifyClient()


async def update_shopify_inventory_only(
    limit: Optional[int] = None,
    env: str = "dev",
    only_zero: bool = False,
    dry_run: bool = False,
    max_concurrency: int = 20,
) -> Dict[str, Any]:
    """Force Shopify variant inventory to match product_normalized.quantity.

    - Reads quantity and shopify_variant_id from product_normalized
    - Calls set_inventory_quantity_by_variant for each linked variant
    - Does NOT touch product details, metafields, or hashes
    """

    shopify_client = _make_shopify_client(env)

    query: Dict[str, Any] = {
        "shopify_variant_id": {"$exists": True, "$ne": None},
        "quantity": {"$exists": True},
    }
    if only_zero:
        # Hard override for only zero-quantity items
        query["quantity"] = 0

    projection = {
        "_id": 1,
        "quantity": 1,
        "shopify_variant_id": 1,
        "shopify_id": 1,
    }

    cursor = db.product_normalized.find(query, projection)

    # Preload up to `limit` docs so we can process them concurrently
    docs: list[Dict[str, Any]] = []
    async for doc in cursor:
        docs.append(doc)
        if limit is not None and len(docs) >= limit:
            break

    total_docs = len(docs)
    attempted = 0
    updated_ok = 0
    skipped_invalid = 0
    errors = 0

    # Use bounded concurrency for Shopify calls; the ShopifyClient itself
    # also has an AsyncLimiter to respect API rate limits.
    sem = asyncio.Semaphore(max_concurrency)

    async def process_doc(doc: Dict[str, Any]) -> None:
        nonlocal attempted, updated_ok, skipped_invalid, errors

        sku = doc.get("_id")
        variant_id = doc.get("shopify_variant_id")
        raw_qty = doc.get("quantity")

        if not variant_id:
            skipped_invalid += 1
            logger.warning("SKU %s has no shopify_variant_id; skipping", sku)
            return

        try:
            qty = int(raw_qty) if raw_qty is not None else 0
        except (TypeError, ValueError):
            skipped_invalid += 1
            logger.warning("SKU %s has invalid quantity %r; skipping", sku, raw_qty)
            return

        attempted += 1
        logger.info(
            "Syncing inventory for SKU %s (variant %s) -> quantity %s",
            sku,
            variant_id,
            qty,
        )

        if dry_run:
            return

        async with sem:
            try:
                ok = await set_inventory_quantity_by_variant(int(variant_id), qty, shopify_client)
                if ok:
                    updated_ok += 1
                else:
                    errors += 1
                    logger.error(
                        "Failed to set Shopify inventory for variant %s (SKU %s)",
                        variant_id,
                        sku,
                    )
            except Exception as e:  # pragma: no cover - defensive
                errors += 1
                logger.error(
                    "Exception while setting Shopify inventory for variant %s (SKU %s): %s",
                    variant_id,
                    sku,
                    e,
                )

    # Kick off concurrent processing
    tasks = [asyncio.create_task(process_doc(d)) for d in docs]
    if tasks:
        await asyncio.gather(*tasks)

    summary: Dict[str, Any] = {
        "total_docs": total_docs,
        "attempted": attempted,
        "updated_ok": updated_ok,
        "skipped_invalid": skipped_invalid,
        "errors": errors,
        "dry_run": dry_run,
        "env": env,
        "only_zero": only_zero,
        "max_concurrency": max_concurrency,
    }

    logger.info("Inventory-only update finished. Summary: %s", summary)
    return summary


async def _async_main(args: argparse.Namespace) -> None:
    summary = await update_shopify_inventory_only(
        limit=args.limit,
        env=args.env,
        only_zero=args.only_zero,
        dry_run=args.dry_run,
        max_concurrency=args.max_concurrency,
    )
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Update ONLY Shopify inventory levels from Mongo product_normalized.quantity, "
            "without touching product details or metafields."
        )
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Which Shopify environment to use (default: dev)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of normalized documents to process",
    )
    parser.add_argument(
        "--only-zero",
        action="store_true",
        help="Only update items where normalized.quantity == 0",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only log actions; do not call Shopify API",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=20,
        help="Maximum number of concurrent Shopify inventory updates (bounded and still rate-limited)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(name)s: %(message)s",
    )

    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
