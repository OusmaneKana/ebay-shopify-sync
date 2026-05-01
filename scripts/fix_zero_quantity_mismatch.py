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
    """Create a Shopify client for the production environment."""
    if env != "prod":
        raise ValueError("Only the prod Shopify environment is supported")
    logger.info("Using Shopify PROD credentials")
    return ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD,
    )


async def fix_zero_quantity_mismatches(
    limit: Optional[int] = None,
    env: str = "prod",
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Fix products where raw shows sold-out but normalized+Shopify still show quantity 1.

    Criteria:
    - product_raw.raw.QuantityAvailable == 0
    - product_raw.raw.QuantityTotal == 1
    - product_normalized.quantity == 1

    Actions (when not in dry_run):
    - Set product_normalized.quantity -> 0
    - For linked Shopify variants, set inventory to 0 via Shopify API.
    """

    shopify_client = _make_shopify_client(env)

    query = {
        "raw.QuantityAvailable": 0,
        "raw.QuantityTotal": 1,
    }

    projection = {
        "_id": 1,
        "sku": 1,
        "raw.QuantityAvailable": 1,
        "raw.QuantityTotal": 1,
    }

    cursor = db.product_raw.find(query, projection)

    total_raw_checked = 0
    candidates = 0
    fixed_normalized = 0
    updated_shopify = 0
    errors = 0

    async for raw_doc in cursor:
        if limit is not None and total_raw_checked >= limit:
            break

        total_raw_checked += 1

        sku = raw_doc.get("_id") or raw_doc.get("sku")
        if not sku:
            continue

        norm = await db.product_normalized.find_one(
            {"_id": sku},
            {"quantity": 1, "shopify_variant_id": 1, "shopify_id": 1},
        )
        if not norm:
            continue

        normalized_qty = norm.get("quantity")
        if normalized_qty != 1:
            # Only care about cases where normalized still shows 1
            continue

        candidates += 1
        logger.info(
            "Mismatch SKU %s: raw.QuantityAvailable=0, raw.QuantityTotal=1, normalized.quantity=%s",
            sku,
            normalized_qty,
        )

        if dry_run:
            # In dry-run mode we only log the mismatches
            continue

        # 1) Fix normalized quantity in Mongo
        try:
            result = await db.product_normalized.update_one(
                {"_id": sku},
                {"$set": {"quantity": 0}},
            )
            if getattr(result, "modified_count", 0) > 0:
                fixed_normalized += 1
                logger.info("Updated product_normalized.quantity -> 0 for SKU %s", sku)
        except Exception as e:  # pragma: no cover - defensive
            errors += 1
            logger.error("Failed to update normalized quantity for SKU %s: %s", sku, e)
            continue

        # 2) Push inventory override to Shopify, if we have a linked variant
        variant_id = norm.get("shopify_variant_id")
        if not variant_id:
            logger.warning(
                "SKU %s has no shopify_variant_id; Mongo fixed but Shopify inventory not updated",
                sku,
            )
            continue

        try:
            ok = await set_inventory_quantity_by_variant(int(variant_id), 0, shopify_client)
            if ok:
                updated_shopify += 1
                logger.info("Set Shopify variant %s inventory -> 0 for SKU %s", variant_id, sku)
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

    summary: Dict[str, Any] = {
        "total_raw_checked": total_raw_checked,
        "mismatched_candidates": candidates,
        "fixed_normalized": fixed_normalized,
        "updated_shopify": updated_shopify,
        "errors": errors,
        "dry_run": dry_run,
        "env": env,
    }

    logger.info("Done. Summary: %s", summary)
    return summary


async def _async_main(args: argparse.Namespace) -> None:
    summary = await fix_zero_quantity_mismatches(
        limit=args.limit,
        env=args.env,
        dry_run=args.dry_run,
    )
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fix products where product_raw shows QuantityAvailable=0 and QuantityTotal=1, "
            "but product_normalized.quantity is still 1, and override Shopify inventory."
        )
    )
    parser.add_argument(
        "--env",
        choices=["prod"],
        default="prod",
        help="Which Shopify environment to use (production only)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of raw documents to scan",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report mismatches; do not modify Mongo or Shopify",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(name)s: %(message)s",
    )

    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
