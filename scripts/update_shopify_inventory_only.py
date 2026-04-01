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
from app.shopify.update_inventory import set_inventory_quantity_by_variant, set_inventory_from_mongo
from app.shopify.inventory_manager import set_inventory_quantity_by_item_id
from app.services.shopify_exclusions import BLOCKED_SHOPIFY_TAGS, has_blocked_shopify_tag
from app.services.inventory_zero_guard import was_already_zeroed, mark_zeroed, clear_zeroed


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
    allow_zero_updates: bool = False,
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
        # Never touch excluded items.
        "tags": {"$nin": list(BLOCKED_SHOPIFY_TAGS)},
    }
    if only_zero:
        # Hard override for only zero-quantity items
        query["quantity"] = 0

    projection = {
        "_id": 1,
        "tags": 1,
        "quantity": 1,
        "shopify_variant_id": 1,
        "shopify_id": 1,
        "inventory_item_id": 1,
        "location_id": 1,
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
    skipped_already_zeroed = 0
    skipped_zero_blocked = 0
    errors = 0

    # Use bounded concurrency for Shopify calls; the ShopifyClient itself
    # also has an AsyncLimiter to respect API rate limits.
    sem = asyncio.Semaphore(max_concurrency)

    async def process_doc(doc: Dict[str, Any]) -> None:
        nonlocal attempted, updated_ok, skipped_invalid, skipped_already_zeroed, skipped_zero_blocked, errors

        sku = doc.get("_id")
        if has_blocked_shopify_tag(doc.get("tags")):
            skipped_invalid += 1
            logger.info("[INVENTORY] Skipped excluded item | sku=%s | blocked_tags=%s", sku, sorted(BLOCKED_SHOPIFY_TAGS))
            return
        variant_id = doc.get("shopify_variant_id")
        raw_qty = doc.get("quantity")
        inventory_item_id = doc.get("inventory_item_id")
        location_id = doc.get("location_id")

        if not variant_id:
            skipped_invalid += 1
            logger.warning("[INVENTORY] Skipped - no variant_id | sku=%s", sku)
            return

        try:
            qty = int(raw_qty) if raw_qty is not None else 0
        except (TypeError, ValueError):
            skipped_invalid += 1
            logger.warning("[INVENTORY] Skipped - invalid quantity | sku=%s | qty=%r", sku, raw_qty)
            return

        if qty == 0 and not allow_zero_updates:
            skipped_zero_blocked += 1
            logger.warning(
                "[INVENTORY] Blocked zero update (safety) | sku=%s | variant_id=%s | pass allow_zero_updates=True to permit",
                sku,
                variant_id,
            )
            return

        attempted += 1
        shopify_id = doc.get("shopify_id")
        logger.info(
            "[INVENTORY] Processing | sku=%s | shopify_id=%s | variant_id=%s | target_qty=%s",
            sku,
            shopify_id,
            variant_id,
            qty,
        )

        # If this is a zero-qty target and we've already successfully applied
        # the same zeroing operation before, skip the Shopify API call.
        if qty == 0 and not dry_run:
            try:
                already = await was_already_zeroed(
                    env=env,
                    sku=str(sku) if sku is not None else None,
                    variant_id=int(variant_id) if variant_id is not None else None,
                    inventory_item_id=int(inventory_item_id) if inventory_item_id is not None else None,
                    location_id=int(location_id) if location_id is not None else None,
                )
                if already:
                    skipped_already_zeroed += 1
                    logger.info(
                        "[INVENTORY] Skipped (already zeroed) | sku=%s | variant_id=%s | item_id=%s | location_id=%s",
                        sku,
                        variant_id,
                        inventory_item_id,
                        location_id,
                    )
                    return
            except Exception as e:  # pragma: no cover - defensive
                # If the guard lookup fails, proceed with the update (safer).
                logger.debug("[INVENTORY] Zero-guard lookup failed | sku=%s | error=%s", sku, e)

        if dry_run:
            logger.info("[INVENTORY] [DRY-RUN] Skipped | sku=%s | shopify_id=%s | variant_id=%s", sku, shopify_id, variant_id)
            return

        async with sem:
            try:
                # PREFERRED: Use inventory_item_id + location_id (no variant fetch)
                if inventory_item_id and location_id:
                    logger.debug(
                        "[INVENTORY] Using optimized method | sku=%s | inventory_item=%s | location=%s",
                        sku,
                        inventory_item_id,
                        location_id,
                    )
                    ok = await set_inventory_quantity_by_item_id(
                        int(inventory_item_id),
                        int(location_id),
                        qty,
                        shopify_client,
                    )
                else:
                    # FALLBACK: Use variant method (requires variant fetch)
                    logger.debug(
                        "[INVENTORY] Using fallback method | sku=%s | variant_id=%s (no inventory_item_id)",
                        sku,
                        variant_id,
                    )
                    ok = await set_inventory_quantity_by_variant(int(variant_id), qty, shopify_client)
                
                if ok:
                    updated_ok += 1
                    logger.info(
                        "[INVENTORY] ✓ Updated | sku=%s | shopify_id=%s | variant_id=%s | qty=%s",
                        sku,
                        shopify_id,
                        variant_id,
                        qty,
                    )

                    if qty == 0:
                        try:
                            await mark_zeroed(
                                env=env,
                                sku=str(sku) if sku is not None else None,
                                variant_id=int(variant_id) if variant_id is not None else None,
                                inventory_item_id=int(inventory_item_id) if inventory_item_id is not None else None,
                                location_id=int(location_id) if location_id is not None else None,
                                source="inventory_only",
                            )
                        except Exception as e:  # pragma: no cover - defensive
                            logger.debug("[INVENTORY] Failed to mark zeroed | sku=%s | error=%s", sku, e)
                    else:
                        try:
                            await clear_zeroed(
                                env=env,
                                sku=str(sku) if sku is not None else None,
                                variant_id=int(variant_id) if variant_id is not None else None,
                                inventory_item_id=int(inventory_item_id) if inventory_item_id is not None else None,
                                location_id=int(location_id) if location_id is not None else None,
                                source="inventory_only_positive_restore",
                            )
                        except Exception as e:  # pragma: no cover - defensive
                            logger.debug("[INVENTORY] Failed to clear zeroed guard | sku=%s | error=%s", sku, e)
                else:
                    errors += 1
                    logger.error(
                        "[INVENTORY] ✗ Failed to set inventory | sku=%s | shopify_id=%s | variant_id=%s | target_qty=%s",
                        sku,
                        shopify_id,
                        variant_id,
                        qty,
                    )
            except Exception as e:  # pragma: no cover - defensive
                errors += 1
                logger.error(
                    "[INVENTORY] ✗ Exception | sku=%s | shopify_id=%s | variant_id=%s | target_qty=%s | error=%s",
                    sku,
                    shopify_id,
                    variant_id,
                    qty,
                    e,
                )

    # Kick off concurrent processing
    tasks = [asyncio.create_task(process_doc(d)) for d in docs]
    if tasks:
        await asyncio.gather(*tasks)

    success_rate = (updated_ok / attempted * 100) if attempted > 0 else 0.0
    summary: Dict[str, Any] = {
        "total_docs": total_docs,
        "attempted": attempted,
        "updated_ok": updated_ok,
        "skipped_invalid": skipped_invalid,
        "skipped_already_zeroed": skipped_already_zeroed,
        "skipped_zero_blocked": skipped_zero_blocked,
        "errors": errors,
        "success_rate": f"{success_rate:.1f}%",
        "dry_run": dry_run,
        "env": env,
        "only_zero": only_zero,
        "allow_zero_updates": allow_zero_updates,
        "max_concurrency": max_concurrency,
    }

    if errors > 0:
        logger.warning(
            "[INVENTORY] ✗ COMPLETED WITH ERRORS | attempted=%s | ok=%s | errors=%s | skipped=%s | success=%s",
            attempted,
            updated_ok,
            errors,
            skipped_invalid,
            f"{success_rate:.1f}%",
        )
    else:
        logger.info(
            "[INVENTORY] ✓ Completed successfully | attempted=%s | ok=%s | skipped=%s",
            attempted,
            updated_ok,
            skipped_invalid,
        )

    return summary


async def _async_main(args: argparse.Namespace) -> None:
    summary = await update_shopify_inventory_only(
        limit=args.limit,
        env=args.env,
        only_zero=args.only_zero,
        allow_zero_updates=args.allow_zero_updates,
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
        "--allow-zero-updates",
        action="store_true",
        help="Allow setting inventory to 0 (disabled by default as a safety guard)",
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
