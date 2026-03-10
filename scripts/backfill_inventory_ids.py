"""
Backfill inventory IDs for all existing products.

Purpose:
  Populate inventory_item_id and location_id for all existing records
  in product_normalized that have shopify_variant_id but are missing
  these inventory IDs.

Algorithm:
  1. Find all products with shopify_variant_id but missing inventory_item_id
  2. Fetch variant data to extract inventory_item_id
  3. Fetch inventory_levels to find location_id
  4. Update MongoDB documents with both IDs
  5. Log progress and summary

Requirements:
  - async
  - rate-limit safe
  - skip records already populated
  - batch processing for efficiency
"""

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
from app.shopify.inventory_manager import get_inventory_item_from_variant, get_primary_location

logger = logging.getLogger(__name__)


def _make_shopify_client(env: str) -> ShopifyClient:
    """Create a Shopify client for the given environment (dev or prod)."""
    if env == "prod":
        logger.info("[BACKFILL] Using Shopify PROD credentials")
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    logger.info("[BACKFILL] Using Shopify DEV credentials")
    return ShopifyClient()


async def _get_location_id_from_existing_product(
) -> Optional[int]:
    """Try to get location_id from an existing product that already has it populated.
    
    This is a workaround for API permission issues when fetching locations directly.
    """
    logger.info("[BACKFILL] Attempting to get location_id from existing products...")
    
    # Query for ANY product that has location_id set
    query = {
        "location_id": {"$exists": True, "$ne": None},
    }
    projection = {"location_id": 1}
    
    doc = await db.product_normalized.find_one(query, projection)
    if doc:
        location_id = doc.get("location_id")
        logger.info("[BACKFILL] ✓ Found location_id=%s from existing product", location_id)
        return location_id
    
    return None


async def _infer_location_id_from_inventory_levels(
    shopify_client: ShopifyClient,
    inventory_item_ids: list[int],
    *,
    max_candidates: int = 10,
) -> Optional[int]:
    """Infer a usable location_id by querying inventory_levels for one item.

    This is a workaround when `GET locations.json` is blocked (missing `read_locations`).
    If the app has inventory read permissions, `GET inventory_levels.json` often still
    returns `location_id` in the payload.
    """
    candidates: list[int] = []
    seen: set[int] = set()
    for item_id in inventory_item_ids:
        if item_id is None:
            continue
        try:
            item_id_int = int(item_id)
        except (TypeError, ValueError):
            continue
        if item_id_int in seen:
            continue
        seen.add(item_id_int)
        candidates.append(item_id_int)
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        return None

    logger.info(
        "[BACKFILL] Attempting to infer location_id via inventory_levels (candidates=%d)...",
        len(candidates),
    )

    for item_id in candidates:
        try:
            resp = await shopify_client.get(
                "inventory_levels.json",
                params={"inventory_item_ids": str(item_id)},
            )
            levels = (resp or {}).get("inventory_levels") or []
            if not levels:
                logger.debug(
                    "[BACKFILL] inventory_levels empty for inventory_item_id=%s | resp=%s",
                    item_id,
                    resp,
                )
                continue
            location_id = levels[0].get("location_id")
            if location_id is not None:
                logger.info(
                    "[BACKFILL] ✓ Inferred location_id=%s from inventory_levels (inventory_item_id=%s)",
                    location_id,
                    item_id,
                )
                return int(location_id)
        except Exception:
            logger.debug(
                "[BACKFILL] inventory_levels lookup failed for inventory_item_id=%s",
                item_id,
                exc_info=True,
            )
            continue

    return None


async def backfill_inventory_ids(
    env: str = "dev",
    limit: Optional[int] = None,
    dry_run: bool = False,
    location_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Backfill inventory_item_id and location_id for existing products.
    
    Args:
        env: "dev" or "prod"
        limit: Maximum number of products to backfill (None = all)
        dry_run: If True, plan but don't update
        location_id: Optional location_id to use. If not provided, will try to:
                     1. Get from existing products in MongoDB
                     2. Fetch from Shopify locations API (if permitted)
        
    Returns: Summary dict with statistics
    """
    
    logger.info("=" * 80)
    logger.info("[BACKFILL] Starting backfill process")
    logger.info("[BACKFILL] env=%s | limit=%s | dry_run=%s | location_id=%s", env, limit, dry_run, location_id)
    logger.info("=" * 80)
    
    shopify_client = _make_shopify_client(env)
    logger.debug("[BACKFILL] ShopifyClient initialized for %s", env)
    
    # Find products with shopify_variant_id but missing inventory_item_id
    query = {
        "shopify_variant_id": {"$exists": True, "$ne": None},
        "$or": [
            {"inventory_item_id": {"$exists": False}},
            {"inventory_item_id": None},
        ],
    }
    
    projection = {
        "_id": 1,
        "shopify_variant_id": 1,
        "shopify_id": 1,
        "inventory_item_id": 1,
        "location_id": 1,
    }
    
    logger.debug("[BACKFILL] MongoDB query filter: %s", query)
    cursor = db.product_normalized.find(query, projection)
    logger.info("[BACKFILL] Executing MongoDB query...")
    
    # Load documents
    docs: list[Dict[str, Any]] = []
    async for doc in cursor:
        docs.append(doc)
        logger.debug("[BACKFILL] Loaded doc: sku=%s | variant=%s | item_id=%s | location=%s",
                     doc.get("_id"),
                     doc.get("shopify_variant_id"),
                     doc.get("inventory_item_id"),
                     doc.get("location_id"))
        if limit is not None and len(docs) >= limit:
            logger.info("[BACKFILL] Reached limit of %d documents", limit)
            break
    
    total_to_process = len(docs)
    processed = 0
    updated_ok = 0
    errors = 0
    already_populated = 0
    
    logger.info("[BACKFILL] ✓ Loaded %d documents from MongoDB", total_to_process)
    
    if not docs:
        logger.warning("[BACKFILL] ⚠ No documents to backfill.")
        logger.warning("[BACKFILL] MongoDB query was: %s", query)
        logger.warning("[BACKFILL] Check if any products exist with: shopify_variant_id={$exists:true} AND (inventory_item_id missing OR null)")
        return {
            "total_to_process": 0,
            "processed": 0,
            "updated_ok": 0,
            "errors": 0,
            "already_populated": 0,
            "env": env,
            "dry_run": dry_run,
        }
    
    logger.info("[BACKFILL] Found %d products to process", total_to_process)
    
    # Collect all inventory_item_ids to fetch in bulk
    inventory_items_to_fetch = []
    doc_map: Dict[int, Dict[str, Any]] = {}  # variant_id -> document
    
    for doc in docs:
        variant_id_raw = doc.get("shopify_variant_id")
        if variant_id_raw is None:
            continue
        try:
            variant_id = int(variant_id_raw)
        except (TypeError, ValueError):
            logger.warning(
                "[BACKFILL] ✗ Invalid shopify_variant_id (not int) | sku=%s | value=%r",
                doc.get("_id"),
                variant_id_raw,
            )
            continue
        inventory_items_to_fetch.append(variant_id)
        doc_map[variant_id] = doc
    
    logger.info("[BACKFILL] Phase 1: Fetching %d variant IDs from Shopify...", len(inventory_items_to_fetch))
    logger.debug("[BACKFILL] Variant IDs to fetch: %s", inventory_items_to_fetch[:5])  # First 5 for debug
    
    # Fetch variants in parallel with rate limiting
    max_concurrency = 10
    sem = asyncio.Semaphore(max_concurrency)
    
    variant_cache: Dict[int, Optional[int]] = {}  # variant_id -> inventory_item_id
    
    async def fetch_variant(variant_id: int) -> None:
        async with sem:
            try:
                logger.debug("[BACKFILL] Fetching variant %s...", variant_id)
                inventory_item_id = await get_inventory_item_from_variant(variant_id, shopify_client)
                variant_cache[variant_id] = inventory_item_id
                if inventory_item_id:
                    logger.debug("[BACKFILL] ✓ variant %s -> inventory_item %s", variant_id, inventory_item_id)
                else:
                    logger.warning("[BACKFILL] ✗ variant %s returned no inventory_item_id", variant_id)
            except Exception as e:
                logger.error("[BACKFILL] ✗ Error fetching variant %s: %s", variant_id, type(e).__name__, exc_info=True)
                variant_cache[variant_id] = None
    
    tasks = [asyncio.create_task(fetch_variant(vid)) for vid in inventory_items_to_fetch]
    if tasks:
        logger.info("[BACKFILL] Starting %d concurrent variant fetches...", len(tasks))
        await asyncio.gather(*tasks)
        logger.info("[BACKFILL] ✓ Phase 1 complete")
    
    # Now fetch the PRIMARY location (all products use the same location)
    logger.info("[BACKFILL] Phase 2: Determining location_id...")
    
    # If location_id was provided, use it
    if location_id is not None:
        logger.info("[BACKFILL] ✓ Using provided location_id=%s", location_id)
    else:
        # Try to get from Shopify API
        logger.info("[BACKFILL] Attempting to fetch from Shopify locations API...")
        location_data = await get_primary_location(shopify_client)
        
        if location_data:
            location_id = location_data.get("id")
            location_name = location_data.get("name")
            logger.info("[BACKFILL] ✓ From Shopify API: %s (id=%s)", location_name, location_id)
        else:
            # Fallback: try to get from existing MongoDB product
            logger.warning("[BACKFILL] Could not get location from Shopify API (permissions/scope issue)")
            inferred_location_id = await _infer_location_id_from_inventory_levels(
                shopify_client,
                [iid for iid in variant_cache.values() if iid is not None],
            )
            if inferred_location_id is not None:
                location_id = inferred_location_id
            else:
                logger.info("[BACKFILL] Attempting fallback: querying existing products...")
                location_id = await _get_location_id_from_existing_product()
            
            if location_id is None:
                logger.error("[BACKFILL] ✗ No location_id found. Provide one using --location-id argument")
                logger.error("[BACKFILL] Example: python -m scripts.backfill_inventory_ids --env=prod --location-id=12345678")
                summary = {
                    "total_to_process": total_to_process,
                    "processed": 0,
                    "updated_ok": 0,
                    "errors": total_to_process,
                    "already_populated": 0,
                    "env": env,
                    "dry_run": dry_run,
                    "reason": "No location_id found or provided",
                }
                return summary
    
    logger.info("[BACKFILL] ✓ Using location_id=%s", location_id)
    logger.info("[BACKFILL] ✓ Phase 2 complete")
    
    # Update MongoDB documents
    logger.info("[BACKFILL] Phase 3: Updating MongoDB documents...")
    
    for doc in docs:
        processed += 1
        sku = doc.get("_id")
        variant_id = doc.get("shopify_variant_id")
        
        if processed % 5 == 0 or processed == 1:
            logger.info("[BACKFILL] Progress: %d/%d processed", processed, total_to_process)
        
        logger.debug("[BACKFILL] Processing doc %d: sku=%s | variant=%s", processed, sku, variant_id)
        
        # Check if already populated
        if doc.get("inventory_item_id") and doc.get("location_id"):
            already_populated += 1
            logger.debug("[BACKFILL] Already populated | sku=%s | item_id=%s | location=%s",
                        sku, doc.get("inventory_item_id"), doc.get("location_id"))
            continue
        
        # Get inventory_item_id from cache
        inventory_item_id = variant_cache.get(variant_id)
        if not inventory_item_id:
            logger.warning("[BACKFILL] ✗ No inventory_item_id in cache | sku=%s | variant=%s", sku, variant_id)
            errors += 1
            continue
        
        logger.debug("[BACKFILL] Found inventory_item_id=%s for sku=%s", inventory_item_id, sku)
        
        # Use the primary location for all products
        logger.debug("[BACKFILL] Using location_id=%s (primary) for sku=%s", location_id, sku)
        
        # Update document
        if dry_run:
            logger.info(
                "[BACKFILL] [DRY-RUN] Would update | sku=%s | inventory_item=%s | location=%s",
                sku,
                inventory_item_id,
                location_id,
            )
        else:
            try:
                logger.debug("[BACKFILL] Updating MongoDB: sku=%s with item_id=%s + location=%s", sku, inventory_item_id, location_id)
                result = await db.product_normalized.update_one(
                    {"_id": sku},
                    {
                        "$set": {
                            "inventory_item_id": inventory_item_id,
                            "location_id": location_id,
                        }
                    },
                )
                if result.modified_count > 0:
                    updated_ok += 1
                    logger.info(
                        "[BACKFILL] ✓ Updated | sku=%s | inventory_item=%s | location=%s",
                        sku,
                        inventory_item_id,
                        location_id,
                    )
                else:
                    logger.warning("[BACKFILL] ⚠ No changes | sku=%s (matched=%d | modified=%d)", sku, result.matched_count, result.modified_count)
            except Exception as e:
                logger.error("[BACKFILL] ✗ Update failed | sku=%s | error=%s | type=%s", sku, e, type(e).__name__, exc_info=True)
                errors += 1
    
    summary = {
        "total_to_process": total_to_process,
        "processed": processed,
        "updated_ok": updated_ok,
        "errors": errors,
        "already_populated": already_populated,
        "env": env,
        "dry_run": dry_run,
    }
    
    logger.info("=" * 80)
    logger.info("[BACKFILL] ✓ Backfill Complete")
    logger.info("[BACKFILL] Total to process:    %d", total_to_process)
    logger.info("[BACKFILL] Processed:           %d", processed)
    logger.info("[BACKFILL] Updated OK:          %d", updated_ok)
    logger.info("[BACKFILL] Already populated:   %d", already_populated)
    logger.info("[BACKFILL] Errors:              %d", errors)
    logger.info("[BACKFILL] Dry run:             %s", dry_run)
    logger.info("=" * 80)
    
    return summary


async def _async_main(args: argparse.Namespace) -> None:
    logger.info("[BACKFILL] ====== MAIN ENTRY ======")
    logger.info("[BACKFILL] Arguments: env=%s | limit=%s | dry_run=%s | location_id=%s", 
                args.env, args.limit, args.dry_run, args.location_id)
    summary = await backfill_inventory_ids(
        env=args.env,
        limit=args.limit,
        dry_run=args.dry_run,
        location_id=args.location_id,
    )
    logger.info("[BACKFILL] ====== RETURNING SUMMARY ======")
    print("\n" + "=" * 80)
    print("=== Backfill Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print("=" * 80 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill inventory_item_id and location_id for all existing products. "
            "Populates these fields for efficient inventory management."
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
        help="Optional limit on number of products to backfill",
    )
    parser.add_argument(
        "--location-id",
        type=int,
        default=None,
        help="Optional Shopify location_id. If not provided, will try to get from Shopify API or existing products.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show plan without making changes",
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more visibility
        format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s",
    )
    
    logger.info("=" * 80)
    logger.info("Starting backfill script")
    logger.info("=" * 80)
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
