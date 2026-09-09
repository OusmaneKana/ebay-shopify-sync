import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from app.config import settings
from app.database.mongo import close_mongo_client, db
from app.services.channel_utils import get_channel
from app.services.multichannel_sync_service import _push_etsy_quantity
from app.shopify.client import ShopifyClient
from app.shopify.update_inventory import set_inventory_quantity_by_variant


logger = logging.getLogger(__name__)
ETSY_ACTIONABLE_STATES = {"active"}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _listing_id_candidates(listing_id: Any) -> list[Any]:
    """Match investigation records regardless of whether the ID was stored as text or int."""
    values: list[Any] = [listing_id]
    text = str(listing_id or "").strip()
    if text and text not in values:
        values.append(text)
    if text.isdigit():
        numeric = int(text)
        if numeric not in values:
            values.append(numeric)
    return values


async def _get_etsy_listing_state(listing_id: Any) -> Optional[str]:
    if not listing_id:
        return None
    listing = await db.etsy_listings_investigation.find_one(
        {"listing_id": {"$in": _listing_id_candidates(listing_id)}},
        {"listing_state": 1},
    )
    state = (listing or {}).get("listing_state")
    return str(state).strip().lower() if state else None


async def _record_etsy_deactivation(sku: str, listing_id: Any) -> None:
    """Persist a confirmed remote deactivation so audits update immediately."""
    confirmed_at = datetime.now(timezone.utc)
    await db.product_normalized.update_one(
        {"_id": sku},
        {
            "$set": {
                "channels.etsy.listing_state": "inactive",
                "channels.etsy.quantity": 0,
                "channels.etsy.last_inventory_sync_at": confirmed_at,
            }
        },
    )
    await db.etsy_listings_investigation.update_one(
        {"listing_id": {"$in": _listing_id_candidates(listing_id)}},
        {
            "$set": {
                "listing_state": "inactive",
                "quantity": 0,
                "deactivation_confirmed_at": confirmed_at,
            }
        },
    )


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
    skip_shopify: bool = False,
    skip_etsy: bool = False,
) -> Dict[str, Any]:
    """Propagate sold-out eBay inventory to normalized, Shopify, and Etsy.

    Criteria:
    - product_raw.raw.QuantityAvailable == 0
    - product_normalized.quantity > 0, or a linked Etsy listing is still actionable

    Actions (when not in dry_run):
    - Set product_normalized.quantity -> 0
    - For linked Shopify variants, set inventory to 0 via Shopify API.
    - Deactivate linked active Etsy listings so they are no longer for sale.
    """
    shopify_client = None
    if not dry_run and not skip_shopify:
        shopify_client = _make_shopify_client(env)

    mode = "DRY RUN — no data will be changed" if dry_run else "LIVE RUN — inventory will be changed"
    logger.info("=" * 72)
    logger.info("ZERO-QUANTITY MULTICHANNEL REPAIR")
    logger.info("Mode: %s", mode)
    logger.info(
        "Targets: Mongo=%s | Shopify=%s | Etsy=%s | scan_limit=%s",
        "preview" if dry_run else "enabled",
        "skipped" if skip_shopify else ("preview" if dry_run else "enabled"),
        "skipped" if skip_etsy else ("preview" if dry_run else "enabled"),
        limit if limit is not None else "none",
    )
    logger.info("=" * 72)

    query = {
        "raw.QuantityAvailable": 0,
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
    normalized_already_zero = 0
    updated_shopify = 0
    skipped_shopify = 0
    failed_shopify = 0
    updated_etsy = 0
    skipped_etsy = 0
    failed_etsy = 0
    errors = 0

    async for raw_doc in cursor:
        if limit is not None and total_raw_checked >= limit:
            break

        total_raw_checked += 1

        sku = raw_doc.get("_id") or raw_doc.get("sku")
        if not sku:
            logger.warning("Skipping raw record without a SKU: %s", raw_doc.get("_id"))
            continue

        norm = await db.product_normalized.find_one(
            {"_id": sku},
            {
                "quantity": 1,
                "shopify_variant_id": 1,
                "shopify_id": 1,
                "channels": 1,
                "title": 1,
            },
        )
        if not norm:
            logger.debug("SKU %s: no normalized record; skipping", sku)
            continue

        normalized_qty = _safe_int(norm.get("quantity"), 0)
        etsy_channel = get_channel(norm, "etsy")
        etsy_listing_id = etsy_channel.get("listing_id")
        etsy_state = await _get_etsy_listing_state(etsy_listing_id)
        etsy_needs_zero = bool(etsy_listing_id and etsy_state in ETSY_ACTIONABLE_STATES)
        normalized_needs_zero = normalized_qty > 0

        if not normalized_needs_zero and not etsy_needs_zero:
            logger.debug(
                "SKU %s: already zero and Etsy state is %s; skipping",
                sku,
                etsy_state or "unlinked/unknown",
            )
            continue

        candidates += 1
        if not normalized_needs_zero:
            normalized_already_zero += 1
        logger.info(
            "[%s] SKU %s | normalized=%s | Shopify variant=%s | Etsy listing=%s state=%s",
            candidates,
            sku,
            normalized_qty,
            norm.get("shopify_variant_id") or "unlinked",
            etsy_listing_id or "unlinked",
            etsy_state or "unknown",
        )

        if dry_run:
            planned = []
            if normalized_needs_zero:
                planned.append("Mongo -> 0")
                if not skip_shopify and norm.get("shopify_variant_id"):
                    planned.append("Shopify -> 0")
            if not skip_etsy and etsy_needs_zero:
                planned.append("Etsy -> inactive")
            logger.info("    DRY RUN would apply: %s", ", ".join(planned) or "no remote changes")
            continue

        # 1) Fix normalized quantity in Mongo when necessary.
        if normalized_needs_zero:
            try:
                result = await db.product_normalized.update_one(
                    {"_id": sku, "quantity": norm.get("quantity")},
                    {"$set": {"quantity": 0}},
                )
                if getattr(result, "modified_count", 0) > 0:
                    fixed_normalized += 1
                    norm["quantity"] = 0
                    logger.info("    ✓ Mongo quantity set to 0")
                else:
                    errors += 1
                    logger.error("    ✗ Mongo quantity was not updated (record may have changed during the run)")
                    continue
            except Exception as exc:  # pragma: no cover - defensive
                errors += 1
                logger.exception("    ✗ Mongo update failed: %s", exc)
                continue
        else:
            logger.info("    ✓ Mongo already has quantity 0")

        # 2) Push inventory override to Shopify for newly corrected quantities.
        variant_id = norm.get("shopify_variant_id")
        if skip_shopify:
            skipped_shopify += 1
            logger.info("    - Shopify skipped by --skip-shopify")
        elif not normalized_needs_zero:
            skipped_shopify += 1
            logger.info("    - Shopify skipped because normalized quantity was already 0")
        elif not variant_id:
            skipped_shopify += 1
            logger.warning("    - Shopify skipped: no linked variant ID")
        else:
            try:
                assert shopify_client is not None
                ok = await set_inventory_quantity_by_variant(int(variant_id), 0, shopify_client)
                if ok:
                    updated_shopify += 1
                    logger.info("    ✓ Shopify variant %s set to 0", variant_id)
                else:
                    failed_shopify += 1
                    errors += 1
                    logger.error("    ✗ Shopify variant %s returned an unsuccessful response", variant_id)
            except Exception as exc:  # pragma: no cover - defensive
                failed_shopify += 1
                errors += 1
                logger.exception("    ✗ Shopify variant %s update failed: %s", variant_id, exc)

        # 3) Deactivate Etsy when its listing is still active.
        if skip_etsy:
            skipped_etsy += 1
            logger.info("    - Etsy skipped by --skip-etsy")
        elif not etsy_listing_id:
            skipped_etsy += 1
            logger.warning("    - Etsy skipped: no linked listing")
        elif etsy_state and etsy_state not in ETSY_ACTIONABLE_STATES:
            skipped_etsy += 1
            logger.info("    - Etsy skipped: listing is already %s", etsy_state)
        else:
            try:
                ok, error = await _push_etsy_quantity(norm, 0)
                if ok:
                    updated_etsy += 1
                    logger.info("    ✓ Etsy listing %s deactivated", etsy_listing_id)
                    try:
                        await _record_etsy_deactivation(str(sku), etsy_listing_id)
                        logger.info("    ✓ Local Etsy snapshot marked inactive; audit is now current")
                    except Exception as exc:  # pragma: no cover - defensive
                        errors += 1
                        logger.exception(
                            "    ✗ Etsy was deactivated, but its local audit snapshot could not be updated: %s",
                            exc,
                        )
                else:
                    failed_etsy += 1
                    errors += 1
                    logger.error("    ✗ Etsy listing %s update failed: %s", etsy_listing_id, error)
            except Exception as exc:  # pragma: no cover - defensive
                failed_etsy += 1
                errors += 1
                logger.exception("    ✗ Etsy listing %s update failed: %s", etsy_listing_id, exc)

    summary: Dict[str, Any] = {
        "total_raw_checked": total_raw_checked,
        "mismatched_candidates": candidates,
        "fixed_normalized": fixed_normalized,
        "normalized_already_zero": normalized_already_zero,
        "updated_shopify": updated_shopify,
        "skipped_shopify": skipped_shopify,
        "failed_shopify": failed_shopify,
        "updated_etsy": updated_etsy,
        "skipped_etsy": skipped_etsy,
        "failed_etsy": failed_etsy,
        "errors": errors,
        "dry_run": dry_run,
        "env": env,
    }

    logger.info("=" * 72)
    logger.info("REPAIR COMPLETE")
    logger.info("Raw records scanned:       %s", total_raw_checked)
    logger.info("Candidates found:          %s", candidates)
    logger.info("Mongo quantities fixed:    %s", fixed_normalized)
    logger.info("Mongo already zero:        %s", normalized_already_zero)
    logger.info("Shopify updated/skipped/failed: %s/%s/%s", updated_shopify, skipped_shopify, failed_shopify)
    logger.info("Etsy updated/skipped/failed:    %s/%s/%s", updated_etsy, skipped_etsy, failed_etsy)
    logger.info("Total errors:              %s", errors)
    if dry_run and candidates:
        logger.info("Dry run finished. Re-run without --dry-run to apply these changes.")
    logger.info("=" * 72)
    return summary


async def _async_main(args: argparse.Namespace) -> None:
    summary = await fix_zero_quantity_mismatches(
        limit=args.limit,
        env=args.env,
        dry_run=args.dry_run,
        skip_shopify=args.skip_shopify,
        skip_etsy=args.skip_etsy,
    )
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fix products where product_raw shows QuantityAvailable=0, "
            "then propagate quantity 0 to Mongo, Shopify, and Etsy."
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
        help="Only report planned changes; do not modify Mongo, Shopify, or Etsy",
    )
    parser.add_argument(
        "--skip-shopify",
        action="store_true",
        help="Do not update Shopify inventory",
    )
    parser.add_argument(
        "--skip-etsy",
        action="store_true",
        help="Do not update Etsy inventory",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show debug logs, including records that were skipped",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(_async_main(args))
    finally:
        close_mongo_client()


if __name__ == "__main__":
    main()
