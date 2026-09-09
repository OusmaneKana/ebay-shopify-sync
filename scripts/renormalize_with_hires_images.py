"""
Script to update image URLs in normalized products to _32 (full resolution).

Usage: python -m scripts.renormalize_with_hires_images [--limit N]
"""

import asyncio
import json
import logging
import re
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

from app.database.mongo import db

logger = logging.getLogger(__name__)

# Configure logging for the script
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "logs"
HIRES_SUFFIX_RE = re.compile(r"_(\d+)(\.jpe?g)(?=($|[?#]))", re.IGNORECASE)


def convert_image_to_hires(img_url: str) -> tuple[str, bool]:
    """
    Convert eBay-style image suffixes (e.g. _0, _1, _12) to _32.
    Leaves URLs already ending in _32 unchanged.
    """

    def _replace(match: re.Match[str]) -> str:
        suffix = match.group(1)
        ext = match.group(2)
        if suffix == "32":
            return match.group(0)
        return f"_32{ext}"

    converted = HIRES_SUFFIX_RE.sub(_replace, img_url)
    changed = converted != img_url
    if changed:
        logger.debug("Converted image: %s... → %s...", img_url[:80], converted[:80])
    return converted, changed


def convert_images_to_hires(images: list[str]) -> tuple[list[str], bool]:
    converted: list[str] = []
    changed_any = False
    for img_url in images:
        hires_url, changed = convert_image_to_hires(str(img_url))
        converted.append(hires_url)
        if changed:
            changed_any = True
    return converted, changed_any


def write_changed_ids_report(changed_items: list[dict]) -> tuple[str, str]:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    unique_shopify_ids = sorted(
        {
            str(item.get("shopify_id")).strip()
            for item in changed_items
            if item.get("shopify_id") is not None and str(item.get("shopify_id")).strip()
        }
    )

    ids_txt_path = LOGS_DIR / f"renormalize_hires_changed_shopify_ids_{stamp}.txt"
    detail_json_path = LOGS_DIR / f"renormalize_hires_changed_items_{stamp}.json"

    ids_txt_path.write_text("\n".join(unique_shopify_ids) + ("\n" if unique_shopify_ids else ""), encoding="utf-8")
    detail_json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "changed_documents": len(changed_items),
                "unique_shopify_ids": len(unique_shopify_ids),
                "shopify_ids_file": str(ids_txt_path),
                "items": changed_items,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return str(ids_txt_path), str(detail_json_path)


async def update_images_to_hires(limit=None):
    """
    Update image URLs in normalized products from _0 to _32 (high-resolution).
    """
    start_time = time.time()
    logger.info("▶ Starting image URL update to high-resolution...")
    
    batch_size = 100
    last_id = None
    updated_count = 0
    skipped_count = 0
    error_count = 0
    changed_items: list[dict] = []

    while True:
        query = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_normalized.find(query).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for norm_doc in cursor:
            batch_docs.append(norm_doc)
            last_id = norm_doc["_id"]

        if not batch_docs:
            break

        for norm_doc in batch_docs:
            if limit and updated_count >= limit:
                logger.info(f"Reached limit of {limit}")
                break

            try:
                sku = norm_doc.get("_id")
                images = norm_doc.get("images", [])
                
                if not images:
                    logger.debug(f"SKU {sku}: No images found, skipping")
                    skipped_count += 1
                    continue
                
                hires_images, changed = convert_images_to_hires(images)
                if not changed:
                    skipped_count += 1
                    continue

                await db.product_normalized.update_one(
                    {"_id": sku},
                    {"$set": {"images": hires_images}},
                )

                logger.info(f"✔ Updated {len(hires_images)} images for SKU {sku}")
                updated_count += 1
                changed_items.append(
                    {
                        "sku": sku,
                        "shopify_id": norm_doc.get("shopify_id"),
                        "shopify_variant_id": norm_doc.get("shopify_variant_id"),
                        "image_count": len(hires_images),
                    }
                )

            except Exception as e:
                logger.error(f"Failed to update images for SKU {sku}: {e}", exc_info=True)
                error_count += 1

        if limit and updated_count >= limit:
            break

    elapsed_time = time.time() - start_time
    ids_txt_path, detail_json_path = write_changed_ids_report(changed_items)

    logger.info("\n✔ Image update complete:")
    logger.info(f"  Updated: {updated_count}")
    logger.info(f"  Skipped: {skipped_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info(f"  Time: {elapsed_time:.2f}s")
    logger.info(f"  Changed Shopify IDs file: {ids_txt_path}")
    logger.info(f"  Changed items report: {detail_json_path}")

    return {
        "updated": updated_count,
        "skipped": skipped_count,
        "errors": error_count,
        "changed_shopify_ids_file": ids_txt_path,
        "changed_items_report_file": detail_json_path,
    }


if __name__ == "__main__":
    parser = ArgumentParser(description="Update image URLs to high-resolution")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of products to update")
    args = parser.parse_args()
    
    result = asyncio.run(update_images_to_hires(limit=args.limit))
    sys.exit(0 if result["errors"] == 0 else 1)
