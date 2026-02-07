"""
Script to update image URLs in normalized products from _0 to _32 (full resolution).

Usage: python -m scripts.renormalize_with_hires_images [--limit N]
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


def convert_images_to_hires(images: list[str]) -> list[str]:
    """
    Convert image URLs from _0.JPG (thumbnail) to _32.JPG (full resolution).
    
    Args:
        images: List of eBay image URLs with _0.JPG
    
    Returns:
        List of image URLs with _32.JPG for full resolution
    """
    converted = []
    for img_url in images:
        # Replace _0.JPG with _32.JPG for full resolution
        hires_url = img_url.replace("_0.JPG", "_32.JPG")
        converted.append(hires_url)
        logger.debug(f"Converted image: {img_url[:50]}... → {hires_url[:50]}...")
    return converted


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
                
                # Convert to high-res
                hires_images = convert_images_to_hires(images)
                
                # Update only the images field
                await db.product_normalized.update_one(
                    {"_id": sku},
                    {"$set": {"images": hires_images}},
                )
                
                logger.info(f"✔ Updated {len(hires_images)} images for SKU {sku}")
                updated_count += 1

            except Exception as e:
                logger.error(f"Failed to update images for SKU {sku}: {e}", exc_info=True)
                error_count += 1

        if limit and updated_count >= limit:
            break

    elapsed_time = time.time() - start_time
    logger.info(f"\n✔ Image update complete:")
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
    parser = ArgumentParser(description="Update image URLs to high-resolution")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of products to update")
    args = parser.parse_args()
    
    result = asyncio.run(update_images_to_hires(limit=args.limit))
    sys.exit(0 if result["errors"] == 0 else 1)
