"""
Script to REPLACE Shopify product images (delete existing first) and then
update variant images using GraphQL bulk mutation — while PRESERVING image order
from MongoDB — and running FASTER via controlled concurrency.

Mongo fields expected (per doc):
  - shopify_id (numeric Product ID)
  - shopify_variant_id (numeric Variant ID)
  - images (list of public image URLs, ORDERED)

Behavior (per product):
- (optional) Delete ALL existing product images (MediaImage) for that product
- Add new images in the SAME ORDER as they appear in Mongo (first-seen across docs)
- Assign each variant its first image (images[0])

Speed improvements:
- Process multiple products concurrently (CONCURRENCY)
- Higher request rate (RPS) while still respecting limiter

Usage:
  python -m scripts.update_shopify_images_graphql --limit 50
  python -m scripts.update_shopify_images_graphql --limit 50 --no-delete
  python -m scripts.update_shopify_images_graphql --concurrency 10 --rps 8
"""

import asyncio
import logging
import sys
import time
from argparse import ArgumentParser
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiolimiter import AsyncLimiter

from app.config import settings
from app.database.mongo import db

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

API_VERSION = "2026-01"

MAX_RETRIES = 6           # total attempts per request
BASE_BACKOFF = 0.75       # seconds
MAX_BACKOFF = 20.0        # cap sleep

# -----------------------------
# GraphQL operations
# -----------------------------

QUERY_MEDIA = """
query ProductMedia($productId: ID!) {
  product(id: $productId) {
    id
    media(first: 250) {
      nodes {
        __typename
        ... on MediaImage {
          id
          image { url }
        }
      }
    }
  }
}
"""

MUTATION_DELETE_MEDIA = """
mutation DeleteProductMedia($productId: ID!, $mediaIds: [ID!]!) {
  productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
    deletedMediaIds
    userErrors { field message }
  }
}
"""

MUTATION_BULK_UPDATE = """
mutation BulkUpdateVariantMedia(
  $productId: ID!,
  $media: [CreateMediaInput!],
  $variants: [ProductVariantsBulkInput!]!
) {
  productVariantsBulkUpdate(
    productId: $productId,
    media: $media,
    variants: $variants,
    allowPartialUpdates: true
  ) {
    product { id }
    productVariants { id }
    userErrors { field message }
  }
}
"""

# -----------------------------
# Utilities
# -----------------------------
def _to_product_gid(shopify_id: Any) -> str:
    return f"gid://shopify/Product/{shopify_id}"


def _to_variant_gid(shopify_variant_id: Any) -> str:
    return f"gid://shopify/ProductVariant/{shopify_variant_id}"


def dedupe_preserve_order(urls: List[str]) -> List[str]:
    """De-dupe URLs while preserving first-seen order."""
    seen = set()
    out: List[str] = []
    for u in urls:
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# -----------------------------
# Shopify GraphQL client
# -----------------------------
import random

async def shopify_graphql(
    session: aiohttp.ClientSession,
    limiter: AsyncLimiter,
    query: str,
    variables: dict,
) -> dict:
    url = f"https://{settings.SHOPIFY_STORE_URL_PROD}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": settings.SHOPIFY_PASSWORD_PROD,
    }

    attempt = 0

    while True:
        attempt += 1

        try:
            async with limiter:
                async with session.post(
                    url,
                    json={"query": query, "variables": variables},
                    headers=headers,
                ) as resp:

                    # ---------- HTTP throttling ----------
                    if resp.status == 429:
                        raise RuntimeError("HTTP 429 THROTTLED")

                    if resp.status >= 500:
                        raise RuntimeError(f"Shopify server error {resp.status}")

                    payload = await resp.json()

            # ---------- GraphQL throttling ----------
            if payload.get("errors"):
                for err in payload["errors"]:
                    code = (err.get("extensions") or {}).get("code", "")
                    msg = err.get("message", "").lower()

                    if code == "THROTTLED" or "throttled" in msg or "exceeded" in msg:
                        raise RuntimeError(f"GraphQL THROTTLED: {err}")

                # Non-throttle GraphQL errors → hard fail
                raise RuntimeError(f"GraphQL errors: {payload['errors']}")

            return payload.get("data") or {}

        except Exception as e:
            is_last = attempt >= MAX_RETRIES

            # Backoff only for throttle-like errors
            if "THROTTLED" in str(e).upper() or "429" in str(e):
                sleep = min(
                    BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 0.5),
                    MAX_BACKOFF,
                )

                logger.warning(
                    f"Shopify throttled (attempt {attempt}/{MAX_RETRIES}) — sleeping {sleep:.2f}s"
                )

                if is_last:
                    logger.error("Max retries reached for throttling")
                    raise

                await asyncio.sleep(sleep)
                continue

            # Non-throttle error → fail immediately
            raise

# -----------------------------
# Media helpers
# -----------------------------
async def get_product_media_image_ids(
    session: aiohttp.ClientSession,
    limiter: AsyncLimiter,
    product_gid: str,
) -> List[str]:
    data = await shopify_graphql(session, limiter, QUERY_MEDIA, {"productId": product_gid})
    product = data.get("product") or {}
    media_nodes = (product.get("media") or {}).get("nodes") or []

    ids: List[str] = []
    for n in media_nodes:
        if n.get("__typename") == "MediaImage" and n.get("id"):
            ids.append(n["id"])
    return ids


async def delete_product_media_images(
    session: aiohttp.ClientSession,
    limiter: AsyncLimiter,
    product_gid: str,
    media_ids: List[str],
) -> None:
    if not media_ids:
        return

    # Smaller chunks + retries to avoid disconnects
    CHUNK = 25
    MAX_RETRIES = 3

    for idx, chunk in enumerate(chunked(media_ids, CHUNK), start=1):
        for attempt in range(MAX_RETRIES):
            try:
                data = await shopify_graphql(
                    session,
                    limiter,
                    MUTATION_DELETE_MEDIA,
                    {"productId": product_gid, "mediaIds": chunk},
                )
                payload = data.get("productDeleteMedia") or {}
                errs = payload.get("userErrors") or []
                if errs:
                    raise RuntimeError(f"productDeleteMedia userErrors: {errs}")
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Delete chunk {idx} failed (attempt {attempt + 1}); retrying in {wait}s: {e}")
                    await asyncio.sleep(wait)
                else:
                    raise


# -----------------------------
# Per-product worker
# -----------------------------
async def process_one_product(
    session: aiohttp.ClientSession,
    limiter: AsyncLimiter,
    sem: asyncio.Semaphore,
    product_gid: str,
    pdata: Dict[str, Any],
    delete_first: bool,
) -> Tuple[int, int, int]:
    """
    Returns: (updated_products_inc, updated_variants_inc, error_inc)
    """
    async with sem:
        try:
            variants: List[Tuple[Any, str, List[str]]] = pdata["variants"]
            ordered_urls_raw: List[str] = pdata["ordered_urls"]
            ordered_urls: List[str] = dedupe_preserve_order(ordered_urls_raw)

            logger.info(
                f"Product {product_gid}: {len(variants)} variants, {len(ordered_urls)} new image URLs (ordered)"
            )

            # 1) Delete existing product images (MediaImage)
            if delete_first:
                existing_media_ids = await get_product_media_image_ids(session, limiter, product_gid)
                if existing_media_ids:
                    logger.info(f"Product {product_gid}: deleting {len(existing_media_ids)} existing images...")
                    await delete_product_media_images(session, limiter, product_gid, existing_media_ids)
                    logger.info(f"Product {product_gid}: deleted existing images")
                else:
                    logger.info(f"Product {product_gid}: no existing images to delete")

            # 2) Add new images IN ORDER + assign variant images
            media_inputs = [
                {"mediaContentType": "IMAGE", "originalSource": url}
                for url in ordered_urls
            ]

            variant_inputs = []
            for sku, variant_gid, urls in variants:
                if not urls:
                    continue
                variant_inputs.append(
                    {
                        "id": variant_gid,
                        "mediaSrc": [urls[0]],
                    }
                )

            if not media_inputs and not variant_inputs:
                logger.info(f"Product {product_gid}: nothing to add/assign; skipping")
                return (0, 0, 0)

            variables = {
                "productId": product_gid,
                "media": media_inputs,
                "variants": variant_inputs,
            }

            result = await shopify_graphql(session, limiter, MUTATION_BULK_UPDATE, variables)
            payload = result.get("productVariantsBulkUpdate") or {}
            user_errors = payload.get("userErrors") or []

            if user_errors:
                logger.warning(f"Product {product_gid}: userErrors: {user_errors}")
                return (0, 0, 1)

            logger.info(f"✔ Product {product_gid}: updated. Variants assigned: {len(variant_inputs)}")
            return (1, len(variant_inputs), 0)

        except Exception as e:
            logger.error(f"❌ Product {product_gid} FAILED: {e}", exc_info=True)
            return (0, 0, 1)


# -----------------------------
# Main logic
# -----------------------------
async def update_shopify_images(
    limit: Optional[int] = None,
    delete_first: bool = True,
    concurrency: int = 8,
    rps: int = 6,
) -> dict:
    start_time = time.time()
    logger.info("▶ Starting Shopify image REPLACE via GraphQL (ordered + concurrent)...")
    logger.info(f"Settings: delete_first={delete_first}, concurrency={concurrency}, rps={rps}")

    # Global limiters
    limiter = AsyncLimiter(rps, 1)
    sem = asyncio.Semaphore(concurrency)

    cursor = db.product_normalized.find(
        {
            "shopify_id": {"$exists": True, "$ne": None},
            "shopify_variant_id": {"$exists": True, "$ne": None},
            "images": {"$exists": True, "$ne": []},
        },
        {
            "_id": 1,
            "shopify_id": 1,
            "shopify_variant_id": 1,
            "images": 1,
        },
    )

    if limit:
        cursor = cursor.limit(limit)

    # Group by product:
    # product_map[productGid] = {
    #   "variants": [(sku, variantGid, [urls...]), ...],
    #   "ordered_urls": [url1, url2, ...]  # preserves Mongo order (first-seen across docs)
    # }
    product_map: Dict[str, Dict[str, Any]] = {}

    async for doc in cursor:
        sku = doc.get("_id")
        shopify_id = doc.get("shopify_id")
        shopify_variant_id = doc.get("shopify_variant_id")
        images = doc.get("images") or []

        if not shopify_id or not shopify_variant_id or not images:
            continue

        product_gid = _to_product_gid(shopify_id)
        variant_gid = _to_variant_gid(shopify_variant_id)

        if product_gid not in product_map:
            product_map[product_gid] = {
                "variants": [],
                "ordered_urls": [],
            }

        product_map[product_gid]["variants"].append((sku, variant_gid, images))
        product_map[product_gid]["ordered_urls"].extend(images)

    if not product_map:
        logger.info("No products found to update.")
        return {"updated_products": 0, "updated_variants": 0, "errors": 0}

    logger.info(f"Found {len(product_map)} products to process")

    updated_products = 0
    updated_variants = 0
    error_count = 0

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(
                process_one_product(session, limiter, sem, product_gid, pdata, delete_first)
            )
            for product_gid, pdata in product_map.items()
        ]

        # Gather results; keep going even if some fail
        results = await asyncio.gather(*tasks, return_exceptions=False)

        for up, uv, err in results:
            updated_products += up
            updated_variants += uv
            error_count += err

    elapsed = time.time() - start_time
    logger.info("\n✔ Update complete:")
    logger.info(f"  Updated products: {updated_products}")
    logger.info(f"  Updated variants:  {updated_variants}")
    logger.info(f"  Errors:           {error_count}")
    logger.info(f"  Time:             {elapsed:.2f}s")

    return {
        "updated_products": updated_products,
        "updated_variants": updated_variants,
        "errors": error_count,
        "seconds": elapsed,
        "concurrency": concurrency,
        "rps": rps,
    }


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    parser = ArgumentParser(description="Replace Shopify product images then update variant images via GraphQL (ordered + concurrent)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of Mongo docs to read")
    parser.add_argument("--no-delete", action="store_true", help="Do not delete existing product images before adding new ones")
    parser.add_argument("--concurrency", type=int, default=8, help="Number of products processed in parallel (default 8)")
    parser.add_argument("--rps", type=int, default=6, help="Max GraphQL requests per second (default 6)")
    args = parser.parse_args()

    res = asyncio.run(
        update_shopify_images(
            limit=args.limit,
            delete_first=(not args.no_delete),
            concurrency=args.concurrency,
            rps=args.rps,
        )
    )
    sys.exit(0 if res["errors"] == 0 else 1)
