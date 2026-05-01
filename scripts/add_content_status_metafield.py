import argparse
import asyncio
import logging
import os
import re
import sys
from urllib.parse import urlparse
from typing import Any, Dict, Optional

# Allow running as either:
# - python -m scripts.add_content_status_metafield
# - python scripts/add_content_status_metafield.py
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)

ALLOWED_VALUES = {"pending", "in_progress", "completed"}
# Shopify REST enforces namespace length >= 3. We use 'ai_' to represent the requested 'ai' namespace.
NAMESPACE = "ai_"
KEY = "content_status"
MF_TYPE = "single_line_text_field"


def _absolute_shopify_url_to_endpoint(url: str | None) -> str | None:
    """Convert Shopify Link-header URLs to a ShopifyClient endpoint.

    Shopify pagination links come back as absolute URLs without embedded basic-auth.
    Our ShopifyClient expects an endpoint like "products.json?...".
    """

    if not url:
        return None
    u = url.strip()
    u = u.strip().lstrip("<").rstrip(">")
    if not (u.startswith("http://") or u.startswith("https://")):
        return u.lstrip("/")

    parsed = urlparse(u)
    path = (parsed.path or "").lstrip("/")

    # Typical path: admin/api/2023-10/products.json
    parts = path.split("/")
    if len(parts) >= 4 and parts[0] == "admin" and parts[1] == "api":
        endpoint = "/".join(parts[3:])
    else:
        endpoint = path

    if parsed.query:
        endpoint = f"{endpoint}?{parsed.query}"
    return endpoint


def _extract_next_link(link_header: str | None) -> str | None:
    """Extract the rel="next" URL from a Shopify Link header."""

    if not link_header:
        return None

    m = re.search(r"<([^>]+)>\s*;\s*rel=\"next\"", link_header)
    if not m:
        return None
    return m.group(1).strip()


def _make_shopify_client(env: str) -> ShopifyClient:
    if env != "prod":
        raise ValueError("Only the prod Shopify environment is supported")
    logger.info("Using Shopify PROD credentials")
    return ShopifyClient(
        api_key=settings.SHOPIFY_API_KEY_PROD,
        password=settings.SHOPIFY_PASSWORD_PROD,
        store_url=settings.SHOPIFY_STORE_URL_PROD,
    )


def _last_status(client: ShopifyClient) -> Optional[int]:
    return getattr(getattr(client, "last_response", None), "status", None)


async def _get_ok(client: ShopifyClient, endpoint: str, *, params: dict | None = None) -> dict:
    res = await client.get(endpoint, params=params)
    status = _last_status(client)
    if status is not None and status >= 400:
        raise RuntimeError(f"Shopify GET {endpoint} failed with HTTP {status}: {res}")
    return res


async def _post_ok(client: ShopifyClient, endpoint: str, payload: dict) -> dict:
    res = await client.post(endpoint, payload)
    status = _last_status(client)
    if status is not None and status >= 400:
        raise RuntimeError(f"Shopify POST {endpoint} failed with HTTP {status}: {res}")
    return res


async def _put_ok(client: ShopifyClient, endpoint: str, payload: dict) -> dict:
    res = await client.put(endpoint, payload)
    status = _last_status(client)
    if status is not None and status >= 400:
        raise RuntimeError(f"Shopify PUT {endpoint} failed with HTTP {status}: {res}")
    return res


async def add_content_status_metafield(
    *,
    env: str = "prod",
    source: str = "shopify",
    value: str = "pending",
    limit: Optional[int] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Add/update Shopify product metafield ai_.content_status.

    Product list source:
      - source='shopify': all products currently in Shopify (paginated)
      - source='mongo': Shopify product IDs from MongoDB `product_normalized.shopify_id`

    - If metafield is missing: creates it.
    - If metafield exists: updates only when `overwrite=True`.
    """

    if value not in ALLOWED_VALUES:
        raise ValueError(f"Invalid value {value!r}. Allowed: {sorted(ALLOWED_VALUES)}")

    shopify_client = _make_shopify_client(env)

    async def iter_product_ids() -> Any:
        if source == "mongo":
            match = {"shopify_id": {"$exists": True, "$ne": None}}
            pipeline = [
                {"$match": match},
                {"$group": {"_id": "$shopify_id"}},
                {"$sort": {"_id": 1}},
            ]
            if limit:
                pipeline.append({"$limit": int(limit)})
            cursor = db.product_normalized.aggregate(pipeline)
            async for row in cursor:
                pid = row.get("_id")
                if pid:
                    yield pid
            return

        # source == 'shopify'
        endpoint = "products.json?limit=250&fields=id"
        fetched = 0
        while endpoint:
            res = await _get_ok(shopify_client, endpoint)

            # IMPORTANT: capture the next-page link immediately from the PRODUCTS response.
            # The consumer of this generator will make additional Shopify calls per product,
            # which would overwrite shopify_client.last_response.
            link_header = getattr(getattr(shopify_client, "last_response", None), "headers", {}).get("Link")
            next_link = _extract_next_link(link_header)
            next_endpoint = _absolute_shopify_url_to_endpoint(next_link) if next_link else None

            products = res.get("products", []) or []
            for p in products:
                pid = p.get("id")
                if not pid:
                    continue
                yield pid
                fetched += 1
                if limit and fetched >= int(limit):
                    return

            endpoint = next_endpoint

    created = 0
    updated = 0
    skipped = 0
    errors = 0
    processed = 0

    async for product_id in iter_product_ids():
        processed += 1

        try:
            # Fast path: when not overwriting, just try to CREATE the metafield.
            # If it already exists, Shopify returns 422; treat that as a skip.
            if not overwrite:
                res = await shopify_client.post(
                    "metafields.json",
                    {
                        "metafield": {
                            "namespace": NAMESPACE,
                            "key": KEY,
                            "value": value,
                            "type": MF_TYPE,
                            "owner_id": product_id,
                            "owner_resource": "product",
                        }
                    },
                )
                status = _last_status(shopify_client)
                if status == 422:
                    skipped += 1
                    logger.info(
                        "Product %s: metafield %s.%s already exists, skipping",
                        product_id,
                        NAMESPACE,
                        KEY,
                    )
                    continue
                if status is not None and status >= 400:
                    raise RuntimeError(f"Shopify POST metafields.json failed with HTTP {status}: {res}")

                created += 1
                logger.info(
                    "Product %s: created metafield %s.%s = %s",
                    product_id,
                    NAMESPACE,
                    KEY,
                    value,
                )
                continue

            # Overwrite mode: read existing metafields and update if needed.
            existing_mf_res = await _get_ok(
                shopify_client,
                "metafields.json",
                params={"owner_id": product_id, "owner_resource": "product"},
            )
            existing_mf = existing_mf_res.get("metafields", []) or []
            target = next(
                (mf for mf in existing_mf if mf.get("namespace") == NAMESPACE and mf.get("key") == KEY),
                None,
            )

            if not target:
                await _post_ok(
                    shopify_client,
                    "metafields.json",
                    {
                        "metafield": {
                            "namespace": NAMESPACE,
                            "key": KEY,
                            "value": value,
                            "type": MF_TYPE,
                            "owner_id": product_id,
                            "owner_resource": "product",
                        }
                    },
                )
                created += 1
                logger.info(
                    "Product %s: created metafield %s.%s = %s",
                    product_id,
                    NAMESPACE,
                    KEY,
                    value,
                )
                continue

            mf_id = target.get("id")
            current_val = target.get("value")

            if str(current_val) == value:
                skipped += 1
                logger.info(
                    "Product %s: metafield %s.%s already value=%r, skipping",
                    product_id,
                    NAMESPACE,
                    KEY,
                    value,
                )
                continue

            if not mf_id:
                raise RuntimeError(f"Product {product_id}: existing metafield missing id: {target}")

            await _put_ok(
                shopify_client,
                f"metafields/{mf_id}.json",
                {"metafield": {"id": mf_id, "value": value, "type": MF_TYPE}},
            )
            updated += 1
            logger.info(
                "Product %s: updated metafield %s.%s -> %s",
                product_id,
                NAMESPACE,
                KEY,
                value,
            )

        except Exception as e:
            errors += 1
            logger.error("Product %s: failed to upsert %s.%s: %s", product_id, NAMESPACE, KEY, e, exc_info=True)

    summary = {
        "processed_products": processed,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "env": env,
        "value": value,
        "overwrite": overwrite,
        "limit": limit,
    }

    logger.info("✔ Done: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upsert Shopify product metafield ai_.content_status (pending|in_progress|completed)",
    )
    parser.add_argument(
        "--env",
        choices=["prod"],
        default="prod",
        help="Which Shopify environment to target (production only)",
    )
    parser.add_argument(
        "--source",
        choices=["shopify", "mongo"],
        default="shopify",
        help="Where to get product IDs: 'shopify' (all Shopify products) or 'mongo' (product_normalized.shopify_id). Default: shopify",
    )
    parser.add_argument(
        "--value",
        choices=sorted(ALLOWED_VALUES),
        default="pending",
        help="Metafield value to set. Default: pending",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of Shopify products to process",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, update existing metafield value instead of skipping",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    asyncio.run(
        add_content_status_metafield(
            env=args.env,
            source=args.source,
            value=args.value,
            limit=args.limit,
            overwrite=args.overwrite,
        )
    )


if __name__ == "__main__":
    main()
