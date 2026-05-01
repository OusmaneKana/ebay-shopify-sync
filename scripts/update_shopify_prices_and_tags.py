import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional, Set

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient

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


async def update_shopify_prices_and_tags(limit: Optional[int] = None, env: str = "prod") -> Dict[str, Any]:
    """Update ONLY prices (variants) and tags (products) in Shopify from normalized data.

    - Price source: product_normalized.price (per SKU / variant)
    - Tags source: product_normalized.tags + category (per product)
    - Only touches:
      - PUT /products/{id}.json  -> tags
      - PUT /variants/{id}.json  -> price
    """

    shopify_client = _make_shopify_client(env)

    logger.info("▶ Starting Shopify prices+tags update from product_normalized")

    cursor = db.product_normalized.find(
        {
            "shopify_id": {"$exists": True, "$ne": None},
            "shopify_variant_id": {"$exists": True, "$ne": None},
            "price": {"$exists": True},
        },
        {
            "_id": 1,
            "shopify_id": 1,
            "shopify_variant_id": 1,
            "price": 1,
            "tags": 1,
            "category": 1,
        },
    )

    if limit:
        cursor = cursor.limit(limit)

    # Group data by product_id so we only update product tags once per product.
    # Structure:
    # product_map[product_id] = {
    #   "category": Optional[str],
    #   "tags": Set[str],
    #   "variants": List[{"sku", "variant_id", "price"}]
    # }
    product_map: Dict[Any, Dict[str, Any]] = {}

    async for doc in cursor:
        sku = doc.get("_id")
        product_id = doc.get("shopify_id")
        variant_id = doc.get("shopify_variant_id")
        raw_price = doc.get("price")

        if not product_id or not variant_id:
            continue

        # Coerce price to float if possible
        price_val: Optional[float]
        try:
            price_val = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            logger.warning(f"SKU {sku}: invalid price {raw_price!r}, skipping price update for this variant")
            price_val = None

        tags = doc.get("tags") or []
        category = doc.get("category")

        if product_id not in product_map:
            product_map[product_id] = {
                "category": category,
                "tags": set(tags),  # type: Set[str]
                "variants": [],
            }
        else:
            if category and not product_map[product_id].get("category"):
                product_map[product_id]["category"] = category
            product_map[product_id]["tags"].update(tags)

        product_map[product_id]["variants"].append(
            {
                "sku": sku,
                "variant_id": variant_id,
                "price": price_val,
            }
        )

    if not product_map:
        logger.info("No products found with Shopify links and prices to update.")
        return {"products": 0, "variants": 0}

    logger.info(f"Found {len(product_map)} Shopify products to update")

    updated_products = 0
    updated_variants = 0

    # Update product tags and variant prices
    for product_id, pdata in product_map.items():
        category = pdata.get("category")
        tags_set: Set[str] = pdata.get("tags", set())
        variants: List[Dict[str, Any]] = pdata.get("variants", [])

        # Build tags string consistent with normal update flow: category + tags
        tag_list: List[str] = []
        if category:
            tag_list.append(category)
        tag_list.extend(tags_set)
        tags_str = ", ".join(sorted(set(tag_list)))

        # 1) Update product tags
        try:
            await shopify_client.put(
                f"products/{product_id}.json",
                {
                    "product": {
                        "id": product_id,
                        "tags": tags_str,
                    }
                },
            )
            updated_products += 1
            logger.info(f"Updated tags for Shopify product {product_id}")
        except Exception as e:
            logger.error(f"Failed to update tags for Shopify product {product_id}: {e}", exc_info=True)

        # 2) Update each variant price
        for v in variants:
            variant_id = v["variant_id"]
            price_val = v.get("price")
            sku = v.get("sku")

            if price_val is None:
                logger.warning(f"SKU {sku} / variant {variant_id}: no valid price, skipping price update")
                continue

            # Shopify expects string price; format to 2 decimal places
            price_str = f"{price_val:.2f}"

            try:
                await shopify_client.put(
                    f"variants/{variant_id}.json",
                    {
                        "variant": {
                            "id": variant_id,
                            "price": price_str,
                        }
                    },
                )
                updated_variants += 1
                logger.info(
                    f"Updated price for Shopify variant {variant_id} (SKU {sku}) -> {price_str}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to update price for Shopify variant {variant_id} (SKU {sku}): {e}",
                    exc_info=True,
                )

    logger.info(
        f"✔ Finished updating Shopify prices and tags: "
        f"{updated_products} products, {updated_variants} variants"
    )
    return {"products": updated_products, "variants": updated_variants}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update Shopify prices (variants) and tags (products) from product_normalized",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of normalized documents to process",
    )
    parser.add_argument(
        "--env",
        choices=["prod"],
        default="prod",
        help="Which Shopify environment to target (production only)",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    asyncio.run(update_shopify_prices_and_tags(limit=args.limit, env=args.env))


if __name__ == "__main__":
    main()
