import os
import time
from collections import defaultdict

import requests
from pymongo import MongoClient

SHOP = os.getenv("SHOPIFY_SHOP")  # e.g. "your-store.myshopify.com"
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")  # or "2025-10"

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "yourdb")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "variant_images")

if not SHOP or not TOKEN:
    raise SystemExit("Missing SHOPIFY_SHOP or SHOPIFY_ADMIN_TOKEN env vars")
if not MONGO_URI:
    raise SystemExit("Missing MONGO_URI env var")

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

MUTATION = """
mutation BulkUpdateVariantMedia($productId: ID!, $media: [CreateMediaInput!], $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, media: $media, variants: $variants, allowPartialUpdates: true) {
    product { id }
    productVariants { id }
    userErrors { field message }
  }
}
"""

def shopify_graphql(query: str, variables: dict):
    resp = requests.post(
        GRAPHQL_URL,
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": TOKEN,
        },
        json={"query": query, "variables": variables},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    # Hard GraphQL errors (syntax, auth, etc.)
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")

    return data["data"]

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    # Pull mappings from Mongo
    # Adjust the query/filter to your needs:
    rows = list(col.find({}, {"_id": 0, "productGid": 1, "variantGid": 1, "imageUrl": 1, "alt": 1}))

    if not rows:
        print("No rows found in Mongo.")
        return

    # Group by product
    by_product = defaultdict(list)
    for r in rows:
        if not r.get("productGid") or not r.get("variantGid") or not r.get("imageUrl"):
            continue
        by_product[r["productGid"]].append(r)

    print(f"Found {len(by_product)} products to update")

    for product_gid, items in by_product.items():
        # Build unique media list for the product
        # (One CreateMediaInput per distinct URL)
        url_to_alt = {}
        for it in items:
            url_to_alt.setdefault(it["imageUrl"], it.get("alt") or "")

        media_inputs = [
            {
                "mediaContentType": "IMAGE",
                "originalSource": url,
                **({"alt": alt} if alt else {}),
            }
            for url, alt in url_to_alt.items()
        ]

        # Build variant updates
        # NOTE: mediaSrc is a list, so we wrap the URL: ["https://..."]
        variant_inputs = [
            {
                "id": it["variantGid"],
                "mediaSrc": [it["imageUrl"]],
            }
            for it in items
        ]

        # Safety: Shopify payloads can get large. Chunk per product if needed.
        # Typical: keep variants <= 50-100 per call.
        # If you have many variants per product, chunk them.
        for variant_chunk in chunked(variant_inputs, 75):
            # Only send media on the first chunk (to avoid re-adding in every chunk)
            send_media = media_inputs if variant_chunk is variant_inputs[:len(variant_chunk)] else []

            variables = {
                "productId": product_gid,
                "media": send_media,
                "variants": variant_chunk,
            }

            try:
                result = shopify_graphql(MUTATION, variables)
                payload = result["productVariantsBulkUpdate"]
                user_errors = payload.get("userErrors") or []
                if user_errors:
                    print(f"[{product_gid}] userErrors:")
                    for e in user_errors:
                        print("  -", e)
                else:
                    print(f"[{product_gid}] updated {len(variant_chunk)} variants")

            except Exception as e:
                print(f"[{product_gid}] FAILED: {e}")

            # tiny pause to be polite with throttling
            time.sleep(0.2)

if __name__ == "__main__":
    main()
