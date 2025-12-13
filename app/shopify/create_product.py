from app.shopify.client import ShopifyClient
from app.database.mongo import db

client = ShopifyClient()


async def create_shopify_product(doc):
    # combine category + attribute tags into Shopify tag string
    tag_list = []

    if doc.get("category"):
        tag_list.append(doc["category"])

    tag_list.extend(doc.get("tags", []))

    tags_str = ", ".join(sorted(set(tag_list)))

    payload = {
        "product": {
            "title": doc["title"],
            "body_html": doc.get("description") or "",
            "tags": tags_str,
            "images": [{"src": img} for img in doc.get("images", [])],
            "variants": [{
                "sku": doc["sku"],
                "price": doc.get("price") or "0",
                "inventory_management": "shopify",
                "inventory_quantity": doc.get("quantity", 0),
            }],
        }
    }

    res = client.post("products.json", payload)
    product = res.get("product")
    if not product:
        print("❌ Shopify creation failed:", res)
        return None

    pid = product["id"]
    vid = product["variants"][0]["id"]

    await db.product_normalized.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "shopify_id": pid,
            "shopify_variant_id": vid,
            "last_synced_hash": doc.get("hash"),
        }}
    )

    print(f"✔ Created Shopify product {doc['_id']} -> {pid}")
    return pid
