from app.shopify.client import ShopifyClient
from app.database.mongo import db

client = ShopifyClient()

async def create_shopify_product(doc):
    payload = {
        "product": {
            "title": doc["title"],
            "body_html": doc["description"],
            "tags": doc["category"],
            "images": [{"src": img} for img in doc["images"]],
            "variants": [{
                "sku": doc["sku"],
                "price": doc["price"],
                "inventory_management": "shopify",
                "inventory_quantity": doc["quantity"]
            }]
        }
    }

    res = client.post("products.json", payload)

    product = res.get("product")
    if not product:
        print("❌ Shopify creation failed:", res)
        return None

    pid = product["id"]
    vid = product["variants"][0]["id"]

    # Save IDs back into Mongo
    await db.product_normalized.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "shopify_id": pid,
            "shopify_variant_id": vid
        }}
    )

    print(f"✔ Created Shopify product {doc['_id']} -> {pid}")
    return pid
