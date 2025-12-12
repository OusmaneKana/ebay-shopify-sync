from app.shopify.client import ShopifyClient

client = ShopifyClient()

async def update_shopify_product(old_doc, new_doc):
    pid = old_doc.get("shopify_id")
    vid = old_doc.get("shopify_variant_id")

    if not pid or not vid:
        return None

    # Update title / description / category
    client.put(f"products/{pid}.json", {
        "product": {
            "id": pid,
            "title": new_doc["title"],
            "body_html": new_doc["description"],
            "tags": new_doc["category"]
        }
    })

    # Update variant (price & inventory)
    client.put(f"variants/{vid}.json", {
        "variant": {
            "id": vid,
            "price": new_doc["price"]
        }
    })

    print(f"✔ Updated Shopify product {pid}")
    return pid
