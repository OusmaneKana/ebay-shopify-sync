from app.shopify.client import ShopifyClient

client = ShopifyClient()


async def update_shopify_product(old_doc, new_doc, shopify_client=None):
    if shopify_client is None:
        shopify_client = client
    pid = old_doc.get("shopify_id")
    vid = old_doc.get("shopify_variant_id")
    if not pid or not vid:
        print(f"⚠ Cannot update Shopify product for {old_doc.get('_id')}: missing IDs")
        return None

    # rebuild tags string from latest normalized doc
    tag_list = []
    if new_doc.get("category"):
        tag_list.append(new_doc["category"])
    tag_list.extend(new_doc.get("tags", []))
    tags_str = ", ".join(sorted(set(tag_list)))

    # update main product properties
    await shopify_client.put(f"products/{pid}.json", {
        "product": {
            "id": pid,
            "title": new_doc["title"],
            "body_html": new_doc.get("description") or "",
            "tags": tags_str,
        }
    })

    # update variant price (inventory can be separate if you want)
    await shopify_client.put(f"variants/{vid}.json", {
        "variant": {
            "id": vid,
            "price": new_doc.get("price") or "0",
        }
    })

    print(f"✔ Updated Shopify product {pid}")
    return pid
