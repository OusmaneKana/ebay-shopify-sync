import asyncio
from app.shopify.client import ShopifyClient

client = ShopifyClient()


async def get_all_products(limit=250):
    """
    Fetch all products using cursor-based pagination.
    """
    products = []
    endpoint = f"products.json?limit={limit}"

    while endpoint:
        res = await client.get(endpoint)

        batch = res.get("products", [])
        products.extend(batch)

        # Shopify pagination via Link header
        link_header = getattr(client.last_response, 'headers', {}).get("Link")
        next_link = None

        if link_header:
            links = link_header.split(",")
            for link in links:
                if 'rel="next"' in link:
                    next_link = link.split(";")[0].strip("<>")

        endpoint = next_link.replace(client.base_url + "/", "") if next_link else None

    return products


async def delete_all_products():
    products = await get_all_products()

    print(f"⚠️ Found {len(products)} products to delete")

    for p in products:
        pid = p["id"]
        try:
            await client.delete(f"products/{pid}.json")
            print(f"🗑️ Deleted product {pid}")
        except Exception as e:
            print(f"❌ Failed to delete {pid}: {e}")

    print("✔ All products deleted")


if __name__ == "__main__":
    asyncio.run(delete_all_products())
    delete_all_products()
