from app.shopify.client import ShopifyClient


def purge_all_shopify_products():
    """
    Delete all products from Shopify store.
    Returns the number of products deleted.
    """
    client = ShopifyClient()
    products = []
    endpoint = "products.json?limit=250"

    while endpoint:
        res = client.get(endpoint)
        batch = res.get("products", [])
        products.extend(batch)

        # Shopify pagination via Link header
        link_header = client.last_response.headers.get("Link")
        next_link = None

        if link_header:
            links = link_header.split(",")
            for link in links:
                if 'rel="next"' in link:
                    next_link = link.split(";")[0].strip("<>")

        endpoint = next_link.replace(client.base_url + "/", "") if next_link else None

    deleted = 0
    for p in products:
        pid = p["id"]
        try:
            client.delete(f"products/{pid}.json")
            deleted += 1
        except Exception as e:
            # Optionally log the error
            pass

    return deleted