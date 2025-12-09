from app.ebay.client import EbayClient

client = EbayClient()

async def fetch_all_ebay_products():
    """
    Fetch all active products from eBay (Inventory API).
    """
    endpoint = "/sell/inventory/v1/inventory_item"
    response = client.get(endpoint)

    items = response.get("inventoryItems", [])

    products = []
    for item in items:
        products.append({
            "sku": item.get("sku"),
            "title": item.get("product", {}).get("title"),
            "categoryId": (
                item.get("product", {})
                .get("aspects", {})
                .get("Category", [None])[0]
            ),
            "images": item.get("product", {}).get("imageUrls", []),
            "quantity": item.get("availability", {})
                              .get("shipToLocationAvailability", {})
                              .get("quantity", 0),
            "price": item.get("product", {})
                         .get("price", {})
                         .get("value"),
            "raw": item,
        })

    return products
