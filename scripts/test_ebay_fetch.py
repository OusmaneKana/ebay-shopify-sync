import asyncio
from app.ebay.fetch_products import fetch_all_ebay_products

async def run():
    products = await fetch_all_ebay_products()
    print(f"Fetched {len(products)} items")

    # Print first product sample
    if products:
        print("\nSample Product:\n", products[0])

asyncio.run(run())
