import asyncio
import time

from app.ebay.fetch_products import fetch_all_ebay_products


async def run():
    start = time.perf_counter()
    products = await fetch_all_ebay_products()
    elapsed = time.perf_counter() - start

    print(f"Fetched {len(products)} items in {elapsed:.2f} seconds")

    # Print first product sample
    if products:
        print("\nSample Product:\n", products[0])


if __name__ == "__main__":
    asyncio.run(run())
