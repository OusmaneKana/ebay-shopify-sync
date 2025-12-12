import sys, os, asyncio
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.services.product_service import sync_ebay_raw_to_mongo

async def main():
    result = await sync_ebay_raw_to_mongo()
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
