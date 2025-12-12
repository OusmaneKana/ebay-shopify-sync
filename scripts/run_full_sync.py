import asyncio, sys, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify

async def main():
    print("=== STEP 1: NORMALIZE ===")
    await normalize_from_raw()

    print("\n=== STEP 2: SHOPIFY SYNC ===")
    await sync_to_shopify()

if __name__ == "__main__":
    asyncio.run(main())
