"""Backfill ebay_posted_at from product_raw to product_normalized."""

import asyncio
import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

if not MONGO_URI or not MONGO_DB:
    raise SystemExit("Missing MONGO_URI or MONGO_DB environment variables")


async def backfill():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[MONGO_DB]

    print("🔄 Backfilling ebay_posted_at from raw to normalized...")

    # Get all raw docs with ebay_posted_at
    cursor = db.product_raw.find({"ebay_posted_at": {"$exists": True, "$ne": None}})
    
    count = 0
    async for raw_doc in cursor:
        sku = raw_doc.get("_id")
        ebay_posted_at = raw_doc.get("ebay_posted_at")
        
        if not sku or not ebay_posted_at:
            continue
        
        # Update the corresponding normalized doc
        result = await db.product_normalized.update_one(
            {"_id": sku},
            {"$set": {"ebay_posted_at": ebay_posted_at}},
        )
        
        if result.modified_count > 0:
            count += 1
            print(f"  ✓ Updated {sku}")
    
    client.close()
    print(f"\n✔ Backfill complete. Updated {count} normalized documents.")


if __name__ == "__main__":
    asyncio.run(backfill())
