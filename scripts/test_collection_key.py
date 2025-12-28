import asyncio
import sys
sys.path.insert(0, '/Users/serigneciss/Desktop/Dev/ebay-shopify-sync')

from pymongo import MongoClient
from app.config import settings

async def check_collection_keys():
    """Check if collection_key is populated in the database."""
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]
    
    collection = db["product_normalized"]
    
    # Check total count
    total = collection.count_documents({})
    print(f"Total documents: {total}")
    
    # Check how many have collection_key
    with_key = collection.count_documents({"collection_key": {"$ne": None, "$exists": True}})
    without_key = collection.count_documents({"collection_key": None})
    missing_key = collection.count_documents({"collection_key": {"$exists": False}})
    
    print(f"With collection_key: {with_key}")
    print(f"With collection_key = None: {without_key}")
    print(f"Missing collection_key field: {missing_key}")
    
    # Show some examples
    print("\nExamples of documents with NULL collection_key:")
    docs = list(collection.find({"collection_key": None}).limit(3))
    for doc in docs:
        print(f"  SKU: {doc['_id']}")
        print(f"  Category: {doc.get('category')}")
        print(f"  Title: {doc.get('title', '')[:50]}...")
        print()
    
    print("\nExamples of documents WITH collection_key:")
    docs = list(collection.find({"collection_key": {"$ne": None}}).limit(3))
    for doc in docs:
        print(f"  SKU: {doc['_id']}")
        print(f"  Collection Key: {doc.get('collection_key')}")
        print(f"  Category: {doc.get('category')}")
        print()

if __name__ == "__main__":
    asyncio.run(check_collection_keys())
