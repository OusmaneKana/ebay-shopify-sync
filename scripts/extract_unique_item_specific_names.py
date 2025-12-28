import sys, os, json, asyncio
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.database.mongo import db

async def extract_item_specific_keys():
    batch_size = 100
    last_id = None
    unique_keys = set()

    while True:
        query = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_raw.find(query).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for doc in cursor:
            batch_docs.append(doc)
            last_id = doc["_id"]

        if not batch_docs:
            break

        for doc in batch_docs:
            raw = doc.get("raw", {})
            item_specifics = raw.get("ItemSpecifics", {})

            if isinstance(item_specifics, dict):
                for key in item_specifics.keys():
                    unique_keys.add(key)

    data = sorted(unique_keys)
    print(json.dumps(data, indent=2))

    # Optional: write to file
    with open("unique_item_specifics.json", "w") as f:
        json.dump(data, f, indent=2)

    print("\n✔ Extracted", len(data), "unique ItemSpecific names.")
    print("Saved to unique_item_specifics.json")

if __name__ == "__main__":
    asyncio.run(extract_item_specific_keys())
