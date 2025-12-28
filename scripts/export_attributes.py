import json
import sys
import os

# Add the app directory to the Python path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.database.mongo import db


async def export_attributes():
    """
    Export all 'attributes' fields from product_normalized collection to a JSON file.
    """
    print("▶ Exporting attributes from MongoDB...")

    batch_size = 100
    last_id = None
    attributes_data = {}
    count = 0

    while True:
        query = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_normalized.find(query, {"attributes": 1, "_id": 1}).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for doc in cursor:
            batch_docs.append(doc)
            last_id = doc["_id"]

        if not batch_docs:
            break

        for doc in batch_docs:
            sku = doc.get("_id")
            attributes = doc.get("attributes", {})

            if attributes:  # Only include documents that have attributes
                attributes_data[sku] = attributes
                count += 1

    # Write to JSON file
    output_file = "exported_attributes.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(attributes_data, f, indent=2, ensure_ascii=False)

    print(f"✔ Exported {count} product attributes to {output_file}")
    return {"exported": count, "file": output_file}


if __name__ == "__main__":
    import asyncio
    result = asyncio.run(export_attributes())
    print(f"Export complete: {result}")