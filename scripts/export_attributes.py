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

    # Query all documents and get only the attributes field
    cursor = db.product_normalized.find({}, {"attributes": 1, "_id": 1})

    attributes_data = {}
    count = 0

    async for doc in cursor:
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