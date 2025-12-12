import hashlib
from app.database.mongo import db

def hash_dict(d: dict) -> str:
    """Stable hash of important fields to detect changes."""
    s = str(sorted(d.items()))
    return hashlib.md5(s.encode()).hexdigest()


async def normalize_from_raw():
    print("▶ Normalizing RAW eBay products...")

    cursor = db.product_raw.find({})
    count = 0

    async for raw_doc in cursor:
        sku = raw_doc.get("SKU") or raw_doc.get("_id")
        if not sku:
            continue

        raw = raw_doc.get("raw", {})

        # Extract usable fields
        title = raw.get("Title", "").strip()
        description = raw.get("Description", "").strip()
        images = raw.get("Images", []) or []
        price = raw.get("Price")
        quantity = raw.get("QuantityAvailable", 0)
        category_id = raw.get("PrimaryCategoryID")

        # Map category (simple mapping example)
        category_map = {
            "37908": "Sculptures & Figurines",
            "28025": "Bookends",
            # add more…
        }
        mapped_category = category_map.get(category_id, "Miscellaneous")

        normalized = {
            "_id": sku,
            "sku": sku,
            "title": title,
            "description": description,
            "images": images,
            "price": price,
            "quantity": quantity,
            "category": mapped_category,
            "attributes": {},  # optional, fill later
        }

        # Compute hash for change detection
        normalized["hash"] = hash_dict({
            "title": title,
            "description": description,
            "images": images,
            "price": price,
            "quantity": quantity,
            "category": mapped_category,
        })

        await db.product_normalized.update_one(
            {"_id": sku},
            {"$set": normalized},
            upsert=True
        )

        count += 1

    print(f"✔ Normalization complete. {count} products updated.")
    return {"normalized": count}
