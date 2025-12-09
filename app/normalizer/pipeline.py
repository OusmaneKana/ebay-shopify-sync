from app.normalizer.normalize_titles import clean_title
from app.normalizer.categorize import map_category
from app.utils.hashing import hash_product

async def normalize_product(raw):
    normalized = {
        "sku": raw["sku"],
        "title": clean_title(raw["title"]),
        "category": map_category(raw.get("categoryId")),
        "images": raw.get("images", []),
        "attributes": raw.get("itemSpecifics", {}),
    }

    normalized["hash"] = hash_product(normalized)
    return normalized
