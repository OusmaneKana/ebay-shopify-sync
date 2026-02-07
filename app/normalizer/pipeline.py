from app.normalizer.normalize_titles import clean_title
from app.normalizer.categorize import map_category
from app.services.normalizer_service import normalize_shipping, hash_dict as hash_product

async def normalize_product(raw):
    shipping_raw = raw.get("raw", {}).get("Shipping", {})
    normalized_shipping = normalize_shipping(shipping_raw)

    normalized = {
        "sku": raw["sku"],
        "title": clean_title(raw["title"]),
        "category": map_category(raw.get("categoryId")),
        "images": raw.get("images", []),
        "attributes": raw.get("itemSpecifics", {}),
        "shipping": normalized_shipping,
    }

    normalized["hash"] = hash_product(normalized)
    return normalized
