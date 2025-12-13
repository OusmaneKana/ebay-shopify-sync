import hashlib
from app.database.mongo import db


def hash_dict(d: dict) -> str:
    """Stable hash of important fields to detect changes."""
    s = str(sorted(d.items()))
    return hashlib.md5(s.encode()).hexdigest()


def build_tags_from_item_specifics(item_specifics: dict) -> list[str]:
    """
    Convert ItemSpecifics into normalized Shopify tags using canonical dimensions.
    Example output tags:
      Brand:Rado, Model:DiaStar, Material:Ceramic, Color:Black, Era:1970s, Origin:Japan, Movement:Quartz
    """
    if not isinstance(item_specifics, dict):
        return []

    # --- canonical groups of keys ---
    brand_keys = {
        "Brand", "Maker", "Manufacturer", "Make", "Artist", "Author",
        "Sculptor", "Publisher", "Giuseppe Armani", "Lladro"
    }

    model_keys = {
        "Model", "Series", "Series Title", "Series/Movie", "Product Line",
        "Game Title", "Movie/TV Title", "TV Show", "TV/Streaming Show", "Book Series"
    }

    material_keys = {
        "Material", "Primary Material", "Case Material", "Band Material",
        "Handle Material", "Handle/Strap Material", "Metal", "Metal Purity",
        "Glass Type", "Glassware Type", "Porcelain Type", "Production Style",
        "Production Technique", "Surface Coating"
    }

    color_keys = {
        "Color", "Colour", "Main Color", "Exterior Color", "Band Color",
        "Case Color", "Dial Color", "Lens Color", "Frame Color",
        "Lining Color", "Light Color", "Cord Color", "Blade Color"
    }

    era_keys = {
        "Decade", "Era", "Time Period", "Time Period Manufactured",
        "Time Period Produced", "Historical Period", "Year Manufactured",
        "Year of Manufacture", "Year", "Year Issued", "Year Printed",
        "Publication Year", "Release Year", "Date of Origin", "Date of Creation",
        "Post-WWII", "Victorian", "Mid-20th century", "early 1900"
    }

    origin_keys = {
        "Country of Origin", "Country/Region of Origin", "Place of Origin",
        "Region", "Region of Origin", "Country", "Country/Region",
        "Country of Manufacture", "Place of Publication", "Culture",
        "Ethnic & Regional Style", "Tribal Affiliation", "Tribe", "Origin"
    }

    style_keys = {
        "Style", "Look", "Occasion", "Season", "Holiday", "Room",
        "Jewelry Department", "Department"
    }

    movement_keys = {
        "Movement", "Escapement Type"
    }

    category_keys = {
        "Object Type", "Product Type", "Product", "Collection",
        "Game Type", "Sport", "Sport/Activity", "Type",
        "Type of Advertising", "Type of Glass", "Type of Tool"
    }

    stone_keys = {
        "Main Stone", "Main Stone Color", "Main Stone Shape",
        "Secondary Stone", "Total Carat Weight", "Diamond Clarity Grade",
        "Diamond Color Grade", "Cut Grade"
    }

    feature_keys = {
        "Features", "Special Features", "Special Attributes", "Limited Edition",
        "Retired", "Handmade", "Autographed", "Signed", "Signed By", "Signed by",
        "Certificate of Authenticity (COA)", "Certification"
    }

    size_keys = {
        "Size", "Length", "Height", "Width (Inches)", "Diameter",
        "Ring Size", "Necklace Length", "Case Size", "Lug Width",
        "Band Width", "Max Wrist Size", "Item Length", "Item Height",
        "Item Width", "Item Diameter", "Scale"
    }

    theme_keys = {
        "Theme", "Subject", "Subject/Theme", "Topic", "Holiday",
        "Series", "Franchise", "Character", "Character Family",
        "Character/Story/Theme", "Superhero Team"
    }

    sport_keys = {
        "Sport", "Sport/Activity", "League", "Team", "Team-Baseball",
        "Event/Tournament", "Player", "Player/Athlete"
    }

    room_keys = {"Room"}

    # --- values to ignore as tags ---
    ignore_values = {"", "No", "Not Water Resistant", "Unknown", "N/A", "na", "NA", "None"}

    tags: set[str] = set()

    def add_tag(prefix: str, value):
        if isinstance(value, list):
            values = value
        else:
            values = [value]

        for v in values:
            v = str(v).strip()
            if not v or v in ignore_values:
                continue
            tags.add(f"{prefix}:{v}")

    for key, value in item_specifics.items():
        k = key.strip()

        if k in brand_keys:
            add_tag("Brand", value)
        elif k in model_keys:
            add_tag("Model", value)
        elif k in material_keys:
            add_tag("Material", value)
        elif k in color_keys:
            add_tag("Color", value)
        elif k in era_keys:
            add_tag("Era", value)
        elif k in origin_keys:
            add_tag("Origin", value)
        elif k in style_keys:
            add_tag("Style", value)
        elif k in movement_keys:
            add_tag("Movement", value)
        elif k in category_keys:
            add_tag("Category", value)
        elif k in stone_keys:
            add_tag("Stone", value)
        elif k in feature_keys:
            add_tag("Feature", value)
        elif k in size_keys:
            add_tag("Size", value)
        elif k in theme_keys:
            add_tag("Theme", value)
        elif k in sport_keys:
            add_tag("Sport", value)
        elif k in room_keys:
            add_tag("Room", value)
        else:
            # ignore noisy one-offs to keep tags clean
            continue

    return sorted(tags)


async def normalize_from_raw():
    """
    Read product_raw, build Shopify-friendly normalized docs in product_normalized.
    """
    print("▶ Normalizing RAW eBay products...")

    cursor = db.product_raw.find({})
    count = 0

    async for raw_doc in cursor:
        sku = raw_doc.get("SKU") or raw_doc.get("_id")
        if not sku:
            continue

        raw = raw_doc.get("raw", {})

        title = raw.get("Title", "").strip()
        description = raw.get("Description", "").strip()
        images = raw.get("Images", []) or []
        price = raw.get("Price")
        quantity = raw.get("QuantityAvailable", 0)
        category_id = raw.get("PrimaryCategoryID")
        item_specifics = raw.get("ItemSpecifics", {}) or {}

        # simple mapping; extend as needed
        category_map = {
            "37908": "Sculptures & Figurines",
            "28025": "Bookends",
        }
        mapped_category = category_map.get(category_id, "Miscellaneous")

        attr_tags = build_tags_from_item_specifics(item_specifics)

        normalized = {
            "_id": sku,
            "sku": sku,
            "title": title,
            "description": description,
            "images": images,
            "price": price,
            "quantity": quantity,
            "category": mapped_category,
            "attributes": item_specifics,
            "tags": attr_tags,
        }

        normalized["hash"] = hash_dict({
            "title": title,
            "description": description,
            "images": tuple(images),
            "price": price,
            "quantity": quantity,
            "category": mapped_category,
            "tags": tuple(attr_tags),
        })

        await db.product_normalized.update_one(
            {"_id": sku},
            {"$set": normalized},
            upsert=True
        )

        count += 1

    print(f"✔ Normalization complete. {count} products updated.")
    return {"normalized": count}
