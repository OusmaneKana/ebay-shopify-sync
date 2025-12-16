import hashlib
from datetime import datetime, timezone, timedelta  # ⬅️ NEW
from app.database.mongo import db

RECENT_DAYS = 30  # ⬅️ How many days count as "recent"


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


# ---------- NEW: eBay taxonomy helpers ----------

BAD_LEAF_NAMES = {
    "Other",
    "Others",
    "Miscellaneous",
    "Mixed Lots",
    "Mixed Lot",
    "Lot",
    "Factory Manufactured",   # ⬅️ NEW: too generic leaf
}



def parse_ebay_category_path(raw: dict, item_specifics: dict):
    """
    Extract and normalize the eBay category path.

    We try, in order:
      - raw-level fields (if you ever add them)
      - ItemSpecifics.PrimaryCategoryName (your current payload)
      - nested PrimaryCategory.CategoryName (for other formats)
    """
    path = (
        raw.get("CategoryPath")
        or raw.get("CategoryName")
        or raw.get("CategoryFullName")
        or raw.get("PrimaryCategoryName")
        or ""
    )

    print(path)

    if not isinstance(path, str):
        return None, None, [], None

    # eBay often uses ":" or " > " etc for hierarchy
    path_normalized = (
        path.replace(" > ", ":")
            .replace("/", ":")
            .replace("|", ":")
    )

    parts = [p.strip() for p in path_normalized.split(":") if p.strip()]
    if not parts:
        return None, None, [], None

    root = parts[0]
    if len(parts) == 1:
        leaf = parts[0]
        ancestors = []
    else:
        leaf = parts[-1]
        ancestors = parts[:-1]

    return path, leaf, ancestors, root

def choose_category_from_path(
    leaf: str | None,
    ancestors: list[str],
    category_id: str | None,
) -> str:
    """
    Decide which string to use as the internal 'category' field:

    - Prefer the leaf (e.g. 'Vintage Folding Knives').
    - If the leaf is junk BUT we have ancestors, use the last ancestor.
    - If still nothing, try an ID-based map.
    - Only then fall back to 'Miscellaneous'.
    """

    # 1) Start from leaf
    category = (leaf or "").strip() or None

    # Only discard a bad leaf if we actually have an ancestor
    if category in BAD_LEAF_NAMES and ancestors:
        category = None

    # 2) If leaf is unusable, fall back to last ancestor
    if not category and ancestors:
        candidate = (ancestors[-1] or "").strip()
        if candidate and candidate not in BAD_LEAF_NAMES:
            category = candidate

    # 3) If still nothing, optionally fall back to a minimal ID-based map
    if not category:
        id_map = {
            "37908": "Sculptures & Figurines",
            "28025": "Bookends",
            "48815": "Vintage Folding Knives",  # ⬅️ you can hard-map this ID too
        }
        if category_id and category_id in id_map:
            category = id_map[category_id]

    # 4) Final fallback
    if not category:
        category = "Miscellaneous"

    return category

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

        raw = raw_doc.get("raw", {}) or {}

        title = (raw.get("Title") or "").strip()
        description = (raw.get("Description") or "").strip()
        images = raw.get("Images", []) or []
        price = raw.get("Price")
        quantity = raw.get("QuantityAvailable", 0)
        category_id = raw.get("PrimaryCategoryID")
        item_specifics = raw.get("ItemSpecifics", {}) or {}


        # --- eBay taxonomy: path → category + tags + metafield-like structure ---
        category_path, category_leaf, category_ancestors, category_root = parse_ebay_category_path(
            raw,
            item_specifics,
        )


        # Category (for Shopify): primarily from the leaf, with fallbacks
        mapped_category = choose_category_from_path(
            category_leaf,
            category_ancestors,
            category_id,
        )

        # 👉 Get existing normalized doc to preserve first_seen_at
        existing_norm = await db.product_normalized.find_one(
            {"_id": sku},
            {"first_seen_at": 1}
        )

        now_utc = datetime.now(timezone.utc)

        if existing_norm and existing_norm.get("first_seen_at"):
            first_seen_at = existing_norm["first_seen_at"]
        else:
            # First time we normalize this SKU
            first_seen_at = now_utc

        # Tags from item specifics
        attr_tags = set(build_tags_from_item_specifics(item_specifics))

        # Add taxonomy tags from ancestors and root
        if category_root:
            attr_tags.add(f"Domain:{category_root}")

        for ancestor in category_ancestors:
            attr_tags.add(f"Category:{ancestor}")

        # Ensure first_seen_at is timezone-aware (UTC)
        if first_seen_at.tzinfo is None:
            first_seen_at = first_seen_at.replace(tzinfo=timezone.utc)

        # Ensure now_utc is also UTC-aware (already is, but explicit is fine)
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)

        if first_seen_at >= (now_utc - timedelta(days=RECENT_DAYS)):
            attr_tags.add("Recently Added")

        # Convert back to sorted list for storage
        all_tags = sorted(attr_tags)

        normalized = {
            "_id": sku,
            "sku": sku,
            "title": title,
            "description": description,
            "images": images,
            "price": price,
            "quantity": quantity,
            # leaf-based (or ancestor-based) category, as discussed
            "category": mapped_category,
            # raw item specifics
            "attributes": item_specifics,
            # combined tags: specifics + taxonomy + recency
            "tags": all_tags,
            # structured metafield-like breakdown of the eBay taxonomy
            "ebay_category": {
                "id": category_id,
                "path": category_path,
                "root": category_root,
                "leaf": category_leaf,
                "ancestors": category_ancestors,
            },
            # when we first saw/imported this product
            "first_seen_at": first_seen_at,
            "last_normalized_at": now_utc,
        }

        normalized["hash"] = hash_dict({
            "title": title,
            "description": description,
            "images": tuple(images),
            "price": price,
            "quantity": quantity,
            "category": mapped_category,
            "tags": tuple(all_tags),
            "last_normalized_at": now_utc,
            # you can include date only if you want, but not required:
            # "first_seen_at": first_seen_at.isoformat(),
        })

        await db.product_normalized.update_one(
            {"_id": sku},
            {"$set": normalized},
            upsert=True
        )

        count += 1

    print(f"✔ Normalization complete. {count} products updated.")
    return {"normalized": count}
