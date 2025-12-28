import hashlib
import logging
from datetime import datetime, timezone, timedelta
from app.database.mongo import db
import re,json
from functools import lru_cache
from openai import OpenAI
from app.config import settings


logger = logging.getLogger(__name__)

RECENT_DAYS = 30  # How many days count as "recent"

# ----------------------------
# Metafield routing (NEW)
# ----------------------------

UNIVERSAL_META_MAP: dict[str, tuple[str, str, str]] = {
    # key: (namespace, key, suggested_type)
    "Country of Origin": ("antique", "country_of_origin", "single_line_text"),
    "Country/Region of Origin": ("antique", "country_of_origin", "single_line_text"),
    "Place of Origin": ("antique", "place_of_origin", "single_line_text"),
    "Region of Origin": ("antique", "region_of_origin", "single_line_text"),
    "Origin": ("antique", "origin", "single_line_text"),
    "Culture": ("antique", "culture", "single_line_text"),

    "Antique": ("antique", "is_antique", "boolean"),
    "Vintage": ("antique", "is_vintage", "boolean"),
    "Era": ("antique", "era", "single_line_text"),
    "Decade": ("antique", "decade", "single_line_text"),
    "Time Period Manufactured": ("antique", "time_period", "single_line_text"),
    "Time Period Produced": ("antique", "time_period_produced", "single_line_text"),
    "Year Manufactured": ("antique", "year_manufactured", "number_integer"),
    "Year of Production": ("antique", "year_of_production", "number_integer"),
    "Year Printed": ("antique", "year_printed", "number_integer"),
    "Publication Year": ("antique", "publication_year", "number_integer"),

    "Brand": ("maker", "brand", "single_line_text"),
    "Maker": ("maker", "maker", "single_line_text"),
    "Manufacturer": ("maker", "manufacturer", "single_line_text"),
    "Publisher": ("maker", "publisher", "single_line_text"),
    "Model": ("maker", "model", "single_line_text"),
    "Product Line": ("maker", "product_line", "single_line_text"),

    "Material": ("material", "primary", "single_line_text"),
    "Primary Material": ("material", "primary", "single_line_text"),
    "Metal": ("material", "metal", "single_line_text"),
    "Finish": ("material", "finish", "single_line_text"),
    "Production Technique": ("material", "technique", "single_line_text"),
    "Production Style": ("material", "production_style", "single_line_text"),
    "Surface Coating": ("material", "surface_coating", "single_line_text"),

    "Color": ("theme", "color", "single_line_text"),
    "Pattern": ("theme", "pattern", "single_line_text"),
    "Theme": ("theme", "primary", "single_line_text"),
    "Subject": ("theme", "subject", "single_line_text"),
    "Style": ("theme", "style", "single_line_text"),
    "Occasion": ("theme", "occasion", "single_line_text"),

    "Handmade": ("collectible", "handmade", "boolean"),
    "Signed": ("collectible", "signed", "boolean"),
    "Signed By": ("collectible", "signed_by", "single_line_text"),
    "Signed by": ("collectible", "signed_by", "single_line_text"),
    "Autograph Authentication": ("collectible", "authentication", "single_line_text"),
    "Certification": ("collectible", "certification", "single_line_text"),
    "Special Attributes": ("collectible", "special_attributes", "list.single_line_text"),
    "Features": ("collectible", "features", "list.single_line_text"),
}

DOMAIN_META_MAP: dict[str, dict[str, tuple[str, str, str]]] = {
    "blade": {
        "Blade Material": ("blade", "blade_material", "single_line_text"),
        "Handle Material": ("blade", "handle_material", "single_line_text"),
        "Blade Type": ("blade", "blade_type", "single_line_text"),
        "Blade Color": ("blade", "blade_color", "single_line_text"),
        "Tang": ("blade", "tang", "single_line_text"),
        "Blade Length": ("blade", "blade_length", "single_line_text"),
        "Type": ("blade", "type", "single_line_text"),
    },
    "book": {
        "Binding": ("book", "binding", "single_line_text"),
        "Language": ("book", "language", "single_line_text"),
        "Author": ("book", "author", "single_line_text"),
        "Illustrator": ("book", "illustrator", "single_line_text"),
        "Topic": ("book", "topic", "single_line_text"),
        "Subject": ("book", "subject", "single_line_text"),
        "Place of Publication": ("book", "place_of_publication", "single_line_text"),
        "Book Title": ("book", "book_title", "single_line_text"),
        "Edition": ("book", "edition", "single_line_text"),
        "Book Series": ("book", "book_series", "single_line_text"),
        "Series Title": ("book", "series_title", "single_line_text"),
    },
    "clock": {
        "Movement": ("clock", "movement", "single_line_text"),
        "Power Source": ("clock", "power_source", "single_line_text"),
        "Chime Sequence": ("clock", "chime_sequence", "single_line_text"),
        "Display Type": ("clock", "display_type", "single_line_text"),
        "Frame Material": ("clock", "frame_material", "single_line_text"),
        "Number Type": ("clock", "number_type", "single_line_text"),
    },
    "art": {
        "Artist": ("art", "artist", "single_line_text"),
        "Production Technique": ("art", "production_technique", "single_line_text"),
        "Type": ("art", "type", "single_line_text"),
        "Size": ("art", "size", "single_line_text"),
        "Framing": ("art", "framing", "single_line_text"),
        "Image Orientation": ("art", "image_orientation", "single_line_text"),
    },
    "militaria": {
        "Conflict": ("militaria", "conflict", "single_line_text"),
        "Theme": ("militaria", "theme", "single_line_text"),
        "Region of Origin": ("militaria", "region_of_origin", "single_line_text"),
    },
}

IGNORE_VALUES = {"", "Unknown", "N/A", "na", "NA", "None", "No Idea", "Does Not Apply"}
COLLECTION_KEYS_PATH = getattr(settings, "COLLECTION_KEYS_PATH", "app/resources/collection_keys.json")
OPENAI_MODEL = getattr(settings, "OPENAI_MODEL_COLLECTION_KEY", "gpt-4.1-mini")
OPENAI_API_KEY = settings.OPENAI_API_KEY
@lru_cache(maxsize=1)
def load_collection_keys() -> dict:
    with open(COLLECTION_KEYS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

@lru_cache(maxsize=1)
def allowed_collection_keys() -> list[str]:
    data = load_collection_keys()
    keys: list[str] = []
    for _group, collections in data.items():
        for _collection_name, sc_key in collections.items():
            if sc_key and isinstance(sc_key, str):
                keys.append(sc_key.strip())
    # de-dupe but keep stable order
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out

def pick_existing_sc_tag(tags: list[str]) -> str | None:
    for t in tags or []:
        if isinstance(t, str) and t.startswith("SC:"):
            return t
    return None

def infer_collection_key_from_mapping(category: str, item_specifics: dict) -> str | None:
    """
    Try to match the product category to a collection key in the mapping file.
    """
    data = load_collection_keys()
    
    # Direct lookup: if category matches a collection name in mapping, return its SC key
    for _group, collections in data.items():
        for collection_name, sc_key in collections.items():
            if collection_name.lower() == category.lower():
                return sc_key
    
    # Partial match: if category contains key words from collection names
    category_lower = category.lower()
    for _group, collections in data.items():
        for collection_name, sc_key in collections.items():
            collection_lower = collection_name.lower()
            # Check if collection name is a substring or close match
            if collection_lower in category_lower or category_lower in collection_lower:
                return sc_key
    
    return None

def build_collection_key_fingerprint(title: str, category: str, tags: list[str], attributes: dict, metafields: dict) -> str:
    # Keep fingerprint tight so minor changes don’t trigger new LLM calls
    core = {
        "title": (title or "")[:180],
        "category": category or "",
        "tags": sorted([t for t in (tags or []) if isinstance(t, str) and (t.startswith("Category:") or t.startswith("Material:") or t.startswith("Domain:"))])[:80],
        "attributes_keys": sorted(list((attributes or {}).keys()))[:80],
        "metafields": metafields.get("system", {}),
    }
    return hash_dict(core)

def infer_collection_key_llm(
    title: str,
    category: str,
    tags: list[str],
    attributes: dict,
    metafields: dict,
) -> str | None:
    if not OPENAI_API_KEY:
        # If you didn't wire OPENAI_API_KEY yet, just skip silently
        return None

    allowed = allowed_collection_keys()
    if not allowed:
        return None

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Keep prompt grounded in YOUR normalized signals
    sys_msg = (
        "You are a product classifier for an antiques Shopify store.\n"
        "Your job: choose exactly ONE collection key tag for this product.\n"
        "Rules:\n"
        "- Only choose from allowed_keys.\n"
        "- Return null if nothing fits.\n"
        "- Choose the most specific match.\n"
        "- Do NOT invent new tags.\n"
    )

    user_payload = {
        "title": title,
        "category": category,
        "tags": tags[:120],
        "metafields_summary": {
            # small but useful
            "system": metafields.get("system", {}),
            "material": metafields.get("material", {}),
            "antique": metafields.get("antique", {}),
            "maker": metafields.get("maker", {}),
            "theme": metafields.get("theme", {}),
        },
        "attributes_sample": dict(list((attributes or {}).items())[:40]),
        "allowed_keys": allowed,
    }

    # Structured output via JSON schema (strict)
    # If your OpenAI SDK version doesn’t support this exact shape, tell me and I’ll adapt to your installed version.
    resp = client.responses.create(
    model=OPENAI_MODEL,
    input=[
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ],
    text={
        "format": {
            "type": "json_schema",
            "name": "collection_key_choice",
            "schema": {
                "type": "object",
                "properties": {
                    "collection_key": {"type": ["string", "null"]},
                    "reason": {"type": "string"}
                },
                "required": ["collection_key", "reason"],
                "additionalProperties": False
            }
        }
    },
)

    # The Responses API gives you a single output_text; parse it
    raw = resp.output_text.strip()
    try:
        data = json.loads(raw)
    except Exception:
        return None

    ck = data.get("collection_key")
    if ck is None:
        return None

    ck = str(ck).strip()
    if ck not in allowed:
        return None

    return ck



def hash_dict(d: dict) -> str:
    """Stable hash of important fields to detect changes."""
    s = str(sorted(d.items()))
    return hashlib.md5(s.encode()).hexdigest()


def _is_ignored_value(v: str) -> bool:
    return v.strip() in IGNORE_VALUES


def coerce_value(value, mf_type: str):
    """
    Coerce raw eBay values into stable values for metafields/tags.
    We keep this conservative: only convert when we're confident.
    """
    if value is None:
        return None

    # Normalize lists
    if isinstance(value, list):
        cleaned = []
        for x in value:
            if x is None:
                continue
            sx = str(x).strip()
            if not sx or _is_ignored_value(sx):
                continue
            cleaned.append(sx)
        return cleaned or None

    # Scalars -> string normalization
    s = str(value).strip()
    if not s or _is_ignored_value(s):
        return None

    if mf_type == "boolean":
        sl = s.lower()
        if sl in {"yes", "true", "y", "1"}:
            return True
        if sl in {"no", "false", "n", "0"}:
            return False
        # If it's not clearly boolean, don't force it
        return None

    if mf_type == "number_integer":
        # Only accept pure-ish integers
        try:
            # Handles "1906", " 1906 "
            if any(ch.isalpha() for ch in s):
                return None
            # Allow digits with minor punctuation (commas)
            si = s.replace(",", "").strip()
            if si.isdigit() or (si.startswith("-") and si[1:].isdigit()):
                return int(si)
        except Exception:
            return None
        return None

    if mf_type == "number_decimal":
        try:
            if any(ch.isalpha() for ch in s):
                return None
            sd = s.replace(",", "").strip()
            return float(sd)
        except Exception:
            return None

    # single_line_text and everything else
    return s


def infer_domain(category: str, item_specifics: dict) -> str | None:
    """
    Lightweight domain inference so we can route attributes into namespaces.
    Uses category + presence of telltale keys.
    """
    cat = (category or "").lower()
    keys = " ".join([str(k).lower() for k in (item_specifics or {}).keys()])

    text = f"{cat} {keys}"

    # blade / knives
    if any(t in text for t in ["blade material", "tang", "blade type", "bowie", "knife", "knives", "solingen", "damascus"]):
        return "blade"

    # books
    if any(t in text for t in ["binding", "publisher", "illustrator", "year printed", "book series", "book title", "hardcover", "paperback"]):
        return "book"

    # clocks
    if any(t in text for t in ["movement", "chime", "chime sequence", "wind up", "display type", "mantel clock", "desk clock", "alarm clock"]):
        return "clock"

    # art
    if any(t in text for t in ["painting", "print", "engraving", "artist", "watercolor", "acrylic", "framing", "image orientation"]):
        return "art"

    # militaria
    if any(t in text for t in ["militaria", "conflict", "ww i", "ww ii", "civil war"]):
        return "militaria"

    return None


def build_structured_metafields(category: str, item_specifics: dict) -> tuple[dict, dict]:
    """
    Returns:
      - structured_metafields: {namespace: {key: value}}
      - raw_leftovers: attributes not mapped into structured fields
    """
    if not isinstance(item_specifics, dict):
        return {}, {}

    domain = infer_domain(category, item_specifics)

    structured: dict[str, dict] = {}
    leftovers: dict[str, object] = {}

    for raw_key, raw_value in item_specifics.items():
        k = str(raw_key).strip()
        target = None

        # universal first
        if k in UNIVERSAL_META_MAP:
            target = UNIVERSAL_META_MAP[k]
        # then domain map
        elif domain and k in DOMAIN_META_MAP.get(domain, {}):
            target = DOMAIN_META_MAP[domain][k]

        if not target:
            leftovers[k] = raw_value
            continue

        namespace, mf_key, mf_type = target
        coerced = coerce_value(raw_value, mf_type)
        if coerced is None:
            continue

        structured.setdefault(namespace, {})[mf_key] = coerced

    # Always store the remaining attributes in a JSON-like bucket
    structured.setdefault("raw", {})["attributes"] = leftovers
    # Also store detected domain for debugging/templating
    if domain:
        structured.setdefault("system", {})["domain"] = domain

    return structured, leftovers


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
        values = value if isinstance(value, list) else [value]
        for v in values:
            v = str(v).strip()
            if not v or v in ignore_values:
                continue
            tags.add(f"{prefix}:{v}")

    for key, value in item_specifics.items():
        k = str(key).strip()

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


# ---------- eBay taxonomy helpers (unchanged) ----------

BAD_LEAF_NAMES = {
    "Other",
    "Others",
    "Miscellaneous",
    "Mixed Lots",
    "Mixed Lot",
    "Lot",
    "Factory Manufactured",   # too generic leaf
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
    category = (leaf or "").strip() or None

    if category in BAD_LEAF_NAMES and ancestors:
        category = None

    if not category and ancestors:
        candidate = (ancestors[-1] or "").strip()
        if candidate and candidate not in BAD_LEAF_NAMES:
            category = candidate

    if not category:
        id_map = {
            "37908": "Sculptures & Figurines",
            "28025": "Bookends",
            "48815": "Vintage Folding Knives",
        }
        if category_id and category_id in id_map:
            category = id_map[category_id]

    if not category:
        category = "Miscellaneous"

    return category


async def normalize_from_raw():
    """
    Read product_raw, build Shopify-friendly normalized docs in product_normalized.
    Adds:
      - normalized["metafields"] namespaced structure
      - normalized["metafields"]["raw"]["attributes"] leftovers
      - normalized["metafields"]["system"]["domain"] inferred domain
    """
    logger.info("▶ Normalizing RAW eBay products...")

    batch_size = 100
    last_id = None
    count = 0

    while True:
        query = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_raw.find(query).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for raw_doc in cursor:
            batch_docs.append(raw_doc)
            last_id = raw_doc["_id"]

        if not batch_docs:
            break

        for raw_doc in batch_docs:
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

            mapped_category = choose_category_from_path(
                category_leaf,
                category_ancestors,
                category_id,
            )

            # NEW: build structured metafields from ItemSpecifics
            structured_metafields, _leftovers = build_structured_metafields(mapped_category, item_specifics)

            # Preserve first_seen_at
            existing_norm = await db.product_normalized.find_one(
                {"_id": sku},
                {"first_seen_at": 1}
            )

            now_utc = datetime.now(timezone.utc)

            if existing_norm and existing_norm.get("first_seen_at"):
                first_seen_at = existing_norm["first_seen_at"]
            else:
                first_seen_at = now_utc

            # Tags from item specifics (unchanged)
            attr_tags = set(build_tags_from_item_specifics(item_specifics))

            # Add taxonomy tags from ancestors and root
            if category_root:
                attr_tags.add(f"Domain:{category_root}")

            for ancestor in category_ancestors:
                attr_tags.add(f"Category:{ancestor}")

            # Ensure tz-aware
            if first_seen_at.tzinfo is None:
                first_seen_at = first_seen_at.replace(tzinfo=timezone.utc)

            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            if first_seen_at >= (now_utc - timedelta(days=RECENT_DAYS)):
                attr_tags.add("Recently Added")

            all_tags = sorted(attr_tags)
                    # --- COLLECTION KEY (SC:...) ---
            existing_sc = pick_existing_sc_tag(all_tags)

            # only re-run the model if we don't already have one OR inputs changed
            ck_fingerprint = build_collection_key_fingerprint(title, mapped_category, all_tags, item_specifics, structured_metafields)
            prev_ck_fp = existing_norm.get("collection_key_fingerprint") if existing_norm else None
            prev_ck = existing_norm.get("collection_key") if existing_norm else None

            collection_key = None

            if existing_sc:
                # Already has SC: tag in tags
                collection_key = existing_sc
            elif prev_ck and prev_ck_fp == ck_fingerprint:
                # Reuse previous collection key if inputs haven't changed
                collection_key = prev_ck
            else:
                # Try mapping-based approach first
                collection_key = infer_collection_key_from_mapping(mapped_category, item_specifics)
                
                # Fall back to LLM if mapping didn't find a match
                if not collection_key:
                    try:
                        collection_key = infer_collection_key_llm(
                            title=title,
                            category=mapped_category,
                            tags=all_tags,
                            attributes=item_specifics,
                            metafields=structured_metafields,
                        )
                    except Exception as e:
                        logger.warning(f"LLM collection-key inference failed for SKU={sku}: {e}")
                        collection_key = None

            if collection_key:
                attr_tags.add(collection_key)
                all_tags = sorted(attr_tags)


            normalized = {
                "_id": sku,
                "sku": sku,
                "title": title,
                "description": description,
                "images": images,
                "price": price,
                "quantity": quantity,

                # leaf-based (or ancestor-based) category
                "category": mapped_category,

                # raw item specifics (keep as-is, useful for audits/debug)
                "attributes": item_specifics,

                # NEW: namespaced, Shopify-ready metafield structure
                "metafields": structured_metafields,

                # combined tags: specifics + taxonomy + recency
                "tags": all_tags,

                # structured breakdown of the eBay taxonomy
                "ebay_category": {
                    "id": category_id,
                    "path": category_path,
                    "root": category_root,
                    "leaf": category_leaf,
                    "ancestors": category_ancestors,
                },

                "first_seen_at": first_seen_at,
                "last_normalized_at": now_utc,
                "collection_key": collection_key,
                "collection_key_fingerprint": ck_fingerprint,

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
            })

            await db.product_normalized.update_one(
                {"_id": sku},
                {"$set": normalized},
                upsert=True
            )

            count += 1

    logger.info(f"✔ Normalization complete. {count} products updated.")
    return {"normalized": count}
