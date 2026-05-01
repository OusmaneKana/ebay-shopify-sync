import hashlib
import logging
from datetime import datetime, timezone, timedelta
from app.database.mongo import db
import re,json
from functools import lru_cache
from openai import OpenAI
from app.config import settings
import asyncio
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation


logger = logging.getLogger(__name__)

RECENT_DAYS = 7  # How many days count as "recent"


def _money_2dp(value: object) -> float | None:
    """Normalize money-like values to a 2-decimal float.

    Important: using Decimal avoids binary float artifacts like 39.989999999999995.
    """

    if value is None:
        return None
    try:
        d = Decimal(str(value).strip())
        d = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return None

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
COLLECTION_KEYS_PATH = getattr(settings, "COLLECTION_KEYS_PATH", "app/resources/category_mapping.json")
OPENAI_MODEL = getattr(settings, "OPENAI_MODEL_COLLECTION_KEY", "gpt-4.1-mini")
OPENAI_API_KEY = settings.OPENAI_API_KEY
OPENAI_CLIENT = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
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

    client = OPENAI_CLIENT
    if client is None:
        return None

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


def canonicalize_title(title: object) -> str:
    """Reduce titles to a stable comparison key for cross-SKU candidate matching."""

    text = str(title or "").strip().lower()
    if not text:
        return ""

    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compute_title_hash(title: object) -> str | None:
    canonical_title = canonicalize_title(title)
    if not canonical_title:
        return None
    return hashlib.md5(canonical_title.encode("utf-8")).hexdigest()


def compute_content_hash(fields: dict) -> str:
    """Stable content hash for normalized Shopify-relevant fields.

    Uses JSON with sorted keys for determinism; falls back to hash_dict if
    something isn't JSON-serializable.
    """

    try:
        payload = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    except TypeError:
        # Fallback: still deterministic, but less structured than JSON
        return hash_dict(fields)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


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


# --- Canonical tag key sets (hoisted for performance) ---

TAG_BRAND_KEYS = {
    "Brand", "Maker", "Manufacturer", "Make", "Artist", "Author",
    "Sculptor", "Publisher", "Giuseppe Armani", "Lladro",
}

TAG_MODEL_KEYS = {
    "Model", "Series", "Series Title", "Series/Movie", "Product Line",
    "Game Title", "Movie/TV Title", "TV Show", "TV/Streaming Show", "Book Series",
}

TAG_MATERIAL_KEYS = {
    "Material", "Primary Material", "Case Material", "Band Material",
    "Handle Material", "Handle/Strap Material", "Metal", "Metal Purity",
    "Glass Type", "Glassware Type", "Porcelain Type", "Production Style",
    "Production Technique", "Surface Coating",
}

TAG_COLOR_KEYS = {
    "Color", "Colour", "Main Color", "Exterior Color", "Band Color",
    "Case Color", "Dial Color", "Lens Color", "Frame Color",
    "Lining Color", "Light Color", "Cord Color", "Blade Color",
}

TAG_ERA_KEYS = {
    "Decade", "Era", "Time Period", "Time Period Manufactured",
    "Time Period Produced", "Historical Period", "Year Manufactured",
    "Year of Manufacture", "Year", "Year Issued", "Year Printed",
    "Publication Year", "Release Year", "Date of Origin", "Date of Creation",
    "Post-WWII", "Victorian", "Mid-20th century", "early 1900",
}

TAG_ORIGIN_KEYS = {
    "Country of Origin", "Country/Region of Origin", "Place of Origin",
    "Region", "Region of Origin", "Country", "Country/Region",
    "Country of Manufacture", "Place of Publication", "Culture",
    "Ethnic & Regional Style", "Tribal Affiliation", "Tribe", "Origin",
}

TAG_STYLE_KEYS = {
    "Style", "Look", "Occasion", "Season", "Holiday", "Room",
    "Jewelry Department", "Department",
}

TAG_MOVEMENT_KEYS = {"Movement", "Escapement Type"}

TAG_CATEGORY_KEYS = {
    "Object Type", "Product Type", "Product", "Collection",
    "Game Type", "Sport", "Sport/Activity", "Type",
    "Type of Advertising", "Type of Glass", "Type of Tool",
}

TAG_STONE_KEYS = {
    "Main Stone", "Main Stone Color", "Main Stone Shape",
    "Secondary Stone", "Total Carat Weight", "Diamond Clarity Grade",
    "Diamond Color Grade", "Cut Grade",
}

TAG_FEATURE_KEYS = {
    "Features", "Special Features", "Special Attributes", "Limited Edition",
    "Retired", "Handmade", "Autographed", "Signed", "Signed By", "Signed by",
    "Certificate of Authenticity (COA)", "Certification",
}

TAG_SIZE_KEYS = {
    "Size", "Length", "Height", "Width (Inches)", "Diameter",
    "Ring Size", "Necklace Length", "Case Size", "Lug Width",
    "Band Width", "Max Wrist Size", "Item Length", "Item Height",
    "Item Width", "Item Diameter", "Scale",
}

TAG_THEME_KEYS = {
    "Theme", "Subject", "Subject/Theme", "Topic", "Holiday",
    "Series", "Franchise", "Character", "Character Family",
    "Character/Story/Theme", "Superhero Team",
}

TAG_SPORT_KEYS = {
    "Sport", "Sport/Activity", "League", "Team", "Team-Baseball",
    "Event/Tournament", "Player", "Player/Athlete",
}

TAG_ROOM_KEYS = {"Room"}

TAG_IGNORE_VALUES = {"", "No", "Not Water Resistant", "Unknown", "N/A", "na", "NA", "None"}


def build_tags_from_item_specifics(item_specifics: dict) -> list[str]:
    """
    Convert ItemSpecifics into normalized Shopify tags using canonical dimensions.
    Example output tags:
      Brand:Rado, Model:DiaStar, Material:Ceramic, Color:Black, Era:1970s, Origin:Japan, Movement:Quartz
    """
    if not isinstance(item_specifics, dict):
        return []
    tags: set[str] = set()

    def add_tag(prefix: str, value):
        values = value if isinstance(value, list) else [value]
        for v in values:
            v = str(v).strip()
            if not v or v in TAG_IGNORE_VALUES:
                continue
            tags.add(f"{prefix}:{v}")

    for key, value in item_specifics.items():
        k = str(key).strip()

        if k in TAG_BRAND_KEYS:
            add_tag("Brand", value)
        elif k in TAG_MODEL_KEYS:
            add_tag("Model", value)
        elif k in TAG_MATERIAL_KEYS:
            add_tag("Material", value)
        elif k in TAG_COLOR_KEYS:
            add_tag("Color", value)
        elif k in TAG_ERA_KEYS:
            add_tag("Era", value)
        elif k in TAG_ORIGIN_KEYS:
            add_tag("Origin", value)
        elif k in TAG_STYLE_KEYS:
            add_tag("Style", value)
        elif k in TAG_MOVEMENT_KEYS:
            add_tag("Movement", value)
        elif k in TAG_CATEGORY_KEYS:
            add_tag("Category", value)
        elif k in TAG_STONE_KEYS:
            add_tag("Stone", value)
        elif k in TAG_FEATURE_KEYS:
            add_tag("Feature", value)
        elif k in TAG_SIZE_KEYS:
            add_tag("Size", value)
        elif k in TAG_THEME_KEYS:
            add_tag("Theme", value)
        elif k in TAG_SPORT_KEYS:
            add_tag("Sport", value)
        elif k in TAG_ROOM_KEYS:
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


def normalize_shipping(shipping_raw: dict) -> list[dict]:
    """
    Normalize shipping details to a list of shipping options with service and cost.
    """
    options = []
    domestic_opts = shipping_raw.get('service_options') or []
    intl_opts = shipping_raw.get('international_service_options') or []
    logger.debug(f"Normalizing shipping data: {len(domestic_opts)} domestic, {len(intl_opts)} international options")

    # Domestic shipping options
    if domestic_opts:
        for opt in domestic_opts:
            service = opt.get("service")
            cost = opt.get("cost")
            if service and cost:
                options.append({
                    "service": service,
                    "cost": cost,
                    "type": "domestic"
                })
                logger.debug(f"Added domestic shipping: {service} - ${cost}")

    # International shipping options
    if intl_opts:
        for opt in intl_opts:
            service = opt.get("service")
            cost = opt.get("cost")
            if service and cost:
                options.append({
                    "service": service,
                    "cost": cost,
                    "type": "international"
                })
                logger.debug(f"Added international shipping: {service} - ${cost}")

    logger.debug(f"Shipping normalization complete: {len(options)} total options")
    return options


async def normalize_from_raw(skus: list[str] | None = None):
    """
    Read product_raw, build Shopify-friendly normalized docs in product_normalized.
    Adds:
      - normalized["metafields"] namespaced structure
      - normalized["metafields"]["raw"]["attributes"] leftovers
      - normalized["metafields"]["system"]["domain"] inferred domain
    """
    target_skus = sorted({str(s).strip() for s in (skus or []) if str(s).strip()})
    if target_skus:
        logger.info("▶ Normalizing RAW products for %s SKU(s)...", len(target_skus))
    else:
        logger.info("▶ Normalizing RAW eBay products...")

    await db.product_normalized.create_index(
        "canonical_title_hash",
        name="idx_product_normalized_canonical_title_hash",
        background=True,
    )

    batch_size = 100
    last_id = None
    count = 0

    # Limit concurrent normalization work so we don't overload Mongo or external services
    sem = asyncio.Semaphore(10)

    while True:
        query = {}
        if target_skus:
            id_query: dict[str, object] = {"$in": target_skus}
            if last_id is not None:
                id_query["$gt"] = last_id
            query["_id"] = id_query
        elif last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = db.product_raw.find(query).limit(batch_size).sort("_id", 1)

        batch_docs = []
        async for raw_doc in cursor:
            batch_docs.append(raw_doc)
            last_id = raw_doc["_id"]

        if not batch_docs:
            break

        # Prefetch existing normalized docs for this batch to avoid N+1 lookups
        sku_list: list = []
        for raw_doc in batch_docs:
            sku = raw_doc.get("SKU") or raw_doc.get("_id")
            if sku:
                sku_list.append(sku)

        existing_by_sku: dict = {}
        if sku_list:
            cursor_norm = db.product_normalized.find(
                {"_id": {"$in": sku_list}},
                {
                    "first_seen_at": 1,
                    "collection_key": 1,
                    "collection_key_fingerprint": 1,
                    "hash": 1,
                    "content_hash": 1,
                    "channels": 1,
                },
            )
            async for doc in cursor_norm:
                existing_by_sku[doc["_id"]] = doc

        title_hashes: set[str] = set()
        for raw_doc in batch_docs:
            raw = raw_doc.get("raw", {}) or {}
            title_hash = compute_title_hash(raw.get("Title"))
            if title_hash:
                title_hashes.add(title_hash)

        existing_by_title_hash: dict[str, list[dict]] = {}
        if title_hashes:
            cursor_title_matches = db.product_normalized.find(
                {"canonical_title_hash": {"$in": list(title_hashes)}},
                {
                    "_id": 1,
                    "title": 1,
                    "canonical_title": 1,
                    "canonical_title_hash": 1,
                    "quantity": 1,
                    "updated_at": 1,
                },
            )
            async for doc in cursor_title_matches:
                title_hash = doc.get("canonical_title_hash")
                if not title_hash:
                    continue
                existing_by_title_hash.setdefault(title_hash, []).append(doc)

        # Single timestamp per batch is sufficient and cheaper
        now_utc = datetime.now(timezone.utc)

        async def process_raw(raw_doc: dict) -> int:
            async with sem:
                sku = raw_doc.get("SKU") or raw_doc.get("_id")
                if not sku:
                    logger.warning("Found raw product with no SKU, skipping")
                    return 0

                logger.debug(f"Processing normalization for SKU: {sku}")
                raw = raw_doc.get("raw", {}) or {}

                title = (raw.get("Title") or "").strip()
                canonical_title = canonicalize_title(title)
                canonical_title_hash = compute_title_hash(title)
                description = (raw.get("Description") or "").strip()
                images = raw.get("Images", []) or []
                # Normalize price to a numeric value (float) when possible
                raw_price = raw.get("Price")
                price = None
                if isinstance(raw_price, (int, float)):
                    price = float(raw_price)
                elif raw_price is not None:
                    try:
                        # Allow common string formats like "49.99" or "$49.99"
                        price_str = str(raw_price).replace("$", "").strip()
                        price = float(price_str) if price_str else None
                    except (TypeError, ValueError):
                        price = None
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

                # Default AI workflow status for downstream content generation.
                # Do NOT overwrite if it already exists (e.g., moved to in_progress/completed).
                ai_ns = structured_metafields.setdefault("ai_", {})
                if not ai_ns.get("content_status"):
                    ai_ns["content_status"] = "pending"

                # Preserve first_seen_at
                existing_norm = existing_by_sku.get(sku)

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

                # Ensure tz-aware for comparisons and storage
                if first_seen_at.tzinfo is None:
                    first_seen_at = first_seen_at.replace(tzinfo=timezone.utc)

                local_now_utc = now_utc
                if local_now_utc.tzinfo is None:
                    local_now_utc = local_now_utc.replace(tzinfo=timezone.utc)

                if first_seen_at >= (local_now_utc - timedelta(days=RECENT_DAYS)):
                    attr_tags.add("Recently Added")

                all_tags = sorted(attr_tags)

                # --- COLLECTION KEY (SC:...) ---
                existing_sc = pick_existing_sc_tag(all_tags)

                # only re-run the model if we don't already have one OR inputs changed
                ck_fingerprint = build_collection_key_fingerprint(
                    title, mapped_category, all_tags, item_specifics, structured_metafields
                )
                prev_ck_fp = existing_norm.get("collection_key_fingerprint") if existing_norm else None
                prev_ck = existing_norm.get("collection_key") if existing_norm else None

                collection_key = None

                if existing_sc:
                    # Already has SC: tag in tags
                    collection_key = existing_sc
                    logger.debug(f"SKU {sku}: Using existing SC tag: {collection_key}")
                elif prev_ck and prev_ck_fp == ck_fingerprint:
                    # Reuse previous collection key if inputs haven't changed
                    collection_key = prev_ck
                    logger.debug(f"SKU {sku}: Reusing previous collection key: {collection_key}")
                else:
                    # Try mapping-based approach first
                    collection_key = infer_collection_key_from_mapping(mapped_category, item_specifics)
                    logger.debug(f"SKU {sku}: Mapping-based collection key: {collection_key}")

                    # Fall back to LLM if mapping didn't find a match
                    if not collection_key:
                        try:
                            collection_key = await asyncio.to_thread(
                                infer_collection_key_llm,
                                title,
                                mapped_category,
                                all_tags,
                                item_specifics,
                                structured_metafields,
                            )
                            logger.debug(f"SKU {sku}: LLM-inferred collection key: {collection_key}")
                        except Exception as e:  # pragma: no cover - defensive around external API
                            logger.warning(f"LLM collection-key inference failed for SKU={sku}: {e}")
                            collection_key = None

                if collection_key:
                    attr_tags.add(collection_key)
                    all_tags = sorted(attr_tags)

                # Normalize shipping
                shipping_raw = raw.get("Shipping", {}) or {}
                normalized_shipping = normalize_shipping(shipping_raw)

                # Extract package weight/dimensions from Shipping.package_details (if present)
                package_details_raw = shipping_raw.get("package_details") or {}
                normalized_package: dict = {}

                def _normalize_measure(measure: object) -> dict | None:
                    if not isinstance(measure, dict):
                        return None
                    value = measure.get("value")
                    if value is None:
                        return None
                    try:
                        v = float(str(value).strip())
                    except Exception:
                        return None
                    out: dict[str, object] = {"value": v}
                    unit = measure.get("unit")
                    if unit:
                        out["unit"] = str(unit)
                    msys = measure.get("measurement_system") or measure.get("measurementSystem")
                    if msys:
                        out["measurement_system"] = str(msys)
                    return out

                if isinstance(package_details_raw, dict):
                    weight_raw = package_details_raw.get("weight") or {}
                    dims_raw = package_details_raw.get("dimensions") or {}

                    # Weight (major/minor, e.g. lb/oz)
                    weight_norm: dict = {}
                    major_norm = _normalize_measure(weight_raw.get("major")) if isinstance(weight_raw, dict) else None
                    minor_norm = _normalize_measure(weight_raw.get("minor")) if isinstance(weight_raw, dict) else None
                    if major_norm is not None:
                        weight_norm["major"] = major_norm
                    if minor_norm is not None:
                        weight_norm["minor"] = minor_norm
                    if weight_norm:
                        normalized_package["weight"] = weight_norm

                    # Dimensions (length/width/height)
                    dims_norm: dict = {}
                    if isinstance(dims_raw, dict):
                        for key in ("length", "width", "height"):
                            m = _normalize_measure(dims_raw.get(key))
                            if m is not None:
                                dims_norm[key] = m
                    if dims_norm:
                        normalized_package["dimensions"] = dims_norm

                # Expose package info as a shipping namespace metafield for Shopify
                if normalized_package:
                    structured_metafields.setdefault("shipping", {})["package"] = normalized_package

                # Adjust price based on shipping cost
                adjusted_price = price
                if normalized_shipping:
                    # Get the first domestic shipping cost
                    shipping_cost = None
                    for opt in normalized_shipping:
                        if opt.get("type") == "domestic":
                            try:
                                shipping_cost = float(opt.get("cost", 0))
                                break
                            except (ValueError, TypeError):
                                continue

                    if shipping_cost is not None:
                        # Apply pricing adjustment based on shipping cost tier
                        if shipping_cost == 8.0:
                            adjusted_price = (price or 0) + 10
                            attr_tags.add("free_shipping")
                            logger.debug(
                                f"SKU {sku}: $8 shipping → +$10 to price, added free_shipping tag"
                            )
                        elif shipping_cost == 14.0:
                            adjusted_price = (price or 0) + 15
                            attr_tags.add("free_shipping")
                            logger.debug(
                                f"SKU {sku}: $14 shipping → +$15 to price, added free_shipping tag"
                            )
                        elif shipping_cost == 18.0:
                            adjusted_price = (price or 0) + 20
                            attr_tags.add("free_shipping")
                            logger.debug(
                                f"SKU {sku}: $18 shipping → +$20 to price, added free_shipping tag"
                            )

                    # Update all_tags with any new tags added
                    all_tags = sorted(attr_tags)

                # Ensure money values are stable (2dp) for storage + downstream integrations.
                # This prevents float artifacts like 39.989999999999995.
                adjusted_price = _money_2dp(adjusted_price)

                # Extract eBay posted date from raw document
                ebay_posted_at = raw_doc.get("ebay_posted_at")

                # Compute a stable "content hash" of the normalized business fields.
                # This intentionally excludes transient fields like last_normalized_at
                # so we can skip writing unchanged documents.
                content_fields = {
                    "title": title,
                    "description": description,
                    "images": tuple(images),
                    "price": adjusted_price,
                    "quantity": quantity,
                    "category": mapped_category,
                    "tags": tuple(all_tags),
                    "metafields": structured_metafields,
                    "shipping": normalized_shipping,
                    "package": normalized_package,
                }

                new_hash = compute_content_hash(content_fields)

                existing_hash = None
                if existing_norm:
                    existing_hash = existing_norm.get("content_hash") or existing_norm.get("hash")
                if existing_hash == new_hash:
                    logger.debug(f"SKU {sku}: normalized hash unchanged, skipping update")
                    return 0

                title_match_candidates: list[dict[str, object]] = []
                if canonical_title_hash:
                    for candidate in existing_by_title_hash.get(canonical_title_hash, []):
                        candidate_sku = candidate.get("_id")
                        if not candidate_sku or candidate_sku == sku:
                            continue
                        title_match_candidates.append(
                            {
                                "sku": str(candidate_sku),
                                "title": candidate.get("title") or "",
                                "quantity": int(candidate.get("quantity") or 0),
                                "updated_at": candidate.get("updated_at"),
                            }
                        )

                title_match_candidates.sort(
                    key=lambda candidate: (
                        -int(candidate.get("quantity") or 0),
                        str(candidate.get("sku") or ""),
                    )
                )
                title_match_candidates = title_match_candidates[:10]

                channels = dict((existing_norm or {}).get("channels") or {})
                channels_ebay = dict(channels.get("ebay") or {})
                channels_ebay.update(
                    {
                        "posted_at": ebay_posted_at,
                        "category": {
                            "id": category_id,
                            "path": category_path,
                            "root": category_root,
                            "leaf": category_leaf,
                            "ancestors": category_ancestors,
                        },
                    }
                )
                channels["ebay"] = channels_ebay

                normalized = {
                    "_id": sku,
                    "sku": sku,
                    "title": title,
                    "canonical_title": canonical_title,
                    "canonical_title_hash": canonical_title_hash,
                    "description": description,
                    "images": images,
                    "price": adjusted_price,
                    "quantity": quantity,

                    # leaf-based (or ancestor-based) category
                    "category": mapped_category,

                    # raw item specifics (keep as-is, useful for audits/debug)
                    "attributes": item_specifics,

                    # NEW: namespaced, Shopify-ready metafield structure
                    "metafields": structured_metafields,

                    # combined tags: specifics + taxonomy + recency
                    "tags": all_tags,

                    # shipping options and costs
                    "shipping": normalized_shipping,

                    # package-level weight and dimensions (already mirrored into metafields.shipping.package)
                    "package": normalized_package,

                    # structured breakdown of the eBay taxonomy
                    "ebay_category": {
                        "id": category_id,
                        "path": category_path,
                        "root": category_root,
                        "leaf": category_leaf,
                        "ancestors": category_ancestors,
                    },

                    "first_seen_at": first_seen_at,
                    "last_normalized_at": local_now_utc,
                    "collection_key": collection_key,
                    "collection_key_fingerprint": ck_fingerprint,
                    "title_match_candidate_count": len(title_match_candidates),
                    "title_match_candidate_skus": [candidate["sku"] for candidate in title_match_candidates],
                    "title_match_candidates": title_match_candidates,
                    # Backwards compatibility: keep legacy 'hash' field, but also
                    # store a more explicit 'content_hash' used by Shopify sync.
                    "hash": new_hash,
                    "content_hash": new_hash,
                    "ebay_posted_at": ebay_posted_at,
                    "channels": channels,

                }

                await db.product_normalized.update_one(
                    {"_id": sku},
                    {"$set": normalized},
                    upsert=True,
                )

                logger.debug(
                    f"✓ Saved normalized product for SKU: {sku} | Category: {mapped_category} | Tags: {len(all_tags)}"
                )

                return 1

        # Process this batch concurrently with bounded concurrency
        tasks = [asyncio.create_task(process_raw(raw_doc)) for raw_doc in batch_docs]
        if tasks:
            results = await asyncio.gather(*tasks)
            count += sum(results)

    logger.info(f"✔ Normalization complete. {count} products updated.")
    return {"normalized": count}
