import re
from app.shopify.client import ShopifyClient
from app.database.mongo import db

client = ShopifyClient()

# ----------------------------------------------------------------------
# Helpers: Shopify metafield key/namespace sanitizing + payload builder
# ----------------------------------------------------------------------

_KEY_RE = re.compile(r"[^a-z0-9_]+")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")

# Shopify constraints (practical-safe defaults):
# - namespace: lowercase, digits, underscores, 3–20 chars
# - key: lowercase, digits, underscores, 3–30 chars
# If you already created metafield definitions, KEEP THEM CONSISTENT with these.
def _shopify_handle(s: str, *, max_len: int) -> str:
    s = (s or "").strip().lower()
    s = s.replace("/", "_").replace("-", "_").replace(" ", "_")
    s = _KEY_RE.sub("_", s)
    s = _MULTI_UNDERSCORE_RE.sub("_", s).strip("_")
    if not s:
        s = "value"
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    # Shopify requires keys/namespaces to start with a letter (best practice)
    if not s[0].isalpha():
        s = f"x_{s}"
        if len(s) > max_len:
            s = s[:max_len].rstrip("_")
    # Ensure minimum length 3
    if len(s) < 3:
        s = (s + "_xx")[:3]
    return s


def _sanitize_namespace(ns: str) -> str:
    return _shopify_handle(ns, max_len=20)


def _sanitize_key(k: str) -> str:
    return _shopify_handle(k, max_len=30)


def _normalize_metafield_type_and_value(value, mf_type: str):
    """
    Shopify REST metafield payload expects:
      - type: e.g. "single_line_text_field", "boolean", "number_integer",
              "number_decimal", "json", "list.single_line_text_field"
      - value: STRING for most types; JSON-serialized string for json;
              list types should be a JSON array string.
    """
    if value is None:
        return None, None

    mf_type = (mf_type or "").strip()

    # Default if missing
    if not mf_type:
        mf_type = "single_line_text_field"

    # Normalize common aliases (if your normalizer uses "single_line_text")
    alias_map = {
        "single_line_text": "single_line_text_field",
        "multi_line_text": "multi_line_text_field",
        "int": "number_integer",
        "integer": "number_integer",
        "float": "number_decimal",
        "decimal": "number_decimal",
        "bool": "boolean",
        "json": "json",
        "list.single_line_text": "list.single_line_text_field",
        "list.string": "list.single_line_text_field",
    }
    mf_type = alias_map.get(mf_type, mf_type)

    # Clean up strings like "na", "N/A", etc.
    def is_ignorable_str(x: str) -> bool:
        return x.strip() in {"", "na", "NA", "N/A", "None", "Unknown"}

    # list.* types: Shopify wants JSON array string
    if mf_type.startswith("list."):
        if isinstance(value, list):
            cleaned = [str(v).strip() for v in value if v is not None and not is_ignorable_str(str(v))]
        else:
            v = str(value).strip()
            cleaned = [v] if v and not is_ignorable_str(v) else []
        if not cleaned:
            return None, None
        # Shopify expects JSON string for list types
        import json
        return mf_type, json.dumps(cleaned)

    if mf_type == "boolean":
        if isinstance(value, bool):
            return mf_type, "true" if value else "false"
        s = str(value).strip().lower()
        if s in {"true", "yes", "y", "1"}:
            return mf_type, "true"
        if s in {"false", "no", "n", "0"}:
            return mf_type, "false"
        return None, None

    if mf_type == "number_integer":
        try:
            if isinstance(value, bool):
                return None, None
            iv = int(str(value).replace(",", "").strip())
            return mf_type, str(iv)
        except Exception:
            return None, None

    if mf_type == "number_decimal":
        try:
            if isinstance(value, bool):
                return None, None
            dv = float(str(value).replace(",", "").strip())
            # keep string representation
            return mf_type, str(dv)
        except Exception:
            return None, None

    if mf_type == "json":
        import json
        try:
            # value can be dict/list already
            return mf_type, json.dumps(value)
        except Exception:
            # fallback: store as string JSON
            return mf_type, json.dumps(str(value))

    # Text fields: Shopify expects a string
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if is_ignorable_str(s):
        return None, None
    # Avoid trailing commas you mentioned
    s = s.rstrip(",")
    return mf_type, s


def process_structured_metafields_to_shopify_payload(metafields_struct: dict) -> list[dict]:
    """
    Takes your new normalized structure:
      doc["metafields"] = {
        "antique": {"era": "...", "year_manufactured": 1906, ...},
        "blade": {"blade_material": "..."},
        "raw": {"attributes": {...}},
        "system": {"domain": "blade"}
      }

    Returns Shopify REST metafields list:
      [{"namespace": "blade", "key": "blade_material", "type": "...", "value": "..."}]
    """
    if not isinstance(metafields_struct, dict):
        return []

    metafields_payload: list[dict] = []

    for ns, kv in metafields_struct.items():
        # Skip internal/system namespaces from being pushed to Shopify
        if ns in {"system"}:
            continue

        # If you want raw.attributes in Shopify, keep it as JSON; otherwise skip
        if ns == "raw":
            raw_attrs = (kv or {}).get("attributes")
            if raw_attrs:
                namespace = _sanitize_namespace("raw")
                key = _sanitize_key("attributes")
                mf_type, mf_value = _normalize_metafield_type_and_value(raw_attrs, "json")
                if mf_type and mf_value:
                    metafields_payload.append({
                        "namespace": namespace,
                        "key": key,
                        "type": mf_type,
                        "value": mf_value
                    })
            continue

        if not isinstance(kv, dict):
            continue

        namespace = _sanitize_namespace(ns)

        for k, v in kv.items():
            key = _sanitize_key(k)
            # If your normalizer stored types separately, adapt here.
            # Right now we infer type from python value:
            inferred_type = None
            if isinstance(v, bool):
                inferred_type = "boolean"
            elif isinstance(v, int) and not isinstance(v, bool):
                inferred_type = "number_integer"
            elif isinstance(v, float):
                inferred_type = "number_decimal"
            elif isinstance(v, list):
                inferred_type = "list.single_line_text_field"
            elif isinstance(v, (dict,)):
                inferred_type = "json"
            else:
                inferred_type = "single_line_text_field"

            mf_type, mf_value = _normalize_metafield_type_and_value(v, inferred_type)
            if not mf_type or mf_value is None:
                continue

            metafields_payload.append({
                "namespace": namespace,
                "key": key,
                "type": mf_type,
                "value": mf_value
            })

    return metafields_payload

# ----------------------------------------------------------------------


async def create_shopify_product(doc, shopify_client=None):
    if shopify_client is None:
        shopify_client = client

    # 1) Build metafields from the new namespaced structure (doc["metafields"])
    metafields_payload = []
    mf_struct = doc.get("metafields", {})
    if mf_struct:
        metafields_payload = process_structured_metafields_to_shopify_payload(mf_struct)

    # 2) Tags (keep your current tags)
    tag_list = []
    tag_list.extend(doc.get("tags", []) or [])
    tags_str = ", ".join(sorted(set([t for t in tag_list if t])))

    # 3) Build payload
    images = [{"src": img} for img in (doc.get("images", []) or []) if img]

    # Ensure price is a string Shopify accepts
    price = doc.get("price")
    price_str = str(price) if price not in (None, "") else "0"

    payload = {
        "product": {
            "title": doc.get("title", ""),
            "body_html": doc.get("description") or "",
            "tags": tags_str,
            "metafields": metafields_payload,
            "images": images,
            "variants": [{
                "sku": doc.get("sku", ""),
                "price": price_str,
                "inventory_management": "shopify",
                "inventory_quantity": int(doc.get("quantity", 0) or 0),
            }],
        }
    }

    res = await shopify_client.post("products.json", payload)
    product = (res or {}).get("product")
    if not product:
        print("❌ Shopify creation failed:", res)
        return None

    pid = product["id"]
    vid = product["variants"][0]["id"]

    await db.product_normalized.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "shopify_id": pid,
            "shopify_variant_id": vid,
            "last_synced_hash": doc.get("hash"),
        }}
    )

    print(f"✔ Created Shopify product {doc['_id']} -> {pid}")
    return product
