from app.shopify.client import ShopifyClient
from app.database.mongo import db

client = ShopifyClient()

# ----------------------------------------------------------------------
# NEW HELPER FUNCTION: Process attributes into Metafields
# ----------------------------------------------------------------------
def process_attributes_to_metafields(attributes: dict) -> list:
    """
    Converts a dictionary of attributes into a list of Shopify Metafield payloads.

    Args:
        attributes: The 'attributes' dictionary from the normalized database document.
    
    Returns:
        A list of dictionaries formatted for the 'metafields' key in the Shopify API payload.
    """
    metafields_list = []
    namespace = "custom"  # Use 'custom' namespace for user-defined metafields

    # Define a simple mapping and data type for your custom attributes
    # NOTE: The keys must match the keys you have defined in your Shopify Admin
    # Setting the 'type' to "single_line_text_field" is the most common default
    
    # You will need to standardize the attribute names from your normalized document.
    # The keys in the attributes dict (e.g., 'Country of Origin') become the metafield key
    
    for key, value in attributes.items():
        # Clean the key for use as a Shopify Metafield key
        # (e.g., 'Country of Origin' -> 'country_of_origin')
        metafield_key = key.lower().replace(' ', '_').replace('/', '_')
        
        # Ensure the value is a string (as required by the API unless using a specific type)
        # We also strip any trailing characters like the comma visible in the screenshot
        if isinstance(value, str):
            cleaned_value = value.strip().rstrip(',')
        elif value is not None:
            cleaned_value = str(value)
        else:
            continue # Skip null values

        metafields_list.append({
            "key": metafield_key,
            "value": cleaned_value,
            "namespace": namespace,
            # For simplicity, we assume text fields, but this could be dynamic
            "type": "single_line_text_field" 
        })

    return metafields_list
# ----------------------------------------------------------------------


async def create_shopify_product(doc, shopify_client=None):
    if shopify_client is None:
        shopify_client = client
    
    # 1. Generate Metafields from the 'attributes' object
    metafields_payload = []
    attributes = doc.get("attributes", {})
    if attributes:
        metafields_payload = process_attributes_to_metafields(attributes)

    # 2. Process Tags
    # We still use tags for simple labels or legacy integration, 
    # but the rich data is moved to Metafields.
    tag_list = []
    # No longer including doc.get("category") in tags since it is in attributes/metafields
    tag_list.extend(doc.get("tags", []))
    tags_str = ", ".join(sorted(set(tag_list)))

    # 3. Construct the Main Payload including Metafields


    payload = {
        "product": {
            "title": doc["title"],
            "body_html": doc.get("description") or "",
            "tags": tags_str,
            # Add the generated metafields list to the product object
            "metafields": metafields_payload, 
            "images": [{"src": img} for img in doc.get("images", [])],
            "variants": [{
                "sku": doc["sku"],
                "price": doc.get("price") or "0",
                "inventory_management": "shopify",
                "inventory_quantity": doc.get("quantity", 0),
            }],
        }
    }

    res = shopify_client.post("products.json", payload)
    product = res.get("product")
    if not product:
        print("❌ Shopify creation failed:", res)
        return None

    pid = product["id"]
    vid = product["variants"][0]["id"]

    # ... (Database update and logging remains the same)
    await db.product_normalized.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "shopify_id": pid,
            "shopify_variant_id": vid,
            "last_synced_hash": doc.get("hash"),
        }}
    )

    print(f"✔ Created Shopify product {doc['_id']} -> {pid}")
    return 