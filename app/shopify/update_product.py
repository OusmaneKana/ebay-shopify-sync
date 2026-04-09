import logging
from app.shopify.client import ShopifyClient
from app.services.channel_utils import get_shopify_field
from app.shopify.create_product import (
    process_structured_metafields_to_shopify_payload,
    extract_weight_for_shopify_variant,
)
from app.services.shopify_sale_pricing import resolve_shopify_variant_pricing

logger = logging.getLogger(__name__)
client = ShopifyClient()


async def update_shopify_product(old_doc, new_doc, shopify_client=None):
    if shopify_client is None:
        shopify_client = client
    
    pid = get_shopify_field(old_doc, "shopify_id")
    vid = get_shopify_field(old_doc, "shopify_variant_id")
    doc_id = old_doc.get('_id')
    
    if not pid or not vid:
        logger.warning(f"⚠ Cannot update Shopify product for {doc_id}: missing IDs (product_id={pid}, variant_id={vid})")
        return None

    try:
        logger.info(f"Updating product ID: {pid}, Variant ID: {vid}")

        # Rebuild tags string from latest normalized doc
        tag_list = []
        if new_doc.get("category"):
            tag_list.append(new_doc["category"])
        tag_list.extend(new_doc.get("tags", []))
        tags_str = ", ".join(sorted(set(tag_list)))

        # Update main product properties
        try:
            await shopify_client.put(f"products/{pid}.json", {
                "product": {
                    "id": pid,
                    "title": new_doc["title"],
                    "body_html": new_doc.get("description") or "",
                    "tags": tags_str,
                }
            })
            logger.debug(f"Updated product properties for {pid}")
        except Exception as e:
            logger.error(f"Failed to update product properties for {pid}: {e}", exc_info=True)
            raise

        # Update variant price and weight
        weight_value, weight_unit = extract_weight_for_shopify_variant(new_doc)

        pricing = resolve_shopify_variant_pricing(new_doc)
        variant_payload = {
            "id": vid,
            "price": pricing["price"],
            # None clears compare-at price when sale is not effective.
            "compare_at_price": pricing["compare_at_price"],
        }

        if weight_value is not None and weight_unit:
            variant_payload["weight"] = weight_value
            variant_payload["weight_unit"] = weight_unit

        try:
            await shopify_client.put(f"variants/{vid}.json", {
                "variant": variant_payload
            })
            logger.debug(f"Updated variant price for {vid}")
        except Exception as e:
            logger.error(f"Failed to update variant price for {vid}: {e}", exc_info=True)
            raise

        # Handle metafields: fetch existing, update or create
        mf_struct = new_doc.get("metafields", {})
        if mf_struct:
            try:
                # Get existing metafields for the product
                existing_mf_res = await shopify_client.get("metafields.json", params={
                    "owner_id": pid,
                    "owner_resource": "product"
                })
                existing_mf = existing_mf_res.get("metafields", [])
                # Map existing by (namespace, key) to metafield dict
                existing_map = {(mf["namespace"], mf["key"]): mf for mf in existing_mf}

                metafields_payload = process_structured_metafields_to_shopify_payload(mf_struct)
                for mf in metafields_payload:
                    key = (mf["namespace"], mf["key"])
                    try:
                        if key in existing_map:
                            # Update existing metafield
                            mf_id = existing_map[key]["id"]
                            update_payload = {
                                "metafield": {
                                    "id": mf_id,
                                    "value": mf["value"],
                                    "type": mf["type"],
                                }
                            }
                            await shopify_client.put(f"metafields/{mf_id}.json", update_payload)
                            logger.debug(f"Updated metafield {key} (id={mf_id}) for product {pid}")
                        else:
                            # Create new metafield
                            mf_payload = {
                                "metafield": {
                                    "namespace": mf["namespace"],
                                    "key": mf["key"],
                                    "value": mf["value"],
                                    "type": mf["type"],
                                    "owner_id": pid,
                                    "owner_resource": "product",
                                }
                            }
                            await shopify_client.post("metafields.json", mf_payload)
                            logger.debug(f"Created metafield {key} for product {pid}")
                    except Exception as e:
                        logger.error(f"Failed to update/create metafield {key} for product {pid}: {e}", exc_info=True)
                        raise
            except Exception as e:
                logger.error(f"Failed to handle metafields for product {pid}: {e}", exc_info=True)
                raise

        logger.info(f"✔ Successfully updated Shopify product {pid} (eBay doc: {doc_id})")
        return pid

    except Exception as e:
        logger.error(f"✗ Failed to update Shopify product {pid} for eBay item {doc_id}: {str(e)}", exc_info=True)
        raise
