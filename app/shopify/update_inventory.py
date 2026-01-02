import logging
from typing import Optional

from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)
client = ShopifyClient()


async def _get_variant(variant_id: int, shopify_client: Optional[ShopifyClient] = None) -> dict | None:
    if shopify_client is None:
        shopify_client = client
    resp = await shopify_client.get(f"variants/{variant_id}.json")
    return (resp or {}).get("variant")


async def _update_variant_quantity(variant_id: int, new_qty: int, shopify_client: Optional[ShopifyClient] = None) -> bool:
    if shopify_client is None:
        shopify_client = client
    payload = {"variant": {"id": variant_id, "inventory_quantity": int(new_qty)}}
    res = await shopify_client.put(f"variants/{variant_id}.json", payload)
    return bool(res and res.get("variant"))


async def _set_inventory_level(inventory_item_id: int, location_id: int, available: int, shopify_client: Optional[ShopifyClient] = None) -> bool:
    if shopify_client is None:
        shopify_client = client
    payload = {
        "location_id": int(location_id),
        "inventory_item_id": int(inventory_item_id),
        "available": int(available),
    }
    res = await shopify_client.post("inventory_levels/set.json", payload)
    return bool(res)


async def decrement_inventory_by_variant(variant_id: int, decrement: int, shopify_client: Optional[ShopifyClient] = None) -> bool:
    """
    Decrement inventory for a given Shopify variant by `decrement` (non-blocking).
    Tries to update the variant directly; if that fails, falls back to inventory_levels/set.
    Returns True on success, False otherwise.
    """
    if shopify_client is None:
        shopify_client = client

    variant = await _get_variant(variant_id, shopify_client)
    if not variant:
        logger.warning("Variant %s not found in Shopify", variant_id)
        return False

    current_qty = variant.get("inventory_quantity")
    try:
        current_qty = int(current_qty) if current_qty is not None else 0
    except Exception:
        current_qty = 0

    new_qty = max(0, current_qty - int(decrement))

    # 1) Try variant update (simpler)
    try:
        ok = await _update_variant_quantity(variant_id, new_qty, shopify_client)
        if ok:
            logger.info("Set variant %s quantity -> %s", variant_id, new_qty)
            return True
    except Exception as e:
        logger.debug("Variant PUT failed: %s", e)

    # 2) Fallback: use inventory_item_id + location -> inventory_levels/set.json
    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        logger.warning("Variant %s missing inventory_item_id; cannot set inventory_levels", variant_id)
        return False

    # fetch locations and pick first (you may want to pick a specific location)
    locations_resp = await shopify_client.get("locations.json")
    locations = (locations_resp or {}).get("locations", [])
    if not locations:
        logger.warning("No Shopify locations found; cannot set inventory level")
        return False
    location_id = locations[0].get("id")

    try:
        ok2 = await _set_inventory_level(inventory_item_id, location_id, new_qty, shopify_client)
        if ok2:
            logger.info("Inventory level set for inventory_item %s @ location %s -> %s", inventory_item_id, location_id, new_qty)
            return True
    except Exception as e:
        logger.debug("inventory_levels/set failed: %s", e)

    logger.error("Failed to update inventory for variant %s", variant_id)
    return False


async def set_inventory_quantity_by_variant(variant_id: int, quantity: int, shopify_client: Optional[ShopifyClient] = None) -> bool:
    """
    Set the inventory for a variant to an exact `quantity`.
    Tries variant PUT, falls back to inventory_levels/set.
    """
    if shopify_client is None:
        shopify_client = client

    variant = await _get_variant(variant_id, shopify_client)
    if not variant:
        logger.warning("Variant %s not found in Shopify", variant_id)
        return False

    try:
        ok = await _update_variant_quantity(variant_id, int(quantity), shopify_client)
        if ok:
            logger.info("Set variant %s quantity -> %s", variant_id, quantity)
            return True
    except Exception as e:
        logger.debug("Variant PUT failed: %s", e)

    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        logger.warning("Variant %s missing inventory_item_id; cannot set inventory_levels", variant_id)
        return False

    locations_resp = await shopify_client.get("locations.json")
    locations = (locations_resp or {}).get("locations", [])
    if not locations:
        logger.warning("No Shopify locations found; cannot set inventory level")
        return False
    location_id = locations[0].get("id")

    try:
        ok2 = await _set_inventory_level(inventory_item_id, location_id, int(quantity), shopify_client)
        if ok2:
            logger.info("Inventory level set for inventory_item %s @ location %s -> %s", inventory_item_id, location_id, quantity)
            return True
    except Exception as e:
        logger.debug("inventory_levels/set failed: %s", e)

    logger.error("Failed to set inventory for variant %s", variant_id)
    return False
