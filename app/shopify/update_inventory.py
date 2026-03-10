import logging
from typing import Optional

from app.shopify.client import ShopifyClient
from app.shopify.inventory_manager import (
    set_inventory_quantity_by_item_id,
    get_inventory_item_from_variant,
    get_primary_location,
)

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


# ============================================================================
# NEW FUNCTIONS: Use inventory_item_id + location_id directly from database
# ============================================================================


async def set_inventory_from_mongo(
    inventory_item_id: Optional[int],
    location_id: Optional[int],
    quantity: int,
    shopify_client: Optional[ShopifyClient] = None,
    sku: Optional[str] = None,
) -> bool:
    """
    Set inventory using values stored in MongoDB (prefered approach).
    
    This is the NEW, optimized approach - do NOT fetch variants.
    Use stored inventory_item_id + location_id from product_normalized.
    
    Args:
        inventory_item_id: From product_normalized.inventory_item_id
        location_id: From product_normalized.location_id
        quantity: Target quantity
        shopify_client: Optional client
        sku: SKU for logging
        
    Returns: True on success
    """
    if shopify_client is None:
        shopify_client = client
    
    if not inventory_item_id or not location_id:
        logger.warning(
            "[INVENTORY] Cannot update from mongo | sku=%s | item_id=%s | location=%s (missing required IDs)",
            sku,
            inventory_item_id,
            location_id,
        )
        return False
    
    return await set_inventory_quantity_by_item_id(
        inventory_item_id,
        location_id,
        quantity,
        shopify_client,
    )


# ============================================================================
# LEGACY FUNCTIONS: Kept for backward compatibility
# ============================================================================


async def decrement_inventory_by_variant(variant_id: int, decrement: int, shopify_client: Optional[ShopifyClient] = None) -> bool:
    """
    Decrement inventory for a given Shopify variant by `decrement` (non-blocking).
    
    DEPRECATED: Use set_inventory_from_mongo() with inventory_item_id from database instead.
    This function fetches the variant (unnecessary API call).
    
    Tries to update the variant directly; if that fails, falls back to inventory_levels/set.
    Returns True on success, False otherwise.
    """
    if shopify_client is None:
        shopify_client = client

    variant = await _get_variant(variant_id, shopify_client)
    if not variant:
        logger.warning("[INVENTORY] Variant not found in Shopify | variant_id=%s", variant_id)
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
            logger.info("[INVENTORY] ✓ Decremented variant inventory | variant_id=%s | new_qty=%s (%s -> %s)", variant_id, new_qty, current_qty, new_qty)
            return True
    except Exception as e:
        logger.debug("[INVENTORY] Variant PUT failed | variant_id=%s | error=%s", variant_id, e)

    # 2) Fallback: use inventory_item_id + location -> inventory_levels/set.json
    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        logger.warning("[INVENTORY] Variant missing inventory_item_id | variant_id=%s", variant_id)
        return False

    # fetch locations and pick first (you may want to pick a specific location)
    location_data = await get_primary_location(shopify_client)
    if not location_data:
        logger.warning("[INVENTORY] No Shopify locations found | variant_id=%s", variant_id)
        return False
    location_id = location_data.get("id")

    try:
        ok2 = await _set_inventory_level(inventory_item_id, location_id, new_qty, shopify_client)
        if ok2:
            logger.info("[INVENTORY] ✓ Set inventory level (fallback) | variant_id=%s | inventory_item=%s | location=%s | qty=%s", variant_id, inventory_item_id, location_id, new_qty)
            return True
    except Exception as e:
        logger.debug("[INVENTORY] inventory_levels/set failed | variant_id=%s | inventory_item=%s | location=%s | error=%s", variant_id, inventory_item_id, location_id, e)

    logger.error("[INVENTORY] ✗ Failed to decrement inventory | variant_id=%s | inventory_item=%s", variant_id, inventory_item_id)
    return False


async def set_inventory_quantity_by_variant(
    variant_id: int,
    quantity: int,
    shopify_client: Optional[ShopifyClient] = None,
) -> bool:
    """
    Set the inventory for a variant to an exact `quantity`.
    
    DEPRECATED: Use set_inventory_from_mongo() with inventory_item_id from database instead.
    This function fetches the variant (unnecessary API call).
    
    Tries variant PUT, falls back to inventory_levels/set.
    """
    if shopify_client is None:
        shopify_client = client

    variant = await _get_variant(variant_id, shopify_client)
    if not variant:
        logger.warning("[INVENTORY] Variant not found in Shopify | variant_id=%s", variant_id)
        return False

    try:
        ok = await _update_variant_quantity(variant_id, int(quantity), shopify_client)
        if ok:
            logger.info("[INVENTORY] ✓ Set variant quantity (direct) | variant_id=%s | new_qty=%s", variant_id, quantity)
            return True
    except Exception as e:
        logger.debug("[INVENTORY] Variant PUT failed | variant_id=%s | error=%s", variant_id, e)

    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        logger.warning("[INVENTORY] Variant missing inventory_item_id | variant_id=%s", variant_id)
        return False

    location_data = await get_primary_location(shopify_client)
    if not location_data:
        logger.warning("[INVENTORY] No Shopify locations found | variant_id=%s", variant_id)
        return False
    location_id = location_data.get("id")

    try:
        ok2 = await _set_inventory_level(inventory_item_id, location_id, int(quantity), shopify_client)
        if ok2:
            logger.info("[INVENTORY] ✓ Set inventory level (fallback) | variant_id=%s | inventory_item=%s | location=%s | qty=%s", variant_id, inventory_item_id, location_id, quantity)
            return True
    except Exception as e:
        logger.debug("[INVENTORY] inventory_levels/set failed | variant_id=%s | inventory_item=%s | location=%s | error=%s", variant_id, inventory_item_id, location_id, e)

    logger.error("[INVENTORY] ✗ Failed to set inventory | variant_id=%s | inventory_item=%s | target_qty=%s", variant_id, inventory_item_id, quantity)
    return False

