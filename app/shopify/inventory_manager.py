"""
Shopify inventory management using inventory_item_id (best practice).

This module provides async functions for managing inventory via Shopify's
inventory_levels endpoints, using inventory_item_id + location_id directly
instead of fetching variants repeatedly.
"""

import logging
from typing import Optional, Dict, Any

from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)
client = ShopifyClient()


async def get_store_locations(shopify_client: Optional[ShopifyClient] = None) -> list[Dict[str, Any]]:
    """Fetch all locations for the Shopify store.
    
    Returns: List of location dicts with id, name, etc.
    Caches result to avoid repeated API calls.
    """
    if shopify_client is None:
        shopify_client = client
    
    resp = await shopify_client.get("locations.json")
    locations = (resp or {}).get("locations", [])
    
    if not locations:
        logger.warning("[INVENTORY] No Shopify locations found")
        return []
    
    logger.debug("[INVENTORY] Retrieved %d Shopify locations", len(locations))
    return locations


async def get_primary_location(shopify_client: Optional[ShopifyClient] = None) -> Optional[Dict[str, Any]]:
    """Get the primary (first) location for the store."""
    locations = await get_store_locations(shopify_client)
    return locations[0] if locations else None


async def get_inventory_item_from_variant(
    variant_id: int,
    shopify_client: Optional[ShopifyClient] = None,
) -> Optional[int]:
    """Fetch a variant and extract its inventory_item_id.
    
    This is a LOOKUP function - use sparingly. Prefer storing inventory_item_id
    in the database and using set_inventory_quantity_by_item_id() directly.
    
    Args:
        variant_id: Shopify variant ID
        shopify_client: Optional client instance
        
    Returns: inventory_item_id or None if not found
    """
    if shopify_client is None:
        shopify_client = client
    
    resp = await shopify_client.get(f"variants/{variant_id}.json")
    variant = (resp or {}).get("variant")
    if not variant:
        logger.warning("[INVENTORY] Variant not found in Shopify | variant_id=%s", variant_id)
        return None
    
    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        logger.warning("[INVENTORY] Variant missing inventory_item_id | variant_id=%s", variant_id)
        return None
    
    return inventory_item_id


async def set_inventory_quantity_by_item_id(
    inventory_item_id: int,
    location_id: int,
    quantity: int,
    shopify_client: Optional[ShopifyClient] = None,
) -> bool:
    """Set inventory to an exact quantity using inventory_item_id + location_id.
    
    This is the PREFERRED method for inventory updates.
    Does NOT fetch variants; uses data already stored in MongoDB.
    
    Args:
        inventory_item_id: Shopify inventory_item_id
        location_id: Shopify location_id
        quantity: Target inventory quantity (non-negative)
        shopify_client: Optional client instance
        
    Returns: True on success, False otherwise
    """
    if shopify_client is None:
        shopify_client = client
    
    try:
        quantity = int(quantity)
        if quantity < 0:
            quantity = 0
    except (TypeError, ValueError):
        logger.error("[INVENTORY] Invalid quantity %r for item %s", quantity, inventory_item_id)
        return False
    
    payload = {
        "location_id": int(location_id),
        "inventory_item_id": int(inventory_item_id),
        "available": quantity,
    }
    
    logger.debug(
        "[INVENTORY] Setting inventory | inventory_item=%s | location=%s | qty=%s",
        inventory_item_id,
        location_id,
        quantity,
    )
    
    res = await shopify_client.post("inventory_levels/set.json", payload)
    
    if res and res.get("inventory_level"):
        logger.info(
            "[INVENTORY] ✓ Set inventory via item_id | inventory_item=%s | location=%s | qty=%s",
            inventory_item_id,
            location_id,
            quantity,
        )
        return True
    
    logger.error(
        "[INVENTORY] ✗ Failed to set inventory | inventory_item=%s | location=%s | qty=%s | response=%s",
        inventory_item_id,
        location_id,
        quantity,
        res,
    )
    return False


async def adjust_inventory_quantity_by_item_id(
    inventory_item_id: int,
    location_id: int,
    quantity_adjustment: int,
    shopify_client: Optional[ShopifyClient] = None,
) -> bool:
    """Adjust inventory by a delta using inventory_item_id + location_id.
    
    Helpful for decrement operations (e.g., order fulfillment).
    
    Args:
        inventory_item_id: Shopify inventory_item_id
        location_id: Shopify location_id
        quantity_adjustment: Delta to add/subtract (negative to decrease)
        shopify_client: Optional client instance
        
    Returns: True on success, False otherwise
    """
    if shopify_client is None:
        shopify_client = client
    
    try:
        quantity_adjustment = int(quantity_adjustment)
    except (TypeError, ValueError):
        logger.error("[INVENTORY] Invalid adjustment %r for item %s", quantity_adjustment, inventory_item_id)
        return False
    
    payload = {
        "location_id": int(location_id),
        "inventory_item_id": int(inventory_item_id),
        "available_adjustment": quantity_adjustment,
    }
    
    logger.debug(
        "[INVENTORY] Adjusting inventory | inventory_item=%s | location=%s | adjustment=%s",
        inventory_item_id,
        location_id,
        quantity_adjustment,
    )
    
    res = await shopify_client.post("inventory_levels/adjust.json", payload)
    
    if res and res.get("inventory_level"):
        logger.info(
            "[INVENTORY] ✓ Adjusted inventory via item_id | inventory_item=%s | location=%s | adjustment=%s",
            inventory_item_id,
            location_id,
            quantity_adjustment,
        )
        return True
    
    logger.error(
        "[INVENTORY] ✗ Failed to adjust inventory | inventory_item=%s | location=%s | adjustment=%s | response=%s",
        inventory_item_id,
        location_id,
        quantity_adjustment,
        res,
    )
    return False


async def get_inventory_levels(
    inventory_item_ids: list[int],
    shopify_client: Optional[ShopifyClient] = None,
) -> Dict[int, list[Dict[str, Any]]]:
    """Fetch inventory levels for multiple inventory_item_ids.
    
    Returns a dict mapping inventory_item_id -> list of inventory_level dicts.
    
    Args:
        inventory_item_ids: List of inventory_item_ids to fetch
        shopify_client: Optional client instance
        
    Returns: Dict[inventory_item_id, list of inventory_level dicts]
    """
    if shopify_client is None:
        shopify_client = client
    
    if not inventory_item_ids:
        return {}
    
    # Shopify accepts comma-separated inventory_item_ids
    ids_str = ",".join(str(id_) for id_ in inventory_item_ids)
    params = {"inventory_item_ids": ids_str}
    
    resp = await shopify_client.get("inventory_levels.json", params=params)
    levels = (resp or {}).get("inventory_levels", [])
    
    # Group by inventory_item_id
    result: Dict[int, list] = {}
    for level in levels:
        item_id = level.get("inventory_item_id")
        if item_id:
            if item_id not in result:
                result[item_id] = []
            result[item_id].append(level)
    
    logger.debug("[INVENTORY] Retrieved inventory levels for %d items", len(result))
    return result
