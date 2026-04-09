import logging
from typing import Any

from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.shopify.update_inventory import set_inventory_quantity_by_variant
from app.services.inventory_zero_guard import was_already_zeroed, mark_zeroed
from app.services.channel_utils import get_shopify_field

logger = logging.getLogger(__name__)


async def handle_ebay_order_webhook(payload: dict[str, Any], shopify_client: ShopifyClient | None = None, make_unavailable: bool = True) -> dict:
    """
    Process an eBay order webhook payload. For each line item:
    - find normalized product by SKU
    - set the Shopify variant inventory to 0
    - optionally mark the Shopify product as draft (unpublished)

    Returns a summary dict.
    """
    if shopify_client is None:
        shopify_client = ShopifyClient()

    processed = []
    errors = 0

    # payload shape may vary depending on eBay event type
    order = payload.get("order") or payload
    order_id = order.get("orderId") or order.get("legacyOrderId") or order.get("orderIdReference")

    line_items = order.get("lineItems") or order.get("lineItems", []) or []
    if isinstance(line_items, dict):
        # some payloads may contain nested structures
        line_items = line_items.get("lineItem", [])

    for li in line_items:
        sku = li.get("sku") or (li.get("lineItem") or {}).get("sku") or (li.get("item") or {}).get("sku")
        if not sku:
            continue

        doc = await db.product_normalized.find_one(
            {"$or": [{"_id": sku}, {"sku": sku}]},
            {
                "shopify_variant_id": 1,
                "shopify_id": 1,
                "inventory_item_id": 1,
                "location_id": 1,
                "channels.shopify": 1,
            },
        )
        if not doc:
            processed.append({"sku": sku, "status": "no_product"})
            continue

        vid = get_shopify_field(doc, "shopify_variant_id")
        pid = get_shopify_field(doc, "shopify_id")
        inventory_item_id = get_shopify_field(doc, "inventory_item_id")
        location_id = get_shopify_field(doc, "location_id")

        if not vid:
            processed.append({"sku": sku, "status": "no_variant"})
            continue

        try:
            already = False
            try:
                already = await was_already_zeroed(
                    env="dev",
                    sku=str(sku),
                    variant_id=int(vid),
                    inventory_item_id=int(inventory_item_id) if inventory_item_id is not None else None,
                    location_id=int(location_id) if location_id is not None else None,
                )
            except Exception as e:
                logger.debug("Zero-guard lookup failed for webhook | sku=%s | error=%s", sku, e)

            if already:
                ok = True
                entry = {"sku": sku, "variant_id": vid, "inventory_set_to": 0, "ok": True, "skipped": "already_zeroed"}
            else:
                ok = await set_inventory_quantity_by_variant(int(vid), 0, shopify_client)
                entry = {"sku": sku, "variant_id": vid, "inventory_set_to": 0, "ok": ok}
                if ok:
                    try:
                        await mark_zeroed(
                            env="dev",
                            sku=str(sku),
                            variant_id=int(vid),
                            inventory_item_id=int(inventory_item_id) if inventory_item_id is not None else None,
                            location_id=int(location_id) if location_id is not None else None,
                            source="ebay_order_webhook",
                        )
                    except Exception as e:
                        logger.debug("Failed to mark zeroed for webhook | sku=%s | error=%s", sku, e)

            # optionally unpublish product
            if make_unavailable and pid and ok:
                try:
                    await shopify_client.put(
                        f"products/{int(pid)}.json",
                        {"product": {"id": int(pid), "status": "draft"}},
                    )
                    entry["product_unpublished"] = True
                except Exception as e:
                    logger.exception("Failed to unpublish Shopify product %s: %s", pid, e)
                    entry["product_unpublished"] = False

            processed.append(entry)
        except Exception as e:
            logger.exception("Error handling SKU %s: %s", sku, e)
            processed.append({"sku": sku, "status": "error", "error": str(e)})
            errors += 1

    # persist webhook processing record
    await db.sync_log.insert_one({"webhook": "ebay_order", "order_id": order_id, "processed": processed})

    return {"processed_count": len(processed), "errors": errors, "details": processed}
