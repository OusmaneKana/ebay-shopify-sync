import logging
from typing import Any

from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.shopify.update_inventory import set_inventory_quantity_by_variant

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

        doc = await db.product_normalized.find_one({"$or": [{"_id": sku}, {"sku": sku}]})
        if not doc:
            processed.append({"sku": sku, "status": "no_product"})
            continue

        vid = doc.get("shopify_variant_id")
        pid = doc.get("shopify_id")
        if not vid:
            processed.append({"sku": sku, "status": "no_variant"})
            continue

        try:
            ok = await set_inventory_quantity_by_variant(int(vid), 0, shopify_client)
            entry = {"sku": sku, "variant_id": vid, "inventory_set_to": 0, "ok": ok}
            # optionally unpublish product
            if make_unavailable and pid and ok:
                try:
                    await shopify_client.put(f"products/{int(pid)}.json", {"product": {"id": int(pid), "status": "draft"}})
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
