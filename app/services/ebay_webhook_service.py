import logging
from datetime import datetime, timezone
from typing import Any

from app.database.mongo import db
from app.services.multichannel_sync_service import ingest_sale_event, run_worker_batch

logger = logging.getLogger(__name__)


async def handle_ebay_order_webhook(payload: dict[str, Any], shopify_client: Any = None, make_unavailable: bool = True) -> dict:
    """
    Process an eBay order webhook payload. For each line item:
    - resolve SKU
    - apply canonical quantity decrement (idempotent inventory event)
    - enqueue multichannel inventory jobs (eBay/Etsy/Shopify except source)
    - optionally run a worker batch immediately

    Returns a summary dict.
    """
    processed = []
    errors = 0
    unresolved = 0

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

        qty_sold = 1
        try:
            qty_sold = int(li.get("quantity") or li.get("lineItemQuantity") or 1)
            if qty_sold <= 0:
                qty_sold = 1
        except Exception:
            qty_sold = 1

        try:
            event_result = await ingest_sale_event(
                source_channel="ebay",
                payload={
                    "order_id": order_id,
                    "line_item": li,
                },
                quantity_sold=qty_sold,
                explicit_sku=str(sku),
                explicit_event_id=f"ebay-order:{order_id}:{sku}",
                enqueue_jobs_flag=bool(make_unavailable),
            )
            processed.append({"sku": sku, "quantity_sold": qty_sold, "event": event_result})
            if event_result.get("status") in {"unresolved_sku", "no_product"}:
                unresolved += 1
        except Exception as e:
            logger.exception("Error handling SKU %s: %s", sku, e)
            processed.append({"sku": sku, "status": "error", "error": str(e)})
            errors += 1

    worker_result = None
    if make_unavailable:
        # Run a short worker pass so important inventory pushes happen quickly.
        worker_result = await run_worker_batch(limit=max(10, len(processed) * 3))

    # persist webhook processing record
    await db.sync_log.insert_one(
        {
            "webhook": "ebay_order",
            "order_id": order_id,
            "processed": processed,
            "unresolved": unresolved,
            "errors": errors,
            "worker": worker_result,
        }
    )

    return {
        "processed_count": len(processed),
        "unresolved": unresolved,
        "errors": errors,
        "worker": worker_result,
        "details": processed,
    }


async def handle_ebay_item_listed(payload: dict[str, Any]) -> dict:
    """
    Handle an ItemListed notification from eBay.
    Persists the event to MongoDB for downstream processing / sync.
    """
    item_data = payload.get("Item") or {}
    item_id = item_data.get("ItemID") or payload.get("ItemID") or ""
    sku = item_data.get("SKU") or item_data.get("ApplicationData") or ""
    title = item_data.get("Title") or ""

    doc = {
        "event_type": "ItemListed",
        "item_id": item_id,
        "sku": sku,
        "title": title,
        "raw": payload,
        "status": "pending_sync",
        "created_at": datetime.now(timezone.utc),
    }

    result = await db.ebay_listing_events.insert_one(doc)
    logger.info(
        "ItemListed event stored | item_id=%s | sku=%s | doc_id=%s",
        item_id, sku, result.inserted_id,
    )

    return {"item_id": item_id, "sku": sku, "stored_id": str(result.inserted_id)}
