import logging
from datetime import datetime, timezone
from typing import Any
from pymongo import ReturnDocument

from app.database.mongo import db
from app.services.normalizer_service import normalize_from_raw
from app.services.shopify_sync import sync_to_shopify
from app.services.multichannel_sync_service import ingest_sale_event, run_worker_batch

logger = logging.getLogger(__name__)

LISTING_SYNC_COLLECTION = "ebay_listing_sync_queue"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _parse_ebay_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text[:-1] + "+00:00")
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _extract_images(item_data: dict[str, Any]) -> list[str]:
    picture_details = item_data.get("PictureDetails") or {}
    picture_url = picture_details.get("PictureURL")
    if isinstance(picture_url, str):
        return [picture_url]
    if isinstance(picture_url, list):
        return [str(url) for url in picture_url if str(url).strip()]
    return []


def _extract_item_specifics(item_data: dict[str, Any]) -> dict[str, Any]:
    specifics = item_data.get("ItemSpecifics") or {}
    nvl = specifics.get("NameValueList") if isinstance(specifics, dict) else None
    if not nvl:
        return {}

    nvl_list = nvl if isinstance(nvl, list) else [nvl]
    out: dict[str, Any] = {}
    for entry in nvl_list:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("Name") or "").strip()
        if not name:
            continue
        value = entry.get("Value")
        if isinstance(value, list):
            out[name] = ", ".join(str(v) for v in value if str(v).strip())
        else:
            out[name] = str(value).strip() if value is not None else ""
    return out


def _to_raw_from_itemlisted(payload: dict[str, Any]) -> tuple[str | None, str | None, dict[str, Any], datetime | None]:
    item_data = payload.get("Item") or {}
    item_id = str(item_data.get("ItemID") or payload.get("ItemID") or "").strip()
    sku = str(item_data.get("SKU") or item_data.get("ApplicationData") or item_id or "").strip() or None

    title = str(item_data.get("Title") or "").strip()
    description = str(item_data.get("Description") or "").strip()
    quantity = _safe_int(item_data.get("Quantity") or item_data.get("QuantityAvailable"), 0)
    price = _safe_float(item_data.get("StartPrice") or item_data.get("BuyItNowPrice") or item_data.get("CurrentPrice"))

    primary_category = item_data.get("PrimaryCategory") or {}
    category_id = primary_category.get("CategoryID")
    listing_start = ((item_data.get("ListingDetails") or {}).get("StartTime"))
    posted_at = _parse_ebay_datetime(listing_start)

    raw_doc = {
        "ItemID": item_id,
        "SKU": sku,
        "Title": title,
        "Description": description,
        "Images": _extract_images(item_data),
        "Price": price,
        "QuantityAvailable": max(0, quantity),
        "PrimaryCategoryID": str(category_id) if category_id is not None else None,
        "ItemSpecifics": _extract_item_specifics(item_data),
        "ListingStartTime": listing_start,
        "_event_type": payload.get("_event_type") or "ItemListed",
        "_raw_event": payload,
    }

    return sku, item_id or None, raw_doc, posted_at


async def process_ebay_listing_sync_queue(limit: int = 10) -> dict[str, Any]:
    """Process queued ItemListed-derived sync jobs (normalize + Shopify sync) by SKU."""
    max_items = max(1, min(int(limit), 200))

    completed = 0
    failed = 0
    picked = 0
    results: list[dict[str, Any]] = []

    for _ in range(max_items):
        job = await db[LISTING_SYNC_COLLECTION].find_one_and_update(
            {"status": "queued"},
            {
                "$set": {"status": "processing", "started_at": datetime.now(timezone.utc)},
                "$inc": {"attempts": 1},
            },
            sort=[("updated_at", 1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if not job:
            break

        picked += 1
        sku = str(job.get("sku") or "").strip()
        if not sku:
            failed += 1
            await db[LISTING_SYNC_COLLECTION].update_one(
                {"_id": job.get("_id")},
                {
                    "$set": {
                        "status": "failed",
                        "error": "missing_sku",
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            results.append({"sku": None, "status": "failed", "error": "missing_sku"})
            continue

        try:
            normalize_result = await normalize_from_raw(skus=[sku])
            shopify_result = await sync_to_shopify(
                None,
                allow_create=True,
                adjust_inventory=True,
                skus=[sku],
            )

            completed += 1
            await db[LISTING_SYNC_COLLECTION].update_one(
                {"_id": job.get("_id")},
                {
                    "$set": {
                        "status": "completed",
                        "normalize_result": normalize_result,
                        "shopify_result": shopify_result,
                        "updated_at": datetime.now(timezone.utc),
                        "completed_at": datetime.now(timezone.utc),
                        "error": None,
                    }
                },
            )
            results.append({
                "sku": sku,
                "status": "completed",
                "normalize": normalize_result,
                "shopify": shopify_result,
            })
        except Exception as exc:
            failed += 1
            logger.exception("ItemListed queue job failed for sku=%s", sku)
            await db[LISTING_SYNC_COLLECTION].update_one(
                {"_id": job.get("_id")},
                {
                    "$set": {
                        "status": "queued",
                        "error": str(exc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            results.append({"sku": sku, "status": "retry", "error": str(exc)})

    return {
        "requested_limit": max_items,
        "picked": picked,
        "completed": completed,
        "failed": failed,
        "results": results,
    }


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
    sku, item_id, raw_doc, posted_at = _to_raw_from_itemlisted(payload)
    title = raw_doc.get("Title") or ""

    if not sku:
        logger.warning("ItemListed notification missing SKU/item_id. payload keys=%s", list(payload.keys()))
        return {"item_id": item_id, "sku": None, "status": "missing_sku"}

    now_utc = datetime.now(timezone.utc)
    await db.product_raw.update_one(
        {"_id": sku},
        {
            "$set": {
                "sku": sku,
                "raw": raw_doc,
                "ebay_posted_at": posted_at,
                "updated_at": now_utc,
                "source": "ebay_itemlisted",
            },
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )

    queue_id = f"ebay-itemlisted:{sku}"
    await db[LISTING_SYNC_COLLECTION].update_one(
        {"_id": queue_id},
        {
            "$set": {
                "sku": sku,
                "item_id": item_id,
                "status": "queued",
                "updated_at": now_utc,
                "source": "ebay_itemlisted",
            },
            "$setOnInsert": {
                "created_at": now_utc,
                "attempts": 0,
            },
        },
        upsert=True,
    )

    doc = {
        "event_type": "ItemListed",
        "item_id": item_id,
        "sku": sku,
        "title": title,
        "raw": payload,
        "status": "queued_for_sync",
        "queue_id": queue_id,
        "created_at": now_utc,
    }

    result = await db.ebay_listing_events.insert_one(doc)
    logger.info(
        "ItemListed ingested to raw and queued | item_id=%s | sku=%s | queue_id=%s | doc_id=%s",
        item_id, sku, queue_id, result.inserted_id,
    )

    # Kick a small background pass for near-real-time processing while still
    # preserving a durable queue document for retries/replay.
    try:
        import asyncio

        asyncio.create_task(process_ebay_listing_sync_queue(limit=3))
    except Exception:
        logger.exception("Failed to schedule ebay listing sync queue worker")

    return {
        "item_id": item_id,
        "sku": sku,
        "stored_id": str(result.inserted_id),
        "queue_id": queue_id,
        "queued": True,
    }
