from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import urlparse

import httpx
from pymongo import ReturnDocument

from app.config import settings
from app.database.mongo import db
from app.ebay.client import EbayClient
from app.services.channel_utils import get_channel, get_shopify_field
from app.shopify.client import ShopifyClient
from app.shopify.update_inventory import set_inventory_from_mongo

logger = logging.getLogger(__name__)

EVENTS_COLLECTION = "inventory_events"
JOBS_COLLECTION = "channel_sync_jobs"
POLICIES_COLLECTION = "inventory_conflict_policies"
POLICY_HISTORY_COLLECTION = "inventory_policy_history"
MAX_JOB_ATTEMPTS = 5
KPI_CACHE_TTL_SECONDS = 90
_live_kpi_cache: dict[str, Any] = {
    "expires_at": None,
    "payload": None,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _resolve_etsy_api_key() -> str | None:
    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        return f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"
    return settings.ETSY_CLIENT_ID


async def _resolve_etsy_shop_id() -> str | None:
    # Prefer linked normalized channel metadata.
    doc = await db.product_normalized.find_one(
        {"channels.etsy.shop_id": {"$exists": True, "$ne": None}},
        {"channels.etsy.shop_id": 1},
    )
    shop_id = ((doc or {}).get("channels") or {}).get("etsy", {}).get("shop_id")
    if shop_id:
        return str(shop_id)

    # Fallback to investigation snapshot metadata.
    etsy_doc = await db.etsy_listings_investigation.find_one(
        {"shop_id": {"$exists": True, "$ne": None}},
        {"shop_id": 1},
    )
    if etsy_doc and etsy_doc.get("shop_id") is not None:
        return str(etsy_doc.get("shop_id"))
    return None


async def _fetch_ebay_active_total() -> tuple[int | None, str | None]:
    client = EbayClient()
    await client.ensure_fresh_token()

    request_xml = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
<GetMyeBaySellingRequest xmlns=\"urn:ebay:apis:eBLBaseComponents\">
  <RequesterCredentials>
    <eBayAuthToken>{client.token}</eBayAuthToken>
  </RequesterCredentials>
  <Version>1209</Version>
  <DetailLevel>ReturnAll</DetailLevel>
  <ActiveList>
    <Include>true</Include>
    <Pagination>
      <EntriesPerPage>1</EntriesPerPage>
      <PageNumber>1</PageNumber>
    </Pagination>
  </ActiveList>
</GetMyeBaySellingRequest>
"""

    try:
        response_xml = await asyncio.to_thread(client.trading_post, "GetMyeBaySelling", request_xml)
        root = ET.fromstring(response_xml)
        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

        ack = root.findtext(".//e:Ack", default="", namespaces=ns)
        if ack != "Success":
            msg = root.findtext(".//e:Errors/e:LongMessage", default="ebay_api_error", namespaces=ns)
            return None, msg

        total_text = root.findtext(
            ".//e:ActiveList/e:PaginationResult/e:TotalNumberOfEntries",
            default=None,
            namespaces=ns,
        )
        if total_text is None:
            return None, "ebay_total_missing"

        return int(total_text), None
    except Exception as exc:
        return None, str(exc)


async def _fetch_etsy_state_count(
    *,
    client: httpx.AsyncClient,
    headers: dict[str, str],
    shop_id: str,
    state: str,
) -> int:
    limit = 100
    offset = 0
    total = 0

    while True:
        response = await client.get(
            f"https://openapi.etsy.com/v3/application/shops/{shop_id}/listings",
            headers=headers,
            params={"state": state, "limit": limit, "offset": offset},
        )
        response.raise_for_status()

        payload = response.json() if response.content else {}
        rows = payload.get("results") or []
        total += len(rows)

        if len(rows) < limit:
            break
        offset += limit

    return total


async def _fetch_etsy_counts() -> tuple[dict[str, int] | None, str | None]:
    token = settings.ETSY_TOKEN
    if not token:
        token_doc = await db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"access_token": 1})
        token = token_doc.get("access_token") if token_doc else None
    if not token:
        return None, "missing_etsy_token"

    api_key = _resolve_etsy_api_key()
    if not api_key:
        return None, "missing_etsy_api_key"

    shop_id = await _resolve_etsy_shop_id()
    if not shop_id:
        return None, "missing_etsy_shop_id"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            active = await _fetch_etsy_state_count(client=client, headers=headers, shop_id=shop_id, state="active")
            draft = await _fetch_etsy_state_count(client=client, headers=headers, shop_id=shop_id, state="draft")
            sold_out = await _fetch_etsy_state_count(client=client, headers=headers, shop_id=shop_id, state="sold_out")

        return {
            "active": int(active),
            "draft": int(draft),
            "sold_out": int(sold_out),
            "total": int(active + draft + sold_out),
        }, None
    except Exception as exc:
        return None, str(exc)


async def _fetch_shopify_product_total() -> tuple[int | None, str | None]:
    try:
        client = ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
        resp = await client.get("products/count.json")
        return int((resp or {}).get("count", 0)), None
    except Exception as exc:
        return None, str(exc)


async def _get_live_api_kpis() -> dict[str, Any]:
    now = _utc_now()
    expires_at = _as_aware_utc(_live_kpi_cache.get("expires_at"))
    cached_payload = _live_kpi_cache.get("payload")
    if expires_at and cached_payload and expires_at > now:
        return cached_payload

    ebay_total, ebay_error = await _fetch_ebay_active_total()
    etsy_counts, etsy_error = await _fetch_etsy_counts()
    shopify_total, shopify_error = await _fetch_shopify_product_total()

    payload = {
        "source": "live_api",
        "generated_at": now,
        "ebay_active_total": ebay_total,
        "etsy": etsy_counts
        or {
            "active": None,
            "draft": None,
            "sold_out": None,
            "total": None,
        },
        "shopify_total_products": shopify_total,
        "errors": {
            "ebay": ebay_error,
            "etsy": etsy_error,
            "shopify": shopify_error,
        },
    }

    _live_kpi_cache["payload"] = payload
    _live_kpi_cache["expires_at"] = now + timedelta(seconds=KPI_CACHE_TTL_SECONDS)
    return payload


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _event_id(source_channel: str, payload: dict[str, Any], sku: str | None, quantity_sold: int) -> str:
    payload_compact = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    raw = f"{source_channel}|{sku or ''}|{quantity_sold}|{payload_compact}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{source_channel}:{digest[:24]}"


def _iter_target_channels(source_channel: str) -> list[str]:
    # Canonical state fans out to other channels.
    all_channels = ["ebay", "etsy", "shopify"]
    return [channel for channel in all_channels if channel != source_channel]


def _extract_channel_qty(doc: dict[str, Any], channel: str) -> int | None:
    if channel == "shopify":
        value = get_shopify_field(doc, "quantity")
        if value is None:
            value = get_shopify_field(doc, "inventory_quantity")
    else:
        value = get_channel(doc, channel).get("quantity")
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _is_channel_linked(doc: dict[str, Any], channel: str) -> bool:
    if channel == "shopify":
        return bool(
            get_shopify_field(doc, "shopify_id")
            or get_shopify_field(doc, "inventory_item_id")
            or get_shopify_field(doc, "shopify_variant_id")
        )
    if channel == "etsy":
        etsy = get_channel(doc, "etsy")
        return bool(etsy.get("listing_id"))
    if channel == "ebay":
        # We can still enqueue and let the worker resolve item id from product_raw.
        return True
    return False


def _normalize_channel(value: str | None) -> str | None:
    if value is None:
        return None
    channel = str(value).strip().lower()
    if not channel:
        return None
    if channel not in {"ebay", "etsy", "shopify"}:
        return None
    return channel


def _shopify_admin_product_url(shopify_store_url: str | None, shopify_id: object) -> str | None:
    if not shopify_store_url or not shopify_id:
        return None
    try:
        pid = int(shopify_id)
    except Exception:
        return None

    host = str(shopify_store_url).strip()
    if host.startswith("https://"):
        host = host[len("https://"):]
    elif host.startswith("http://"):
        host = host[len("http://"):]
    host = host.rstrip("/")
    if not host:
        return None
    return f"https://{host}/admin/products/{pid}"


async def get_conflict_policy(sku: str) -> dict[str, Any]:
    doc = await db[POLICIES_COLLECTION].find_one({"_id": str(sku)})
    if not doc:
        return {
            "sku": str(sku),
            "priority_channel": None,
            "strict_priority": False,
            "max_delta_guard": None,
            "note": None,
            "updated_at": None,
        }

    return {
        "sku": str(doc.get("_id")),
        "priority_channel": _normalize_channel(doc.get("priority_channel")),
        "strict_priority": bool(doc.get("strict_priority", False)),
        "max_delta_guard": _safe_int(doc.get("max_delta_guard"), default=0) or None,
        "note": doc.get("note"),
        "updated_at": doc.get("updated_at"),
    }


async def set_conflict_policy(
    *,
    sku: str,
    priority_channel: str | None,
    max_delta_guard: int | None,
    strict_priority: bool = False,
    note: str | None = None,
) -> dict[str, Any]:
    normalized_channel = _normalize_channel(priority_channel)

    update_data: dict[str, Any] = {
        "priority_channel": normalized_channel,
        "strict_priority": bool(strict_priority),
        "max_delta_guard": None if max_delta_guard is None else max(1, int(max_delta_guard)),
        "note": note,
        "updated_at": _utc_now(),
    }

    await db[POLICIES_COLLECTION].update_one(
        {"_id": str(sku)},
        {
            "$set": update_data,
            "$setOnInsert": {"created_at": _utc_now()},
        },
        upsert=True,
    )

    await db[POLICY_HISTORY_COLLECTION].insert_one(
        {
            "sku": str(sku),
            "priority_channel": normalized_channel,
            "strict_priority": bool(strict_priority),
            "max_delta_guard": update_data.get("max_delta_guard"),
            "note": note,
            "changed_at": _utc_now(),
        }
    )

    return await get_conflict_policy(str(sku))


async def get_item_timeline(*, sku: str, limit: int = 100) -> dict[str, Any]:
    max_items = max(1, min(int(limit), 250))

    product = await db.product_normalized.find_one(
        {"_id": str(sku)},
        {
            "_id": 1,
            "title": 1,
            "quantity": 1,
            "updated_at": 1,
            "channels": 1,
            "shopify_id": 1,
            "inventory_item_id": 1,
            "shopify_variant_id": 1,
        },
    )

    if not product:
        return {
            "ok": False,
            "reason": "product_not_found",
            "sku": str(sku),
            "entries": [],
        }

    events = await db[EVENTS_COLLECTION].find(
        {"sku": str(sku)},
        {
            "_id": 1,
            "source_channel": 1,
            "type": 1,
            "status": 1,
            "quantity_sold": 1,
            "quantity_before": 1,
            "quantity_after": 1,
            "created_at": 1,
            "policy": 1,
        },
    ).sort("created_at", -1).limit(max_items).to_list(None)

    jobs = await db[JOBS_COLLECTION].find(
        {"sku": str(sku)},
        {
            "_id": 1,
            "event_id": 1,
            "target_channel": 1,
            "target_qty": 1,
            "status": 1,
            "attempts": 1,
            "error": 1,
            "reason": 1,
            "created_at": 1,
            "started_at": 1,
            "finished_at": 1,
        },
    ).sort("created_at", -1).limit(max_items).to_list(None)

    policy_changes = await db[POLICY_HISTORY_COLLECTION].find(
        {"sku": str(sku)},
        {
            "priority_channel": 1,
            "strict_priority": 1,
            "max_delta_guard": 1,
            "note": 1,
            "changed_at": 1,
        },
    ).sort("changed_at", -1).limit(max_items).to_list(None)

    entries: list[dict[str, Any]] = []

    for event in events:
        entries.append(
            {
                "kind": "event",
                "timestamp": event.get("created_at"),
                "status": event.get("status"),
                "title": f"{event.get('type') or 'event'} from {event.get('source_channel') or 'unknown'}",
                "summary": {
                    "event_id": event.get("_id"),
                    "source_channel": event.get("source_channel"),
                    "quantity_sold": event.get("quantity_sold"),
                    "quantity_before": event.get("quantity_before"),
                    "quantity_after": event.get("quantity_after"),
                },
                "raw": event,
            }
        )

    for job in jobs:
        entries.append(
            {
                "kind": "job",
                "timestamp": job.get("finished_at") or job.get("started_at") or job.get("created_at"),
                "status": job.get("status"),
                "title": f"push to {job.get('target_channel') or 'unknown'}",
                "summary": {
                    "job_id": job.get("_id"),
                    "event_id": job.get("event_id"),
                    "target_channel": job.get("target_channel"),
                    "target_qty": job.get("target_qty"),
                    "attempts": job.get("attempts"),
                    "error": job.get("error"),
                    "reason": job.get("reason"),
                },
                "raw": job,
            }
        )

    for change in policy_changes:
        entries.append(
            {
                "kind": "policy",
                "timestamp": change.get("changed_at"),
                "status": "saved",
                "title": "conflict policy updated",
                "summary": {
                    "priority_channel": change.get("priority_channel"),
                    "strict_priority": bool(change.get("strict_priority", False)),
                    "max_delta_guard": change.get("max_delta_guard"),
                    "note": change.get("note"),
                },
                "raw": change,
            }
        )

    entries.sort(key=lambda item: item.get("timestamp") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    entries = entries[:max_items]

    return {
        "ok": True,
        "generated_at": _utc_now(),
        "sku": str(sku),
        "product": {
            "sku": str(product.get("_id")),
            "title": product.get("title"),
            "canonical_qty": max(0, _safe_int(product.get("quantity"), 0)),
            "updated_at": product.get("updated_at"),
            "channels": {
                "ebay": get_channel(product, "ebay"),
                "etsy": get_channel(product, "etsy"),
                "shopify": get_channel(product, "shopify"),
            },
        },
        "counts": {
            "events": len(events),
            "jobs": len(jobs),
            "policy_changes": len(policy_changes),
            "entries": len(entries),
        },
        "entries": entries,
    }


async def replay_failed_jobs(
    *,
    target_channel: str | None = None,
    sku: str | None = None,
    error_contains: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    max_items = max(1, min(int(limit), 1000))
    query: dict[str, Any] = {"status": "failed"}

    normalized_target = _normalize_channel(target_channel)
    if normalized_target:
        query["target_channel"] = normalized_target
    if sku:
        query["sku"] = str(sku)
    if error_contains:
        query["error"] = {"$regex": str(error_contains), "$options": "i"}

    cursor = db[JOBS_COLLECTION].find(query, {"_id": 1}).sort("created_at", 1).limit(max_items)
    job_ids = [str(doc.get("_id")) async for doc in cursor]

    if not job_ids:
        return {
            "matched": 0,
            "requeued": 0,
            "job_ids": [],
        }

    result = await db[JOBS_COLLECTION].update_many(
        {"_id": {"$in": job_ids}, "status": "failed"},
        {
            "$set": {
                "status": "retry",
                "error": None,
                "updated_at": _utc_now(),
                "replay_requested_at": _utc_now(),
            }
        },
    )

    return {
        "matched": len(job_ids),
        "requeued": int(getattr(result, "modified_count", 0)),
        "job_ids": job_ids,
    }


async def enqueue_reconcile_jobs_for_sku(
    *,
    sku: str,
    target_channels: list[str] | None = None,
    reason: str = "manual_reconcile",
) -> dict[str, Any]:
    doc = await db.product_normalized.find_one(
        {"_id": str(sku)},
        {
            "_id": 1,
            "quantity": 1,
            "shopify_id": 1,
            "inventory_item_id": 1,
            "shopify_variant_id": 1,
            "channels": 1,
        },
    )

    if not doc:
        return {"ok": False, "reason": "product_not_found", "sku": str(sku)}

    canonical_qty = max(0, _safe_int(doc.get("quantity"), 0))

    requested_channels = target_channels or ["ebay", "etsy", "shopify"]
    normalized_channels = [channel for channel in (_normalize_channel(c) for c in requested_channels) if channel]

    if not normalized_channels:
        return {"ok": False, "reason": "no_valid_channels", "sku": str(sku)}

    timestamp = int(_utc_now().timestamp())
    event_id = f"reconcile:{sku}:{timestamp}"
    queued = 0
    skipped_unlinked = 0

    for channel in normalized_channels:
        if not _is_channel_linked(doc, channel):
            skipped_unlinked += 1
            continue

        job_id = f"{event_id}:{channel}"
        result = await db[JOBS_COLLECTION].update_one(
            {"_id": job_id},
            {
                "$setOnInsert": {
                    "_id": job_id,
                    "event_id": event_id,
                    "sku": str(sku),
                    "target_channel": channel,
                    "target_qty": int(canonical_qty),
                    "status": "queued",
                    "attempts": 0,
                    "created_at": _utc_now(),
                    "reason": reason,
                },
                "$set": {"updated_at": _utc_now()},
            },
            upsert=True,
        )
        if getattr(result, "upserted_id", None):
            queued += 1

    return {
        "ok": True,
        "sku": str(sku),
        "event_id": event_id,
        "canonical_qty": canonical_qty,
        "queued": queued,
        "skipped_unlinked": skipped_unlinked,
        "channels": normalized_channels,
    }


async def get_inventory_command_center(
    *,
    status: str | None = None,
    drift_only: bool = False,
    search: str | None = None,
    limit: int = 100,
    skip: int = 0,
) -> dict[str, Any]:
    # Allow larger pages for operations review while still guarding extremes.
    max_limit = max(1, min(int(limit), 5000))
    offset = max(0, int(skip))

    query: dict[str, Any] = {}
    if search:
        safe_search = str(search).strip()
        if safe_search:
            query["$or"] = [
                {"_id": {"$regex": safe_search, "$options": "i"}},
                {"title": {"$regex": safe_search, "$options": "i"}},
            ]

    projection = {
        "_id": 1,
        "title": 1,
        "quantity": 1,
        "updated_at": 1,
        "shopify_id": 1,
        "inventory_item_id": 1,
        "shopify_variant_id": 1,
        "channels": 1,
    }

    total = await db.product_normalized.count_documents(query)
    cursor = db.product_normalized.find(query, projection).sort("updated_at", -1).skip(offset).limit(max_limit)
    products = [doc async for doc in cursor]

    skus = [str(doc.get("_id")) for doc in products if doc.get("_id")]
    job_stats: dict[str, dict[str, int]] = {sku: {} for sku in skus}
    ebay_item_ids: dict[str, str] = {}

    if skus:
        raw_cursor = db.product_raw.find(
            {"_id": {"$in": skus}},
            {"_id": 1, "raw.ItemID": 1},
        )
        async for raw_doc in raw_cursor:
            sku = str(raw_doc.get("_id") or "")
            item_id = ((raw_doc.get("raw") or {}).get("ItemID"))
            if sku and item_id:
                ebay_item_ids[sku] = str(item_id)

    if skus:
        pipeline = [
            {"$match": {"sku": {"$in": skus}}},
            {"$group": {"_id": {"sku": "$sku", "status": "$status"}, "count": {"$sum": 1}}},
        ]
        rows = await db[JOBS_COLLECTION].aggregate(pipeline).to_list(None)
        for row in rows:
            key = row.get("_id") or {}
            sku = str(key.get("sku"))
            status_name = str(key.get("status"))
            if sku not in job_stats:
                job_stats[sku] = {}
            job_stats[sku][status_name] = int(row.get("count", 0))

    latest_events: dict[str, dict[str, Any]] = {}
    if skus:
        event_cursor = db[EVENTS_COLLECTION].find(
            {"sku": {"$in": skus}},
            {"sku": 1, "status": 1, "source_channel": 1, "quantity_before": 1, "quantity_after": 1, "created_at": 1},
        ).sort("created_at", -1)

        async for event in event_cursor:
            sku = str(event.get("sku"))
            if sku not in latest_events:
                latest_events[sku] = event
            if len(latest_events) >= len(skus):
                break

    now = _utc_now()
    rows_out: list[dict[str, Any]] = []

    for doc in products:
        sku = str(doc.get("_id"))
        canonical_qty = max(0, _safe_int(doc.get("quantity"), 0))

        # eBay is the source of truth: mirror canonical Mongo quantity for eBay view.
        ebay_qty = canonical_qty
        etsy_qty = _extract_channel_qty(doc, "etsy")
        shopify_qty = _extract_channel_qty(doc, "shopify")

        drift_ebay = None if ebay_qty is None else ebay_qty - canonical_qty
        drift_etsy = None if etsy_qty is None else etsy_qty - canonical_qty
        drift_shopify = None if shopify_qty is None else shopify_qty - canonical_qty

        stats = job_stats.get(sku, {})
        queued_jobs = stats.get("queued", 0) + stats.get("retry", 0)
        processing_jobs = stats.get("processing", 0)
        failed_jobs = stats.get("failed", 0)

        latest_event = latest_events.get(sku)
        latest_event_status = str((latest_event or {}).get("status") or "")
        updated_at = doc.get("updated_at")

        stale = False
        if isinstance(updated_at, datetime):
            stale = (now - updated_at).total_seconds() > 86400

        has_drift = any(
            value is not None and value != 0 for value in [drift_ebay, drift_etsy, drift_shopify]
        )

        row_status = "in_sync"
        if latest_event_status.startswith("blocked_"):
            row_status = "conflict"
        elif failed_jobs > 0:
            row_status = "failed"
        elif queued_jobs > 0 or processing_jobs > 0:
            row_status = "pending"
        elif has_drift:
            row_status = "drift"
        elif stale:
            row_status = "stale"

        if drift_only and not has_drift:
            continue
        if status and status != "all" and row_status != status:
            continue

        rows_out.append(
            {
                "sku": sku,
                "title": doc.get("title"),
                "canonical_qty": canonical_qty,
                "channel": {
                    "ebay": {
                        "qty": ebay_qty,
                        "state": get_channel(doc, "ebay").get("listing_state"),
                        "drift": drift_ebay,
                        "url": (
                            f"https://www.ebay.com/itm/{ebay_item_ids.get(sku)}"
                            if ebay_item_ids.get(sku)
                            else f"https://www.ebay.com/sch/i.html?_nkw={sku}"
                        ),
                    },
                    "etsy": {
                        "qty": etsy_qty,
                        "state": get_channel(doc, "etsy").get("listing_state"),
                        "drift": drift_etsy,
                        "url": (
                            get_channel(doc, "etsy").get("url")
                            or (
                                f"https://www.etsy.com/listing/{get_channel(doc, 'etsy').get('listing_id')}"
                                if get_channel(doc, "etsy").get("listing_id")
                                else None
                            )
                        ),
                    },
                    "shopify": {
                        "qty": shopify_qty,
                        "state": get_channel(doc, "shopify").get("status"),
                        "drift": drift_shopify,
                        "url": _shopify_admin_product_url(
                            settings.SHOPIFY_STORE_URL_PROD,
                            get_shopify_field(doc, "shopify_id"),
                        ),
                    },
                },
                "status": row_status,
                "jobs": {
                    "queued": queued_jobs,
                    "processing": processing_jobs,
                    "failed": failed_jobs,
                },
                "latest_event": {
                    "status": latest_event_status or None,
                    "source_channel": (latest_event or {}).get("source_channel"),
                    "created_at": (latest_event or {}).get("created_at"),
                },
                "updated_at": updated_at,
            }
        )

    status_counts: dict[str, int] = {}
    for row in rows_out:
        row_status = row.get("status")
        status_counts[row_status] = status_counts.get(row_status, 0) + 1

    live_kpis = await _get_live_api_kpis()

    return {
        "generated_at": now,
        "query": {
            "status": status or "all",
            "drift_only": bool(drift_only),
            "search": search or "",
            "limit": max_limit,
            "skip": offset,
        },
        "total_products": total,
        "returned": len(rows_out),
        "status_counts": status_counts,
        "live_kpis": live_kpis,
        "rows": rows_out,
    }


async def _resolve_sku_from_etsy_payload(payload: dict[str, Any]) -> str | None:
    # If transaction SKU happens to match our canonical SKU, resolve directly.
    for candidate_sku in [
        payload.get("sku"),
        (payload.get("transaction") or {}).get("sku"),
    ]:
        if candidate_sku:
            doc = await db.product_normalized.find_one({"_id": str(candidate_sku)}, {"_id": 1})
            if doc:
                return str(doc.get("_id"))

    listing_id = payload.get("listing_id") or (payload.get("transaction") or {}).get("listing_id")

    # Etsy webhook can include resource_url like /shops/{shop_id}/listings/{listing_id}
    if not listing_id:
        parsed = _parse_etsy_resource_url(payload.get("resource_url"))
        listing_id = parsed.get("listing_id")

    if listing_id:
        resolved = await _resolve_sku_from_etsy_listing_id(listing_id)
        if resolved:
            return resolved

    # For receipt URLs, fetch transactions and try to resolve by transaction listing_id/SKU.
    receipt_transactions = await get_etsy_receipt_transactions_from_payload(payload)
    for tx in receipt_transactions:
        tx_sku = tx.get("sku")
        if tx_sku:
            doc = await db.product_normalized.find_one({"_id": str(tx_sku)}, {"_id": 1})
            if doc:
                return str(doc.get("_id"))

        tx_listing_id = tx.get("listing_id")
        if tx_listing_id:
            resolved = await _resolve_sku_from_etsy_listing_id(tx_listing_id)
            if resolved:
                return resolved

    return None


def _parse_etsy_resource_url(resource_url: Any) -> dict[str, Any]:
    text = str(resource_url or "").strip()
    if not text:
        return {}

    parsed = urlparse(text)
    path = parsed.path if parsed.scheme else text
    parts = [part for part in path.split("/") if part]

    result: dict[str, Any] = {}

    for idx, part in enumerate(parts):
        if part == "shops" and idx + 1 < len(parts):
            result["shop_id"] = parts[idx + 1]
        elif part == "listings" and idx + 1 < len(parts):
            result["listing_id"] = parts[idx + 1]
        elif part == "receipts" and idx + 1 < len(parts):
            result["receipt_id"] = parts[idx + 1]

    return result


async def _resolve_sku_from_etsy_listing_id(listing_id: Any) -> str | None:
    listing_id_int = _safe_int(listing_id, default=-1)
    if listing_id_int <= 0:
        return None

    doc = await db.product_normalized.find_one(
        {"channels.etsy.listing_id": listing_id_int},
        {"_id": 1},
    )
    if not doc:
        return None
    return str(doc.get("_id"))


async def _get_etsy_access_token() -> str | None:
    token = settings.ETSY_TOKEN
    if token:
        return token

    token_doc = await db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"access_token": 1})
    return token_doc.get("access_token") if token_doc else None


async def get_etsy_receipt_transactions_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parsed = _parse_etsy_resource_url(payload.get("resource_url"))

    receipt_id = payload.get("receipt_id") or parsed.get("receipt_id")
    shop_id = payload.get("shop_id") or parsed.get("shop_id")

    receipt_id_int = _safe_int(receipt_id, default=-1)
    shop_id_int = _safe_int(shop_id, default=-1)
    if receipt_id_int <= 0 or shop_id_int <= 0:
        return []

    token = await _get_etsy_access_token()
    api_key = _resolve_etsy_api_key()
    if not token or not api_key:
        logger.warning(
            "Cannot resolve Etsy receipt transactions: missing auth | shop_id=%s | receipt_id=%s",
            shop_id_int,
            receipt_id_int,
        )
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    url = f"https://openapi.etsy.com/v3/application/shops/{shop_id_int}/receipts/{receipt_id_int}/transactions"
    limit = 100
    offset = 0
    transactions: list[dict[str, Any]] = []

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                response = await client.get(url, headers=headers, params={"limit": limit, "offset": offset})
                response.raise_for_status()
                body = response.json() if response.content else {}
                rows = body.get("results") or []
                transactions.extend([row for row in rows if isinstance(row, dict)])

                if len(rows) < limit:
                    break
                offset += limit
    except Exception:
        logger.exception(
            "Failed to fetch Etsy receipt transactions | shop_id=%s | receipt_id=%s",
            shop_id_int,
            receipt_id_int,
        )
        return []

    return transactions


def _etsy_tx_event_id(base_event_id: str, tx: dict[str, Any], index: int) -> str:
    tx_id = tx.get("transaction_id")
    if tx_id:
        return f"{base_event_id}:tx:{tx_id}"
    return f"{base_event_id}:tx:{index}"


def _merge_etsy_payload_with_tx(payload: dict[str, Any], tx: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    merged["transaction"] = tx

    if tx.get("listing_id") and not merged.get("listing_id"):
        merged["listing_id"] = tx.get("listing_id")

    if tx.get("sku") and not merged.get("sku"):
        merged["sku"] = tx.get("sku")

    return merged


async def replay_unresolved_etsy_receipt_events(*, limit: int = 200, event_id: str | None = None) -> dict[str, Any]:
    max_items = max(1, min(int(limit), 1000))

    query: dict[str, Any] = {
        "source_channel": "etsy",
        "status": "unresolved_sku",
        "payload.resource_url": {"$regex": "/receipts/", "$options": "i"},
    }
    if event_id:
        query["_id"] = str(event_id)

    cursor = db[EVENTS_COLLECTION].find(query).sort("created_at", 1).limit(max_items)
    events = [doc async for doc in cursor]

    if not events:
        return {
            "matched": 0,
            "processed": 0,
            "resolved_transactions": 0,
            "created_events": 0,
            "duplicates": 0,
            "still_unresolved": 0,
            "errors": 0,
            "events": [],
        }

    out: list[dict[str, Any]] = []
    processed = 0
    resolved_transactions = 0
    created_events = 0
    duplicates = 0
    still_unresolved = 0
    errors = 0

    for event in events:
        processed += 1
        base_event_id = str(event.get("_id"))
        payload = event.get("payload") or {}

        txs = await get_etsy_receipt_transactions_from_payload(payload)
        if not txs:
            still_unresolved += 1
            out.append(
                {
                    "event_id": base_event_id,
                    "transactions": 0,
                    "resolved": 0,
                    "created": 0,
                    "duplicates": 0,
                    "status": "no_transactions",
                }
            )
            continue

        event_resolved = 0
        event_created = 0
        event_duplicates = 0

        for index, tx in enumerate(txs, start=1):
            tx_payload = _merge_etsy_payload_with_tx(payload, tx)
            qty = max(1, _safe_int(tx.get("quantity"), 1))
            tx_event_id = _etsy_tx_event_id(base_event_id, tx, index)
            try:
                result = await ingest_sale_event(
                    source_channel="etsy",
                    payload=tx_payload,
                    quantity_sold=qty,
                    explicit_sku=None,
                    explicit_event_id=tx_event_id,
                    enqueue_jobs_flag=True,
                )
            except Exception:
                errors += 1
                logger.exception("Failed replay ingestion for Etsy transaction | event_id=%s", base_event_id)
                continue

            status = str(result.get("status") or "")
            if status == "duplicate":
                event_duplicates += 1
                duplicates += 1
                continue

            event_created += 1
            created_events += 1

            if status not in {"unresolved_sku", "no_product"}:
                event_resolved += 1
                resolved_transactions += 1

        new_status = "resolved_from_receipt" if event_resolved > 0 else "unresolved_after_receipt_replay"
        await db[EVENTS_COLLECTION].update_one(
            {"_id": base_event_id},
            {
                "$set": {
                    "status": new_status,
                    "replay": {
                        "type": "receipt_transactions",
                        "attempted_at": _utc_now(),
                        "transactions": len(txs),
                        "resolved": event_resolved,
                        "created": event_created,
                        "duplicates": event_duplicates,
                    },
                }
            },
        )

        if event_resolved == 0:
            still_unresolved += 1

        out.append(
            {
                "event_id": base_event_id,
                "transactions": len(txs),
                "resolved": event_resolved,
                "created": event_created,
                "duplicates": event_duplicates,
                "status": new_status,
            }
        )

    return {
        "matched": len(events),
        "processed": processed,
        "resolved_transactions": resolved_transactions,
        "created_events": created_events,
        "duplicates": duplicates,
        "still_unresolved": still_unresolved,
        "errors": errors,
        "events": out,
    }


async def _resolve_sku(source_channel: str, payload: dict[str, Any], explicit_sku: str | None) -> str | None:
    if explicit_sku:
        return explicit_sku

    if source_channel == "ebay":
        sku = payload.get("sku") or (payload.get("lineItem") or {}).get("sku") or (payload.get("item") or {}).get("sku")
        return str(sku) if sku else None

    if source_channel == "etsy":
        return await _resolve_sku_from_etsy_payload(payload)

    return None


async def enqueue_inventory_jobs(
    *,
    sku: str,
    target_qty: int,
    source_channel: str,
    source_event_id: str,
) -> dict[str, int]:
    queued = 0
    deduped = 0

    for target_channel in _iter_target_channels(source_channel):
        job_id = f"{source_event_id}:{target_channel}"
        result = await db[JOBS_COLLECTION].update_one(
            {"_id": job_id},
            {
                "$setOnInsert": {
                    "_id": job_id,
                    "event_id": source_event_id,
                    "sku": sku,
                    "target_channel": target_channel,
                    "target_qty": int(target_qty),
                    "status": "queued",
                    "attempts": 0,
                    "created_at": _utc_now(),
                },
                "$set": {
                    "updated_at": _utc_now(),
                },
            },
            upsert=True,
        )

        if getattr(result, "upserted_id", None):
            queued += 1
        else:
            deduped += 1

    return {"queued": queued, "deduped": deduped}


async def ingest_sale_event(
    *,
    source_channel: str,
    payload: dict[str, Any] | None = None,
    quantity_sold: int = 1,
    explicit_sku: str | None = None,
    explicit_event_id: str | None = None,
    enqueue_jobs_flag: bool = True,
) -> dict[str, Any]:
    payload = payload or {}
    sold_qty = max(1, _safe_int(quantity_sold, 1))

    sku = await _resolve_sku(source_channel, payload, explicit_sku)
    event_id = explicit_event_id or _event_id(source_channel, payload, sku, sold_qty)

    exists = await db[EVENTS_COLLECTION].find_one({"_id": event_id}, {"_id": 1, "status": 1})
    if exists:
        return {
            "event_id": event_id,
            "status": "duplicate",
            "sku": sku,
        }

    if not sku:
        await db[EVENTS_COLLECTION].insert_one(
            {
                "_id": event_id,
                "source_channel": source_channel,
                "type": "sale",
                "quantity_sold": sold_qty,
                "status": "unresolved_sku",
                "payload": payload,
                "created_at": _utc_now(),
            }
        )
        return {
            "event_id": event_id,
            "status": "unresolved_sku",
            "sku": None,
        }

    product = await db.product_normalized.find_one({"_id": sku}, {"_id": 1, "quantity": 1})
    if not product:
        await db[EVENTS_COLLECTION].insert_one(
            {
                "_id": event_id,
                "source_channel": source_channel,
                "type": "sale",
                "sku": sku,
                "quantity_sold": sold_qty,
                "status": "no_product",
                "payload": payload,
                "created_at": _utc_now(),
            }
        )
        return {
            "event_id": event_id,
            "status": "no_product",
            "sku": sku,
        }

    policy = await get_conflict_policy(str(sku))

    max_delta_guard = policy.get("max_delta_guard")
    if max_delta_guard is not None and sold_qty > int(max_delta_guard):
        await db[EVENTS_COLLECTION].insert_one(
            {
                "_id": event_id,
                "source_channel": source_channel,
                "type": "sale",
                "sku": sku,
                "quantity_sold": sold_qty,
                "status": "blocked_by_max_delta_guard",
                "policy": policy,
                "payload": payload,
                "created_at": _utc_now(),
            }
        )
        return {
            "event_id": event_id,
            "status": "blocked_by_max_delta_guard",
            "sku": sku,
            "policy": policy,
        }

    priority_channel = policy.get("priority_channel")
    strict_priority = bool(policy.get("strict_priority", False))
    if strict_priority and priority_channel and source_channel != priority_channel:
        await db[EVENTS_COLLECTION].insert_one(
            {
                "_id": event_id,
                "source_channel": source_channel,
                "type": "sale",
                "sku": sku,
                "quantity_sold": sold_qty,
                "status": "blocked_by_priority_channel",
                "policy": policy,
                "payload": payload,
                "created_at": _utc_now(),
            }
        )
        return {
            "event_id": event_id,
            "status": "blocked_by_priority_channel",
            "sku": sku,
            "policy": policy,
        }

    current_qty = max(0, _safe_int(product.get("quantity"), 0))
    new_qty = max(0, current_qty - sold_qty)

    await db.product_normalized.update_one(
        {"_id": sku},
        {"$set": {"quantity": new_qty, "updated_at": _utc_now()}},
    )

    event_doc = {
        "_id": event_id,
        "source_channel": source_channel,
        "type": "sale",
        "sku": sku,
        "quantity_sold": sold_qty,
        "quantity_before": current_qty,
        "quantity_after": new_qty,
        "status": "applied",
        "policy": policy,
        "payload": payload,
        "created_at": _utc_now(),
    }
    await db[EVENTS_COLLECTION].insert_one(event_doc)

    queue_stats = {"queued": 0, "deduped": 0}
    if enqueue_jobs_flag:
        queue_stats = await enqueue_inventory_jobs(
            sku=sku,
            target_qty=new_qty,
            source_channel=source_channel,
            source_event_id=event_id,
        )

    return {
        "event_id": event_id,
        "status": "applied",
        "sku": sku,
        "quantity_before": current_qty,
        "quantity_after": new_qty,
        "policy": policy,
        "jobs": queue_stats,
    }


async def _push_shopify_quantity(doc: dict[str, Any], target_qty: int) -> tuple[bool, str | None]:
    inventory_item_id = get_shopify_field(doc, "inventory_item_id")
    location_id = get_shopify_field(doc, "location_id")

    if not inventory_item_id or not location_id:
        return False, "missing_shopify_inventory_ids"

    ok = await set_inventory_from_mongo(
        inventory_item_id=_safe_int(inventory_item_id),
        location_id=_safe_int(location_id),
        quantity=int(target_qty),
        shopify_client=ShopifyClient(),
        sku=str(doc.get("_id")),
    )
    return bool(ok), None if ok else "shopify_inventory_update_failed"


async def _push_etsy_quantity(doc: dict[str, Any], target_qty: int) -> tuple[bool, str | None]:
    etsy_channel = get_channel(doc, "etsy")
    listing_id = etsy_channel.get("listing_id")
    shop_id = etsy_channel.get("shop_id")

    if not listing_id or not shop_id:
        return False, "missing_etsy_link"

    token = settings.ETSY_TOKEN
    if not token:
        token_doc = await db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"access_token": 1})
        token = token_doc.get("access_token") if token_doc else None

    if not token:
        return False, "missing_etsy_token"

    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        api_key = f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"
    else:
        api_key = settings.ETSY_CLIENT_ID

    if not api_key:
        return False, "missing_etsy_api_key"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": "application/json",
    }

    # Etsy requires PATCH on listing resource for quantity updates in this setup.
    url = f"https://openapi.etsy.com/v3/application/shops/{shop_id}/listings/{listing_id}"
    payload = {"quantity": int(target_qty)}

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.patch(url, headers=headers, data=payload)

    if response.status_code >= 400:
        return False, f"etsy_update_failed:{response.status_code}"
    return True, None


def _parse_ebay_trading_response(response_text: str) -> tuple[bool, str | None]:
    """Parse eBay Trading API XML response and check Ack status.

    Returns (ok, error_message). Treats 'Success' and 'Warning' as ok.
    """
    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
    try:
        root = ET.fromstring(response_text)
    except ET.ParseError as exc:
        return False, f"ebay_xml_parse_error:{exc}"
    ack = root.findtext(".//e:Ack", default="", namespaces=ns)
    if ack not in ("Success", "Warning"):
        msg = root.findtext(".//e:Errors/e:LongMessage", default="ebay_api_error", namespaces=ns)
        return False, f"ebay_ack_{ack}:{msg}"
    return True, None


async def _push_ebay_quantity(sku: str, target_qty: int) -> tuple[bool, str | None]:
    raw_doc = await db.product_raw.find_one({"_id": sku}, {"raw.ItemID": 1})
    item_id = ((raw_doc or {}).get("raw") or {}).get("ItemID")

    if not item_id:
        return False, "missing_ebay_item_id"

    client = EbayClient()
    await client.ensure_fresh_token()

    try:
        if target_qty <= 0:
            # End the listing entirely when quantity reaches zero
            end_xml = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
<EndFixedPriceItemRequest xmlns=\"urn:ebay:apis:eBLBaseComponents\">
  <ItemID>{item_id}</ItemID>
  <EndingReason>NotAvailable</EndingReason>
</EndFixedPriceItemRequest>"""
            response_text = client.trading_post("EndFixedPriceItem", end_xml)
            ok, error = _parse_ebay_trading_response(response_text)
            if not ok:
                logger.warning("eBay EndFixedPriceItem failed for SKU=%s item=%s: %s", sku, item_id, error)
                return False, error
            logger.info("eBay listing ended (qty=0) | SKU=%s | item_id=%s", sku, item_id)
            return True, None

        request_xml = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
<ReviseInventoryStatusRequest xmlns=\"urn:ebay:apis:eBLBaseComponents\">
  <InventoryStatus>
    <ItemID>{item_id}</ItemID>
    <SKU>{sku}</SKU>
    <Quantity>{int(target_qty)}</Quantity>
  </InventoryStatus>
</ReviseInventoryStatusRequest>"""
        response_text = client.trading_post("ReviseInventoryStatus", request_xml)
        ok, error = _parse_ebay_trading_response(response_text)
        if not ok:
            logger.warning("eBay ReviseInventoryStatus failed for SKU=%s item=%s: %s", sku, item_id, error)
            return False, error
        return True, None

    except Exception as exc:  # pragma: no cover - network call
        return False, f"ebay_update_failed:{exc}"


async def process_single_job(job_id: str) -> dict[str, Any]:
    job = await db[JOBS_COLLECTION].find_one_and_update(
        {"_id": job_id, "status": {"$in": ["queued", "retry"]}},
        {"$set": {"status": "processing", "started_at": _utc_now()}, "$inc": {"attempts": 1}},
        return_document=ReturnDocument.AFTER,
    )

    if not job:
        return {"job_id": job_id, "status": "not_claimed"}

    sku = str(job.get("sku"))
    target_channel = str(job.get("target_channel"))
    target_qty = _safe_int(job.get("target_qty"), 0)

    doc = await db.product_normalized.find_one(
        {"_id": sku},
        {
            "_id": 1,
            "quantity": 1,
            "shopify_variant_id": 1,
            "shopify_id": 1,
            "inventory_item_id": 1,
            "location_id": 1,
            "channels": 1,
        },
    )

    if not doc:
        await db[JOBS_COLLECTION].update_one(
            {"_id": job_id},
            {"$set": {"status": "failed", "error": "product_not_found", "finished_at": _utc_now()}},
        )
        return {"job_id": job_id, "status": "failed", "error": "product_not_found"}

    ok = False
    error: str | None = None

    if target_channel == "shopify":
        ok, error = await _push_shopify_quantity(doc, target_qty)
    elif target_channel == "etsy":
        ok, error = await _push_etsy_quantity(doc, target_qty)
    elif target_channel == "ebay":
        ok, error = await _push_ebay_quantity(sku, target_qty)
    else:
        ok = False
        error = f"unsupported_channel:{target_channel}"

    if ok:
        await db[JOBS_COLLECTION].update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "completed",
                    "finished_at": _utc_now(),
                    "error": None,
                }
            },
        )
        return {"job_id": job_id, "status": "completed", "channel": target_channel, "sku": sku}

    attempts = _safe_int(job.get("attempts"), 1)
    next_status = "failed" if attempts >= MAX_JOB_ATTEMPTS else "retry"
    await db[JOBS_COLLECTION].update_one(
        {"_id": job_id},
        {
            "$set": {
                "status": next_status,
                "error": error,
                "finished_at": _utc_now(),
            }
        },
    )
    return {
        "job_id": job_id,
        "status": next_status,
        "channel": target_channel,
        "sku": sku,
        "error": error,
    }


async def run_worker_batch(limit: int = 25) -> dict[str, Any]:
    max_items = max(1, min(int(limit), 500))

    cursor = db[JOBS_COLLECTION].find(
        {"status": {"$in": ["queued", "retry"]}},
        {"_id": 1},
    ).sort("created_at", 1).limit(max_items)

    job_ids = [str(doc.get("_id")) async for doc in cursor]

    processed: list[dict[str, Any]] = []
    for job_id in job_ids:
        result = await process_single_job(job_id)
        processed.append(result)

    summary = {
        "requested_limit": max_items,
        "picked": len(job_ids),
        "completed": sum(1 for item in processed if item.get("status") == "completed"),
        "retry": sum(1 for item in processed if item.get("status") == "retry"),
        "failed": sum(1 for item in processed if item.get("status") == "failed"),
        "results": processed,
    }
    return summary


async def get_sync_dashboard(limit_recent_jobs: int = 50) -> dict[str, Any]:
    status_pipeline = [
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    status_rows = await db[JOBS_COLLECTION].aggregate(status_pipeline).to_list(None)

    jobs_by_status = {str(row.get("_id")): int(row.get("count", 0)) for row in status_rows}

    recent_jobs = await db[JOBS_COLLECTION].find(
        {},
        {
            "_id": 1,
            "event_id": 1,
            "sku": 1,
            "target_channel": 1,
            "target_qty": 1,
            "status": 1,
            "attempts": 1,
            "error": 1,
            "created_at": 1,
            "finished_at": 1,
        },
    ).sort("created_at", -1).limit(max(1, min(int(limit_recent_jobs), 250))).to_list(None)

    mismatched_shopify = await db.product_normalized.count_documents(
        {
            "inventory_item_id": {"$exists": True, "$ne": None},
            "location_id": {"$exists": True, "$ne": None},
            "$expr": {"$ne": ["$quantity", "$channels.shopify.quantity"]},
        }
    )

    policies_count = await db[POLICIES_COLLECTION].count_documents({})

    return {
        "generated_at": _utc_now(),
        "jobs_by_status": jobs_by_status,
        "queued_jobs": jobs_by_status.get("queued", 0) + jobs_by_status.get("retry", 0),
        "policies_count": policies_count,
        "mismatch_hints": {
            "shopify_vs_canonical": mismatched_shopify,
        },
        "recent_jobs": recent_jobs,
    }
