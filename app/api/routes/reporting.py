import re
import json
from pathlib import Path
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException

from app.database.mongo import db
from app.config import settings
from app.shopify.client import ShopifyClient
router = APIRouter()

MATCH_REPORT_PATH = Path("logs/etsy_title_match_analysis.json")
REVIEW_COLLECTION = "etsy_match_review"


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _rx(text: str) -> dict:
    # Escape to avoid accidental regex patterns coming from user input
    return {"$regex": re.escape(text), "$options": "i"}


def _load_match_report() -> dict:
    if not MATCH_REPORT_PATH.exists():
        raise HTTPException(status_code=404, detail="Match report not found. Export analysis first.")
    with MATCH_REPORT_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _build_shopify_links(shopify_id: object) -> dict | None:
    if not shopify_id:
        return None
    try:
        pid = int(shopify_id)
    except Exception:
        return None

    links: dict[str, str] = {}
    if settings.SHOPIFY_STORE_URL:
        links["dev"] = f"https://{settings.SHOPIFY_STORE_URL}/admin/products/{pid}"
    if settings.SHOPIFY_STORE_URL_PROD:
        links["prod"] = f"https://{settings.SHOPIFY_STORE_URL_PROD}/admin/products/{pid}"
    return links or None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, dict):
        amount = value.get("amount")
        divisor = value.get("divisor")
        if amount is None:
            return None
        try:
            amount_f = float(amount)
            divisor_f = float(divisor or 1)
            if divisor_f == 0:
                return None
            return round(amount_f / divisor_f, 2)
        except Exception:
            return None
    try:
        return float(value)
    except Exception:
        return None


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _cmp_values(left: object, right: object, *, numeric: bool = False) -> str:
    if left is None and right is None:
        return "missing"
    if left is None or right is None:
        return "missing"

    if numeric:
        left_f = _to_float(left)
        right_f = _to_float(right)
        if left_f is None or right_f is None:
            return "diff"
        return "same" if abs(left_f - right_f) < 0.01 else "diff"

    return "same" if str(left).strip() == str(right).strip() else "diff"


async def _fetch_shopify_snapshot(shopify_id: object) -> dict | None:
    sid = _to_int(shopify_id)
    if sid is None:
        return None

    clients = [
        ("dev", ShopifyClient()),
    ]
    if settings.SHOPIFY_API_KEY_PROD and settings.SHOPIFY_PASSWORD_PROD and settings.SHOPIFY_STORE_URL_PROD:
        clients.append(
            (
                "prod",
                ShopifyClient(
                    api_key=settings.SHOPIFY_API_KEY_PROD,
                    password=settings.SHOPIFY_PASSWORD_PROD,
                    store_url=settings.SHOPIFY_STORE_URL_PROD,
                ),
            )
        )

    product = None
    source_store = None
    last_error = None
    for store_name, client in clients:
        try:
            resp = await client.get(f"products/{sid}.json")
            candidate = (resp or {}).get("product")
            if isinstance(candidate, dict):
                product = candidate
                source_store = store_name
                break
        except Exception as exc:
            last_error = str(exc)

    if not isinstance(product, dict):
        return {
            "product_id": sid,
            "error": last_error or "shopify_product_not_found",
        }

    variants = product.get("variants") or []
    first_variant = variants[0] if variants else {}
    images = product.get("images") or []
    image_src = None
    if images and isinstance(images[0], dict):
        image_src = images[0].get("src")

    return {
        "product_id": sid,
        "store": source_store,
        "variant_id": first_variant.get("id") if isinstance(first_variant, dict) else None,
        "title": product.get("title"),
        "status": product.get("status"),
        "handle": product.get("handle"),
        "price": _to_float(first_variant.get("price") if isinstance(first_variant, dict) else None),
        "quantity": _to_int(first_variant.get("inventory_quantity") if isinstance(first_variant, dict) else None),
        "image": image_src,
    }


async def _apply_match_row(match_row: dict, *, matched_by: str) -> tuple[bool, str]:
    listing_id = match_row.get("etsy_listing_id")
    sku = match_row.get("normalized_sku")
    if not listing_id or not sku:
        return False, "missing_listing_or_sku"

    etsy_doc = await db.etsy_listings_investigation.find_one(
        {"listing_id": listing_id},
        {
            "listing_id": 1,
            "listing_state": 1,
            "title": 1,
            "url": 1,
            "shop_id": 1,
            "price": 1,
            "quantity": 1,
        },
    )
    if not etsy_doc:
        return False, "etsy_listing_not_found"

    channels_etsy = {
        "listing_id": etsy_doc.get("listing_id"),
        "listing_state": etsy_doc.get("listing_state"),
        "title": etsy_doc.get("title"),
        "url": etsy_doc.get("url"),
        "shop_id": etsy_doc.get("shop_id"),
        "price": etsy_doc.get("price"),
        "quantity": etsy_doc.get("quantity"),
        "match": {
            "status": "approved",
            "matched_by": matched_by,
            "score": match_row.get("score"),
            "bucket": match_row.get("bucket"),
            "matched_at": _now_utc(),
        },
    }

    result = await db.product_normalized.update_one(
        {"_id": sku},
        {
            "$set": {
                "channels.etsy": channels_etsy,
            }
        },
    )
    if result.matched_count == 0:
        return False, "normalized_sku_not_found"

    await db[REVIEW_COLLECTION].update_one(
        {"etsy_listing_id": listing_id, "normalized_sku": sku},
        {
            "$set": {
                "status": "approved",
                "bucket": match_row.get("bucket"),
                "score": match_row.get("score"),
                "matched_by": matched_by,
                "updated_at": _now_utc(),
            }
        },
        upsert=True,
    )

    return True, "applied"


@router.get("/items")
async def report_items(
    q: str | None = Query(default=None, description="Search across SKU / title / item id"),
    size: str | None = Query(default=None, description="Size attribute (string contains match)"),
    posted_after: datetime | None = Query(default=None, description="eBay listing start time >= this (ISO 8601)"),
    posted_before: datetime | None = Query(default=None, description="eBay listing start time <= this (ISO 8601)"),
    min_weight: float | None = Query(default=None, description="Min package weight major value"),
    max_weight: float | None = Query(default=None, description="Max package weight major value"),
    min_length: float | None = Query(default=None, description="Min package length"),
    max_length: float | None = Query(default=None, description="Max package length"),
    min_width: float | None = Query(default=None, description="Min package width"),
    max_width: float | None = Query(default=None, description="Max package width"),
    min_height: float | None = Query(default=None, description="Min package height"),
    max_height: float | None = Query(default=None, description="Max package height"),
    available_only: bool = Query(default=False, description="Only items with quantity > 0"),
    soldout_only: bool = Query(default=False, description="Only items with quantity <= 0"),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
):
    posted_after = _ensure_utc(posted_after)
    posted_before = _ensure_utc(posted_before)

    and_conditions: list[dict] = []

    # Posting date filter (stored on product_raw)
    if posted_after or posted_before:
        date_q: dict[str, object] = {}
        if posted_after:
            date_q["$gte"] = posted_after
        if posted_before:
            date_q["$lte"] = posted_before
        and_conditions.append({"ebay_posted_at": date_q})

    # Search query
    if q:
        rx = _rx(q)
        and_conditions.append(
            {
                "$or": [
                    {"_id": rx},
                    {"raw.ItemID": rx},
                    {"raw.SKU": rx},
                    {"raw.Title": rx},
                    {"normalized.title": rx},
                    {"normalized.category": rx},
                ]
            }
        )

    # Size filter (best-effort)
    if size:
        rx = _rx(size)
        and_conditions.append(
            {
                "$or": [
                    {"raw.ItemSpecifics.Size": rx},
                    {"normalized.attributes.Size": rx},
                    {"normalized.metafields.art.size": rx},
                ]
            }
        )

    # Weight filter (normalized.package.weight.major.value)
    if min_weight is not None or max_weight is not None:
        wq: dict[str, object] = {}
        if min_weight is not None:
            wq["$gte"] = float(min_weight)
        if max_weight is not None:
            wq["$lte"] = float(max_weight)
        and_conditions.append({"normalized.package.weight.major.value": wq})

    # Dimensions filters
    def _dim_cond(field: str, min_v: float | None, max_v: float | None) -> None:
        if min_v is None and max_v is None:
            return
        dq: dict[str, object] = {}
        if min_v is not None:
            dq["$gte"] = float(min_v)
        if max_v is not None:
            dq["$lte"] = float(max_v)
        and_conditions.append({field: dq})

    _dim_cond("normalized.package.dimensions.length.value", min_length, max_length)
    _dim_cond("normalized.package.dimensions.width.value", min_width, max_width)
    _dim_cond("normalized.package.dimensions.height.value", min_height, max_height)

    # Availability filter (prefer normalized.quantity, fall back to raw.QuantityAvailable)
    # If both checkboxes are set (or neither), treat as no filter.
    if available_only and not soldout_only:
        and_conditions.append(
            {
                "$expr": {
                    "$gt": [
                        {"$ifNull": ["$normalized.quantity", "$raw.QuantityAvailable"]},
                        0,
                    ]
                }
            }
        )
    elif soldout_only and not available_only:
        and_conditions.append(
            {
                "$expr": {
                    "$lte": [
                        {"$ifNull": ["$normalized.quantity", "$raw.QuantityAvailable"]},
                        0,
                    ]
                }
            }
        )

    match_stage: dict | None = None
    if and_conditions:
        match_stage = {"$match": {"$and": and_conditions}}

    lookup = {
        "$lookup": {
            "from": "product_normalized",
            "localField": "_id",
            "foreignField": "_id",
            "as": "normalized",
        }
    }
    unwind = {"$unwind": {"path": "$normalized", "preserveNullAndEmptyArrays": True}}

    base_pipeline: list[dict] = [lookup, unwind]
    if match_stage:
        base_pipeline.append(match_stage)

    # Count pipeline (same filters)
    count_pipeline = base_pipeline + [{"$count": "total"}]
    count_docs = await db.product_raw.aggregate(count_pipeline).to_list(length=1)
    total = int(count_docs[0]["total"]) if count_docs else 0

    # Result pipeline
    result_pipeline = (
        base_pipeline
        + [
            {"$sort": {"ebay_posted_at": -1, "_id": 1}},
            {"$skip": int(skip)},
            {"$limit": int(limit)},
            {
                "$project": {
                    "_id": 0,
                    "sku": "$_id",
                    "ebay_posted_at": 1,
                    "image_url": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$normalized.images", 0]},
                            {"$arrayElemAt": ["$raw.Images", 0]},
                        ]
                    },
                    "image_urls": {"$ifNull": ["$normalized.images", "$raw.Images"]},
                    "raw": "$raw",
                    "normalized": "$normalized",
                }
            },
        ]
    )

    items = await db.product_raw.aggregate(result_pipeline).to_list(length=limit)

    return {
        "items": items,
        "total": total,
        "skip": skip,
        "limit": limit,
        "has_more": (skip + len(items)) < total,
    }


@router.get("/etsy-match/summary")
async def etsy_match_summary():
    """Get current Etsy match summary from database (live counts)."""
    # Count by bucket/status from review collection
    pipeline = [
        {
            '$group': {
                '_id': {'bucket': '$bucket', 'status': '$status'},
                'count': {'$sum': 1}
            }
        }
    ]
    review_breakdown = await db.etsy_match_review.aggregate(pipeline).to_list(None)
    
    # Count linked products
    linked = await db.product_normalized.count_documents({'channels.etsy.listing_id': {'$exists': True}})
    
    # Total unmatched
    total_etsy = await db.etsy_listings_investigation.count_documents({})
    unmatched = total_etsy - linked
    
    # Build summary object
    summary = {
        'etsy_total': total_etsy,
        'linked': linked,
        'unmatched': unmatched,
    }
    
    # Extract approved counts by bucket
    for row in review_breakdown:
        bucket = row['_id']['bucket']
        status = row['_id']['status']
        count = row['count']
        
        if status == 'approved':
            if bucket == 'exact_ci':
                summary['exact'] = count
            elif bucket == 'normalized_exact':
                summary['normalized_exact'] = count
            elif bucket == 'high_confidence':
                summary['high'] = count
            elif bucket == 'medium_confidence':
                summary['medium'] = count
            elif bucket == 'low_confidence':
                summary['low'] = count
    
    # Count pending by bucket for UI
    pending_counts = {}
    for row in review_breakdown:
        bucket = row['_id']['bucket']
        status = row['_id']['status']
        count = row['count']
        
        if status == 'pending':
            pending_counts[bucket] = count
    
    summary['pending'] = pending_counts
    
    return {
        "summary": summary,
        "generated_at": _now_utc(),
    }


@router.post("/etsy-match/apply-exact")
async def etsy_match_apply_exact():
    report = _load_match_report()
    rows = (report.get("exact_matches") or []) + (report.get("normalized_exact_matches") or [])

    applied = 0
    skipped = 0
    errors: list[dict] = []

    for row in rows:
        ok, reason = await _apply_match_row(row, matched_by="auto_exact")
        if ok:
            applied += 1
        else:
            skipped += 1
            errors.append(
                {
                    "etsy_listing_id": row.get("etsy_listing_id"),
                    "normalized_sku": row.get("normalized_sku"),
                    "reason": reason,
                }
            )

    return {
        "matched_candidates": len(rows),
        "applied": applied,
        "skipped": skipped,
        "errors": errors[:50],
    }


@router.get("/etsy-match/review")
async def etsy_match_review(
    bucket: str = Query(default="all", description="all|high|medium|low|unmatched"),
    status: str = Query(default="pending", description="pending|approved|denied|all"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    report = _load_match_report()

    bucket_map = {
        "high": "high_confidence_matches",
        "medium": "medium_confidence_matches",
        "low": "low_confidence_matches",
        "unmatched": "unmatched",
    }

    if bucket == "all":
        source_rows = (
            (report.get("high_confidence_matches") or [])
            + (report.get("medium_confidence_matches") or [])
            + (report.get("low_confidence_matches") or [])
            + (report.get("unmatched") or [])
        )
    else:
        key = bucket_map.get(bucket)
        if not key:
            raise HTTPException(status_code=400, detail="Invalid bucket")
        source_rows = report.get(key) or []

    out: list[dict] = []
    for row in source_rows:
        listing_id = row.get("etsy_listing_id")
        sku = row.get("normalized_sku")

        review_doc = await db[REVIEW_COLLECTION].find_one(
            {"etsy_listing_id": listing_id, "normalized_sku": sku},
            {"_id": 0, "status": 1, "reason": 1, "updated_at": 1},
        )
        row_status = (review_doc or {}).get("status", "pending")

        if status != "all" and row_status != status:
            continue

        etsy_doc = None
        if listing_id is not None:
            etsy_doc = await db.etsy_listings_investigation.find_one(
                {"listing_id": listing_id},
                {"_id": 0, "price": 1, "quantity": 1, "listing_state": 1},
            )

        normalized_doc = None
        if sku:
            normalized_doc = await db.product_normalized.find_one(
                {"_id": sku},
                {
                    "_id": 1,
                    "price": 1,
                    "shipping": 1,
                    "shopify_id": 1,
                    "channels.shopify.shopify_id": 1,
                },
            )

        shopify_id = None
        if normalized_doc:
            shopify_id = (
                (normalized_doc.get("channels") or {}).get("shopify", {}).get("shopify_id")
                or normalized_doc.get("shopify_id")
            )

        out.append(
            {
                **row,
                "review_status": row_status,
                "review_reason": (review_doc or {}).get("reason"),
                "review_updated_at": (review_doc or {}).get("updated_at"),
                "etsy_link": f"https://www.etsy.com/listing/{listing_id}" if listing_id else None,
                "shopify_id": shopify_id,
                "shopify_links": _build_shopify_links(shopify_id),
                "normalized_price": (normalized_doc or {}).get("price"),
                "normalized_shipping": (normalized_doc or {}).get("shipping"),
                "etsy_price": (etsy_doc or {}).get("price"),
                "etsy_state_live": (etsy_doc or {}).get("listing_state"),
            }
        )

        if len(out) >= limit:
            break

    return {
        "bucket": bucket,
        "status": status,
        "count": len(out),
        "items": out,
    }


@router.post("/etsy-match/review/approve")
async def etsy_match_approve(payload: dict):
    listing_id = payload.get("etsy_listing_id")
    sku = payload.get("normalized_sku")
    if not listing_id or not sku:
        raise HTTPException(status_code=400, detail="etsy_listing_id and normalized_sku are required")

    row = {
        "etsy_listing_id": listing_id,
        "normalized_sku": sku,
        "score": payload.get("score"),
        "bucket": payload.get("bucket"),
    }
    ok, reason = await _apply_match_row(row, matched_by="manual_review")
    if not ok:
        raise HTTPException(status_code=400, detail=reason)
    return {"ok": True}


@router.post("/etsy-match/review/deny")
async def etsy_match_deny(payload: dict):
    listing_id = payload.get("etsy_listing_id")
    sku = payload.get("normalized_sku")
    reason = payload.get("reason")
    if not listing_id:
        raise HTTPException(status_code=400, detail="etsy_listing_id is required")

    await db[REVIEW_COLLECTION].update_one(
        {"etsy_listing_id": listing_id, "normalized_sku": sku},
        {
            "$set": {
                "status": "denied",
                "reason": reason,
                "updated_at": _now_utc(),
            }
        },
        upsert=True,
    )
    return {"ok": True}


@router.get("/channel-compare/list")
async def channel_compare_list(
    q: str | None = Query(default=None, description="Search SKU/title"),
    drift_only: bool = Query(default=False, description="Only rows with detected drift"),
    limit: int = Query(default=100, ge=1, le=500),
):
    query: dict = {"channels.etsy.listing_id": {"$exists": True}}
    if q:
        rx = _rx(q)
        query["$or"] = [
            {"_id": rx},
            {"sku": rx},
            {"title": rx},
            {"channels.etsy.title": rx},
        ]

    cursor = db.product_normalized.find(
        query,
        {
            "_id": 1,
            "sku": 1,
            "title": 1,
            "price": 1,
            "quantity": 1,
            "images": 1,
            "shopify_id": 1,
            "channels.etsy": 1,
            "channels.shopify": 1,
            "last_normalized_at": 1,
        },
    ).sort("last_normalized_at", -1)

    docs = await cursor.to_list(length=limit * 3)
    items: list[dict] = []
    for doc in docs:
        channels = doc.get("channels") or {}
        etsy = channels.get("etsy") or {}
        shopify = channels.get("shopify") or {}
        shopify_id = shopify.get("shopify_id") or doc.get("shopify_id")

        drift_fields: list[str] = []
        if _cmp_values(doc.get("title"), etsy.get("title")) == "diff":
            drift_fields.append("title")
        if _cmp_values(doc.get("price"), etsy.get("price"), numeric=True) == "diff":
            drift_fields.append("price")
        if _cmp_values(doc.get("quantity"), etsy.get("quantity"), numeric=True) == "diff":
            drift_fields.append("quantity")
        if not shopify_id:
            drift_fields.append("shopify_missing")

        if drift_only and not drift_fields:
            continue

        items.append(
            {
                "sku": doc.get("_id") or doc.get("sku"),
                "title": doc.get("title"),
                "image": (doc.get("images") or [None])[0],
                "etsy_listing_id": etsy.get("listing_id"),
                "shopify_id": shopify_id,
                "match_bucket": ((etsy.get("match") or {}).get("bucket")),
                "drift_fields": drift_fields,
                "drift_count": len(drift_fields),
            }
        )
        if len(items) >= limit:
            break

    return {
        "count": len(items),
        "items": items,
    }


@router.get("/channel-compare/{sku}")
async def channel_compare_detail(sku: str):
    doc = await db.product_normalized.find_one(
        {"$or": [{"_id": sku}, {"sku": sku}]},
        {
            "_id": 1,
            "sku": 1,
            "title": 1,
            "price": 1,
            "quantity": 1,
            "images": 1,
            "shopify_id": 1,
            "channels.etsy": 1,
            "channels.shopify": 1,
            "last_normalized_at": 1,
        },
    )
    if not doc:
        raise HTTPException(status_code=404, detail="SKU not found")

    channels = doc.get("channels") or {}
    etsy = channels.get("etsy") or {}
    shopify_channel = channels.get("shopify") or {}
    shopify_id = shopify_channel.get("shopify_id") or doc.get("shopify_id")
    shopify_live = await _fetch_shopify_snapshot(shopify_id)

    normalized_price = _to_float(doc.get("price"))
    normalized_qty = _to_int(doc.get("quantity"))
    etsy_price = _to_float(etsy.get("price"))
    etsy_qty = _to_int(etsy.get("quantity"))
    shopify_price = _to_float((shopify_live or {}).get("price"))
    shopify_qty = _to_int((shopify_live or {}).get("quantity"))

    comparisons = {
        "title": {
            "etsy_vs_shopify": _cmp_values(etsy.get("title"), (shopify_live or {}).get("title")),
            "normalized_vs_etsy": _cmp_values(doc.get("title"), etsy.get("title")),
            "normalized_vs_shopify": _cmp_values(doc.get("title"), (shopify_live or {}).get("title")),
        },
        "price": {
            "etsy_vs_shopify": _cmp_values(etsy_price, shopify_price, numeric=True),
            "normalized_vs_etsy": _cmp_values(normalized_price, etsy_price, numeric=True),
            "normalized_vs_shopify": _cmp_values(normalized_price, shopify_price, numeric=True),
        },
        "quantity": {
            "etsy_vs_shopify": _cmp_values(etsy_qty, shopify_qty, numeric=True),
            "normalized_vs_etsy": _cmp_values(normalized_qty, etsy_qty, numeric=True),
            "normalized_vs_shopify": _cmp_values(normalized_qty, shopify_qty, numeric=True),
        },
        "status": {
            "etsy_vs_shopify": _cmp_values(etsy.get("listing_state"), (shopify_live or {}).get("status")),
        },
    }

    return {
        "sku": doc.get("_id") or doc.get("sku"),
        "normalized": {
            "title": doc.get("title"),
            "price": normalized_price,
            "quantity": normalized_qty,
            "image": (doc.get("images") or [None])[0],
            "last_normalized_at": doc.get("last_normalized_at"),
        },
        "etsy": {
            "listing_id": etsy.get("listing_id"),
            "title": etsy.get("title"),
            "price": etsy_price,
            "quantity": etsy_qty,
            "status": etsy.get("listing_state"),
            "url": etsy.get("url") or (
                f"https://www.etsy.com/listing/{etsy.get('listing_id')}" if etsy.get("listing_id") else None
            ),
            "image": etsy.get("image"),
            "match": etsy.get("match") or {},
        },
        "shopify": {
            "shopify_id": shopify_id,
            "links": _build_shopify_links(shopify_id),
            "live": shopify_live,
        },
        "comparisons": comparisons,
    }
