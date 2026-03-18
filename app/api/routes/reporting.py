import re
from datetime import datetime, timezone

from fastapi import APIRouter, Query

from app.database.mongo import db
router = APIRouter()


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _rx(text: str) -> dict:
    # Escape to avoid accidental regex patterns coming from user input
    return {"$regex": re.escape(text), "$options": "i"}


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
