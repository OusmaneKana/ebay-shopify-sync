import re
import json
import copy
import asyncio
from pathlib import Path
from datetime import datetime, timezone

import httpx
from openpyxl import load_workbook
from fastapi import APIRouter, Query, HTTPException

from app.database.mongo import db
from app.config import settings
from app.shopify.client import ShopifyClient
router = APIRouter()

MATCH_REPORT_PATH = Path("logs/etsy_title_match_analysis.json")
REVIEW_COLLECTION = "etsy_match_review"
EXCEL_REVIEW_PATTERNS = [
    "active_etsy_no_ebay_match_with_mongo_candidates_and_shopify_link_*.xlsx",
    "active_etsy_no_ebay_match_with_mongo_candidates_*.xlsx",
    "active_etsy_no_ebay_match_*.xlsx",
]
ETSY_BASE_URL = "https://openapi.etsy.com/v3/application"


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

    if settings.SHOPIFY_STORE_URL_PROD:
        store = str(settings.SHOPIFY_STORE_URL_PROD).strip()
        if store.startswith("http://") or store.startswith("https://"):
            base = store.rstrip("/")
        else:
            base = f"https://{store}".rstrip("/")
        return {"prod": f"{base}/admin/products/{pid}"}
    return None


def _find_latest_excel_review_path() -> Path:
    root = Path(".")
    candidates: list[Path] = []
    for pattern in EXCEL_REVIEW_PATTERNS:
        candidates.extend(root.glob(pattern))

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail="No Etsy review workbook found. Generate the Excel report first.",
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _normalize_header(value: object) -> str:
    return str(value or "").strip().lower()


def _load_excel_rows(workbook_path: Path) -> tuple[list[dict], dict[str, int]]:
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = next(rows_iter, None)
    if not headers:
        return [], {}

    header_map = {_normalize_header(h): i for i, h in enumerate(headers)}
    out_rows: list[dict] = []
    for values in rows_iter:
        row: dict = {}
        for key, idx in header_map.items():
            row[key] = values[idx] if idx < len(values) else None
        out_rows.append(row)
    return out_rows, header_map


def _extract_etsy_image_from_doc(etsy_doc: dict | None) -> str | None:
    if not etsy_doc:
        return None

    raw = (etsy_doc.get("raw") or {}) if isinstance(etsy_doc, dict) else {}
    images = raw.get("images")
    if isinstance(images, list) and images:
        image = images[0] if isinstance(images[0], dict) else None
        if image:
            for key in ("url_fullxfull", "url_570xN", "url_170x135", "url_75x75"):
                val = image.get(key)
                if val:
                    return str(val)
    return None


async def _resolve_etsy_auth_headers_for_review() -> dict[str, str]:
    token = settings.ETSY_TOKEN
    if not token:
        token_doc = await db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"access_token": 1})
        token = token_doc.get("access_token") if token_doc else None
    if not token:
        raise HTTPException(status_code=400, detail="Missing Etsy token")

    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        api_key = f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"
    else:
        api_key = settings.ETSY_CLIENT_ID
    if not api_key:
        raise HTTPException(status_code=400, detail="Missing Etsy API key")

    return {
        "Authorization": f"Bearer {token}",
        "x-api-key": str(api_key),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _money_to_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        amount = value.get("amount")
        divisor = value.get("divisor")
        if amount is None or divisor in (None, 0):
            raise ValueError(f"invalid_money_value={value!r}")
        return float(amount) / float(divisor)
    raise ValueError(f"unsupported_price_value={value!r}")


def _build_etsy_inventory_update_payload(inventory: dict, new_sku: str, target_quantity: int) -> dict:
    products = inventory.get("products") or []
    if not products:
        raise HTTPException(status_code=400, detail="etsy_inventory_missing_products")

    payload_products: list[dict] = []
    for product in products:
        offerings = product.get("offerings") or []
        if not offerings:
            raise HTTPException(status_code=400, detail="etsy_inventory_product_missing_offerings")

        payload_offerings: list[dict] = []
        positive_quantity_found = False
        for offering in offerings:
            quantity = int(offering.get("quantity") or 0)
            if quantity > 0:
                positive_quantity_found = True
            payload_offerings.append(
                {
                    "price": _money_to_float(offering.get("price")),
                    "quantity": quantity,
                    "is_enabled": bool(offering.get("is_enabled")),
                    "readiness_state_id": offering.get("readiness_state_id"),
                }
            )

        if not positive_quantity_found and target_quantity > 0:
            first_enabled_index = next(
                (index for index, item in enumerate(payload_offerings) if item["is_enabled"]),
                0,
            )
            payload_offerings[first_enabled_index]["quantity"] = int(target_quantity)

        payload_product: dict = {
            "sku": str(new_sku),
            "offerings": payload_offerings,
        }
        property_values = product.get("property_values") or []
        if property_values:
            payload_product["property_values"] = copy.deepcopy(property_values)
        payload_products.append(payload_product)

    return {
        "products": payload_products,
        "price_on_property": copy.deepcopy(inventory.get("price_on_property") or []),
        "quantity_on_property": copy.deepcopy(inventory.get("quantity_on_property") or []),
        "sku_on_property": copy.deepcopy(inventory.get("sku_on_property") or []),
        "readiness_state_on_property": copy.deepcopy(inventory.get("readiness_state_on_property") or []),
    }


async def _update_etsy_listing_sku_for_review(
    *,
    listing_id: int,
    new_sku: str,
    target_quantity: int,
    headers: dict[str, str],
) -> None:
    async with httpx.AsyncClient(timeout=45.0) as client:
        get_response = await client.get(
            f"{ETSY_BASE_URL}/listings/{listing_id}/inventory",
            headers=headers,
            params={"show_deleted": "true"},
        )
        if get_response.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail=f"etsy_inventory_get_failed status={get_response.status_code}",
            )

        inventory_payload = get_response.json() if get_response.content else {}
        update_payload = _build_etsy_inventory_update_payload(inventory_payload, new_sku, target_quantity)

        put_response = await client.put(
            f"{ETSY_BASE_URL}/listings/{listing_id}/inventory",
            headers=headers,
            content=json.dumps(update_payload),
        )
        if put_response.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail=f"etsy_inventory_put_failed status={put_response.status_code}",
            )


async def _fetch_etsy_main_image_from_api(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    listing_id: int,
) -> str | None:
    response = await client.get(
        f"{ETSY_BASE_URL}/listings/{listing_id}/images",
        headers=headers,
    )
    if response.status_code >= 400:
        return None
    payload = response.json() if response.content else {}
    results = payload.get("results") or []
    if not results:
        return None

    first = results[0] if isinstance(results[0], dict) else None
    if not first:
        return None
    for key in ("url_fullxfull", "url_570xN", "url_170x135", "url_75x75"):
        value = first.get(key)
        if value:
            return str(value)
    return None


def _remove_listing_from_excel(workbook_path: Path, listing_id: int) -> bool:
    wb = load_workbook(workbook_path)
    ws = wb.active
    headers = [str(c.value or "").strip().lower() for c in ws[1]]
    if "listing_id" not in headers:
        raise HTTPException(status_code=400, detail="Workbook missing listing_id column")

    listing_col = headers.index("listing_id") + 1
    target_row = None
    for row_num in range(2, ws.max_row + 1):
        value = ws.cell(row=row_num, column=listing_col).value
        try:
            if int(value) == int(listing_id):
                target_row = row_num
                break
        except Exception:
            continue

    if target_row is None:
        wb.close()
        return False

    ws.delete_rows(target_row, 1)
    wb.save(workbook_path)
    wb.close()
    return True


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

    client = None
    if settings.SHOPIFY_API_KEY_PROD and settings.SHOPIFY_PASSWORD_PROD and settings.SHOPIFY_STORE_URL_PROD:
        client = ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )

    if client is None:
        return {
            "product_id": sid,
            "error": "shopify_prod_credentials_not_configured",
        }

    product = None
    source_store = None
    last_error = None
    try:
        resp = await client.get(f"products/{sid}.json")
        candidate = (resp or {}).get("product")
        if isinstance(candidate, dict):
            product = candidate
            source_store = "prod"
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


@router.get("/etsy-match-excel/summary")
async def etsy_match_excel_summary():
    workbook_path = _find_latest_excel_review_path()
    rows, _ = _load_excel_rows(workbook_path)
    with_shopify = sum(1 for row in rows if row.get("matched_shopify_link"))
    with_sku = sum(1 for row in rows if row.get("matched_mongo_sku"))
    return {
        "workbook": str(workbook_path),
        "count": len(rows),
        "with_shopify_link": with_shopify,
        "with_matched_sku": with_sku,
        "generated_at": _now_utc(),
    }


@router.get("/etsy-match-excel/review")
async def etsy_match_excel_review(
    limit: int = Query(default=500, ge=1, le=2000),
):
    workbook_path = _find_latest_excel_review_path()
    rows, _ = _load_excel_rows(workbook_path)
    selected = rows[:limit]

    listing_ids: list[int] = []
    skus: list[str] = []
    for row in selected:
        try:
            listing_ids.append(int(row.get("listing_id")))
        except Exception:
            pass
        sku = row.get("matched_mongo_sku")
        if sku:
            skus.append(str(sku))

    etsy_docs_by_listing: dict[int, dict] = {}
    if listing_ids:
        cursor = db.etsy_listings_investigation.find(
            {"listing_id": {"$in": listing_ids}},
            {"_id": 0, "listing_id": 1, "title": 1, "url": 1, "raw.images": 1},
        )
        async for doc in cursor:
            try:
                etsy_docs_by_listing[int(doc.get("listing_id"))] = doc
            except Exception:
                continue

    norm_docs_by_sku: dict[str, dict] = {}
    if skus:
        cursor = db.product_normalized.find(
            {"_id": {"$in": skus}},
            {
                "_id": 1,
                "title": 1,
                "images": 1,
                "quantity": 1,
                "shopify_id": 1,
                "channels.shopify.shopify_id": 1,
            },
        )
        async for doc in cursor:
            norm_docs_by_sku[str(doc.get("_id"))] = doc

    etsy_images_by_listing: dict[int, str | None] = {}
    listings_missing_image: list[int] = []

    for listing_id in listing_ids:
        etsy_doc = etsy_docs_by_listing.get(listing_id)
        image = _extract_etsy_image_from_doc(etsy_doc)
        etsy_images_by_listing[listing_id] = image
        if not image:
            listings_missing_image.append(listing_id)

    if listings_missing_image:
        headers = None
        try:
            headers = await _resolve_etsy_auth_headers_for_review()
        except HTTPException:
            headers = None

        if headers:
            semaphore = asyncio.Semaphore(6)
            async with httpx.AsyncClient(timeout=20.0) as client:
                async def _fetch_one(lid: int) -> None:
                    async with semaphore:
                        image_url = await _fetch_etsy_main_image_from_api(client, headers, lid)
                        if image_url:
                            etsy_images_by_listing[lid] = image_url

                await asyncio.gather(*(_fetch_one(lid) for lid in listings_missing_image))

    out: list[dict] = []
    for row in selected:
        listing_id_raw = row.get("listing_id")
        try:
            listing_id = int(listing_id_raw)
        except Exception:
            listing_id = None

        sku = str(row.get("matched_mongo_sku") or "").strip() or None
        etsy_doc = etsy_docs_by_listing.get(listing_id) if listing_id is not None else None
        norm_doc = norm_docs_by_sku.get(sku) if sku else None

        shopify_id = None
        if norm_doc:
            shopify_id = (
                (norm_doc.get("channels") or {}).get("shopify", {}).get("shopify_id")
                or norm_doc.get("shopify_id")
            )

        shopify_image = None
        if norm_doc:
            images = norm_doc.get("images") or []
            if images:
                shopify_image = images[0]

        etsy_image = etsy_images_by_listing.get(listing_id) if listing_id is not None else None

        out.append(
            {
                "listing_id": listing_id,
                "etsy_title": row.get("live_title") or (etsy_doc or {}).get("title"),
                "etsy_link": row.get("live_url") or (etsy_doc or {}).get("url"),
                "etsy_image": etsy_image,
                "matched_mongo_title": row.get("matched_mongo_title") or (norm_doc or {}).get("title"),
                "matched_mongo_sku": sku,
                "confidence_rate": row.get("confidence_rate"),
                "matched_shopify_link": row.get("matched_shopify_link"),
                "shopify_image": shopify_image,
                "shopify_id": shopify_id,
                "shopify_links": _build_shopify_links(shopify_id),
                "mongo_quantity": (norm_doc or {}).get("quantity"),
            }
        )

    return {
        "workbook": str(workbook_path),
        "count": len(out),
        "items": out,
    }


@router.post("/etsy-match-excel/review/approve")
async def etsy_match_excel_approve(payload: dict):
    return await _approve_excel_match(payload)


async def _approve_excel_match(
    payload: dict,
    *,
    headers: dict[str, str] | None = None,
    workbook_path: Path | None = None,
) -> dict:
    listing_id = payload.get("listing_id")
    sku = str(payload.get("matched_mongo_sku") or "").strip()
    if not listing_id or not sku:
        raise HTTPException(status_code=400, detail="listing_id and matched_mongo_sku are required")

    try:
        listing_id_int = int(listing_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="invalid_listing_id") from exc

    norm_doc = await db.product_normalized.find_one({"_id": sku}, {"_id": 1, "quantity": 1})
    if not norm_doc:
        raise HTTPException(status_code=404, detail="matched_mongo_sku_not_found")

    etsy_headers = headers or await _resolve_etsy_auth_headers_for_review()
    target_quantity = int(norm_doc.get("quantity") or 0)
    await _update_etsy_listing_sku_for_review(
        listing_id=listing_id_int,
        new_sku=sku,
        target_quantity=target_quantity,
        headers=etsy_headers,
    )

    ok, reason = await _apply_match_row(
        {
            "etsy_listing_id": listing_id_int,
            "normalized_sku": sku,
            "score": payload.get("confidence_rate"),
            "bucket": "excel_review",
        },
        matched_by="excel_manual_review",
    )
    if not ok:
        raise HTTPException(status_code=400, detail=reason)

    workbook = workbook_path or _find_latest_excel_review_path()
    removed = _remove_listing_from_excel(workbook, listing_id_int)

    return {
        "ok": True,
        "listing_id": listing_id_int,
        "matched_mongo_sku": sku,
        "removed_from_excel": removed,
        "workbook": str(workbook),
    }


@router.put("/etsy-match-excel/review/approve-batch")
async def etsy_match_excel_approve_batch(payload: dict):
    approvals = payload.get("approvals") if isinstance(payload, dict) else None
    if not isinstance(approvals, list) or not approvals:
        raise HTTPException(status_code=400, detail="approvals list is required")

    workbook_path = _find_latest_excel_review_path()
    headers = await _resolve_etsy_auth_headers_for_review()

    successes: list[dict] = []
    failures: list[dict] = []
    for item in approvals:
        try:
            result = await _approve_excel_match(item, headers=headers, workbook_path=workbook_path)
            successes.append(
                {
                    "listing_id": result.get("listing_id"),
                    "matched_mongo_sku": result.get("matched_mongo_sku"),
                    "removed_from_excel": result.get("removed_from_excel"),
                }
            )
        except HTTPException as exc:
            failures.append(
                {
                    "listing_id": item.get("listing_id") if isinstance(item, dict) else None,
                    "matched_mongo_sku": (item or {}).get("matched_mongo_sku") if isinstance(item, dict) else None,
                    "detail": exc.detail,
                    "status_code": exc.status_code,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "listing_id": item.get("listing_id") if isinstance(item, dict) else None,
                    "matched_mongo_sku": (item or {}).get("matched_mongo_sku") if isinstance(item, dict) else None,
                    "detail": str(exc),
                    "status_code": 500,
                }
            )

    return {
        "ok": len(failures) == 0,
        "workbook": str(workbook_path),
        "requested": len(approvals),
        "applied": len(successes),
        "failed": len(failures),
        "successes": successes,
        "failures": failures,
    }


@router.post("/etsy-match-excel/review/deny")
async def etsy_match_excel_deny(payload: dict):
    listing_id = payload.get("listing_id")
    sku = str(payload.get("matched_mongo_sku") or "").strip() or None
    reason = payload.get("reason")
    if not listing_id:
        raise HTTPException(status_code=400, detail="listing_id is required")

    await db[REVIEW_COLLECTION].update_one(
        {"etsy_listing_id": listing_id, "normalized_sku": sku},
        {
            "$set": {
                "status": "denied",
                "reason": reason,
                "bucket": "excel_review",
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


@router.get("/channel-compare/kpis")
async def channel_compare_kpis():
    total_products = await db.product_normalized.count_documents({})

    etsy_linked = await db.product_normalized.count_documents(
        {"channels.etsy.listing_id": {"$exists": True, "$ne": None}}
    )
    shopify_linked = await db.product_normalized.count_documents(
        {"$or": [
            {"channels.shopify.shopify_id": {"$exists": True, "$ne": None}},
            {"shopify_id": {"$exists": True, "$ne": None}},
        ]}
    )

    inventory_pipeline = [
        {
            "$group": {
                "_id": None,
                "ebay_total_inventory": {
                    "$sum": {"$ifNull": ["$quantity", 0]}
                },
                "etsy_total_inventory": {
                    "$sum": {"$ifNull": ["$channels.etsy.quantity", 0]}
                },
                "shopify_total_inventory": {
                    "$sum": {"$ifNull": ["$channels.shopify.quantity", 0]}
                },
            }
        }
    ]
    agg = await db.product_normalized.aggregate(inventory_pipeline).to_list(length=1)
    totals = agg[0] if agg else {}

    return {
        "kpis": {
            "ebay_total_inventory": int(totals.get("ebay_total_inventory", 0) or 0),
            "etsy_total_inventory": int(totals.get("etsy_total_inventory", 0) or 0),
            "shopify_total_inventory": int(totals.get("shopify_total_inventory", 0) or 0),
            "total_products": int(total_products),
            "etsy_linked_products": int(etsy_linked),
            "shopify_linked_products": int(shopify_linked),
            "etsy_missing_products": max(0, int(total_products) - int(etsy_linked)),
            "shopify_missing_products": max(0, int(total_products) - int(shopify_linked)),
        }
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
