"""
Daily audit endpoints — designed to be polled by N8N and reported via Telegram.

All endpoints return a consistent envelope:
  {
    "audit": "<name>",
    "status": "ok" | "warning" | "critical",
    "count": <int>,
    "items": [...],        # trimmed list for Telegram preview
    "ran_at": "<iso>"
  }

GET /audit/summary                → all audits in one call (for Telegram daily digest)
GET /audit/sold-still-live        → Etsy listings for products sold on eBay
GET /audit/duplicates             → SKUs with multiple active Etsy listings
GET /audit/missing-skus           → Etsy listings with no SKU set
GET /audit/unlinked               → Etsy listings not linked to normalized DB
GET /audit/publish-queue          → products ready to post to Etsy (not yet listed)
GET /audit/low-res-images         → Shopify products with non-_32 image URLs
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from app.database.mongo import db

logger = logging.getLogger(__name__)
router = APIRouter()

ETSY_ACTIVE_STATES = ["active"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _status(count: int, warn: int = 1, crit: int = 5) -> str:
    if count == 0:
        return "ok"
    if count < crit:
        return "warning"
    return "critical"


# ── helpers ──────────────────────────────────────────────────────────────────

async def _audit_sold_still_live() -> dict:
    """Active Etsy listings for products where qty=0 in normalized DB."""
    listings = await db.etsy_listings_investigation.find(
        {"listing_state": {"$in": ETSY_ACTIVE_STATES}},
        {"listing_id": 1, "title": 1, "listing_state": 1, "raw.skus": 1},
    ).to_list(length=None)

    listing_ids = [d["listing_id"] for d in listings]
    sold_norms = await db.product_normalized.find(
        {"channels.etsy.listing_id": {"$in": listing_ids}, "quantity": 0},
        {"channels.etsy.listing_id": 1, "sku": 1, "title": 1},
    ).to_list(length=None)

    sold_lids = {d["channels"]["etsy"]["listing_id"] for d in sold_norms}
    sold_norm_map = {d["channels"]["etsy"]["listing_id"]: d for d in sold_norms}

    items = []
    for d in listings:
        lid = d["listing_id"]
        if lid in sold_lids:
            norm = sold_norm_map[lid]
            items.append({
                "listing_id": lid,
                "listing_state": d.get("listing_state"),
                "sku": norm.get("sku") or str(norm.get("_id")),
                "title": (d.get("title") or "")[:80],
            })

    return {
        "audit": "sold_still_live",
        "status": _status(len(items), warn=1, crit=3),
        "count": len(items),
        "items": items[:10],
        "ran_at": _now_iso(),
    }


async def _audit_duplicates() -> dict:
    """SKUs with more than one active Etsy listing."""
    listings = await db.etsy_listings_investigation.find(
        {"listing_state": {"$in": ETSY_ACTIVE_STATES}, "raw.skus": {"$exists": True, "$not": {"$size": 0}}},
        {"listing_id": 1, "listing_state": 1, "raw.skus": 1, "title": 1},
    ).to_list(length=None)

    sku_to_listings: dict[str, list] = {}
    for d in listings:
        skus = d.get("raw", {}).get("skus") or []
        for sku in skus:
            sku_to_listings.setdefault(sku, []).append(d)

    items = []
    for sku, dups in sku_to_listings.items():
        if len(dups) > 1:
            items.append({
                "sku": sku,
                "count": len(dups),
                "listing_ids": [d["listing_id"] for d in dups],
                "states": [d.get("listing_state") for d in dups],
                "title": (dups[0].get("title") or "")[:80],
            })

    return {
        "audit": "duplicates",
        "status": _status(len(items), warn=1, crit=3),
        "count": len(items),
        "items": items[:10],
        "ran_at": _now_iso(),
    }


async def _audit_missing_skus() -> dict:
    """Etsy listings that have no SKU set in their inventory."""
    listings = await db.etsy_listings_investigation.find(
        {
            "listing_state": {"$in": ETSY_ACTIVE_STATES},
            "$or": [
                {"raw.skus": {"$exists": False}},
                {"raw.skus": None},
                {"raw.skus": []},
            ],
        },
        {"listing_id": 1, "listing_state": 1, "title": 1},
    ).to_list(length=None)

    items = [
        {
            "listing_id": d["listing_id"],
            "listing_state": d.get("listing_state"),
            "title": (d.get("title") or "")[:80],
        }
        for d in listings
    ]

    return {
        "audit": "missing_skus",
        "status": _status(len(items), warn=1, crit=5),
        "count": len(items),
        "items": items[:10],
        "ran_at": _now_iso(),
    }


async def _audit_unlinked() -> dict:
    """Etsy listings not linked to any normalized DB record via channels.etsy.listing_id."""
    listings = await db.etsy_listings_investigation.find(
        {"listing_state": {"$in": ETSY_ACTIVE_STATES}},
        {"listing_id": 1, "listing_state": 1, "title": 1, "raw.skus": 1},
    ).to_list(length=None)

    all_ids = [d["listing_id"] for d in listings]
    linked = await db.product_normalized.find(
        {"channels.etsy.listing_id": {"$in": all_ids}},
        {"channels.etsy.listing_id": 1},
    ).to_list(length=None)
    linked_set = {d["channels"]["etsy"]["listing_id"] for d in linked}

    items = []
    for d in listings:
        if d["listing_id"] not in linked_set:
            skus = d.get("raw", {}).get("skus") or []
            items.append({
                "listing_id": d["listing_id"],
                "listing_state": d.get("listing_state"),
                "sku": skus[0] if skus else None,
                "title": (d.get("title") or "")[:80],
            })

    return {
        "audit": "unlinked",
        "status": _status(len(items), warn=1, crit=5),
        "count": len(items),
        "items": items[:10],
        "ran_at": _now_iso(),
    }


async def _audit_publish_queue() -> dict:
    """
    Products in normalized DB ready to post to Etsy but not yet listed.
    Criteria: quantity >= 1, no channels.etsy, has weight + all 3 dimensions.
    """
    pipeline = [
        {
            "$match": {
                "quantity": {"$gte": 1},
                "channels.etsy": {"$exists": False},
                "weight_kg": {"$exists": True, "$ne": None, "$gt": 0},
                "length_cm": {"$exists": True, "$ne": None, "$gt": 0},
                "width_cm":  {"$exists": True, "$ne": None, "$gt": 0},
                "height_cm": {"$exists": True, "$ne": None, "$gt": 0},
            }
        },
        {"$count": "total"},
    ]
    result = await db.product_normalized.aggregate(pipeline).to_list(length=1)
    count = result[0]["total"] if result else 0

    # Grab a few samples
    samples = await db.product_normalized.find(
        {
            "quantity": {"$gte": 1},
            "channels.etsy": {"$exists": False},
            "weight_kg": {"$exists": True, "$ne": None, "$gt": 0},
            "length_cm": {"$exists": True, "$ne": None, "$gt": 0},
            "width_cm":  {"$exists": True, "$ne": None, "$gt": 0},
            "height_cm": {"$exists": True, "$ne": None, "$gt": 0},
        },
        {"sku": 1, "title": 1, "price": 1},
    ).limit(5).to_list(length=5)

    items = [
        {"sku": str(d.get("sku") or d["_id"]), "title": (d.get("title") or "")[:80]}
        for d in samples
    ]

    # Publish queue: low count is good, high count is just informational
    status = "ok" if count == 0 else "warning"

    return {
        "audit": "publish_queue",
        "status": status,
        "count": count,
        "items": items,
        "ran_at": _now_iso(),
    }


async def _audit_low_res_images() -> dict:
    """
    Shopify products in normalized DB whose image URLs still contain a non-_32 suffix
    (e.g. _0.jpg, _1.jpg) indicating low-resolution eBay images.
    """
    import re

    LOW_RES_RE = re.compile(r"_\d+\.jpe?g", re.IGNORECASE)
    HIGH_RES_RE = re.compile(r"_32\.jpe?g", re.IGNORECASE)

    # Only check products that have a shopify_id and images array
    docs = await db.product_normalized.find(
        {"shopify_id": {"$exists": True}, "images": {"$exists": True, "$not": {"$size": 0}}},
        {"sku": 1, "title": 1, "images": 1, "shopify_id": 1},
    ).to_list(length=None)

    items = []
    for d in docs:
        images = d.get("images") or []
        for url in images:
            if isinstance(url, str) and LOW_RES_RE.search(url) and not HIGH_RES_RE.search(url):
                items.append({
                    "sku": str(d.get("sku") or d["_id"]),
                    "shopify_id": d.get("shopify_id"),
                    "title": (d.get("title") or "")[:80],
                    "sample_url": url[:120],
                })
                break  # one hit per product is enough

    return {
        "audit": "low_res_images",
        "status": _status(len(items), warn=1, crit=10),
        "count": len(items),
        "items": items[:10],
        "ran_at": _now_iso(),
    }


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/sold-still-live")
async def audit_sold_still_live():
    return await _audit_sold_still_live()


@router.get("/duplicates")
async def audit_duplicates():
    return await _audit_duplicates()


@router.get("/missing-skus")
async def audit_missing_skus():
    return await _audit_missing_skus()


@router.get("/unlinked")
async def audit_unlinked():
    return await _audit_unlinked()


@router.get("/publish-queue")
async def audit_publish_queue():
    return await _audit_publish_queue()


@router.get("/low-res-images")
async def audit_low_res_images():
    return await _audit_low_res_images()


@router.get("/summary")
async def audit_summary():
    """
    Run all audits in parallel and return a combined summary.
    This is the endpoint to call from N8N for the daily Telegram digest.
    """
    import asyncio

    results = await asyncio.gather(
        _audit_sold_still_live(),
        _audit_duplicates(),
        _audit_missing_skus(),
        _audit_unlinked(),
        _audit_publish_queue(),
        _audit_low_res_images(),
    )

    audits = {r["audit"]: r for r in results}

    # Overall store health
    statuses = [r["status"] for r in results]
    if "critical" in statuses:
        overall = "critical"
    elif "warning" in statuses:
        overall = "warning"
    else:
        overall = "ok"

    STATUS_EMOJI = {"ok": "🟢", "warning": "🟡", "critical": "🔴"}

    # Pre-built Telegram message block
    lines = [
        f"📊 *Daily Store Audit* — {datetime.now(timezone.utc).strftime('%b %d, %Y')}",
        "",
        f"{STATUS_EMOJI[audits['sold_still_live']['status']]} Sold still live on Etsy: *{audits['sold_still_live']['count']}*",
        f"{STATUS_EMOJI[audits['duplicates']['status']]} Duplicate Etsy listings: *{audits['duplicates']['count']}*",
        f"{STATUS_EMOJI[audits['missing_skus']['status']]} Missing SKUs: *{audits['missing_skus']['count']}*",
        f"{STATUS_EMOJI[audits['unlinked']['status']]} Unlinked listings: *{audits['unlinked']['count']}*",
        f"{STATUS_EMOJI[audits['publish_queue']['status']]} Etsy publish queue: *{audits['publish_queue']['count']}* ready",
        f"{STATUS_EMOJI[audits['low_res_images']['status']]} Low-res images: *{audits['low_res_images']['count']}*",
    ]

    return {
        "overall": overall,
        "telegram_message": "\n".join(lines),
        "audits": audits,
        "ran_at": _now_iso(),
    }
