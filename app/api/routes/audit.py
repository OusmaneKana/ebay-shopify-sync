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
GET /audit/zero-quantity-unverified → zero-quantity products without eBay sale evidence
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

def _positive_int(value) -> bool:
    try:
        return int(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _collect_ebay_sale_evidence(
    skus: set[str],
    raw_docs: list[dict],
    event_docs: list[dict],
    insight_snapshots: list[dict],
) -> tuple[dict[str, set[str]], dict[str, dict]]:
    """Return confirmed sale sources and the newest eBay snapshot per SKU.

    A zero quantity alone is deliberately not sale evidence: the active-list
    sync also sets ended or missing listings to zero. Evidence must come from
    eBay's QuantitySold field, a persisted eBay sale event, or order/sale data
    retained in an Insights snapshot.
    """
    evidence: dict[str, set[str]] = {sku: set() for sku in skus}
    latest_listing: dict[str, dict] = {}

    for document in raw_docs:
        raw = document.get("raw") or {}
        sku = str(raw.get("SKU") or document.get("sku") or document.get("_id") or "")
        if sku in evidence and _positive_int(raw.get("QuantitySold")):
            evidence[sku].add("ebay_quantity_sold")

    for event in event_docs:
        sku = str(event.get("sku") or "")
        if (
            sku in evidence
            and event.get("source_channel") == "ebay"
            and event.get("type") == "sale"
            and _positive_int(event.get("quantity_sold"))
        ):
            evidence[sku].add("ebay_sale_event")

    # Snapshots are supplied newest first. Keep the first listing snapshot for
    # display, while retaining sale evidence from any historical snapshot.
    for snapshot in insight_snapshots:
        for product in snapshot.get("products") or []:
            sku = str(product.get("sku") or "")
            if sku not in evidence:
                continue
            latest_listing.setdefault(sku, product)
            if (
                _positive_int(product.get("lifetime_sold"))
                or _positive_int(product.get("orders"))
                or _positive_int(product.get("units"))
                or bool(product.get("sold_at"))
            ):
                evidence[sku].add("ebay_orders")

    return evidence, latest_listing


async def _audit_etsy_zero_quantity() -> tuple[dict, dict]:
    """Split active Etsy/zero-quantity records by confirmed eBay sale evidence."""
    listings = await db.etsy_listings_investigation.find(
        {"listing_state": {"$in": ETSY_ACTIVE_STATES}},
        {"listing_id": 1, "title": 1, "listing_state": 1, "raw.skus": 1},
    ).to_list(length=None)

    listing_ids = [d["listing_id"] for d in listings]
    sold_norms = await db.product_normalized.find(
        {"channels.etsy.listing_id": {"$in": listing_ids}, "quantity": 0},
        {"channels.etsy.listing_id": 1, "sku": 1, "title": 1},
    ).to_list(length=None)

    zero_lids = {d["channels"]["etsy"]["listing_id"] for d in sold_norms}
    zero_norm_map = {d["channels"]["etsy"]["listing_id"]: d for d in sold_norms}
    skus = {str(d.get("sku") or d.get("_id")) for d in sold_norms}

    raw_docs = []
    event_docs = []
    insight_snapshots = []
    if skus:
        raw_docs = await db.product_raw.find(
            {"_id": {"$in": list(skus)}},
            {
                "sku": 1,
                "raw.SKU": 1,
                "raw.QuantityAvailable": 1,
                "raw.QuantitySold": 1,
            },
        ).to_list(length=None)
        event_docs = await db.inventory_events.find(
            {
                "sku": {"$in": list(skus)},
                "source_channel": "ebay",
                "type": "sale",
                "quantity_sold": {"$gt": 0},
            },
            {"sku": 1, "source_channel": 1, "type": 1, "quantity_sold": 1},
        ).to_list(length=None)
        insight_snapshots = await db.ebay_insight_snapshots.find(
            {"products.sku": {"$in": list(skus)}},
            {
                "captured_at": 1,
                "products.sku": 1,
                "products.state": 1,
                "products.quantity": 1,
                "products.lifetime_sold": 1,
                "products.orders": 1,
                "products.units": 1,
                "products.sold_at": 1,
            },
        ).sort("captured_at", -1).to_list(length=None)

    sale_evidence, latest_ebay = _collect_ebay_sale_evidence(
        skus, raw_docs, event_docs, insight_snapshots
    )
    raw_by_sku = {
        str((d.get("raw") or {}).get("SKU") or d.get("sku") or d.get("_id")): d
        for d in raw_docs
    }

    sold_items = []
    unverified_items = []
    for d in listings:
        lid = d["listing_id"]
        if lid in zero_lids:
            norm = zero_norm_map[lid]
            sku = str(norm.get("sku") or norm.get("_id"))
            item = {
                "listing_id": lid,
                "listing_state": d.get("listing_state"),
                "sku": sku,
                "title": (d.get("title") or "")[:80],
            }
            sources = sorted(sale_evidence.get(sku) or [])
            if sources:
                item["sale_evidence"] = sources
                sold_items.append(item)
            else:
                raw = (raw_by_sku.get(sku) or {}).get("raw") or {}
                ebay = latest_ebay.get(sku) or {}
                item.update({
                    "ebay_state": ebay.get("state") or "unknown",
                    "ebay_quantity": ebay.get("quantity"),
                    "ebay_quantity_sold": raw.get("QuantitySold"),
                })
                unverified_items.append(item)

    sold_audit = {
        "audit": "sold_still_live",
        "status": _status(len(sold_items), warn=1, crit=3),
        "count": len(sold_items),
        "items": sold_items[:10],
        "ran_at": _now_iso(),
    }
    unverified_audit = {
        "audit": "zero_quantity_unverified",
        "status": _status(len(unverified_items), warn=1, crit=3),
        "count": len(unverified_items),
        "items": unverified_items[:10],
        "ran_at": _now_iso(),
    }
    return sold_audit, unverified_audit


async def _audit_sold_still_live() -> dict:
    sold, _ = await _audit_etsy_zero_quantity()
    return sold


async def _audit_zero_quantity_unverified() -> dict:
    _, unverified = await _audit_etsy_zero_quantity()
    return unverified


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


@router.get("/zero-quantity-unverified")
async def audit_zero_quantity_unverified():
    return await _audit_zero_quantity_unverified()


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

    zero_quantity_audits, *results = await asyncio.gather(
        _audit_etsy_zero_quantity(),
        _audit_duplicates(),
        _audit_missing_skus(),
        _audit_unlinked(),
        _audit_publish_queue(),
        _audit_low_res_images(),
    )
    results = [*zero_quantity_audits, *results]

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
        f"{STATUS_EMOJI[audits['zero_quantity_unverified']['status']]} Zero quantity, sale unverified: *{audits['zero_quantity_unverified']['count']}*",
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
