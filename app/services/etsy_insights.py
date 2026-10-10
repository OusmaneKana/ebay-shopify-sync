"""Read-only Etsy collection and deterministic product performance calculations."""
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
from bson import BSON
from pymongo import ReturnDocument

from app.config import settings
from app.database.mongo import db
from app.services.etsy_auth_service import get_valid_token
from app.services.insight_metrics import build_category_performance, category_group

BASE = "https://openapi.etsy.com/v3/application"
log = logging.getLogger(__name__)


def money(value):
    if not isinstance(value, dict) or not value.get("divisor"):
        return 0.0
    return float(Decimal(str(value.get("amount", 0))) / Decimal(str(value["divisor"])))


def stamp(value):
    return int(value or 0)


def window_start(days, now=None):
    now = now or datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)


def taxonomy_category_index(nodes):
    """Map every Etsy taxonomy node to its root category and full path."""
    result = {}

    def visit(rows, names=None):
        names = names or []
        for node in rows or []:
            name = str(node.get("name") or "").strip()
            taxonomy_id = node.get("id")
            path = [*names, name] if name else names
            if taxonomy_id is not None and path:
                result[int(taxonomy_id)] = {
                    "category": path[0],
                    "category_path": " > ".join(path),
                }
            visit(node.get("children") or [], path)

    visit(nodes)
    return result


def build_dashboard(current, baseline, days, now=None):
    now = now or datetime.now(timezone.utc)
    cutoff = int(window_start(days, now).timestamp())
    previous = {str(p["listing_id"]): p for p in (baseline or {}).get("products", [])}
    metrics = {}
    currencies = set()
    daily = {}
    paid_orders = 0
    for receipt in current.get("receipts", []):
        if not receipt.get("is_paid") or receipt.get("is_canceled") or receipt["timestamp"] < cutoff:
            continue
        paid_orders += 1
        date = datetime.fromtimestamp(receipt["timestamp"], timezone.utc).date().isoformat()
        day = daily.setdefault(date, {"date": date, "units": 0, "orders": 0, "revenue": {}})
        day["orders"] += 1
        for sale in receipt.get("transactions", []):
            key = str(sale["listing_id"])
            metric = metrics.setdefault(key, {"units": 0, "orders": set(), "revenue": {}, "sold_at": None})
            currency = sale["currency"]
            currencies.add(currency)
            revenue = round(sale["price"] * sale["quantity"], 2)
            metric["units"] += sale["quantity"]
            metric["orders"].add(receipt["receipt_id"])
            metric["sold_at"] = min(
                value for value in (metric.get("sold_at"), receipt["timestamp"]) if value
            )
            metric["revenue"][currency] = round(metric["revenue"].get(currency, 0) + revenue, 2)
            day["units"] += sale["quantity"]
            day["revenue"][currency] = round(day["revenue"].get(currency, 0) + revenue, 2)
    products = []
    for item in current.get("products", []):
        p = dict(item)
        listed_timestamp = stamp(p.get("listed_timestamp"))
        p["listed_at"] = (
            datetime.fromtimestamp(listed_timestamp, timezone.utc).isoformat()
            if listed_timestamp
            else None
        )
        p["days_listed"] = (
            max(0, int((now.timestamp() - listed_timestamp) // 86400))
            if listed_timestamp
            else None
        )
        old = previous.get(str(p["listing_id"]))
        m = metrics.pop(str(p["listing_id"]), {"units": 0, "orders": set(), "revenue": {}, "sold_at": None})
        p.update(units=m["units"], orders=len(m["orders"]), revenue=m["revenue"])
        sold_timestamp = stamp(m.get("sold_at"))
        p["sold_at"] = datetime.fromtimestamp(sold_timestamp, timezone.utc).isoformat() if sold_timestamp else None
        p["days_to_sale"] = (
            max(0, int((sold_timestamp - listed_timestamp) // 86400))
            if sold_timestamp and listed_timestamp
            else None
        )
        p["category_group"] = category_group(p.get("category"))
        current_views = p.get("views")
        previous_views = old.get("views") if old else None
        p["views_for_sale"] = None
        if p["units"] > 0:
            if isinstance(current_views, (int, float)) and current_views > 0:
                p["views_for_sale"] = current_views
            elif isinstance(previous_views, (int, float)) and previous_views > 0:
                # Inactive Etsy listings often return a zero view counter.  The
                # last positive snapshot is the best available view-at-sale value.
                p["views_for_sale"] = previous_views
        p["views_per_sale"] = (
            round(p["views_for_sale"] / p["units"], 1)
            if p["views_for_sale"] is not None and p["units"]
            else None
        )
        p["favorite_change"] = p["favorites"] - old["favorites"] if old else None
        # Zero can represent a failed daily tabulation. Inactive listings do not tabulate.
        valid = old and p["state"] == old["state"] == "active" and p.get("views") and old.get("views") and p["views"] >= old["views"]
        p["view_change"] = p["views"] - old["views"] if valid else None
        p["signal"] = "steady"
        if p["state"] == "sold_out" or (p["state"] == "active" and p["quantity"] <= 2 and p["units"] > 0):
            p["signal"] = "restock"
        elif p.get("review_count", 0) >= 3 and p.get("rating", 5) < 4:
            p["signal"] = "review"
        elif p["favorite_change"] is not None and p["favorite_change"] >= 3 and p["units"] == 0:
            p["signal"] = "interest"
        elif p["units"] >= 3:
            p["signal"] = "performer"
        products.append(p)
    # Preserve revenue for purchases whose listings were subsequently deleted.
    for listing_id, m in metrics.items():
        products.append(dict(listing_id=int(listing_id), title="Archived listing " + listing_id, state="archived", quantity=0,
                             favorites=0, views=None, price=0, currency="USD", image=None, tags=[], units=m["units"],
                             orders=len(m["orders"]), revenue=m["revenue"], favorite_change=None, view_change=None, signal="steady",
                             review_count=0, rating=None, category=None, category_group="Uncategorized", sold_at=None,
                             days_to_sale=None, views_for_sale=None, views_per_sale=None))
    totals = {}
    for p in products:
        for currency, amount in p["revenue"].items():
            totals[currency] = round(totals.get(currency, 0) + amount, 2)
    series = []
    for i in range(days):
        date = (now - timedelta(days=days - 1 - i)).date().isoformat()
        series.append(daily.get(date, {"date": date, "units": 0, "orders": 0, "revenue": {}}))
    return {"products": products, "series": series, "days": days, "captured_at": current.get("captured_at"),
            "baseline_at": (baseline or {}).get("captured_at"), "warnings": current.get("warnings", []),
            "category_performance": build_category_performance(products, days),
            "summary": {"revenue": totals, "orders": paid_orders, "units": sum(p["units"] for p in products),
                        "active": sum(p["state"] == "active" for p in products), "favorites": sum(p["favorites"] for p in products),
                        "favorite_change": sum(p["favorite_change"] or 0 for p in products) if baseline else None,
                        "restock": sum(p["signal"] == "restock" for p in products)}}


async def request_json(client, path, headers, params=None):
    max_attempts = 6
    for attempt in range(max_attempts):
        # Etsy currently permits five requests per second for this app. Stay below
        # the boundary so paginated refreshes do not produce avoidable 429s.
        await asyncio.sleep(0.25)
        response = await client.get(BASE + path, headers=headers, params=params)
        if response.status_code == 401 and attempt == 0:
            headers["Authorization"] = "Bearer " + await get_valid_token(force_refresh=True)
            continue
        if response.status_code == 429:
            if response.headers.get("x-remaining-today") == "0":
                raise RuntimeError(
                    "Etsy's daily API request limit has been reached. Your last saved Insights snapshot "
                    "is still available; retry after Etsy resets the daily quota."
                )
            if attempt < max_attempts - 1:
                try:
                    delay = min(60, max(1, float(response.headers.get("Retry-After", 2 ** (attempt + 1)))))
                except ValueError:
                    delay = 2 ** (attempt + 1)
                await asyncio.sleep(delay)
                continue
            raise RuntimeError(
                "Etsy is temporarily rate-limiting Insights. Your last saved snapshot is still "
                "available; wait a minute, then retry."
            )
        if response.status_code >= 500:
            if attempt < max_attempts - 1:
                await asyncio.sleep(min(30, 2 ** (attempt + 1)))
                continue
        if response.status_code >= 400:
            raise RuntimeError(
                f"Etsy returned HTTP {response.status_code}. Check shop access and OAuth permissions, then retry."
            )
        return response.json()
    raise RuntimeError("Etsy is temporarily unavailable. Retry later.")


async def pages(client, path, headers, params=None):
    rows = []
    for offset in range(0, 100000, 100):
        payload = await request_json(client, path, headers, dict(params or {}, limit=100, offset=offset))
        batch = payload.get("results", [])
        rows.extend(batch)
        if len(batch) < 100 or len(rows) >= payload.get("count", float("inf")):
            return rows
    raise RuntimeError("Etsy pagination limit reached; refresh was not saved because data is incomplete.")


async def collect(shop_id):
    key = settings.ETSY_CLIENT_ID or os.getenv("ETSY_API_KEY")
    if not key:
        raise RuntimeError("Configure your Etsy API key before refreshing.")
    if settings.ETSY_CLIENT_SECRET:
        key += ":" + settings.ETSY_CLIENT_SECRET
    headers = {"x-api-key": key, "Authorization": "Bearer " + await get_valid_token()}
    now = datetime.now(timezone.utc)
    warnings = []
    products = []
    try:
        taxonomy_snapshot = await db.etsy_seller_taxonomy_snapshots.find_one(
            {"active": True}, sort=[("fetched_at", -1)]
        )
        taxonomy_index = taxonomy_category_index((taxonomy_snapshot or {}).get("nodes", []))
    except Exception:
        taxonomy_index = {}
    async with httpx.AsyncClient(timeout=45) as client:
        for state in ("active", "inactive", "sold_out", "draft", "expired"):
            await db.etsy_insight_status.update_one({"_id": shop_id}, {"$set": {"progress": f"Reading {state} listings"}})
            state_rows = await pages(client, f"/shops/{shop_id}/listings", headers, {"state": state, "includes": "Images"})
            for position, listing in enumerate(state_rows):
                if position % 25 == 0:
                    await db.etsy_insight_status.update_one({"_id": shop_id}, {"$set": {"progress": f"Reading {state} listing {position + 1} of {len(state_rows)}"}})
                # The shop-listings response already includes the cumulative view
                # counter. Re-fetching every active listing separately consumed over
                # a thousand unnecessary requests for this shop.
                views = listing.get("views")
                images = listing.get("images") or []
                taxonomy_id = listing.get("taxonomy_id")
                taxonomy = taxonomy_index.get(int(taxonomy_id)) if taxonomy_id is not None else None
                products.append({"listing_id": listing["listing_id"], "title": listing["title"], "state": state,
                                 "quantity": listing.get("quantity", 0), "favorites": listing.get("num_favorers", 0), "views": views,
                                 "listed_timestamp": stamp(listing.get("original_creation_timestamp") or listing.get("creation_timestamp") or listing.get("created_timestamp")),
                                 "price": money(listing.get("price")), "currency": (listing.get("price") or {}).get("currency_code", "USD"),
                                 "image": (images[0].get("url_170x135") if images else None), "tags": listing.get("tags", []),
                                 "taxonomy_id": taxonomy_id, "category": (taxonomy or {}).get("category"),
                                 "category_path": (taxonomy or {}).get("category_path"), "rating": None, "review_count": 0})
        await db.etsy_insight_status.update_one({"_id": shop_id}, {"$set": {"progress": "Reading the last 90 days of orders"}})
        raw = await pages(client, f"/shops/{shop_id}/receipts", headers, {"min_created": int((now - timedelta(days=90)).timestamp())})
        receipts = []
        for r in raw:
            receipts.append({"receipt_id": r["receipt_id"], "timestamp": stamp(r.get("created_timestamp") or r.get("create_timestamp")),
                             "is_paid": r.get("is_paid", False), "is_canceled": r.get("status") == "canceled",
                             "transactions": [{"listing_id": t["listing_id"], "quantity": t.get("quantity", 0), "price": money(t.get("price")),
                                               "currency": (t.get("price") or {}).get("currency_code", "USD")} for t in r.get("transactions", []) if t.get("listing_id")]})
        if any(r.get("refunds") for r in raw):
            warnings.append("Some orders have refunds. Revenue is gross merchandise value before discounts, refunds, shipping, taxes, and fees.")
        await db.etsy_insight_status.update_one({"_id": shop_id}, {"$set": {"progress": "Reading customer reviews"}})
        try:
            reviews = await pages(client, f"/shops/{shop_id}/reviews", headers)
            by_listing = {}
            for review in reviews:
                by_listing.setdefault(review["listing_id"], []).append(review["rating"])
            for p in products:
                ratings = by_listing.get(p["listing_id"], [])
                p.update(review_count=len(ratings), rating=round(sum(ratings) / len(ratings), 2) if ratings else None)
        except (RuntimeError, httpx.HTTPError):
            warnings.append("Reviews could not be refreshed; ratings are unavailable.")
    # Compact snapshots never contain customer names, addresses, or access tokens.
    snapshot = {"_id": f"{shop_id}:{now.date().isoformat()}", "shop_id": shop_id, "captured_at": now.isoformat(),
                "products": list({p["listing_id"]: p for p in products}.values()), "receipts": receipts, "warnings": warnings}
    if len(BSON.encode(snapshot)) > 15_000_000:
        raise RuntimeError("Snapshot exceeds the storage limit. Refresh was not saved.")
    await db.etsy_insight_snapshots.replace_one({"_id": snapshot["_id"]}, snapshot, upsert=True)


async def refresh(shop_id):
    try:
        await asyncio.wait_for(collect(shop_id), timeout=3 * 60 * 60)
        await db.etsy_insight_status.update_one(
            {"_id": shop_id},
            {
                "$set": {
                    "status": "ready",
                    "error": None,
                    "progress": "Refresh complete",
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )
    except Exception as exc:
        log.warning("Etsy insight refresh failed (%s)", type(exc).__name__)
        message = str(exc) if isinstance(exc, RuntimeError) else "Refresh failed. Check Etsy authorization and database connectivity, then retry."
        await db.etsy_insight_status.update_one({"_id": shop_id}, {"$set": {"status": "error", "error": message, "progress": "Refresh failed"}})


async def acquire_refresh(shop_id):
    now = datetime.now(timezone.utc)
    await db.etsy_insight_status.update_one({"_id": shop_id}, {"$setOnInsert": {"status": "empty"}}, upsert=True)
    return await db.etsy_insight_status.find_one_and_update(
        {"_id": shop_id, "$or": [{"status": {"$ne": "refreshing"}}, {"lease_until": {"$lt": now}}]},
        {"$set": {"status": "refreshing", "progress": "Connecting to Etsy", "error": None, "lease_until": now + timedelta(hours=4)}},
        return_document=ReturnDocument.AFTER)
