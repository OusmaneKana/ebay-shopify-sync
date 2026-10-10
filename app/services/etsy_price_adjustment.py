from __future__ import annotations

import asyncio
import copy
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable

import httpx
from pymongo import ReturnDocument

from app.config import settings
from app.database.mongo import db
from app.services.etsy_auth_service import get_valid_token as get_valid_etsy_token


ETSY_BASE_URL = "https://openapi.etsy.com/v3/application"
PRICE_RUNS_COLLECTION = "etsy_price_adjustment_runs"
PRICE_RUN_ITEMS_COLLECTION = "etsy_price_adjustment_run_items"
MAX_PRICE_MARKUP_PERCENT = Decimal("500")
CENT = Decimal("0.01")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_price_markup_percent(value: Any) -> Decimal:
    try:
        percent = Decimal(str(value if value is not None else 0))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("price_markup_percent must be a number") from exc
    if not percent.is_finite() or percent < 0 or percent > MAX_PRICE_MARKUP_PERCENT:
        raise ValueError("price_markup_percent must be between 0 and 500")
    return percent.quantize(CENT, rounding=ROUND_HALF_UP)


def apply_price_markup(price: Any, percentage: Any) -> float:
    try:
        amount = Decimal(str(price))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid_price:{price!r}") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError(f"invalid_price:{price!r}")
    percent = normalize_price_markup_percent(percentage)
    adjusted = (amount * (Decimal("1") + percent / Decimal("100"))).quantize(
        CENT,
        rounding=ROUND_HALF_UP,
    )
    return float(adjusted)


def etsy_money_to_float(value: Any) -> float:
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, dict):
        amount = value.get("amount")
        divisor = value.get("divisor")
        if amount is None or divisor in (None, 0):
            raise ValueError(f"invalid_etsy_money:{value!r}")
        return float(Decimal(str(amount)) / Decimal(str(divisor)))
    raise ValueError(f"invalid_etsy_money:{value!r}")


def _clean_property_values(values: Any) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for value in values or []:
        if not isinstance(value, dict):
            continue
        row = {
            key: copy.deepcopy(value[key])
            for key in ("property_id", "property_name", "scale_id", "value_ids", "values")
            if key in value and value[key] is not None
        }
        if row:
            cleaned.append(row)
    return cleaned


def build_adjusted_inventory_payload(
    inventory: dict[str, Any],
    percentage: Any,
) -> tuple[dict[str, Any], list[float], list[float]]:
    products = inventory.get("products") or []
    if not products:
        raise ValueError("etsy_inventory_missing_products")

    source_prices: list[float] = []
    target_prices: list[float] = []
    payload_products: list[dict[str, Any]] = []
    for product in products:
        offerings = product.get("offerings") or []
        if not offerings:
            raise ValueError("etsy_inventory_product_missing_offerings")

        payload_offerings: list[dict[str, Any]] = []
        for offering in offerings:
            source_price = etsy_money_to_float(offering.get("price"))
            target_price = apply_price_markup(source_price, percentage)
            source_prices.append(round(source_price, 2))
            target_prices.append(target_price)
            clean_offering: dict[str, Any] = {
                "price": target_price,
                "quantity": max(0, int(offering.get("quantity") or 0)),
                "is_enabled": bool(offering.get("is_enabled")),
            }
            if offering.get("readiness_state_id") is not None:
                clean_offering["readiness_state_id"] = offering.get("readiness_state_id")
            payload_offerings.append(clean_offering)

        payload_product: dict[str, Any] = {
            "sku": product.get("sku"),
            "offerings": payload_offerings,
        }
        property_values = _clean_property_values(product.get("property_values"))
        if property_values:
            payload_product["property_values"] = property_values
        payload_products.append(payload_product)

    payload = {
        "products": payload_products,
        "price_on_property": copy.deepcopy(inventory.get("price_on_property") or []),
        "quantity_on_property": copy.deepcopy(inventory.get("quantity_on_property") or []),
        "sku_on_property": copy.deepcopy(inventory.get("sku_on_property") or []),
        "readiness_state_on_property": copy.deepcopy(
            inventory.get("readiness_state_on_property") or []
        ),
    }
    return payload, source_prices, target_prices


def _resolve_api_key() -> str:
    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        return f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"
    api_key = settings.ETSY_CLIENT_ID or os.getenv("ETSY_API_KEY") or os.getenv("ETSY_KEYSTRING")
    if not api_key:
        raise RuntimeError("Missing Etsy API key")
    return str(api_key)


async def _request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    content: str | None = None,
) -> httpx.Response:
    response: httpx.Response | None = None
    for attempt in range(5):
        response = await client.request(method, url, headers=headers, params=params, content=content)
        if response.status_code not in {429, 500, 502, 503, 504}:
            return response
        retry_after = response.headers.get("Retry-After")
        try:
            delay = max(float(retry_after or 0), min(8.0, 0.5 * (2**attempt)))
        except ValueError:
            delay = min(8.0, 0.5 * (2**attempt))
        await asyncio.sleep(delay)
    assert response is not None
    return response


async def create_price_adjustment_run(*, percentage: Any, state: str = "active") -> dict[str, Any]:
    percent = normalize_price_markup_percent(percentage)
    now = utc_now()
    run_id = f"etsy-price-{uuid.uuid4()}"
    query = {
        "listing_state": state,
        "inactive_in_latest_fetch": {"$ne": True},
        "listing_id": {"$exists": True, "$ne": None},
    }
    listings = await db.etsy_listings_investigation.find(
        query,
        {"listing_id": 1, "listing_state": 1, "title": 1, "price": 1, "shop_id": 1},
    ).sort("listing_id", 1).to_list(length=None)
    run_doc = {
        "_id": run_id,
        "status": "queued",
        "percentage": float(percent),
        "listing_state": state,
        "listing_count": len(listings),
        "created_at": now,
        "updated_at": now,
        "scope": copy.deepcopy(query),
        "counts": {"total": len(listings), "pending": len(listings), "completed": 0, "failed": 0},
    }
    await db[PRICE_RUNS_COLLECTION].insert_one(run_doc)
    if listings:
        await db[PRICE_RUN_ITEMS_COLLECTION].insert_many(
            [
                {
                    "_id": f"{run_id}:{int(listing['listing_id'])}",
                    "run_id": run_id,
                    "listing_id": int(listing["listing_id"]),
                    "listing_state": listing.get("listing_state"),
                    "title": listing.get("title"),
                    "snapshot_price": listing.get("price"),
                    "status": "pending",
                    "created_at": now,
                    "updated_at": now,
                }
                for listing in listings
            ],
            ordered=False,
        )
    await db[PRICE_RUN_ITEMS_COLLECTION].create_index([("run_id", 1), ("status", 1)])
    return run_doc


async def _refresh_run_counts(run_id: str) -> dict[str, int]:
    collection = db[PRICE_RUN_ITEMS_COLLECTION]
    counts = {
        "total": await collection.count_documents({"run_id": run_id}),
        "pending": await collection.count_documents(
            {"run_id": run_id, "status": {"$in": ["pending", "prepared", "processing"]}}
        ),
        "completed": await collection.count_documents({"run_id": run_id, "status": "completed"}),
        "failed": await collection.count_documents({"run_id": run_id, "status": "failed"}),
    }
    status = "completed"
    if counts["pending"]:
        status = "processing"
    elif counts["failed"]:
        status = "completed_with_errors"
    await db[PRICE_RUNS_COLLECTION].update_one(
        {"_id": run_id},
        {"$set": {"counts": counts, "status": status, "updated_at": utc_now()}},
    )
    return counts


def _money_snapshot(original: Any, price: float) -> Any:
    if not isinstance(original, dict):
        return price
    divisor = int(original.get("divisor") or 100)
    return {
        "amount": int((Decimal(str(price)) * Decimal(divisor)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
        "divisor": divisor,
        "currency_code": original.get("currency_code") or "USD",
    }


async def _update_local_price_snapshots(item: dict[str, Any], target_prices: list[float]) -> None:
    if not target_prices:
        return
    listing_id = int(item["listing_id"])
    listing_doc = await db.etsy_listings_investigation.find_one(
        {"listing_id": listing_id},
        {"price": 1},
    )
    min_price = min(target_prices)
    stored_price = _money_snapshot((listing_doc or {}).get("price"), min_price)
    now = utc_now()
    await db.etsy_listings_investigation.update_one(
        {"listing_id": listing_id},
        {
            "$set": {
                "price": stored_price,
                "raw.price": stored_price,
                "price_adjusted_at": now,
                "price_adjustment_run_id": item["run_id"],
            }
        },
    )
    await db.product_normalized.update_many(
        {
            "$or": [
                {"channels.etsy.listing_id": listing_id},
                {"channels.etsy.listing_id": str(listing_id)},
            ]
        },
        {
            "$set": {
                "channels.etsy.price": stored_price,
                "channels.etsy.price_adjusted_at": now,
                "channels.etsy.price_adjustment_run_id": item["run_id"],
            }
        },
    )


async def _process_item(
    *,
    client: httpx.AsyncClient,
    headers: dict[str, str],
    run: dict[str, Any],
    item: dict[str, Any],
) -> bool:
    listing_id = int(item["listing_id"])
    inventory_url = f"{ETSY_BASE_URL}/listings/{listing_id}/inventory"
    try:
        target_payload = item.get("target_payload")
        source_prices = item.get("source_prices") or []
        target_prices = item.get("target_prices") or []
        if not target_payload:
            response = await _request_with_retries(
                client,
                "GET",
                inventory_url,
                headers=headers,
                params={"show_deleted": "true"},
            )
            if response.status_code >= 400:
                raise RuntimeError(f"inventory_get_failed status={response.status_code} body={response.text[:2000]}")
            inventory = response.json() if response.content else {}
            target_payload, source_prices, target_prices = build_adjusted_inventory_payload(
                inventory,
                run["percentage"],
            )
            # Persist the exact target before the write. A resumed run therefore re-sends
            # the same prices instead of compounding the percentage a second time.
            await db[PRICE_RUN_ITEMS_COLLECTION].update_one(
                {"_id": item["_id"]},
                {
                    "$set": {
                        "status": "prepared",
                        "source_prices": source_prices,
                        "target_prices": target_prices,
                        "target_payload": target_payload,
                        "prepared_at": utc_now(),
                        "updated_at": utc_now(),
                    }
                },
            )

        response = await _request_with_retries(
            client,
            "PUT",
            inventory_url,
            headers=headers,
            content=json.dumps(target_payload),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"inventory_put_failed status={response.status_code} body={response.text[:4000]}")
        await _update_local_price_snapshots(item, [float(value) for value in target_prices])
        await db[PRICE_RUN_ITEMS_COLLECTION].update_one(
            {"_id": item["_id"]},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": utc_now(),
                    "updated_at": utc_now(),
                    "error": None,
                }
            },
        )
        return True
    except Exception as exc:
        await db[PRICE_RUN_ITEMS_COLLECTION].update_one(
            {"_id": item["_id"]},
            {
                "$set": {
                    "status": "failed",
                    "error": str(exc),
                    "failed_at": utc_now(),
                    "updated_at": utc_now(),
                },
                "$inc": {"failure_count": 1},
            },
        )
        return False


async def execute_price_adjustment_run(
    run_id: str,
    *,
    concurrency: int = 4,
    progress: Callable[[dict[str, int]], None] | None = None,
) -> dict[str, Any]:
    run = await db[PRICE_RUNS_COLLECTION].find_one({"_id": run_id})
    if not run:
        raise ValueError(f"price adjustment run not found: {run_id}")

    stale_before = utc_now() - timedelta(minutes=10)
    stale = await db[PRICE_RUN_ITEMS_COLLECTION].find(
        {"run_id": run_id, "status": "processing", "processing_started_at": {"$lt": stale_before}},
        {"_id": 1, "target_payload": 1},
    ).to_list(length=None)
    for item in stale:
        await db[PRICE_RUN_ITEMS_COLLECTION].update_one(
            {"_id": item["_id"]},
            {"$set": {"status": "prepared" if item.get("target_payload") else "pending", "updated_at": utc_now()}},
        )

    token = await get_valid_etsy_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": _resolve_api_key(),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    await db[PRICE_RUNS_COLLECTION].update_one(
        {"_id": run_id},
        {"$set": {"status": "processing", "started_at": run.get("started_at") or utc_now(), "updated_at": utc_now()}},
    )

    semaphore = asyncio.Semaphore(max(1, min(int(concurrency), 8)))
    completed_since_update = 0
    lock = asyncio.Lock()
    queued_items = await db[PRICE_RUN_ITEMS_COLLECTION].find(
        {"run_id": run_id, "status": {"$in": ["pending", "prepared", "failed"]}},
        {"_id": 1},
    ).sort("listing_id", 1).to_list(length=None)

    async with httpx.AsyncClient(timeout=60.0) as client:
        async def worker(item_id: str) -> None:
            nonlocal completed_since_update
            async with semaphore:
                item = await db[PRICE_RUN_ITEMS_COLLECTION].find_one_and_update(
                    {"_id": item_id, "run_id": run_id, "status": {"$in": ["pending", "prepared", "failed"]}},
                    {"$set": {"status": "processing", "processing_started_at": utc_now(), "updated_at": utc_now()}},
                    return_document=ReturnDocument.AFTER,
                )
                if not item:
                    return
                await _process_item(client=client, headers=headers, run=run, item=item)
                async with lock:
                    completed_since_update += 1
                    if progress and completed_since_update % 25 == 0:
                        progress(await _refresh_run_counts(run_id))
                await asyncio.sleep(0.15)

        # Each item present at invocation start is attempted exactly once. Failed items
        # remain resumable, but are never picked up again in the same pass.
        await asyncio.gather(*(worker(str(item["_id"])) for item in queued_items))

    counts = await _refresh_run_counts(run_id)
    return (await db[PRICE_RUNS_COLLECTION].find_one({"_id": run_id})) or {"_id": run_id, "counts": counts}
