import argparse
import asyncio
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.config import settings
from app.database.mongo import db, close_mongo_client
from app.services.etsy_auth_service import get_valid_token as get_valid_etsy_token

BASE_URL = "https://openapi.etsy.com/v3/application"
INVESTIGATION_COLLECTION = "etsy_listings_investigation"


async def resolve_access_token(explicit_token: str | None) -> str:
    if explicit_token:
        return explicit_token
    try:
        return await get_valid_etsy_token()
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def resolve_api_key(explicit_api_key: str | None) -> str:
    if explicit_api_key:
        return explicit_api_key

    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        return f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"

    if settings.ETSY_CLIENT_ID:
        return settings.ETSY_CLIENT_ID

    raise RuntimeError(
        "No Etsy API key found. Provide --api-key or set ETSY_CLIENT_ID/ETSY_CLIENT_SECRET."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh Etsy channel quantities/states in product_normalized from live Etsy API data.",
    )
    parser.add_argument("--api-key", default=None, help="Override Etsy x-api-key")
    parser.add_argument("--token", default=None, help="Override Etsy bearer token")
    parser.add_argument("--limit", type=int, default=None, help="Only refresh the first N linked listings")
    parser.add_argument("--batch-size", type=int, default=100, help="Etsy batch size")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent Etsy batch requests")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch live Etsy state without writing any Mongo updates",
    )
    return parser.parse_args()


def chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalized_available_quantity(live_row: dict[str, Any]) -> int:
    state = str(live_row.get("state") or "").lower()
    raw_quantity = int(live_row.get("quantity") or 0)
    if state != "active":
        return 0
    return raw_quantity


async def fetch_linked_etsy_channels(limit: int | None) -> tuple[dict[int, list[dict[str, Any]]], list[int]]:
    query = {"channels.etsy.listing_id": {"$exists": True, "$ne": None}}
    projection = {
        "_id": 1,
        "channels.etsy": 1,
    }

    linked_by_listing_id: dict[int, list[dict[str, Any]]] = {}
    ordered_listing_ids: list[int] = []

    cursor = db.product_normalized.find(query, projection).sort("_id", 1)
    async for doc in cursor:
        etsy = (doc.get("channels") or {}).get("etsy") or {}
        listing_id = etsy.get("listing_id")
        if listing_id is None:
            continue

        try:
            listing_id_int = int(listing_id)
        except (TypeError, ValueError):
            continue

        if listing_id_int not in linked_by_listing_id:
            linked_by_listing_id[listing_id_int] = []
            ordered_listing_ids.append(listing_id_int)

        linked_by_listing_id[listing_id_int].append(
            {
                "sku": doc.get("_id"),
                "shop_id": etsy.get("shop_id"),
                "existing": etsy,
            }
        )

        if limit and len(ordered_listing_ids) >= limit:
            break

    return linked_by_listing_id, ordered_listing_ids


async def fetch_batch(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    listing_ids: list[int],
    retries: int = 3,
) -> tuple[bool, list[dict[str, Any]]]:
    params = {"listing_ids": ",".join(str(listing_id) for listing_id in listing_ids)}
    for attempt in range(retries):
        try:
            response = await client.get(f"{BASE_URL}/listings/batch", headers=headers, params=params)
            if response.status_code == 429:
                await asyncio.sleep(10)
                continue
            if response.status_code >= 400:
                if attempt < retries - 1 and response.status_code >= 500:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False, []
            payload = response.json()
            return True, payload.get("results") or []
        except (httpx.ReadError, httpx.ConnectError, httpx.TimeoutException):
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            return False, []

    return False, []


def build_channel_update(existing: dict[str, Any], live_row: dict[str, Any], fetched_at: datetime) -> dict[str, Any]:
    updated = dict(existing)
    updated["listing_id"] = existing.get("listing_id")
    updated["listing_state"] = live_row.get("state") or updated.get("listing_state")
    updated["title"] = live_row.get("title") or updated.get("title")
    updated["url"] = live_row.get("url") or updated.get("url")
    updated["price"] = live_row.get("price")
    updated["live_quantity"] = int(live_row.get("quantity") or 0)
    updated["quantity"] = normalized_available_quantity(live_row)
    updated["refreshed_at"] = fetched_at
    return updated


def build_missing_channel_update(existing: dict[str, Any], fetched_at: datetime) -> dict[str, Any]:
    updated = dict(existing)
    updated["listing_id"] = existing.get("listing_id")
    updated["listing_state"] = "not_found"
    updated["quantity"] = 0
    updated["refreshed_at"] = fetched_at
    return updated


def build_investigation_doc(
    listing_id: int,
    shop_id: str | None,
    live_row: dict[str, Any],
    fetched_at: datetime,
) -> dict[str, Any]:
    live_quantity = int(live_row.get("quantity") or 0)
    return {
        "_id": f"ETSY-{listing_id}",
        "source": "etsy",
        "shop_id": str(shop_id) if shop_id is not None else None,
        "listing_id": listing_id,
        "listing_state": live_row.get("state"),
        "title": live_row.get("title"),
        "url": live_row.get("url"),
        "price": live_row.get("price"),
        "quantity": normalized_available_quantity(live_row),
        "live_quantity": live_quantity,
        "tags": live_row.get("tags") or [],
        "materials": live_row.get("materials") or [],
        "taxonomy_id": live_row.get("taxonomy_id"),
        "views": live_row.get("views"),
        "num_favorers": live_row.get("num_favorers"),
        "raw": live_row,
        "fetched_at": fetched_at,
        "investigation": {
            "collection": INVESTIGATION_COLLECTION,
            "refresh_source": "refresh_etsy_channel_inventory",
        },
    }


async def refresh_inventory(
    *,
    api_key: str,
    token: str,
    limit: int | None,
    batch_size: int,
    concurrency: int,
    dry_run: bool,
) -> dict[str, Any]:
    linked_by_listing_id, ordered_listing_ids = await fetch_linked_etsy_channels(limit)
    if not ordered_listing_ids:
        return {
            "ok": True,
            "linked_listing_ids": 0,
            "message": "No Etsy-linked channel records found.",
        }

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": "application/json",
    }
    fetched_at = utc_now()
    listing_ids_by_chunk = chunked(ordered_listing_ids, batch_size)
    live_by_listing_id: dict[int, dict[str, Any]] = {}
    successful_chunks: list[list[int]] = []
    failed_chunks: list[list[int]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def run_chunk(chunk: list[int]) -> None:
            async with semaphore:
                ok, rows = await fetch_batch(client, headers, chunk)
                if not ok:
                    failed_chunks.append(chunk)
                    return

                successful_chunks.append(chunk)
                for row in rows:
                    listing_id = row.get("listing_id")
                    if listing_id is None:
                        continue
                    try:
                        live_by_listing_id[int(listing_id)] = row
                    except (TypeError, ValueError):
                        continue

        await asyncio.gather(*(run_chunk(chunk) for chunk in listing_ids_by_chunk))

    state_counts: Counter[str] = Counter()
    updated_channel_docs = 0
    updated_investigation_docs = 0
    missing_listing_ids: list[int] = []

    successful_listing_ids = {listing_id for chunk in successful_chunks for listing_id in chunk}

    for listing_id in ordered_listing_ids:
        channel_rows = linked_by_listing_id[listing_id]
        live_row = live_by_listing_id.get(listing_id)
        if live_row is None:
            if listing_id not in successful_listing_ids:
                continue
            missing_listing_ids.append(listing_id)
            state_counts["not_found"] += len(channel_rows)
            if dry_run:
                continue

            for channel_row in channel_rows:
                updated_channel = build_missing_channel_update(channel_row["existing"], fetched_at)
                result = await db.product_normalized.update_many(
                    {"channels.etsy.listing_id": listing_id},
                    {"$set": {"channels.etsy": updated_channel}},
                )
                updated_channel_docs += result.modified_count
                break

            await db[INVESTIGATION_COLLECTION].update_one(
                {"listing_id": listing_id},
                {
                    "$set": {
                        "source": "etsy",
                        "shop_id": str(channel_rows[0].get("shop_id")) if channel_rows[0].get("shop_id") is not None else None,
                        "listing_id": listing_id,
                        "listing_state": "not_found",
                        "quantity": 0,
                        "fetched_at": fetched_at,
                        "inactive_in_latest_fetch": True,
                        "investigation.refresh_source": "refresh_etsy_channel_inventory",
                    }
                },
                upsert=True,
            )
            updated_investigation_docs += 1
            continue

        state = str(live_row.get("state") or "unknown")
        state_counts[state] += len(channel_rows)
        if dry_run:
            continue

        for channel_row in channel_rows:
            updated_channel = build_channel_update(channel_row["existing"], live_row, fetched_at)
            result = await db.product_normalized.update_one(
                {"_id": channel_row["sku"]},
                {"$set": {"channels.etsy": updated_channel}},
            )
            updated_channel_docs += result.modified_count

        investigation_doc = build_investigation_doc(
            listing_id,
            channel_rows[0].get("shop_id"),
            live_row,
            fetched_at,
        )
        await db[INVESTIGATION_COLLECTION].replace_one(
            {"_id": investigation_doc["_id"]},
            investigation_doc,
            upsert=True,
        )
        updated_investigation_docs += 1

    return {
        "ok": True,
        "dry_run": dry_run,
        "fetched_at": fetched_at.isoformat(),
        "linked_listing_ids": len(ordered_listing_ids),
        "successful_batches": len(successful_chunks),
        "failed_batches": len(failed_chunks),
        "failed_listing_ids": sum(len(chunk) for chunk in failed_chunks),
        "live_rows_returned": len(live_by_listing_id),
        "missing_listing_ids": len(missing_listing_ids),
        "state_counts": dict(state_counts),
        "updated_channel_docs": updated_channel_docs,
        "updated_investigation_docs": updated_investigation_docs,
    }


async def main() -> None:
    args = parse_args()
    token = await resolve_access_token(args.token)
    api_key = resolve_api_key(args.api_key)

    result = await refresh_inventory(
        api_key=api_key,
        token=token,
        limit=args.limit,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
    )
    print(result)
    close_mongo_client()


if __name__ == "__main__":
    asyncio.run(main())