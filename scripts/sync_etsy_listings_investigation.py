import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.config import settings
from app.database.mongo import db, close_mongo_client
from app.services.etsy_auth_service import get_valid_token as get_valid_etsy_token

BASE_URL = "https://openapi.etsy.com/v3/application"
TARGET_COLLECTION = "etsy_listings_investigation"

# Etsy listing states commonly used by the listings endpoint.
LISTING_STATES = ["active", "inactive", "draft", "sold_out", "expired"]


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

    # Etsy listings endpoint may require keystring in "client_id:client_secret" format.
    if settings.ETSY_CLIENT_ID and settings.ETSY_CLIENT_SECRET:
        return f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}"

    # Etsy docs usually refer to this as the keystring.
    for candidate in [
        settings.ETSY_CLIENT_ID,
        os.getenv("ETSY_API_KEY"),
        os.getenv("ETSY_KEYSTRING"),
    ]:
        if candidate:
            return candidate

    raise RuntimeError(
        "No Etsy API key found. Provide --api-key or set ETSY_CLIENT_ID/ETSY_API_KEY in environment."
    )


async def fetch_state_listings(
    client: httpx.AsyncClient,
    *,
    shop_id: str,
    state: str,
    headers: dict,
    page_size: int = 100,
) -> list[dict]:
    all_rows: list[dict] = []
    offset = 0

    while True:
        params = {
            "limit": page_size,
            "offset": offset,
            "state": state,
        }

        response = await client.get(
            f"{BASE_URL}/shops/{shop_id}/listings",
            headers=headers,
            params=params,
        )

        if response.status_code == 404:
            raise RuntimeError(f"Shop {shop_id} was not found or is not accessible.")

        if response.status_code >= 400:
            raise RuntimeError(
                f"Etsy API error {response.status_code} for state={state}: {response.text}"
            )

        response.raise_for_status()
        payload = response.json()
        rows = payload.get("results") or []
        if not rows:
            break

        all_rows.extend(rows)
        if len(rows) < page_size:
            break

        offset += page_size

    return all_rows


async def sync_etsy_listings_for_investigation(
    *,
    shop_id: str,
    api_key: str,
    token: str,
) -> dict:
    headers = {
        "x-api-key": api_key,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    fetched_at = datetime.now(timezone.utc)
    seen_listing_ids: set[int] = set()
    upserted = 0
    by_state: dict[str, int] = {}

    async with httpx.AsyncClient(timeout=45.0) as client:
        for state in LISTING_STATES:
            rows = await fetch_state_listings(
                client,
                shop_id=shop_id,
                state=state,
                headers=headers,
            )
            by_state[state] = len(rows)

            for row in rows:
                listing_id = row.get("listing_id")
                if listing_id is None:
                    continue

                try:
                    listing_id_int = int(listing_id)
                except Exception:
                    continue

                seen_listing_ids.add(listing_id_int)
                doc_id = f"ETSY-{listing_id_int}"

                doc = {
                    "_id": doc_id,
                    "source": "etsy",
                    "shop_id": str(shop_id),
                    "listing_id": listing_id_int,
                    "listing_state": row.get("state") or state,
                    "title": row.get("title"),
                    "url": row.get("url"),
                    "price": row.get("price"),
                    "quantity": row.get("quantity"),
                    "tags": row.get("tags") or [],
                    "materials": row.get("materials") or [],
                    "taxonomy_id": row.get("taxonomy_id"),
                    "views": row.get("views"),
                    "num_favorers": row.get("num_favorers"),
                    "raw": row,
                    "fetched_at": fetched_at,
                    "investigation": {
                        "collection": TARGET_COLLECTION,
                        "state_query": state,
                    },
                }

                await db[TARGET_COLLECTION].replace_one({"_id": doc_id}, doc, upsert=True)
                upserted += 1

    result = await db[TARGET_COLLECTION].update_many(
        {
            "source": "etsy",
            "shop_id": str(shop_id),
            "listing_id": {"$nin": sorted(seen_listing_ids)},
        },
        {
            "$set": {
                "inactive_in_latest_fetch": True,
                "fetched_at": fetched_at,
            }
        },
    )

    await db[TARGET_COLLECTION].create_index([("shop_id", 1), ("listing_id", 1)], unique=True)
    await db[TARGET_COLLECTION].create_index("listing_state")
    await db[TARGET_COLLECTION].create_index("fetched_at")

    return {
        "collection": TARGET_COLLECTION,
        "shop_id": str(shop_id),
        "states_fetched": by_state,
        "seen_listing_ids": len(seen_listing_ids),
        "upsert_operations": upserted,
        "marked_inactive_in_latest_fetch": getattr(result, "modified_count", 0),
        "fetched_at": fetched_at.isoformat(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Etsy listings into Mongo collection etsy_listings_investigation",
    )
    parser.add_argument("--shop-id", required=True, help="Etsy numeric shop ID")
    parser.add_argument("--api-key", default=None, help="Etsy x-api-key (keystring)")
    parser.add_argument("--token", default=None, help="Etsy OAuth bearer token")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    api_key = resolve_api_key(args.api_key)
    token = await resolve_access_token(args.token)

    result = await sync_etsy_listings_for_investigation(
        shop_id=args.shop_id,
        api_key=api_key,
        token=token,
    )
    print(result)
    close_mongo_client()


if __name__ == "__main__":
    asyncio.run(main())