"""Create Etsy discount sections, snapshot product moves, and reassign listings.

This script is intentionally one-off and operational.
It will:
1) find active Etsy-linked normalized products with tags like discount_15
2) ensure Etsy sections exist for each discount value
3) write a full snapshot of intended moves to MongoDB
4) patch Etsy listings to the corresponding section
5) update channels.etsy.shop_section_id in product_normalized on success
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pymongo import MongoClient

from app.config import settings

DISCOUNT_TAG_RE = re.compile(r"^discount_(\d+)$", re.IGNORECASE)
ETSY_BASE = "https://api.etsy.com/v3/application"
SNAPSHOT_COLLECTION = "etsy_discount_section_snapshot"
REQUEST_TIMEOUT = 30.0
ETSY_TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token"


@dataclass
class Candidate:
    normalized_id: Any
    sku: str | None
    listing_id: int
    shop_id: int
    old_section_id: int | None
    old_section_title: str | None
    discount_value: int


class EtsyClient:
    def __init__(self, access_token: str):
        if not settings.ETSY_CLIENT_ID or not settings.ETSY_CLIENT_SECRET:
            raise RuntimeError("Missing ETSY_CLIENT_ID or ETSY_CLIENT_SECRET in environment")
        self._client = httpx.Client(timeout=REQUEST_TIMEOUT)
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "x-api-key": f"{settings.ETSY_CLIENT_ID}:{settings.ETSY_CLIENT_SECRET}",
        }

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        url = f"{ETSY_BASE}{path}"
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            response = self._client.request(method, url, headers=self._headers, **kwargs)
            if response.status_code not in (429, 500, 502, 503, 504):
                return response
            if attempt == max_attempts:
                return response
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_s = int(retry_after)
            else:
                sleep_s = min(2**attempt, 8)
            time.sleep(sleep_s)
        return response

    def get_sections(self, shop_id: int) -> list[dict[str, Any]]:
        response = self._request("GET", f"/shops/{shop_id}/sections")
        response.raise_for_status()
        body = response.json()
        return body.get("results") or []

    def create_section(self, shop_id: int, title: str) -> dict[str, Any]:
        response = self._request("POST", f"/shops/{shop_id}/sections", data={"title": title})
        response.raise_for_status()
        return response.json()

    def move_listing_to_section(self, shop_id: int, listing_id: int, section_id: int) -> dict[str, Any]:
        response = self._request(
            "PATCH",
            f"/shops/{shop_id}/listings/{listing_id}",
            data={"shop_section_id": section_id},
        )
        response.raise_for_status()
        return response.json()


def load_access_token(db) -> str:
    token_doc = db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"access_token": 1})
    if token_doc and token_doc.get("access_token"):
        return token_doc["access_token"]
    if settings.ETSY_TOKEN:
        return settings.ETSY_TOKEN
    raise RuntimeError("No Etsy access token found (DB etsy_oauth_tokens.primary or ETSY_TOKEN)")


def refresh_access_token_if_possible(db) -> str | None:
    if not settings.ETSY_CLIENT_ID or not settings.ETSY_CLIENT_SECRET:
        return None

    token_doc = db["etsy_oauth_tokens"].find_one({"_id": "primary"}, {"refresh_token": 1})
    refresh_token = (token_doc or {}).get("refresh_token")
    if not refresh_token:
        return None

    payload = {
        "grant_type": "refresh_token",
        "client_id": settings.ETSY_CLIENT_ID,
        "refresh_token": refresh_token,
    }

    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        response = client.post(
            ETSY_TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            auth=(settings.ETSY_CLIENT_ID, settings.ETSY_CLIENT_SECRET),
        )

    if response.status_code != 200:
        return None

    token_data = response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return None

    now = datetime.now(UTC)
    expires_in = token_data.get("expires_in", 3600)
    db["etsy_oauth_tokens"].replace_one(
        {"_id": "primary"},
        {
            "_id": "primary",
            "access_token": access_token,
            "refresh_token": token_data.get("refresh_token") or refresh_token,
            "token_type": token_data.get("token_type", "Bearer"),
            "scope": token_data.get("scope"),
            "expires_in": expires_in,
            "expires_at": now + timedelta(seconds=expires_in),
            "updated_at": now,
        },
        upsert=True,
    )
    return access_token


def parse_discount_value(tags: list[Any]) -> int | None:
    values: list[int] = []
    for tag in tags:
        if not isinstance(tag, str):
            continue
        match = DISCOUNT_TAG_RE.match(tag.strip())
        if match:
            values.append(int(match.group(1)))
    if not values:
        return None
    return min(values)


def main() -> None:
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]
    normalized = db.product_normalized
    snapshot_col = db[SNAPSHOT_COLLECTION]

    query = {
        "tags": {"$elemMatch": {"$regex": r"^discount_", "$options": "i"}},
        "channels.etsy.listing_id": {"$exists": True, "$ne": None},
        "channels.etsy.listing_state": "active",
    }
    projection = {
        "_id": 1,
        "sku": 1,
        "tags": 1,
        "channels.etsy.listing_id": 1,
        "channels.etsy.shop_id": 1,
        "channels.etsy.shop_section_id": 1,
    }

    raw_docs = list(normalized.find(query, projection))
    if not raw_docs:
        print("No active Etsy-linked discount-tagged products found. Nothing to do.")
        return

    candidates: list[Candidate] = []
    seen_listing_ids: set[int] = set()

    for doc in raw_docs:
        etsy = (doc.get("channels") or {}).get("etsy") or {}
        listing_id = etsy.get("listing_id")
        shop_id = etsy.get("shop_id")
        if listing_id is None or shop_id is None:
            continue

        try:
            listing_id_i = int(listing_id)
            shop_id_i = int(shop_id)
        except (TypeError, ValueError):
            continue

        if listing_id_i in seen_listing_ids:
            continue
        seen_listing_ids.add(listing_id_i)

        discount_value = parse_discount_value(doc.get("tags") or [])
        if discount_value is None:
            continue

        old_section_id = etsy.get("shop_section_id")
        try:
            old_section_id_i = int(old_section_id) if old_section_id is not None else None
        except (TypeError, ValueError):
            old_section_id_i = None

        candidates.append(
            Candidate(
                normalized_id=doc.get("_id"),
                sku=doc.get("sku"),
                listing_id=listing_id_i,
                shop_id=shop_id_i,
                old_section_id=old_section_id_i,
                old_section_title=None,
                discount_value=discount_value,
            )
        )

    if not candidates:
        print("No valid candidates after filtering. Nothing to do.")
        return

    shop_ids = {c.shop_id for c in candidates}
    if len(shop_ids) != 1:
        raise RuntimeError(f"Expected one Etsy shop_id, found: {sorted(shop_ids)}")
    shop_id = next(iter(shop_ids))

    token = load_access_token(db)
    etsy = EtsyClient(token)

    run_id = f"discount-sections-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC)

    try:
        try:
            existing_sections = etsy.get_sections(shop_id)
        except httpx.HTTPStatusError as exc:
            if exc.response is None or exc.response.status_code != 401:
                raise
            refreshed = refresh_access_token_if_possible(db)
            if not refreshed:
                raise
            etsy.close()
            etsy = EtsyClient(refreshed)
            existing_sections = etsy.get_sections(shop_id)
        section_by_title = {
            (s.get("title") or "").strip().lower(): s for s in existing_sections if s.get("title")
        }
        section_by_id = {
            int(s["shop_section_id"]): s
            for s in existing_sections
            if s.get("shop_section_id") is not None
        }

        discount_values = sorted({c.discount_value for c in candidates})
        discount_section_ids: dict[int, int] = {}

        for dv in discount_values:
            title = f"Discount {dv}%"
            key = title.lower()
            section = section_by_title.get(key)
            if not section:
                section = etsy.create_section(shop_id, title)
                section_by_title[key] = section
                if section.get("shop_section_id") is not None:
                    section_by_id[int(section["shop_section_id"])] = section
            sid = section.get("shop_section_id")
            if sid is None:
                raise RuntimeError(f"Section creation/fetch missing shop_section_id for title={title}")
            discount_section_ids[dv] = int(sid)

        # Attach old section titles now that section map is available.
        for c in candidates:
            if c.old_section_id is not None and c.old_section_id in section_by_id:
                c.old_section_title = section_by_id[c.old_section_id].get("title")

        snapshot_docs = []
        for c in candidates:
            new_section_id = discount_section_ids[c.discount_value]
            new_title = f"Discount {c.discount_value}%"
            snapshot_docs.append(
                {
                    "run_id": run_id,
                    "created_at": now,
                    "shop_id": c.shop_id,
                    "normalized_id": c.normalized_id,
                    "sku": c.sku,
                    "listing_id": c.listing_id,
                    "discount_value": c.discount_value,
                    "old_section_id": c.old_section_id,
                    "old_section_title": c.old_section_title,
                    "new_section_id": new_section_id,
                    "new_section_title": new_title,
                    "move_status": "pending",
                    "move_error": None,
                }
            )

        if snapshot_docs:
            snapshot_col.insert_many(snapshot_docs)

        moved = 0
        failed = 0

        for c in candidates:
            target_section_id = discount_section_ids[c.discount_value]
            try:
                etsy.move_listing_to_section(c.shop_id, c.listing_id, target_section_id)
                normalized.update_one(
                    {"_id": c.normalized_id},
                    {
                        "$set": {
                            "channels.etsy.shop_section_id": target_section_id,
                            "channels.etsy.discount_section_run_id": run_id,
                            "channels.etsy.discount_section_moved_at": datetime.now(UTC),
                        }
                    },
                )
                snapshot_col.update_one(
                    {"run_id": run_id, "listing_id": c.listing_id},
                    {
                        "$set": {
                            "move_status": "moved",
                            "moved_at": datetime.now(UTC),
                        }
                    },
                )
                moved += 1
            except Exception as exc:  # noqa: BLE001
                snapshot_col.update_one(
                    {"run_id": run_id, "listing_id": c.listing_id},
                    {
                        "$set": {
                            "move_status": "failed",
                            "move_error": str(exc),
                            "moved_at": datetime.now(UTC),
                        }
                    },
                )
                failed += 1

        print(f"run_id={run_id}")
        print(f"shop_id={shop_id}")
        print(f"candidates={len(candidates)}")
        print(f"moved={moved}")
        print(f"failed={failed}")
        for dv in sorted(discount_section_ids):
            print(f"section_discount_{dv}={discount_section_ids[dv]}")
        print(f"snapshot_collection={SNAPSHOT_COLLECTION}")

    finally:
        etsy.close()


if __name__ == "__main__":
    main()
