import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional

from app.database.mongo import db


COLLECTION_NAME = "shopify_inventory_zero_guard"


def _stable_json_dumps(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def make_zero_guard_key(
    *,
    env: str,
    sku: Optional[str],
    variant_id: Optional[int],
    inventory_item_id: Optional[int],
    location_id: Optional[int],
) -> str:
    if inventory_item_id and location_id:
        return f"{env}:item:{int(inventory_item_id)}:loc:{int(location_id)}"
    if variant_id:
        return f"{env}:variant:{int(variant_id)}"
    if sku:
        return f"{env}:sku:{sku}"
    return f"{env}:unknown"


def compute_zero_guard_hash(
    *,
    env: str,
    sku: Optional[str],
    variant_id: Optional[int],
    inventory_item_id: Optional[int],
    location_id: Optional[int],
    target_qty: int = 0,
) -> str:
    payload = {
        "env": env,
        "sku": sku,
        "variant_id": int(variant_id) if variant_id is not None else None,
        "inventory_item_id": int(inventory_item_id) if inventory_item_id is not None else None,
        "location_id": int(location_id) if location_id is not None else None,
        "target_qty": int(target_qty),
    }
    raw = _stable_json_dumps(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


async def was_already_zeroed(
    *,
    env: str,
    sku: Optional[str],
    variant_id: Optional[int],
    inventory_item_id: Optional[int],
    location_id: Optional[int],
) -> bool:
    key = make_zero_guard_key(
        env=env,
        sku=sku,
        variant_id=variant_id,
        inventory_item_id=inventory_item_id,
        location_id=location_id,
    )
    expected_hash = compute_zero_guard_hash(
        env=env,
        sku=sku,
        variant_id=variant_id,
        inventory_item_id=inventory_item_id,
        location_id=location_id,
        target_qty=0,
    )

    doc = await db[COLLECTION_NAME].find_one({"_id": key}, {"hash": 1, "target_qty": 1})
    if not doc:
        return False

    return doc.get("target_qty") == 0 and doc.get("hash") == expected_hash


async def mark_zeroed(
    *,
    env: str,
    sku: Optional[str],
    variant_id: Optional[int],
    inventory_item_id: Optional[int],
    location_id: Optional[int],
    source: str,
) -> None:
    key = make_zero_guard_key(
        env=env,
        sku=sku,
        variant_id=variant_id,
        inventory_item_id=inventory_item_id,
        location_id=location_id,
    )
    state_hash = compute_zero_guard_hash(
        env=env,
        sku=sku,
        variant_id=variant_id,
        inventory_item_id=inventory_item_id,
        location_id=location_id,
        target_qty=0,
    )

    now = datetime.now(timezone.utc)

    await db[COLLECTION_NAME].update_one(
        {"_id": key},
        {
            "$set": {
                "env": env,
                "sku": sku,
                "variant_id": int(variant_id) if variant_id is not None else None,
                "inventory_item_id": int(inventory_item_id) if inventory_item_id is not None else None,
                "location_id": int(location_id) if location_id is not None else None,
                "target_qty": 0,
                "hash": state_hash,
                "updated_at": now,
                "source": source,
                "cleared": False,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


async def clear_zeroed(
    *,
    env: str,
    sku: Optional[str],
    variant_id: Optional[int],
    inventory_item_id: Optional[int],
    location_id: Optional[int],
    source: str,
) -> None:
    key = make_zero_guard_key(
        env=env,
        sku=sku,
        variant_id=variant_id,
        inventory_item_id=inventory_item_id,
        location_id=location_id,
    )

    now = datetime.now(timezone.utc)

    await db[COLLECTION_NAME].update_one(
        {"_id": key},
        {
            "$set": {
                "env": env,
                "sku": sku,
                "variant_id": int(variant_id) if variant_id is not None else None,
                "inventory_item_id": int(inventory_item_id) if inventory_item_id is not None else None,
                "location_id": int(location_id) if location_id is not None else None,
                "target_qty": None,
                "hash": None,
                "updated_at": now,
                "source": source,
                "cleared": True,
                "cleared_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )
