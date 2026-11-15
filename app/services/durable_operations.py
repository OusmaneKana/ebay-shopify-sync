from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo import ReturnDocument

from app.config import settings
from app.database.mongo import db


COLLECTION = "durable_operations"
ACTIVE_STATUSES = {"queued", "running", "pause_requested", "paused"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
LEASE_MINUTES = 20


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def for_response(operation: dict[str, Any] | None) -> dict[str, Any] | None:
    if not operation:
        return None
    result = dict(operation)
    result["operation_id"] = str(result.pop("_id"))
    result.pop("lease_token", None)
    result.pop("callback_url", None)
    return result


async def ensure_indexes() -> None:
    collection = db[COLLECTION]
    await collection.create_index([("created_at", -1)])
    await collection.create_index([("status", 1), ("updated_at", -1)])


async def create_operation(
    kind: str,
    *,
    params: dict[str, Any] | None,
    callback_url: str,
) -> dict[str, Any]:
    await ensure_indexes()
    now = utcnow()
    operation_id = str(uuid.uuid4())
    callback_url = callback_url.format(operation_id=operation_id)
    operation = {
        "_id": operation_id,
        "kind": kind,
        "status": "queued",
        "phase": "queued",
        "params": params or {},
        "progress": {},
        "result": None,
        "error": None,
        "attempts": 0,
        "callback_url": callback_url,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "lease_until": None,
    }
    await db[COLLECTION].insert_one(operation)
    return operation


async def get_operation(operation_id: str) -> dict[str, Any] | None:
    return await db[COLLECTION].find_one({"_id": operation_id})


async def get_latest_operation(*, kinds: list[str] | None = None) -> dict[str, Any] | None:
    query: dict[str, Any] = {}
    if kinds:
        query["kind"] = {"$in": kinds}
    return await db[COLLECTION].find_one(query, sort=[("created_at", -1)])


async def claim_operation(operation_id: str) -> dict[str, Any] | None:
    now = utcnow()
    lease_token = str(uuid.uuid4())
    return await db[COLLECTION].find_one_and_update(
        {
            "_id": operation_id,
            "status": {"$in": ["queued", "running"]},
            "$or": [
                {"lease_until": None},
                {"lease_until": {"$exists": False}},
                {"lease_until": {"$lt": now}},
            ],
        },
        {
            "$set": {
                "status": "running",
                "lease_token": lease_token,
                "lease_until": now + timedelta(minutes=LEASE_MINUTES),
                "started_at": now,
                "updated_at": now,
            },
            "$inc": {"attempts": 1},
        },
        return_document=ReturnDocument.AFTER,
    )


async def release_operation(
    operation_id: str,
    lease_token: str,
    *,
    status: str,
    updates: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    now = utcnow()
    values = {"status": status, "updated_at": now, "lease_until": None, **(updates or {})}
    if status in TERMINAL_STATUSES:
        values["finished_at"] = now
    return await db[COLLECTION].find_one_and_update(
        {"_id": operation_id, "lease_token": lease_token},
        {"$set": values, "$unset": {"lease_token": ""}},
        return_document=ReturnDocument.AFTER,
    )


async def record_retryable_error(
    operation_id: str,
    lease_token: str,
    *,
    error: str,
    terminal: bool,
) -> None:
    await release_operation(
        operation_id,
        lease_token,
        status="failed" if terminal else "queued",
        updates={"error": error},
    )


async def request_pause(operation_id: str) -> dict[str, Any] | None:
    now = utcnow()
    operation = await db[COLLECTION].find_one({"_id": operation_id})
    if not operation:
        return None
    if operation.get("status") == "queued":
        status = "paused"
    elif operation.get("status") == "running":
        status = "pause_requested"
    else:
        return operation
    return await db[COLLECTION].find_one_and_update(
        {"_id": operation_id, "status": operation.get("status")},
        {"$set": {"status": status, "updated_at": now}},
        return_document=ReturnDocument.AFTER,
    )


async def resume_operation(operation_id: str) -> dict[str, Any] | None:
    return await db[COLLECTION].find_one_and_update(
        {"_id": operation_id, "status": {"$in": ["paused", "pause_requested", "failed"]}},
        {
            "$set": {
                "status": "queued",
                "error": None,
                "lease_until": None,
                "updated_at": utcnow(),
                "finished_at": None,
            },
            "$unset": {"lease_token": ""},
        },
        return_document=ReturnDocument.AFTER,
    )


async def dispatch_operation(operation: dict[str, Any]) -> str:
    """Enqueue one Cloud Tasks request for the operation's next bounded step."""
    project = settings.GOOGLE_CLOUD_PROJECT
    location = settings.CLOUD_TASKS_LOCATION or settings.GOOGLE_CLOUD_REGION
    queue = settings.CLOUD_TASKS_QUEUE
    callback_url = operation.get("callback_url")
    missing = [
        name
        for name, value in (
            ("GOOGLE_CLOUD_PROJECT", project),
            ("CLOUD_TASKS_LOCATION/GOOGLE_CLOUD_REGION", location),
            ("CLOUD_TASKS_QUEUE", queue),
            ("callback_url", callback_url),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Cloud Tasks is not configured: missing {', '.join(missing)}")

    try:
        from google.cloud import tasks_v2
    except ImportError as exc:  # pragma: no cover - deployment packaging guard
        raise RuntimeError("google-cloud-tasks is not installed") from exc

    client = tasks_v2.CloudTasksAsyncClient()
    parent = client.queue_path(str(project), str(location), str(queue))
    headers = {"Content-Type": "application/json"}
    if settings.CLOUD_TASKS_WORKER_SECRET:
        headers["X-Worker-Secret"] = settings.CLOUD_TASKS_WORKER_SECRET.strip()
    elif settings.ADMIN_PASSKEY:
        headers["X-Admin-Passkey"] = settings.ADMIN_PASSKEY

    http_request: dict[str, Any] = {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": str(callback_url),
        "headers": headers,
        "body": json.dumps({"operation_id": str(operation["_id"])}).encode(),
    }
    if settings.CLOUD_TASKS_SERVICE_ACCOUNT_EMAIL:
        http_request["oidc_token"] = {
            "service_account_email": settings.CLOUD_TASKS_SERVICE_ACCOUNT_EMAIL,
            "audience": str(callback_url).split("/reporting/", 1)[0],
        }
    task = await client.create_task(parent=parent, task={"http_request": http_request})
    return str(task.name)
