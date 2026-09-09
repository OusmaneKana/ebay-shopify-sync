import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException
from pymongo import ReturnDocument

from app.database.mongo import db


logger = logging.getLogger(__name__)

ETSY_PUBLISH_QUEUE_COLLECTION = "etsy_publish_jobs"
MAX_ETSY_PUBLISH_ATTEMPTS = 5


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def enqueue_etsy_publish_job(
    sku: str,
    *,
    source: str = "manual",
    source_job_id: str | None = None,
) -> dict[str, Any]:
    """Durably enqueue one Etsy draft-creation job per SKU."""
    normalized_sku = str(sku or "").strip()
    if not normalized_sku:
        return {"ok": False, "reason": "missing_sku", "queued": False}

    product = await db.product_normalized.find_one(
        {"$or": [{"_id": normalized_sku}, {"sku": normalized_sku}]},
        {"_id": 1, "sku": 1, "channels.etsy.listing_id": 1},
    )
    if not product:
        return {
            "ok": False,
            "reason": "product_not_normalized",
            "sku": normalized_sku,
            "queued": False,
        }

    existing_listing_id = ((product.get("channels") or {}).get("etsy") or {}).get("listing_id")
    if existing_listing_id:
        return {
            "ok": True,
            "reason": "already_linked",
            "sku": normalized_sku,
            "listing_id": existing_listing_id,
            "queued": False,
        }

    now = _utc_now()
    job_id = f"etsy-publish:{normalized_sku}"
    result = await db[ETSY_PUBLISH_QUEUE_COLLECTION].update_one(
        {"_id": job_id},
        {
            "$setOnInsert": {
                "_id": job_id,
                "sku": normalized_sku,
                "status": "queued",
                "attempts": 0,
                "created_at": now,
            },
            "$set": {
                "source": source,
                "source_job_id": source_job_id,
                "updated_at": now,
            },
        },
        upsert=True,
    )

    job = await db[ETSY_PUBLISH_QUEUE_COLLECTION].find_one(
        {"_id": job_id},
        {"_id": 1, "status": 1, "attempts": 1},
    )
    return {
        "ok": True,
        "sku": normalized_sku,
        "job_id": job_id,
        "queued": bool(result.upserted_id),
        "status": (job or {}).get("status"),
        "attempts": int((job or {}).get("attempts") or 0),
    }


def _result_error(result: dict[str, Any]) -> str:
    error = result.get("error")
    if error:
        return str(error)
    response = result.get("etsy_response")
    if isinstance(response, dict):
        return str(response.get("error") or response)
    return str(response or "etsy_create_draft_failed")


async def process_single_etsy_publish_job(job_id: str) -> dict[str, Any]:
    job = await db[ETSY_PUBLISH_QUEUE_COLLECTION].find_one_and_update(
        {"_id": job_id, "status": {"$in": ["queued", "retry"]}},
        {
            "$set": {"status": "processing", "started_at": _utc_now(), "updated_at": _utc_now()},
            "$inc": {"attempts": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    if not job:
        return {"job_id": job_id, "status": "not_claimed"}

    sku = str(job.get("sku") or "").strip()
    attempts = int(job.get("attempts") or 1)

    try:
        # Imported lazily to avoid coupling application startup to the reporting router.
        from app.api.routes.reporting import etsy_publish_create_draft

        result = await etsy_publish_create_draft(sku, body=None)
        if result.get("ok") and result.get("linked_listing_id"):
            await db[ETSY_PUBLISH_QUEUE_COLLECTION].update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "listing_id": result.get("linked_listing_id"),
                        "result": result,
                        "error": None,
                        "finished_at": _utc_now(),
                        "updated_at": _utc_now(),
                    }
                },
            )
            return {
                "job_id": job_id,
                "sku": sku,
                "status": "completed",
                "listing_id": result.get("linked_listing_id"),
            }

        status_code = result.get("etsy_status_code")
        retryable = status_code == 429 or (isinstance(status_code, int) and status_code >= 500)
        next_status = "retry" if retryable and attempts < MAX_ETSY_PUBLISH_ATTEMPTS else "failed"
        error = _result_error(result)
    except HTTPException as exc:
        status_code = exc.status_code
        retryable = status_code == 429 or status_code >= 500
        next_status = "retry" if retryable and attempts < MAX_ETSY_PUBLISH_ATTEMPTS else "failed"
        error = str(exc.detail)
        result = None
    except Exception as exc:
        logger.exception("Etsy publish job failed | job_id=%s | sku=%s", job_id, sku)
        status_code = None
        next_status = "retry" if attempts < MAX_ETSY_PUBLISH_ATTEMPTS else "failed"
        error = str(exc)
        result = None

    await db[ETSY_PUBLISH_QUEUE_COLLECTION].update_one(
        {"_id": job_id},
        {
            "$set": {
                "status": next_status,
                "error": error,
                "etsy_status_code": status_code,
                "result": result,
                "finished_at": _utc_now(),
                "updated_at": _utc_now(),
            }
        },
    )
    return {"job_id": job_id, "sku": sku, "status": next_status, "error": error}


async def run_etsy_publish_worker(limit: int = 10) -> dict[str, Any]:
    max_items = max(1, min(int(limit), 100))
    cursor = db[ETSY_PUBLISH_QUEUE_COLLECTION].find(
        {"status": {"$in": ["queued", "retry"]}},
        {"_id": 1},
    ).sort([("created_at", 1), ("updated_at", 1)]).limit(max_items)
    job_ids = [str(doc["_id"]) async for doc in cursor]
    results = [await process_single_etsy_publish_job(job_id) for job_id in job_ids]
    return {
        "requested_limit": max_items,
        "picked": len(job_ids),
        "completed": sum(item.get("status") == "completed" for item in results),
        "retry": sum(item.get("status") == "retry" for item in results),
        "failed": sum(item.get("status") == "failed" for item in results),
        "results": results,
    }
