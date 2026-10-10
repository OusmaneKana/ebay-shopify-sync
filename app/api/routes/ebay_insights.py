import asyncio

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from app.config import settings
from app.database.mongo import db
from app.security.passkey import require_authorized
from app.services.ebay_insights import acquire_refresh, refresh

router = APIRouter(dependencies=[Depends(require_authorized)])

SUPPORTED_MARKETPLACES = {settings.EBAY_MARKETPLACE_ID}


def _validate_marketplace(value: str) -> str:
    if value not in SUPPORTED_MARKETPLACES:
        raise HTTPException(422, "Select a supported eBay marketplace.")
    return value


@router.get("/dashboard")
async def dashboard(
    days: int = Query(30, ge=1, le=90),
    marketplace_id: str = Query(settings.EBAY_MARKETPLACE_ID, pattern=r"^EBAY_[A-Z]{2}$"),
):
    marketplace_id = _validate_marketplace(marketplace_id)
    status_id = f"{marketplace_id}:{days}"
    try:
        latest, status = await asyncio.gather(
            asyncio.wait_for(
                db.ebay_insight_snapshots.find_one(
                    {"marketplace_id": marketplace_id, "days": days},
                    sort=[("captured_at", -1)],
                ),
                8,
            ),
            asyncio.wait_for(
                db.ebay_insight_status.find_one(
                    {"_id": status_id}, {"_id": 0, "lease_until": 0}
                ),
                8,
            ),
        )
    except Exception:
        raise HTTPException(503, "eBay Insights could not be loaded. Check the database connection.")
    if not latest:
        return {
            "empty": True,
            "days": days,
            "marketplace_id": marketplace_id,
            "status": status or {"status": "empty"},
        }
    latest.pop("_id", None)
    latest.update(empty=False, status=status or {"status": "ready"})
    return latest


@router.post("/refresh", status_code=202)
async def refresh_dashboard(
    tasks: BackgroundTasks,
    days: int = Query(30, ge=1, le=90),
    marketplace_id: str = Query(settings.EBAY_MARKETPLACE_ID, pattern=r"^EBAY_[A-Z]{2}$"),
):
    marketplace_id = _validate_marketplace(marketplace_id)
    try:
        lease = await asyncio.wait_for(acquire_refresh(marketplace_id, days), 8)
    except Exception:
        raise HTTPException(503, "The eBay refresh could not start. Check the database connection.")
    if not lease:
        raise HTTPException(409, "This eBay period is already refreshing.")
    tasks.add_task(refresh, marketplace_id, days)
    return {"status": "refreshing", "marketplace_id": marketplace_id, "days": days}
