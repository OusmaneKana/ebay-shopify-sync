from fastapi import APIRouter
from app.services.sync_manager import full_sync

router = APIRouter()

@router.post("/run")
async def run_sync():
    result = await full_sync()
    return {"message": "Sync completed", "result": result}
