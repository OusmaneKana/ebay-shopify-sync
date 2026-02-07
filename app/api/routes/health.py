import time

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def health_check():
    start = time.perf_counter()
    status = {"status": "ok"}
    elapsed = time.perf_counter() - start
    return {**status, "elapsed_seconds": elapsed}
