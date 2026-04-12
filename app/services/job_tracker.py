from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Awaitable, Callable


# In-memory background job tracker for long-running sync operations.
# This prevents UI requests from hanging and allows polling for completion.
_JOBS: dict[str, dict[str, Any]] = {}
_LOCK = asyncio.Lock()
_MAX_JOBS = 200


def _prune_jobs_unlocked() -> None:
    if len(_JOBS) <= _MAX_JOBS:
        return
    # Drop oldest finished jobs first; keep running jobs.
    finished = [j for j in _JOBS.values() if j.get("status") != "running"]
    finished.sort(key=lambda j: float(j.get("finished_at") or j.get("started_at") or 0.0))
    to_remove = len(_JOBS) - _MAX_JOBS
    for job in finished[:to_remove]:
        _JOBS.pop(job["id"], None)


async def start_job(name: str, fn: Callable[[], Awaitable[Any]]) -> dict[str, Any]:
    """Start async job and return initial job metadata."""

    job_id = str(uuid.uuid4())
    now = time.time()
    job = {
        "id": job_id,
        "name": name,
        "status": "running",
        "started_at": now,
        "finished_at": None,
        "result": None,
        "error": None,
    }

    async with _LOCK:
        _JOBS[job_id] = job
        _prune_jobs_unlocked()

    async def _runner() -> None:
        try:
            result = await fn()
            status = "completed"
            error = None
        except Exception as exc:  # pragma: no cover - defensive catch for async task
            result = None
            status = "failed"
            error = str(exc)

        async with _LOCK:
            current = _JOBS.get(job_id)
            if current is None:
                return
            current["status"] = status
            current["result"] = result
            current["error"] = error
            current["finished_at"] = time.time()

    asyncio.create_task(_runner())
    return dict(job)


async def get_job(job_id: str) -> dict[str, Any] | None:
    async with _LOCK:
        job = _JOBS.get(job_id)
        return dict(job) if job else None
