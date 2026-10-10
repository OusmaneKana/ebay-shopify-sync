from __future__ import annotations

import argparse
import asyncio
import json

from app.database.mongo import close_mongo_client
from app.services.etsy_price_adjustment import (
    create_price_adjustment_run,
    execute_price_adjustment_run,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply a resumable percentage increase to Etsy listing prices")
    parser.add_argument("--percent", type=float, help="Percentage increase for a new run")
    parser.add_argument("--state", default="active", choices=["active"], help="Etsy listing state to update")
    parser.add_argument("--resume", help="Existing etsy-price-* run ID to resume without compounding prices")
    parser.add_argument("--concurrency", type=int, default=4)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    if args.resume:
        run_id = args.resume
    else:
        if args.percent is None:
            raise SystemExit("--percent is required unless --resume is supplied")
        run = await create_price_adjustment_run(percentage=args.percent, state=args.state)
        run_id = str(run["_id"])
        print(json.dumps({"event": "run_created", "run_id": run_id, "counts": run["counts"]}), flush=True)

    def report(counts: dict[str, int]) -> None:
        print(json.dumps({"event": "progress", "run_id": run_id, "counts": counts}), flush=True)

    result = await execute_price_adjustment_run(
        run_id,
        concurrency=args.concurrency,
        progress=report,
    )
    print(json.dumps({"event": "finished", "run_id": run_id, "status": result.get("status"), "counts": result.get("counts")}, default=str), flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()
