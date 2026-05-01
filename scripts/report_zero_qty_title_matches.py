"""Generate an Excel report of zero-qty SKUs matched to active SKUs by exact title.

Usage:
  /Users/administrator/Desktop/Dev/ebay-shopify-sync/venv/bin/python -m scripts.report_zero_qty_title_matches
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openpyxl import Workbook

from app.database.mongo import close_mongo_client, db


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


async def collect_rows() -> list[dict[str, Any]]:
    pipeline = [
        {
            "$match": {
                "quantity": 0,
                "title": {"$type": "string", "$ne": ""},
            }
        },
        {
            "$lookup": {
                "from": "product_normalized",
                "let": {
                    "old_title": "$title",
                    "old_sku": "$_id",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$title", "$$old_title"]},
                                    {"$gt": ["$quantity", 0]},
                                    {"$ne": ["$_id", "$$old_sku"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 1,
                            "quantity": 1,
                            "updated_at": 1,
                            "channels.etsy.listing_id": 1,
                            "channels.etsy.listing_state": 1,
                        }
                    },
                ],
                "as": "matches",
            }
        },
        {"$match": {"matches.0": {"$exists": True}}},
        {
            "$project": {
                "_id": 1,
                "title": 1,
                "quantity": 1,
                "updated_at": 1,
                "channels.etsy.listing_id": 1,
                "channels.etsy.listing_state": 1,
                "matches": 1,
            }
        },
        {"$sort": {"title": 1, "_id": 1}},
    ]

    rows: list[dict[str, Any]] = []
    cursor = db.product_normalized.aggregate(pipeline, allowDiskUse=True)
    async for doc in cursor:
        old_sku = _safe_str(doc.get("_id"))
        title = _safe_str(doc.get("title"))
        old_qty = int(doc.get("quantity") or 0)
        old_updated = doc.get("updated_at")
        old_etsy = (doc.get("channels") or {}).get("etsy") or {}

        for match in doc.get("matches") or []:
            new_sku = _safe_str(match.get("_id"))
            new_qty = int(match.get("quantity") or 0)
            new_updated = match.get("updated_at")
            new_etsy = (match.get("channels") or {}).get("etsy") or {}

            rows.append(
                {
                    "old_sku": old_sku,
                    "old_qty": old_qty,
                    "title": title,
                    "new_sku": new_sku,
                    "new_qty": new_qty,
                    "old_etsy_listing_id": old_etsy.get("listing_id"),
                    "old_etsy_state": old_etsy.get("listing_state"),
                    "new_etsy_listing_id": new_etsy.get("listing_id"),
                    "new_etsy_state": new_etsy.get("listing_state"),
                    "old_updated_at": old_updated,
                    "new_updated_at": new_updated,
                }
            )

    return rows


def write_xlsx(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "zero_qty_title_matches"

    headers = [
        "old_sku",
        "old_qty",
        "title",
        "new_sku",
        "new_qty",
        "old_etsy_listing_id",
        "old_etsy_state",
        "new_etsy_listing_id",
        "new_etsy_state",
        "old_updated_at",
        "new_updated_at",
    ]
    ws.append(headers)

    for row in rows:
        ws.append([row.get(key) for key in headers])

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:K{max(1, len(rows) + 1)}"

    widths = {
        "A": 18,
        "B": 10,
        "C": 70,
        "D": 18,
        "E": 10,
        "F": 18,
        "G": 16,
        "H": 18,
        "I": 16,
        "J": 24,
        "K": 24,
    }
    for col, width in widths.items():
        ws.column_dimensions[col].width = width

    summary = wb.create_sheet("summary")
    zero_skus = {row.get("old_sku") for row in rows if row.get("old_sku")}
    new_skus = {row.get("new_sku") for row in rows if row.get("new_sku")}
    summary.append(["generated_at_utc", datetime.now(timezone.utc).isoformat()])
    summary.append(["matched_rows", len(rows)])
    summary.append(["unique_zero_qty_skus_with_match", len(zero_skus)])
    summary.append(["unique_candidate_new_skus", len(new_skus)])

    wb.save(output_path)


async def main() -> None:
    rows = await collect_rows()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = Path("logs") / f"zero_qty_exact_title_matches_{stamp}.xlsx"
    write_xlsx(rows, output_path)
    print(f"Wrote report: {output_path}")
    print(f"Matched rows: {len(rows)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()