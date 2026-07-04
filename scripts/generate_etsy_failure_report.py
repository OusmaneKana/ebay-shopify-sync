"""
Generate a failure report from stored Etsy draft-create attempts.

For each SKU that has at least one attempt, we take the *latest* attempt and
classify the outcome into actionable buckets so we can isolate the truly
postable subset.

Postability buckets
-------------------
ok               – already posted successfully
fixable_title    – title has all_caps / invalid_characters → normalise title
fixable_taxonomy – taxonomy_id rejected / not found → retry with better taxonomy
fixable_rate     – rate-limited → just retry
fixable_shipping – invalid shipping data → fix shipping profile
no_taxonomy      – AI couldn't find any taxonomy candidate → needs manual pick
blocked_marketplace – Etsy's "cannot sell this item" → not postable on Etsy
blocked_risk     – filtered by local risk scanner
blocked_other    – other hard errors

Usage
-----
  ./venv/bin/python -m scripts.generate_etsy_failure_report
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.database.mongo import close_mongo_client, db


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _flatten(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return " ".join(_flatten(v) for v in value)
    if isinstance(value, dict):
        return " ".join(_flatten(v) for v in value.values())
    return str(value)


def _classify(attempt: dict[str, Any]) -> str:
    """Return a postability bucket name for a single attempt document."""
    if attempt.get("ok"):
        return "ok"

    error_code = attempt.get("error_code") or ""
    response = attempt.get("response") or {}
    resp_text = _flatten(response).lower()

    # Risk filter hit (local, never sent to Etsy)
    if error_code == "risky_listing_filtered":
        return "blocked_risk"

    # AI had no taxonomy suggestion
    if error_code == "no_openai_taxonomy_suggestion":
        return "no_taxonomy"

    # Etsy: cannot sell on marketplace
    if "invalid_marketplace" in resp_text or "cannot sell this item" in resp_text:
        return "blocked_marketplace"

    # Etsy: taxonomy id not accepted
    if "invalid taxonomy_id" in resp_text or "invalid_taxonomy_id" in resp_text:
        return "fixable_taxonomy"

    # Etsy: title problems (all_caps, invalid_characters, too_many_invalid_characters)
    if "all_caps" in resp_text or "invalid_characters" in resp_text or "too_many_invalid_characters" in resp_text:
        # also check for a concurrent marketplace block
        if "invalid_marketplace" in resp_text or "cannot sell this item" in resp_text:
            return "blocked_marketplace"
        return "fixable_title"

    # Rate limiting
    if "rate limit" in resp_text or "per second" in resp_text or "429" in str(attempt.get("etsy_status_code", "")):
        return "fixable_rate"

    # Shipping data
    if "shipping" in resp_text or "shipping_profile" in resp_text:
        return "fixable_shipping"

    # Missing required fields
    if error_code == "missing_required_fields":
        return "blocked_other"

    # Generic Etsy 400 with no specific signal
    if attempt.get("etsy_status_code") == 400:
        return "blocked_other"

    return "blocked_other"


def _short_error(response: Any) -> str:
    """Return a compact human-readable error summary."""
    if not response:
        return ""
    if isinstance(response, dict):
        return response.get("error") or _flatten(response)[:200]
    if isinstance(response, list):
        parts = []
        for item in response:
            if isinstance(item, dict):
                msg = item.get("message") or item.get("type") or _flatten(item)
                parts.append(str(msg))
        return "; ".join(parts)[:300]
    return str(response)[:200]


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    print("Fetching latest attempt per SKU …")

    # Aggregate: for each sku, keep the attempt with the latest attempted_at.
    # If any attempt is ok=True we want to surface that, so we sort ok desc first.
    pipeline = [
        {"$sort": {"attempted_at": -1}},
        {
            "$group": {
                "_id": "$sku",
                "latest": {"$first": "$$ROOT"},
            }
        },
        {
            "$replaceRoot": {"newRoot": "$latest"}
        },
    ]

    rows: list[dict[str, Any]] = []
    async for doc in db.etsy_create_draft_attempts.aggregate(pipeline):
        sku = str(doc.get("sku") or doc.get("_id") or "")
        taxonomy_id = (doc.get("request_payload") or {}).get("taxonomy_id")
        attempted_at = doc.get("attempted_at")
        bucket = _classify(doc)
        response = doc.get("response")

        rows.append({
            "sku": sku,
            "postability": bucket,
            "error_code": doc.get("error_code") or "",
            "etsy_status_code": doc.get("etsy_status_code") or "",
            "taxonomy_id": taxonomy_id if taxonomy_id is not None else "",
            "short_error": _short_error(response),
            "response_json": json.dumps(response, ensure_ascii=False, default=str)[:500] if response else "",
            "attempted_at": attempted_at.isoformat() if isinstance(attempted_at, datetime) else str(attempted_at or ""),
        })

    if not rows:
        print("No draft attempt documents found in etsy_create_draft_attempts.")
        return

    # ── summary counts ────────────────────────────────────────────────────────
    from collections import Counter
    bucket_counts = Counter(r["postability"] for r in rows)
    total = len(rows)
    print(f"\n{'─'*55}")
    print(f"  TOTAL unique SKUs with attempt history: {total:>5}")
    print(f"{'─'*55}")
    bucket_order = [
        "ok",
        "fixable_title",
        "fixable_taxonomy",
        "fixable_rate",
        "fixable_shipping",
        "no_taxonomy",
        "blocked_marketplace",
        "blocked_risk",
        "blocked_other",
    ]
    for bucket in bucket_order:
        count = bucket_counts.get(bucket, 0)
        if count:
            print(f"  {bucket:<25} {count:>5}  ({100*count/total:.1f}%)")
    print(f"{'─'*55}")

    postable_buckets = {"fixable_title", "fixable_taxonomy", "fixable_rate", "fixable_shipping"}
    postable_count = sum(bucket_counts.get(b, 0) for b in postable_buckets)
    print(f"  Truly postable (fixable):   {postable_count:>5}")
    print(f"  Already posted (ok):        {bucket_counts.get('ok', 0):>5}")
    print(f"  Hard blocks:                {bucket_counts.get('blocked_marketplace', 0) + bucket_counts.get('blocked_risk', 0) + bucket_counts.get('blocked_other', 0):>5}")
    print(f"  No taxonomy / needs work:   {bucket_counts.get('no_taxonomy', 0):>5}")
    print(f"{'─'*55}\n")

    # ── write CSV ─────────────────────────────────────────────────────────────
    logs_dir = Path(ROOT) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now_utc().strftime("%Y%m%d_%H%M%S")
    csv_path = logs_dir / f"etsy_failure_report_{stamp}.csv"

    fieldnames = [
        "sku",
        "postability",
        "error_code",
        "etsy_status_code",
        "taxonomy_id",
        "short_error",
        "attempted_at",
        "response_json",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        # sort: ok first, then fixable, then blocked
        priority = {b: i for i, b in enumerate(bucket_order)}
        rows.sort(key=lambda r: (priority.get(r["postability"], 99), r["sku"]))
        writer.writerows(rows)

    print(f"Full report written to: {csv_path}")

    # ── write postable-only list (SKUs only) ──────────────────────────────────
    postable_skus_path = logs_dir / f"etsy_postable_skus_{stamp}.txt"
    with postable_skus_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            if row["postability"] in postable_buckets:
                fh.write(row["sku"] + "\n")
    print(f"Postable SKU list:    {postable_skus_path}")

    # ── fixable_title: list the distinct title errors ─────────────────────────
    title_rows = [r for r in rows if r["postability"] == "fixable_title"]
    if title_rows:
        print(f"\n  fixable_title samples (first 10):")
        for r in title_rows[:10]:
            print(f"    {r['sku']:25s}  {r['short_error'][:80]}")

    result = close_mongo_client()
    if asyncio.iscoroutine(result):
        await result


if __name__ == "__main__":
    asyncio.run(main())
