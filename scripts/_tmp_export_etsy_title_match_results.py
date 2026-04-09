import asyncio
import json
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher
from datetime import datetime, timezone

from app.database.mongo import db, close_mongo_client

STOP = {
    "the", "a", "an", "and", "or", "for", "with", "by", "of", "to", "in", "on", "at", "from",
    "vintage", "antique", "old", "rare", "beautiful", "nice", "collectible",
}


def normalize(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokens(text: str) -> list[str]:
    return [t for t in normalize(text).split() if t and t not in STOP]


def key_token(text: str) -> str | None:
    tks = tokens(text)
    if not tks:
        return None
    for t in tks:
        if len(t) >= 3:
            return t
    return tks[0]


def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


async def main() -> None:
    etsy_docs = await db.etsy_listings_investigation.find(
        {"title": {"$exists": True, "$ne": None}},
        {"_id": 1, "listing_id": 1, "title": 1, "listing_state": 1},
    ).to_list(length=None)

    normalized_docs = await db.product_normalized.find(
        {"title": {"$exists": True, "$ne": None}},
        {"_id": 1, "sku": 1, "title": 1},
    ).to_list(length=None)

    etsy_rows = [
        {
            "etsy_doc_id": d.get("_id"),
            "etsy_listing_id": d.get("listing_id"),
            "etsy_state": d.get("listing_state"),
            "etsy_title": d.get("title") or "",
        }
        for d in etsy_docs
    ]

    norm_rows = [
        {
            "normalized_id": d.get("_id"),
            "normalized_sku": d.get("sku") or d.get("_id"),
            "normalized_title": d.get("title") or "",
        }
        for d in normalized_docs
    ]

    norm_exact = defaultdict(list)
    norm_normalized = defaultdict(list)
    norm_bucket = defaultdict(list)

    for row in norm_rows:
        title = row["normalized_title"]
        title_ci = title.strip().lower()
        title_norm = normalize(title)

        norm_exact[title_ci].append(row)
        norm_normalized[title_norm].append(row)

        kt = key_token(title)
        if kt:
            norm_bucket[kt].append((row, title_norm))

    buckets = {
        "exact_ci": [],
        "normalized_exact": [],
        "high_confidence": [],
        "medium_confidence": [],
        "low_confidence": [],
        "unmatched": [],
    }

    for et in etsy_rows:
        et_title = et["etsy_title"]
        et_ci = et_title.strip().lower()
        et_norm = normalize(et_title)

        if et_ci in norm_exact:
            n = norm_exact[et_ci][0]
            buckets["exact_ci"].append(
                {
                    **et,
                    "normalized_sku": n["normalized_sku"],
                    "normalized_title": n["normalized_title"],
                    "score": 1.0,
                    "bucket": "exact_ci",
                }
            )
            continue

        if et_norm in norm_normalized:
            n = norm_normalized[et_norm][0]
            buckets["normalized_exact"].append(
                {
                    **et,
                    "normalized_sku": n["normalized_sku"],
                    "normalized_title": n["normalized_title"],
                    "score": 0.99,
                    "bucket": "normalized_exact",
                }
            )
            continue

        kt = key_token(et_title)
        candidates = norm_bucket.get(kt, []) if kt else []

        best = None
        best_score = 0.0
        for n_row, n_norm in candidates:
            score = sim(et_norm, n_norm)
            if score > best_score:
                best_score = score
                best = n_row

        if best and best_score >= 0.90:
            buckets["high_confidence"].append(
                {
                    **et,
                    "normalized_sku": best["normalized_sku"],
                    "normalized_title": best["normalized_title"],
                    "score": round(best_score, 4),
                    "bucket": "high_confidence",
                }
            )
        elif best and best_score >= 0.82:
            buckets["medium_confidence"].append(
                {
                    **et,
                    "normalized_sku": best["normalized_sku"],
                    "normalized_title": best["normalized_title"],
                    "score": round(best_score, 4),
                    "bucket": "medium_confidence",
                }
            )
        elif best and best_score >= 0.74:
            buckets["low_confidence"].append(
                {
                    **et,
                    "normalized_sku": best["normalized_sku"],
                    "normalized_title": best["normalized_title"],
                    "score": round(best_score, 4),
                    "bucket": "low_confidence",
                }
            )
        else:
            buckets["unmatched"].append(
                {
                    **et,
                    "normalized_sku": None,
                    "normalized_title": None,
                    "score": None,
                    "bucket": "unmatched",
                }
            )

    matched_total = (
        len(buckets["exact_ci"]) + len(buckets["normalized_exact"]) +
        len(buckets["high_confidence"]) + len(buckets["medium_confidence"]) +
        len(buckets["low_confidence"])
    )

    unique_skus = set()
    for key in ["exact_ci", "normalized_exact", "high_confidence", "medium_confidence", "low_confidence"]:
        for row in buckets[key]:
            if row.get("normalized_sku"):
                unique_skus.add(row["normalized_sku"])

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "etsy_total": len(etsy_rows),
            "normalized_total": len(norm_rows),
            "exact_ci": len(buckets["exact_ci"]),
            "normalized_exact": len(buckets["normalized_exact"]),
            "high_confidence_ge_0_90": len(buckets["high_confidence"]),
            "medium_confidence_0_82_to_0_90": len(buckets["medium_confidence"]),
            "low_confidence_0_74_to_0_82": len(buckets["low_confidence"]),
            "unmatched": len(buckets["unmatched"]),
            "matched_total": matched_total,
            "unique_normalized_skus_matched": len(unique_skus),
        },
        "low_confidence_matches": buckets["low_confidence"],
        "medium_confidence_matches": buckets["medium_confidence"],
        "high_confidence_matches": buckets["high_confidence"],
        "normalized_exact_matches": buckets["normalized_exact"],
        "exact_matches": buckets["exact_ci"],
        "unmatched": buckets["unmatched"],
    }

    output_path = os.path.join("logs", "etsy_title_match_analysis.json")
    os.makedirs("logs", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print({
        "output_path": output_path,
        "summary": report["summary"],
        "low_confidence_count": len(report["low_confidence_matches"]),
    })

    close_mongo_client()


if __name__ == "__main__":
    asyncio.run(main())
