import argparse
import ast
import asyncio
import csv
import json
import logging
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.database.mongo import close_mongo_client, db
from app.api.routes.reporting import (
    _build_etsy_publish_comparison,
    _suggest_etsy_taxonomy,
    _to_int,
    etsy_publish_create_draft,
)


logger = logging.getLogger("retry_etsy_bulk_errors")


TITLE_ALLOWED_CHARS_RE = re.compile(r"[^A-Za-z0-9 '&,./()\-:+]+")
MULTI_SPACE_RE = re.compile(r"\s+")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry Etsy bulk posting failures from CSV with title fixes and taxonomy fallback.",
    )
    parser.add_argument(
        "--input-csv",
        required=True,
        help="Path to etsy_bulk_post_errors_*.csv",
    )
    parser.add_argument("--limit", type=int, default=200, help="Maximum CSV rows to process")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent SKU retries")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def parse_error_detail(value: str | None) -> Any:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        return ast.literal_eval(text)
    except Exception:
        return text


def iter_error_dicts(detail: Any) -> list[dict[str, Any]]:
    if isinstance(detail, list):
        return [item for item in detail if isinstance(item, dict)]
    if isinstance(detail, dict):
        return [detail]
    return []


def has_marketplace_error(detail: Any) -> bool:
    for err in iter_error_dicts(detail):
        path = str(err.get("path") or "")
        err_type = str(err.get("type") or "")
        msg = str(err.get("message") or "")
        if path == "/marketplace" or err_type == "invalid_marketplace":
            return True
        if "cannot sell this item on etsy" in msg.lower():
            return True
    return False


def has_title_error(detail: Any) -> bool:
    title_types = {"all_caps", "invalid_characters", "too_many_invalid_characters"}
    for err in iter_error_dicts(detail):
        path = str(err.get("path") or "")
        err_type = str(err.get("type") or "")
        if path.startswith("/title") and err_type in title_types:
            return True
    return False


def is_invalid_taxonomy_error(error: Any, detail: Any) -> bool:
    if isinstance(error, str) and "invalid taxonomy_id" in error.lower():
        return True
    if isinstance(detail, dict):
        msg = str(detail.get("error") or "")
        if "invalid taxonomy_id" in msg.lower():
            return True
    return False


def sanitize_title(title: str | None) -> str | None:
    if not title:
        return title

    clean = TITLE_ALLOWED_CHARS_RE.sub(" ", str(title))
    clean = MULTI_SPACE_RE.sub(" ", clean).strip()

    # Etsy rejects repeated '&' usage in some titles.
    amp_count = clean.count("&")
    if amp_count > 1:
        first = clean.find("&")
        prefix = clean[: first + 1]
        suffix = clean[first + 1 :].replace("&", " and ")
        clean = MULTI_SPACE_RE.sub(" ", f"{prefix}{suffix}").strip()

    keep_upper = {"WWI", "WWII", "US", "USA", "UK", "EU", "U.S.", "U.K."}
    out_tokens: list[str] = []
    for tok in clean.split(" "):
        base = tok.strip(".,()")
        if base.isupper() and len(base) > 2 and base not in keep_upper:
            tok = tok.capitalize()
        out_tokens.append(tok)

    clean = " ".join(out_tokens)
    clean = MULTI_SPACE_RE.sub(" ", clean).strip()
    if len(clean) > 140:
        clean = clean[:140].rstrip()
    return clean or None


async def get_popular_taxonomy_ids(limit: int = 50) -> list[int]:
    pipeline = [
        {
            "$match": {
                "channels.etsy.listing_id": {"$exists": True, "$ne": None},
                "channels.etsy.taxonomy_id": {"$exists": True, "$ne": None},
            }
        },
        {
            "$group": {
                "_id": "$channels.etsy.taxonomy_id",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]

    out: list[int] = []
    async for row in db.product_normalized.aggregate(pipeline):
        taxonomy_id = _to_int(row.get("_id"))
        if taxonomy_id is not None:
            out.append(taxonomy_id)
    return out


async def build_taxonomy_candidates(doc: dict[str, Any], row: dict[str, Any], popular: list[int]) -> list[int]:
    candidates: list[int] = []

    row_taxonomy = _to_int(row.get("taxonomy_id"))
    if row_taxonomy is not None:
        candidates.append(row_taxonomy)

    detail = _build_etsy_publish_comparison(doc)
    suggestion = await _suggest_etsy_taxonomy(detail, limit=20)

    ai_choice = (suggestion or {}).get("ai_choice") or {}
    ai_tax = _to_int(ai_choice.get("taxonomy_id"))
    if ai_tax is not None:
        candidates.append(ai_tax)

    for item in (suggestion or {}).get("suggestions") or []:
        sid = _to_int((item or {}).get("taxonomy_id"))
        if sid is not None:
            candidates.append(sid)

    candidates.extend(popular)

    seen: set[int] = set()
    uniq: list[int] = []
    for tax in candidates:
        if tax not in seen:
            seen.add(tax)
            uniq.append(tax)
    return uniq


def should_skip_row(row: dict[str, Any], detail: Any) -> tuple[bool, str | None]:
    risk_reasons = str(row.get("risk_reasons") or "")
    if "ip_risk_brand_reference" in risk_reasons:
        return True, "skip_ip_risk_brand_reference"

    if has_marketplace_error(detail):
        return True, "skip_marketplace_blocked"

    return False, None


async def retry_one(
    row: dict[str, Any],
    popular_taxonomies: list[int],
) -> dict[str, Any]:
    sku = str(row.get("sku") or "").strip()
    if not sku:
        return {
            "sku": "",
            "status": "skipped",
            "note": "missing_sku",
            "attempts": 0,
            "listing_id": None,
            "taxonomy_id": None,
        }

    detail_obj = parse_error_detail(row.get("error_detail"))
    skip, skip_reason = should_skip_row(row, detail_obj)
    if skip:
        return {
            "sku": sku,
            "status": "skipped",
            "note": skip_reason,
            "attempts": 0,
            "listing_id": None,
            "taxonomy_id": None,
        }

    doc = await db.product_normalized.find_one(
        {"$or": [{"_id": sku}, {"sku": sku}]},
        {
            "_id": 1,
            "sku": 1,
            "title": 1,
            "description": 1,
            "price": 1,
            "quantity": 1,
            "tags": 1,
            "images": 1,
            "attributes": 1,
            "category": 1,
            "package": 1,
            "shipping": 1,
            "channels.etsy": 1,
            "last_normalized_at": 1,
        },
    )
    if not doc:
        return {
            "sku": sku,
            "status": "failed",
            "note": "sku_not_found",
            "attempts": 0,
            "listing_id": None,
            "taxonomy_id": None,
        }

    existing_listing = ((doc.get("channels") or {}).get("etsy") or {}).get("listing_id")
    if existing_listing:
        return {
            "sku": sku,
            "status": "skipped",
            "note": "already_linked",
            "attempts": 0,
            "listing_id": existing_listing,
            "taxonomy_id": _to_int(((doc.get("channels") or {}).get("etsy") or {}).get("taxonomy_id")),
        }

    title_override = None
    if has_title_error(detail_obj):
        title_override = sanitize_title(doc.get("title"))

    taxonomy_candidates = await build_taxonomy_candidates(doc, row, popular_taxonomies)
    if not taxonomy_candidates:
        return {
            "sku": sku,
            "status": "failed",
            "note": "no_taxonomy_candidates",
            "attempts": 0,
            "listing_id": None,
            "taxonomy_id": None,
        }

    attempts = 0
    last_note = ""
    for taxonomy_id in taxonomy_candidates:
        attempts += 1
        payload_override: dict[str, Any] = {"taxonomy_id": taxonomy_id}
        if title_override:
            payload_override["title"] = title_override

        try:
            result = await etsy_publish_create_draft(sku, body={"payload": payload_override})
        except httpx.TimeoutException:
            last_note = f"timeout_taxonomy_{taxonomy_id}"
            continue
        except Exception as exc:
            last_note = f"exception_{type(exc).__name__}"
            logger.warning("%s | retry exception on taxonomy %s: %s", sku, taxonomy_id, exc)
            continue

        if bool(result.get("ok")):
            return {
                "sku": sku,
                "status": "uploaded",
                "note": "success",
                "attempts": attempts,
                "listing_id": result.get("linked_listing_id"),
                "taxonomy_id": taxonomy_id,
            }

        status_code = result.get("etsy_status_code")
        etsy_response = result.get("etsy_response")
        error_code = result.get("error")

        if is_invalid_taxonomy_error(error_code, etsy_response):
            last_note = f"invalid_taxonomy_{taxonomy_id}"
            continue

        parsed = parse_error_detail(etsy_response if isinstance(etsy_response, str) else json.dumps(etsy_response) if etsy_response is not None else "")
        if has_marketplace_error(parsed):
            return {
                "sku": sku,
                "status": "skipped",
                "note": "skip_marketplace_blocked_on_retry",
                "attempts": attempts,
                "listing_id": None,
                "taxonomy_id": taxonomy_id,
            }

        if status_code == 400 and has_title_error(parsed) and title_override:
            last_note = f"title_still_invalid_taxonomy_{taxonomy_id}"
            continue

        last_note = f"non_retryable_error_{error_code or status_code}"
        break

    return {
        "sku": sku,
        "status": "failed",
        "note": last_note or "exhausted_taxonomy_candidates",
        "attempts": attempts,
        "listing_id": None,
        "taxonomy_id": None,
    }


def write_report(rows: list[dict[str, Any]]) -> tuple[str, str]:
    logs_dir = Path(ROOT) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    csv_path = logs_dir / f"etsy_bulk_retry_report_{stamp}.csv"
    json_path = logs_dir / f"etsy_bulk_retry_report_{stamp}.json"

    fieldnames = ["sku", "status", "note", "attempts", "listing_id", "taxonomy_id"]

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    summary = Counter(row.get("status") for row in rows)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": dict(summary),
        "items": rows,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return str(csv_path), str(json_path)


async def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    rows: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
            if len(rows) >= max(1, int(args.limit)):
                break

    popular_taxonomies = await get_popular_taxonomy_ids(limit=60)
    logger.info("Loaded %s rows | popular_taxonomies=%s", len(rows), len(popular_taxonomies))

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))

    async def run_row(row: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            try:
                result = await retry_one(row, popular_taxonomies)
            except Exception as exc:
                result = {
                    "sku": str(row.get("sku") or ""),
                    "status": "failed",
                    "note": f"worker_exception_{type(exc).__name__}",
                    "attempts": 0,
                    "listing_id": None,
                    "taxonomy_id": None,
                }
                logger.exception("Retry worker failed for %s", row.get("sku"))
            logger.info(
                "%s | %s | note=%s | attempts=%s",
                result.get("sku"),
                result.get("status"),
                result.get("note"),
                result.get("attempts"),
            )
            return result

    tasks = [asyncio.create_task(run_row(row)) for row in rows]
    results: list[dict[str, Any]] = []
    interrupted = False

    try:
        for task in asyncio.as_completed(tasks):
            try:
                results.append(await task)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.exception("Task collection failure: %s", exc)
    except KeyboardInterrupt:
        interrupted = True
        logger.warning("Interrupted; cancelling pending retry tasks and writing partial report.")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        for task in tasks:
            if task.done() and not task.cancelled():
                try:
                    result = task.result()
                except Exception:
                    continue
                if isinstance(result, dict) and result not in results:
                    results.append(result)

    csv_report, json_report = write_report(results)
    summary = Counter(row.get("status") for row in results)
    logger.info("Summary: %s", dict(summary))
    logger.info("Retry CSV report: %s", csv_report)
    logger.info("Retry JSON report: %s", json_report)
    if interrupted:
        logger.info("Report is partial due to interruption.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()
