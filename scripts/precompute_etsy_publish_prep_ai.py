import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.database.mongo import db, close_mongo_client
import app.api.routes.reporting as reporting_module
from app.api.routes.reporting import (
    ETSY_TAXONOMY_RESULT_LIMIT,
    _apply_etsy_detail_optimizations,
    _build_etsy_publish_comparison,
    _compute_etsy_prep_cache_version,
    _generate_etsy_seo_optimization,
    _generate_etsy_tag_suggestions,
    _has_weight_value_filter,
    _load_etsy_prep_cache,
    _missing_shipping_measurement_filter,
    _now_utc,
    _save_etsy_prep_cache_fields,
    _suggest_etsy_taxonomy,
)


logger = logging.getLogger("etsy_prep_precompute")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Precompute Etsy publish prep AI outputs (SEO, tags, taxonomy) into Mongo cache.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N matching items")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent SKU workers")
    parser.add_argument("--openai-concurrency", type=int, default=None, help="Override OpenAI parallelism for this run")
    parser.add_argument("--openai-delay", type=float, default=None, help="Override OpenAI pacing delay in seconds for this run")
    parser.add_argument("--etsy-concurrency", type=int, default=None, help="Override Etsy parallelism for this run")
    parser.add_argument("--etsy-delay", type=float, default=None, help="Override Etsy pacing delay in seconds for this run")
    parser.add_argument("--taxonomy-limit", type=int, default=ETSY_TAXONOMY_RESULT_LIMIT, help="Number of taxonomy suggestions to cache")
    parser.add_argument("--force-detail", action="store_true", help="Regenerate cached SEO and tags")
    parser.add_argument("--force-taxonomy", action="store_true", help="Regenerate cached taxonomy suggestions")
    parser.add_argument("--missing-shipping-only", action="store_true", help="Only process items missing required weight or dimensions")
    parser.add_argument("--has-shipping-only", action="store_true", help="Only process items that already have required weight and dimensions")
    parser.add_argument("--skus", nargs="*", default=None, help="Optional explicit SKU list")
    parser.add_argument("--log-every", type=int, default=10, help="Emit a progress summary every N completed items")
    parser.add_argument("--verbose", action="store_true", help="Enable debug-level logging")
    return parser.parse_args()


def configure_runtime(args: argparse.Namespace) -> None:
    if args.openai_concurrency is not None:
        reporting_module.OPENAI_MAX_CONCURRENT_REQUESTS = max(1, int(args.openai_concurrency))
        reporting_module._openai_semaphore = None
    if args.openai_delay is not None:
        reporting_module.OPENAI_REQUEST_DELAY_SECONDS = max(0.0, float(args.openai_delay))
    if args.etsy_concurrency is not None:
        reporting_module.ETSY_MAX_CONCURRENT_REQUESTS = max(1, int(args.etsy_concurrency))
        reporting_module._etsy_semaphore = None
    if args.etsy_delay is not None:
        reporting_module.ETSY_REQUEST_DELAY_SECONDS = max(0.0, float(args.etsy_delay))

    logger.info(
        "Runtime tuning | sku_concurrency=%s | openai_concurrency=%s | openai_delay=%.3fs | etsy_concurrency=%s | etsy_delay=%.3fs",
        max(1, int(args.concurrency)),
        reporting_module.OPENAI_MAX_CONCURRENT_REQUESTS,
        reporting_module.OPENAI_REQUEST_DELAY_SECONDS,
        reporting_module.ETSY_MAX_CONCURRENT_REQUESTS,
        reporting_module.ETSY_REQUEST_DELAY_SECONDS,
    )


def _base_query(args: argparse.Namespace) -> dict[str, Any]:
    query: dict[str, Any] = {
        "quantity": {"$gte": 1},
        "$or": [
            {"channels.etsy.listing_id": {"$exists": False}},
            {"channels.etsy.listing_id": None},
            {"channels.etsy.listing_id": ""},
        ],
    }
    and_conditions: list[dict[str, Any]] = []

    if args.skus:
        and_conditions.append({"$or": [{"_id": {"$in": args.skus}}, {"sku": {"$in": args.skus}}]})

    if args.missing_shipping_only:
        and_conditions.append(_missing_shipping_measurement_filter())

    if args.has_shipping_only:
        and_conditions.extend(
            [
                _has_weight_value_filter(),
                {"package.dimensions.length.value": {"$gt": 0}},
                {"package.dimensions.width.value": {"$gt": 0}},
                {"package.dimensions.height.value": {"$gt": 0}},
            ]
        )

    if and_conditions:
        query["$and"] = and_conditions
    return query


async def _process_doc(doc: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    started_at = time.perf_counter()
    sku = str(doc.get("_id") or doc.get("sku"))
    cache_version = _compute_etsy_prep_cache_version(doc)
    cache_doc = await _load_etsy_prep_cache(sku, cache_version)
    detail_cache = ((cache_doc or {}).get("detail") or {}) if cache_doc else {}
    taxonomy_by_limit = dict((cache_doc or {}).get("taxonomy_by_limit") or {}) if cache_doc else {}

    detail = _build_etsy_publish_comparison(doc)

    seo_optimization = detail_cache.get("seo_optimization") if not args.force_detail else None
    tag_suggestions = detail_cache.get("tag_suggestions") if not args.force_detail else None

    generated_detail = False
    detail_tasks: list[asyncio.Task] = []
    if not seo_optimization:
        detail_tasks.append(asyncio.create_task(_generate_etsy_seo_optimization(detail)))
    if not tag_suggestions:
        detail_tasks.append(asyncio.create_task(_generate_etsy_tag_suggestions(detail)))

    if detail_tasks:
        detail_results = await asyncio.gather(*detail_tasks)
        result_index = 0
        if not seo_optimization:
            seo_optimization = detail_results[result_index]
            result_index += 1
            generated_detail = True
        if not tag_suggestions:
            tag_suggestions = detail_results[result_index]
            generated_detail = True

    seo_optimization = seo_optimization or {}
    tag_suggestions = tag_suggestions or {}

    detail["etsy_optimized"]["title"] = seo_optimization.get("seo_title") or detail["etsy_optimized"].get("title")
    detail["etsy_optimized"]["when_made"] = seo_optimization.get("when_made") or detail["etsy_optimized"].get("when_made")

    detail = _apply_etsy_detail_optimizations(
        detail,
        seo_optimization=seo_optimization,
        tag_suggestions=tag_suggestions,
    )

    taxonomy_key = str(args.taxonomy_limit)
    taxonomy_cache = None if args.force_taxonomy else taxonomy_by_limit.get(taxonomy_key)
    generated_taxonomy = False
    if not taxonomy_cache:
        taxonomy_cache = await _suggest_etsy_taxonomy(detail, limit=args.taxonomy_limit)
        taxonomy_by_limit[taxonomy_key] = taxonomy_cache
        generated_taxonomy = True

    fields: dict[str, Any] = {}
    if generated_detail or args.force_detail:
        fields["detail"] = {
            "seo_optimization": seo_optimization,
            "tag_suggestions": tag_suggestions,
        }
    if generated_taxonomy or args.force_taxonomy:
        fields["taxonomy_by_limit"] = taxonomy_by_limit

    if fields:
        await _save_etsy_prep_cache_fields(sku, cache_version, fields)

    return {
        "sku": sku,
        "detail": "generated" if generated_detail else "cached",
        "taxonomy": "generated" if generated_taxonomy else "cached",
        "updated_at": _now_utc().isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
    }


async def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    configure_runtime(args)
    projection = {
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
    }
    query = _base_query(args)
    cursor = db.product_normalized.find(query, projection).sort("last_normalized_at", -1)
    if args.limit:
        cursor = cursor.limit(int(args.limit))

    docs = await cursor.to_list(length=args.limit)
    if not docs:
        logger.info("No matching Etsy publish prep items found.")
        return

    logger.info(
        "Starting Etsy prep precompute | matched=%s | taxonomy_limit=%s | force_detail=%s | force_taxonomy=%s",
        len(docs),
        args.taxonomy_limit,
        bool(args.force_detail),
        bool(args.force_taxonomy),
    )

    semaphore = asyncio.Semaphore(max(1, int(args.concurrency)))

    async def run_one(doc: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            logger.debug("Start SKU %s", str(doc.get("_id") or doc.get("sku")))
            return await _process_doc(doc, args)

    processed = 0
    detail_generated = 0
    detail_cached = 0
    taxonomy_generated = 0
    taxonomy_cached = 0
    errors = 0
    started_at = time.perf_counter()

    tasks = [asyncio.create_task(run_one(doc)) for doc in docs]

    for future in asyncio.as_completed(tasks):
        try:
            result = await future
        except Exception:
            errors += 1
            logger.exception("Worker failure")
            continue

        processed += 1
        if result["detail"] == "generated":
            detail_generated += 1
        else:
            detail_cached += 1
        if result["taxonomy"] == "generated":
            taxonomy_generated += 1
        else:
            taxonomy_cached += 1

        logger.info(
            "SKU %s finished | detail=%s | taxonomy=%s | elapsed=%.3fs | progress=%s/%s",
            result["sku"],
            result["detail"],
            result["taxonomy"],
            result["elapsed_seconds"],
            processed,
            len(docs),
        )

        if args.log_every > 0 and processed % args.log_every == 0:
            total_elapsed = time.perf_counter() - started_at
            rate = processed / total_elapsed if total_elapsed > 0 else 0.0
            logger.info(
                "Progress summary | processed=%s/%s | detail_generated=%s | taxonomy_generated=%s | errors=%s | rate=%.2f items/s | elapsed=%.1fs",
                processed,
                len(docs),
                detail_generated,
                taxonomy_generated,
                errors,
                rate,
                total_elapsed,
            )

    total_elapsed = time.perf_counter() - started_at
    rate = processed / total_elapsed if total_elapsed > 0 else 0.0
    logger.info("Summary")
    logger.info("Processed: %s", processed)
    logger.info("Detail generated: %s", detail_generated)
    logger.info("Detail cached: %s", detail_cached)
    logger.info("Taxonomy generated: %s", taxonomy_generated)
    logger.info("Taxonomy cached: %s", taxonomy_cached)
    logger.info("Errors: %s", errors)
    logger.info("Elapsed: %.1fs", total_elapsed)
    logger.info("Throughput: %.2f items/s", rate)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()