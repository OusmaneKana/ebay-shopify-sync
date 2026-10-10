import argparse
import asyncio
import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.database.mongo import close_mongo_client, db
from app.services.etsy_auth_service import get_valid_token as get_valid_etsy_token
from app.api.routes.reporting import (
    ETSY_BASE_URL,
    ETSY_REQUEST_DELAY_SECONDS,
    ETSY_TAXONOMY_RESULT_LIMIT,
    _build_etsy_create_listing_payload,
    _build_etsy_publish_comparison,
    _clean_list,
    _compute_etsy_prep_cache_version,
    _get_etsy_semaphore,
    _get_missing_etsy_required_fields,
    _has_weight_value_filter,
    _load_etsy_prep_cache,
    _now_utc,
    _record_etsy_draft_attempt,
    _resolve_etsy_api_key,
    _resolve_etsy_readiness_state_id,
    _resolve_etsy_shop_id,
    _save_etsy_prep_cache_fields,
    _suggest_etsy_taxonomy,
    _to_etsy_form_data,
    _to_int,
    _update_etsy_listing_sku_for_review,
    _upload_etsy_listing_images,
)


logger = logging.getLogger("bulk_post_etsy_staged")


RISKY_PATTERNS: dict[str, re.Pattern[str]] = {
    "prohibited_weapon": re.compile(
        r"\b(ghost\s*gun|silencer|suppressor|switchblade|butterfly\s*knife|brass\s*knuckles|taser|stun\s*gun|ammo|ammunition|firearm|gun\s*part)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_drugs_or_paraphernalia": re.compile(
        r"\b(cocaine|heroin|meth|fentanyl|psilocybin|drug\s*paraphernalia|bong|dab\s*rig)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_tobacco_or_vape": re.compile(
        r"\b(vape\s*pen|e-?cig|nicotine\s*pod|hookah|tobacco\s*pipe)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_alcohol": re.compile(
        r"\b(whiskey|vodka|tequila|rum|wine\s*bottle\s*for\s*sale|alcohol\s*for\s*sale)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_hate_or_extremist": re.compile(
        r"\b(nazi|swastika|kkk|white\s*power|1488|extremist\s*symbol)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_human_remains": re.compile(
        r"\b(human\s*remains|human\s*bones?|teeth\s*from\s*human|real\s*human\s*skull)\b",
        flags=re.IGNORECASE,
    ),
    "prohibited_live_animals": re.compile(
        r"\b(live\s*animal|live\s*insect|live\s*fish|puppy\s*for\s*sale|kitten\s*for\s*sale)\b",
        flags=re.IGNORECASE,
    ),
}


IP_RISK_PATTERNS: dict[str, re.Pattern[str]] = {
    "ip_risk_counterfeit_language": re.compile(
        r"\b(replica|dupe|knockoff|counterfeit|fake\s*(designer|brand)|1:1\s*copy|inspired\s*by)\b",
        flags=re.IGNORECASE,
    ),
    "ip_risk_brand_reference": re.compile(
        r"\b(disney|marvel|star\s*wars|pokemon|harry\s*potter|hello\s*kitty|nike|adidas|gucci|chanel|louis\s*vuitton|rolex)\b",
        flags=re.IGNORECASE,
    ),
}


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-post staged Etsy drafts (shipping-ready items only) using highest-confidence OpenAI taxonomy.",
    )
    parser.add_argument("--limit", type=int, default=200, help="Maximum number of items to process")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent SKU workers")
    parser.add_argument("--taxonomy-limit", type=int, default=ETSY_TAXONOMY_RESULT_LIMIT, help="Taxonomy candidate limit")
    parser.add_argument("--force-taxonomy", action="store_true", help="Regenerate taxonomy suggestion instead of using cached value")
    parser.add_argument("--skus", nargs="*", default=None, help="Optional specific SKU list")
    parser.add_argument("--dry-run", action="store_true", help="Resolve taxonomy only; do not create Etsy drafts")
    parser.add_argument(
        "--risk-mode",
        choices=["skip", "warn", "off"],
        default="skip",
        help="How to handle risky listings: skip (default), warn, or off",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    return ""


def _build_risk_scan_text(doc: dict[str, Any]) -> str:
    channels = doc.get("channels") or {}
    etsy = channels.get("etsy") or {}
    parts = [
        _flatten_text(doc.get("title")),
        _flatten_text(doc.get("description")),
        _flatten_text(doc.get("category")),
        _flatten_text(doc.get("tags") or []),
        _flatten_text(doc.get("attributes") or {}),
        _flatten_text(etsy.get("materials") or []),
    ]
    text = " ".join(p for p in parts if p)
    return re.sub(r"\s+", " ", text).strip()


def _evaluate_listing_risk(doc: dict[str, Any]) -> list[dict[str, Any]]:
    text = _build_risk_scan_text(doc)
    if not text:
        return []

    hits: list[dict[str, Any]] = []
    for reason, pattern in RISKY_PATTERNS.items():
        matched = sorted(set(m.group(0) for m in pattern.finditer(text)))
        if matched:
            hits.append({"reason": reason, "terms": matched[:5], "severity": "block"})

    for reason, pattern in IP_RISK_PATTERNS.items():
        matched = sorted(set(m.group(0) for m in pattern.finditer(text)))
        if matched:
            hits.append({"reason": reason, "terms": matched[:5], "severity": "review"})

    return hits


def _shipping_ready_query(args: argparse.Namespace) -> dict[str, Any]:
    query: dict[str, Any] = {
        "quantity": {"$gte": 1},
        "$or": [
            {"channels.etsy.listing_id": {"$exists": False}},
            {"channels.etsy.listing_id": None},
            {"channels.etsy.listing_id": ""},
        ],
        "$and": [
            _has_weight_value_filter(),
            {"package.dimensions.length.value": {"$gt": 0}},
            {"package.dimensions.width.value": {"$gt": 0}},
            {"package.dimensions.height.value": {"$gt": 0}},
        ],
    }

    if args.skus:
        query["$and"].append({"$or": [{"_id": {"$in": args.skus}}, {"sku": {"$in": args.skus}}]})

    return query


def _extract_best_openai_taxonomy(taxonomy_data: dict[str, Any]) -> tuple[int | None, float, str]:
    ai_choice = taxonomy_data.get("ai_choice") or {}
    taxonomy_id = _to_int(ai_choice.get("taxonomy_id"))

    confidence = ai_choice.get("confidence")
    try:
        confidence_value = max(0.0, min(1.0, float(confidence)))
    except Exception:
        confidence_value = 0.0

    reason = str(ai_choice.get("reason") or "")

    # Keep a defensive fallback in case ai_choice is missing but suggestion was flagged.
    if taxonomy_id is None:
        for item in taxonomy_data.get("suggestions") or []:
            if item.get("ai_selected"):
                taxonomy_id = _to_int(item.get("taxonomy_id"))
                fallback_conf = item.get("ai_confidence")
                try:
                    confidence_value = max(0.0, min(1.0, float(fallback_conf)))
                except Exception:
                    confidence_value = 0.0
                reason = str(item.get("ai_reason") or reason)
                break

    return taxonomy_id, round(confidence_value, 3), reason


def _pick_highest_confidence_taxonomy(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_confidence = -1.0
    for item in candidates:
        ai_choice = (item or {}).get("ai_choice") or {}
        taxonomy_id = _to_int(ai_choice.get("taxonomy_id"))
        if taxonomy_id is None:
            continue
        try:
            confidence = max(0.0, min(1.0, float(ai_choice.get("confidence") or 0.0)))
        except Exception:
            confidence = 0.0
        if confidence > best_confidence:
            best = item
            best_confidence = confidence
    return best


def _ordered_taxonomy_candidates(taxonomy_data: dict[str, Any]) -> list[dict[str, Any]]:
    suggestions = [dict(item) for item in (taxonomy_data.get("suggestions") or []) if _to_int((item or {}).get("taxonomy_id")) is not None]
    if not suggestions:
        return []

    seen: set[int] = set()
    ordered: list[dict[str, Any]] = []

    ai_choice = taxonomy_data.get("ai_choice") or {}
    ai_taxonomy_id = _to_int(ai_choice.get("taxonomy_id"))

    if ai_taxonomy_id is not None:
        for item in suggestions:
            taxonomy_id = _to_int(item.get("taxonomy_id"))
            if taxonomy_id == ai_taxonomy_id and taxonomy_id not in seen:
                item["ai_selected"] = True
                item["ai_confidence"] = ai_choice.get("confidence")
                item["ai_reason"] = ai_choice.get("reason")
                ordered.append(item)
                seen.add(taxonomy_id)
                break

    for item in suggestions:
        taxonomy_id = _to_int(item.get("taxonomy_id"))
        if taxonomy_id is None or taxonomy_id in seen:
            continue
        ordered.append(item)
        seen.add(taxonomy_id)

    return ordered


def _is_retryable_taxonomy_create_failure(result: dict[str, Any]) -> bool:
    if result.get("ok"):
        return False

    status_code = _to_int(result.get("etsy_status_code"))
    if status_code not in {400, 429}:
        return False

    error_text = _flatten_text(result.get("error_detail")).lower()
    return any(
        phrase in error_text
        for phrase in (
            "invalid taxonomy_id",
            "taxonomy_id",
            "too many requests",
            "rate limit",
            "invalid_taxonomy_id",
        )
    ) or result.get("error") == "etsy_create_draft_failed"


async def _get_taxonomy_data_for_doc(
    doc: dict[str, Any],
    *,
    detail: dict[str, Any],
    taxonomy_limit: int,
    force_taxonomy: bool,
) -> dict[str, Any]:
    sku = str(doc.get("_id") or doc.get("sku"))
    cache_version = _compute_etsy_prep_cache_version(doc)
    cache_doc = await _load_etsy_prep_cache(sku, cache_version)
    taxonomy_by_limit = dict((cache_doc or {}).get("taxonomy_by_limit") or {})

    if not force_taxonomy and taxonomy_by_limit:
        best_cached = _pick_highest_confidence_taxonomy(list(taxonomy_by_limit.values()))
        if best_cached is not None:
            return best_cached

    cache_key = str(max(1, int(taxonomy_limit)))
    taxonomy_data = None if force_taxonomy else taxonomy_by_limit.get(cache_key)
    if taxonomy_data is None:
        taxonomy_data = await _suggest_etsy_taxonomy(detail, limit=max(1, int(taxonomy_limit)))
        taxonomy_by_limit[cache_key] = taxonomy_data
        await _save_etsy_prep_cache_fields(
            sku,
            cache_version,
            {"taxonomy_by_limit": taxonomy_by_limit},
        )
    return taxonomy_data


async def _create_etsy_draft_with_taxonomy(
    *,
    doc: dict[str, Any],
    token: str,
    api_key: str,
    shop_id: str,
    client: httpx.AsyncClient,
    taxonomy_id: int,
) -> dict[str, Any]:
    sku = str(doc.get("_id") or doc.get("sku"))
    detail = _build_etsy_publish_comparison(doc)

    request_payload = _build_etsy_create_listing_payload(
        detail,
        payload_override={"taxonomy_id": taxonomy_id},
    )

    request_payload["readiness_state_id"] = await _resolve_etsy_readiness_state_id(
        shop_id=str(shop_id),
        payload=request_payload,
    )

    missing_fields = _get_missing_etsy_required_fields(request_payload)
    if request_payload.get("type") == "physical" and _to_int(request_payload.get("readiness_state_id")) is None:
        missing_fields.append("readiness_state_id")

    if missing_fields:
        failure_body = {
            "ok": False,
            "error": "missing_required_fields",
            "sku": sku,
            "missing_fields": missing_fields,
            "request_payload": request_payload,
        }
        await _record_etsy_draft_attempt(
            sku=sku,
            shop_id=str(shop_id),
            request_payload=request_payload,
            request_form_data=None,
            response_status_code=400,
            response_payload=failure_body,
            ok=False,
            linked_listing_id=None,
            error_code="missing_required_fields",
        )
        return {
            "ok": False,
            "sku": sku,
            "error": "missing_required_fields",
            "missing_fields": missing_fields,
            "etsy_status_code": 400,
            "linked_listing_id": None,
        }

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": str(api_key),
        "Accept": "application/json",
    }

    form_data = _to_etsy_form_data(request_payload)
    url = f"{ETSY_BASE_URL}/shops/{shop_id}/listings"

    semaphore = _get_etsy_semaphore()
    async with semaphore:
        response = await client.post(url, headers=headers, data=form_data)
        if ETSY_REQUEST_DELAY_SECONDS > 0:
            await asyncio.sleep(ETSY_REQUEST_DELAY_SECONDS)

    raw_text = response.text
    try:
        etsy_response: Any = response.json() if raw_text else {}
    except Exception:
        etsy_response = raw_text

    listing_id = None
    if isinstance(etsy_response, dict):
        listing_id = _to_int(etsy_response.get("listing_id"))

    ok = response.status_code < 300

    await _record_etsy_draft_attempt(
        sku=sku,
        shop_id=str(shop_id),
        request_payload=request_payload,
        request_form_data=form_data,
        response_status_code=response.status_code,
        response_payload=etsy_response,
        ok=ok,
        linked_listing_id=listing_id,
        error_code=None if ok else "etsy_create_draft_failed",
    )

    if ok and listing_id:
        inventory_sync: dict[str, Any] = {
            "attempted": True,
            "ok": False,
            "error": None,
        }

        try:
            inventory_headers = {
                "Authorization": f"Bearer {token}",
                "x-api-key": str(api_key),
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            await _update_etsy_listing_sku_for_review(
                listing_id=listing_id,
                new_sku=sku,
                target_quantity=max(1, _to_int(request_payload.get("quantity")) or 1),
                headers=inventory_headers,
            )
            inventory_sync["ok"] = True
        except Exception as exc:
            inventory_sync["error"] = str(exc)

        image_urls = _clean_list((detail.get("etsy_optimized") or {}).get("images") or doc.get("images") or [], limit=10)
        image_upload = await _upload_etsy_listing_images(
            shop_id=shop_id,
            listing_id=listing_id,
            image_urls=image_urls,
            headers=headers,
        )

        await db.product_normalized.update_one(
            {"_id": sku},
            {
                "$set": {
                    "channels.etsy.listing_id": listing_id,
                    "channels.etsy.shop_id": _to_int(shop_id) or shop_id,
                    "channels.etsy.listing_state": "draft",
                    "channels.etsy.title": request_payload.get("title"),
                    "channels.etsy.price": request_payload.get("price"),
                    "channels.etsy.quantity": request_payload.get("quantity"),
                    "channels.etsy.type": request_payload.get("type"),
                    "channels.etsy.who_made": request_payload.get("who_made"),
                    "channels.etsy.when_made": request_payload.get("when_made"),
                    "channels.etsy.taxonomy_id": request_payload.get("taxonomy_id"),
                    "channels.etsy.shipping_profile_id": request_payload.get("shipping_profile_id"),
                    "channels.etsy.return_policy_id": request_payload.get("return_policy_id"),
                    "channels.etsy.readiness_state_id": request_payload.get("readiness_state_id"),
                    "channels.etsy.last_create_draft_request": request_payload,
                    "channels.etsy.last_create_draft_response": etsy_response,
                    "channels.etsy.last_create_draft_http_status": response.status_code,
                    "channels.etsy.last_create_draft_ok": True,
                    "channels.etsy.last_create_draft_at": _now_utc(),
                    "channels.etsy.last_create_draft_inventory_sync": inventory_sync,
                    "channels.etsy.last_create_draft_image_upload": image_upload,
                }
            },
        )

    return {
        "ok": ok,
        "sku": sku,
        "error": None if ok else (etsy_response.get("error") if isinstance(etsy_response, dict) else "etsy_create_draft_failed"),
        "error_detail": etsy_response,
        "etsy_status_code": response.status_code,
        "linked_listing_id": listing_id,
        "request_payload": request_payload,
    }


def _write_error_csv(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None

    logs_dir = Path(ROOT) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = logs_dir / f"etsy_bulk_post_errors_{stamp}.csv"

    fieldnames = [
        "sku",
        "error",
        "error_detail",
        "etsy_status_code",
        "taxonomy_id",
        "taxonomy_confidence",
        "taxonomy_reason",
        "linked_listing_id",
        "risk_reasons",
        "risk_terms",
    ]

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    return str(path)


async def main() -> None:
    args = _parse_args()
    _configure_logging(args.verbose)

    query = _shipping_ready_query(args)
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

    cursor = db.product_normalized.find(query, projection).sort("last_normalized_at", -1).limit(max(1, int(args.limit)))
    docs = await cursor.to_list(length=max(1, int(args.limit)))

    if not docs:
        logger.info("No staged shipping-ready items found.")
        return

    token = await get_valid_etsy_token()
    api_key = _resolve_etsy_api_key()
    if not api_key:
        raise RuntimeError("Missing Etsy API key")

    shop_id = await _resolve_etsy_shop_id()
    if not shop_id:
        raise RuntimeError("Unable to resolve Etsy shop_id")

    logger.info(
        "Starting bulk Etsy draft create | items=%s | concurrency=%s | dry_run=%s | risk_mode=%s",
        len(docs),
        max(1, int(args.concurrency)),
        bool(args.dry_run),
        str(args.risk_mode),
    )

    semaphore = asyncio.Semaphore(max(1, int(args.concurrency)))
    failures: list[dict[str, Any]] = []
    created = 0
    processed = 0

    async with httpx.AsyncClient(timeout=45.0) as client:
        async def process_doc(doc: dict[str, Any]) -> dict[str, Any]:
            sku = str(doc.get("_id") or doc.get("sku"))
            async with semaphore:
                try:
                    risk_hits = _evaluate_listing_risk(doc)
                    if risk_hits:
                        risk_reasons = ",".join(hit.get("reason", "") for hit in risk_hits)
                        risk_terms = "; ".join(
                            f"{hit.get('reason')}: {', '.join(hit.get('terms') or [])}" for hit in risk_hits
                        )
                        if args.risk_mode == "skip":
                            return {
                                "ok": False,
                                "sku": sku,
                                "error": "risky_listing_filtered",
                                "error_detail": risk_hits,
                                "etsy_status_code": None,
                                "linked_listing_id": None,
                                "taxonomy_id": None,
                                "taxonomy_confidence": 0.0,
                                "taxonomy_reason": "",
                                "risk_reasons": risk_reasons,
                                "risk_terms": risk_terms,
                            }
                        if args.risk_mode == "warn":
                            logger.warning("Risk warning for %s: %s", sku, risk_terms)

                    detail = _build_etsy_publish_comparison(doc)
                    taxonomy_data = await _get_taxonomy_data_for_doc(
                        doc,
                        detail=detail,
                        taxonomy_limit=max(1, int(args.taxonomy_limit)),
                        force_taxonomy=bool(args.force_taxonomy),
                    )
                    taxonomy_candidates = _ordered_taxonomy_candidates(taxonomy_data)
                    taxonomy_id, taxonomy_confidence, taxonomy_reason = _extract_best_openai_taxonomy(taxonomy_data)

                    if taxonomy_id is None:
                        return {
                            "ok": False,
                            "sku": sku,
                            "error": "no_openai_taxonomy_suggestion",
                            "error_detail": taxonomy_data,
                            "etsy_status_code": None,
                            "linked_listing_id": None,
                            "taxonomy_id": None,
                            "taxonomy_confidence": taxonomy_confidence,
                            "taxonomy_reason": taxonomy_reason,
                            "risk_reasons": "",
                            "risk_terms": "",
                        }

                    if args.dry_run:
                        return {
                            "ok": True,
                            "sku": sku,
                            "error": None,
                            "error_detail": None,
                            "etsy_status_code": None,
                            "linked_listing_id": None,
                            "taxonomy_id": taxonomy_id,
                            "taxonomy_confidence": taxonomy_confidence,
                            "taxonomy_reason": taxonomy_reason,
                            "risk_reasons": ",".join(hit.get("reason", "") for hit in risk_hits) if risk_hits else "",
                            "risk_terms": "; ".join(
                                f"{hit.get('reason')}: {', '.join(hit.get('terms') or [])}" for hit in risk_hits
                            ) if risk_hits else "",
                        }

                    last_result: dict[str, Any] | None = None
                    for candidate in taxonomy_candidates[:5]:
                        candidate_taxonomy_id = _to_int(candidate.get("taxonomy_id"))
                        if candidate_taxonomy_id is None:
                            continue

                        candidate_confidence = candidate.get("ai_confidence")
                        try:
                            candidate_confidence = float(candidate_confidence) if candidate_confidence is not None else taxonomy_confidence
                        except Exception:
                            candidate_confidence = taxonomy_confidence

                        create_result = await _create_etsy_draft_with_taxonomy(
                            doc=doc,
                            token=token,
                            api_key=str(api_key),
                            shop_id=str(shop_id),
                            client=client,
                            taxonomy_id=candidate_taxonomy_id,
                        )
                        create_result["taxonomy_id"] = candidate_taxonomy_id
                        create_result["taxonomy_confidence"] = round(float(candidate_confidence), 3)
                        create_result["taxonomy_reason"] = str(candidate.get("ai_reason") or taxonomy_reason)
                        create_result["risk_reasons"] = ",".join(hit.get("reason", "") for hit in risk_hits) if risk_hits else ""
                        create_result["risk_terms"] = "; ".join(
                            f"{hit.get('reason')}: {', '.join(hit.get('terms') or [])}" for hit in risk_hits
                        ) if risk_hits else ""
                        if create_result.get("ok"):
                            return create_result

                        last_result = create_result
                        if not _is_retryable_taxonomy_create_failure(create_result):
                            return create_result
                        await asyncio.sleep(0.2)

                    return last_result or {
                        "ok": False,
                        "sku": sku,
                        "error": "no_retryable_taxonomy_candidate",
                        "error_detail": taxonomy_data,
                        "etsy_status_code": None,
                        "linked_listing_id": None,
                        "taxonomy_id": taxonomy_id,
                        "taxonomy_confidence": taxonomy_confidence,
                        "taxonomy_reason": taxonomy_reason,
                        "risk_reasons": ",".join(hit.get("reason", "") for hit in risk_hits) if risk_hits else "",
                        "risk_terms": "; ".join(
                            f"{hit.get('reason')}: {', '.join(hit.get('terms') or [])}" for hit in risk_hits
                        ) if risk_hits else "",
                    }
                except HTTPException as exc:
                    return {
                        "ok": False,
                        "sku": sku,
                        "error": "http_exception",
                        "error_detail": exc.detail,
                        "etsy_status_code": exc.status_code,
                        "linked_listing_id": None,
                        "taxonomy_id": None,
                        "taxonomy_confidence": 0.0,
                        "taxonomy_reason": "",
                        "risk_reasons": "",
                        "risk_terms": "",
                    }
                except Exception as exc:
                    return {
                        "ok": False,
                        "sku": sku,
                        "error": "unexpected_exception",
                        "error_detail": str(exc),
                        "etsy_status_code": None,
                        "linked_listing_id": None,
                        "taxonomy_id": None,
                        "taxonomy_confidence": 0.0,
                        "taxonomy_reason": "",
                        "risk_reasons": "",
                        "risk_terms": "",
                    }

        tasks = [asyncio.create_task(process_doc(doc)) for doc in docs]

        for task in asyncio.as_completed(tasks):
            result = await task
            processed += 1
            if result.get("ok"):
                created += 1
                logger.info(
                    "[%s/%s] %s | ok | taxonomy=%s (%.3f) | listing_id=%s",
                    processed,
                    len(docs),
                    result.get("sku"),
                    result.get("taxonomy_id"),
                    float(result.get("taxonomy_confidence") or 0.0),
                    result.get("linked_listing_id"),
                )
            else:
                failures.append(result)
                logger.error(
                    "[%s/%s] %s | failed | error=%s | taxonomy=%s (%.3f)",
                    processed,
                    len(docs),
                    result.get("sku"),
                    result.get("error"),
                    result.get("taxonomy_id"),
                    float(result.get("taxonomy_confidence") or 0.0),
                )

    csv_path = _write_error_csv(failures)

    logger.info("Summary")
    logger.info("Processed: %s", processed)
    logger.info("Created: %s", created)
    logger.info("Failed: %s", len(failures))
    if csv_path:
        logger.info("Error CSV: %s", csv_path)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()
