from __future__ import annotations

import hashlib
import html
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable


AUTO_APPROVED = "AUTO_APPROVED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
BLOCKED = "BLOCKED"
EXCLUDED = "EXCLUDED"

HISTORICAL_MIN_SUPPORT = 5
HISTORICAL_MIN_AGREEMENT = 0.90
AI_AUTO_MIN_CONFIDENCE = 0.90
AI_REVIEW_MIN_CONFIDENCE = 0.70


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(_text(item) for item in value if _text(item))
    if isinstance(value, dict):
        for key in ("value", "Value", "name", "Name"):
            if value.get(key) is not None:
                return _text(value[key])
        return ", ".join(f"{key}: {_text(val)}" for key, val in value.items() if _text(val))
    return re.sub(r"\s+", " ", str(value)).strip()


def _plain_text(value: Any) -> str:
    """Collapse eBay description HTML without letting markup influence classification."""
    text = html.unescape(_text(value))
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text)).strip()


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _text(value).lower()).strip("_")


def _attribute_lookup(attributes: dict[str, Any], wanted: Iterable[str]) -> str:
    normalized = {_slug(key): value for key, value in (attributes or {}).items()}
    for key in wanted:
        value = normalized.get(_slug(key))
        if value not in (None, "", [], {}):
            return _text(value)
    return ""


def extract_primary_classification_inputs(
    normalized: dict[str, Any],
    raw_product: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build taxonomy inputs from original eBay facts, before Etsy SEO generation."""
    raw = ((raw_product or {}).get("raw") or raw_product or {}) if raw_product else {}
    channels = normalized.get("channels") or {}
    ebay_channel = channels.get("ebay") or {}
    ebay_category = normalized.get("ebay_category") or ebay_channel.get("category") or {}
    attributes = raw.get("ItemSpecifics") or normalized.get("attributes") or {}
    if not isinstance(attributes, dict):
        attributes = {}

    category_id = _text(
        raw.get("PrimaryCategoryID")
        or ((raw.get("PrimaryCategory") or {}).get("CategoryID") if isinstance(raw.get("PrimaryCategory"), dict) else None)
        or ebay_category.get("id")
    )
    category_path = _text(
        raw.get("PrimaryCategoryPath")
        or raw.get("CategoryPath")
        or raw.get("CategoryFullName")
        or raw.get("PrimaryCategoryName")
        or ((raw.get("PrimaryCategory") or {}).get("CategoryName") if isinstance(raw.get("PrimaryCategory"), dict) else None)
        or ebay_category.get("path")
        or normalized.get("category")
    )

    selected_attributes = {
        "type": _attribute_lookup(attributes, ["type", "item type", "object type", "product"]),
        "material": _attribute_lookup(attributes, ["material", "materials"]),
        "style": _attribute_lookup(attributes, ["style"]),
        "subject": _attribute_lookup(attributes, ["subject"]),
        "theme": _attribute_lookup(attributes, ["theme"]),
        "era": _attribute_lookup(attributes, ["era", "time period manufactured", "year", "year made"]),
        "brand": _attribute_lookup(attributes, ["brand", "maker", "manufacturer"]),
        "department": _attribute_lookup(attributes, ["department", "gender"]),
        "production_technique": _attribute_lookup(attributes, ["production technique", "printing technique"]),
        "closure": _attribute_lookup(attributes, ["closure", "chain type"]),
        "genre": _attribute_lookup(attributes, ["genre"]),
        "country_region": _attribute_lookup(attributes, ["country/region of manufacture", "country of origin"]),
    }
    selected_attributes = {key: value for key, value in selected_attributes.items() if value}

    return {
        "original_title": _text(raw.get("Title") or normalized.get("title")),
        "ebay_category_id": category_id,
        "ebay_category_path": category_path,
        "normalized_category": _text(normalized.get("category")),
        "item_type": selected_attributes.get("type", ""),
        "attributes": selected_attributes,
        "original_description": _plain_text(raw.get("Description") or normalized.get("description"))[:2000],
    }


def taxonomy_mapping_keys(inputs: dict[str, Any]) -> list[str]:
    category_id = _slug(inputs.get("ebay_category_id"))
    item_type = _slug(inputs.get("item_type"))
    if not category_id:
        return []
    keys: list[str] = []
    if item_type:
        keys.append(f"ebay:{category_id}|type:{item_type}")
    keys.append(f"ebay:{category_id}")
    return keys


def flatten_taxonomy_tree(
    nodes: list[dict[str, Any]],
    ancestors: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    ancestors = ancestors or []
    flattened: list[dict[str, Any]] = []
    for node in nodes or []:
        try:
            taxonomy_id = int(node.get("id"))
        except (TypeError, ValueError):
            continue
        name = _text(node.get("name"))
        if not name:
            continue
        lineage = [*ancestors, {"id": taxonomy_id, "name": name}]
        children = node.get("children") or []
        flattened.append(
            {
                "taxonomy_id": taxonomy_id,
                "name": name,
                "full_path": " > ".join(part["name"] for part in lineage),
                "full_path_taxonomy_ids": [part["id"] for part in lineage],
                "level": int(node.get("level") or max(0, len(lineage) - 1)),
                "leaf": not bool(children),
            }
        )
        if children:
            flattened.extend(flatten_taxonomy_tree(children, lineage))
    return flattened


def taxonomy_tree_hash(nodes: list[dict[str, Any]]) -> str:
    canonical = json.dumps(nodes, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_historical_mapping_records(
    normalized_products: list[dict[str, Any]],
    listings_by_id: dict[str, dict[str, Any]],
    taxonomy_index: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Summarize existing linked listings into evidence-based category mappings."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    now = utc_now()

    for product in normalized_products:
        etsy = ((product.get("channels") or {}).get("etsy") or {})
        listing_id = etsy.get("listing_id")
        listing = listings_by_id.get(str(listing_id))
        if not listing:
            continue
        try:
            taxonomy_id = int(listing.get("taxonomy_id") or etsy.get("taxonomy_id"))
        except (TypeError, ValueError):
            continue
        taxonomy = taxonomy_index.get(taxonomy_id)
        if not taxonomy:
            continue

        inputs = extract_primary_classification_inputs(product)
        for mapping_key in taxonomy_mapping_keys(inputs):
            grouped[mapping_key].append(
                {
                    "sku": str(product.get("_id") or product.get("sku") or ""),
                    "title": _text(product.get("title"))[:160],
                    "taxonomy_id": taxonomy_id,
                    "taxonomy_path": taxonomy.get("full_path"),
                    "ebay_category_id": inputs.get("ebay_category_id"),
                    "ebay_category_path": inputs.get("ebay_category_path"),
                    "item_type": inputs.get("item_type"),
                }
            )

    records: list[dict[str, Any]] = []
    for mapping_key, examples in grouped.items():
        counts = Counter(row["taxonomy_id"] for row in examples)
        taxonomy_id, dominant_count = counts.most_common(1)[0]
        total = len(examples)
        agreement = dominant_count / total if total else 0.0
        dominant = taxonomy_index[taxonomy_id]
        alternatives = [
            {
                "taxonomy_id": candidate_id,
                "taxonomy_path": taxonomy_index.get(candidate_id, {}).get("full_path"),
                "count": count,
                "rate": round(count / total, 4),
            }
            for candidate_id, count in counts.most_common(5)
            if candidate_id != taxonomy_id
        ]
        first = examples[0]
        records.append(
            {
                "_id": hashlib.sha1(mapping_key.encode("utf-8")).hexdigest(),
                "mapping_key": mapping_key,
                "ebay_category_id": first.get("ebay_category_id"),
                "ebay_category_path": first.get("ebay_category_path"),
                "item_type": first.get("item_type") if "|type:" in mapping_key else None,
                "evidence_taxonomy_id": taxonomy_id,
                "evidence_taxonomy_path": dominant.get("full_path"),
                "taxonomy_id": taxonomy_id,
                "taxonomy_path": dominant.get("full_path"),
                "taxonomy_leaf": bool(dominant.get("leaf")),
                "historical_examples": total,
                "dominant_examples": dominant_count,
                "agreement_rate": round(agreement, 4),
                "strength": (
                    "strong"
                    if total >= HISTORICAL_MIN_SUPPORT
                    and agreement >= HISTORICAL_MIN_AGREEMENT
                    else "weak"
                ),
                "alternatives": alternatives,
                "example_products": examples[:5],
                "computed_at": now,
            }
        )
    return records


def backtest_historical_mappings(
    normalized_products: list[dict[str, Any]],
    listings_by_id: dict[str, dict[str, Any]],
    taxonomy_index: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    """Leave one product out and measure only mappings eligible for auto approval."""
    observations: list[tuple[str, int]] = []
    for product in normalized_products:
        etsy = ((product.get("channels") or {}).get("etsy") or {})
        listing = listings_by_id.get(str(etsy.get("listing_id")))
        if not listing:
            continue
        try:
            taxonomy_id = int(listing.get("taxonomy_id") or etsy.get("taxonomy_id"))
        except (TypeError, ValueError):
            continue
        if taxonomy_id not in taxonomy_index:
            continue
        keys = taxonomy_mapping_keys(extract_primary_classification_inputs(product))
        if keys:
            observations.append((keys[0], taxonomy_id))

    by_key: dict[str, list[int]] = defaultdict(list)
    for key, taxonomy_id in observations:
        by_key[key].append(taxonomy_id)

    auto_predictions = 0
    exact = 0
    for key, actual in observations:
        remaining = list(by_key[key])
        try:
            remaining.remove(actual)
        except ValueError:
            continue
        if len(remaining) < HISTORICAL_MIN_SUPPORT:
            continue
        counts = Counter(remaining)
        predicted, predicted_count = counts.most_common(1)[0]
        agreement = predicted_count / len(remaining)
        if agreement < HISTORICAL_MIN_AGREEMENT:
            continue
        auto_predictions += 1
        if predicted == actual:
            exact += 1

    return {
        "known_products": len(observations),
        "auto_predictions": auto_predictions,
        "exact_predictions": exact,
        "auto_exact_accuracy": round(exact / auto_predictions, 4) if auto_predictions else None,
        "coverage": round(auto_predictions / len(observations), 4) if observations else 0.0,
        "method": "leave_one_out_historical_mapping",
    }


def select_historical_mapping(
    inputs: dict[str, Any],
    mappings_by_key: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for key in taxonomy_mapping_keys(inputs):
        mapping = mappings_by_key.get(key)
        if not mapping:
            continue
        if mapping.get("review_status") == "rejected":
            continue
        if mapping.get("review_status") == "approved" or mapping.get("strength") == "strong":
            return mapping
    return None


def decide_taxonomy(
    *,
    selected_taxonomy_id: int | None,
    taxonomy_index: dict[int, dict[str, Any]],
    source: str,
    historical_mapping: dict[str, Any] | None = None,
    ai_confidence: float = 0.0,
    local_top_taxonomy_id: int | None = None,
    technical_validation_ok: bool = True,
) -> dict[str, Any]:
    reasons: list[str] = []
    taxonomy = taxonomy_index.get(int(selected_taxonomy_id)) if selected_taxonomy_id else None

    if not taxonomy:
        return {
            "status": BLOCKED,
            "reason": "taxonomy_not_in_current_seller_tree",
            "reasons": ["The proposed taxonomy ID is not in the current Etsy Seller Taxonomy."],
        }
    if not technical_validation_ok:
        return {
            "status": BLOCKED,
            "reason": "taxonomy_technical_validation_failed",
            "reasons": ["Etsy did not accept the seller-taxonomy property lookup for this ID."],
        }
    if source == "manual":
        reasons.append("A reviewer selected this current Seller Taxonomy category.")
        status = AUTO_APPROVED
    elif source == "historical":
        mapping = historical_mapping or {}
        support = int(mapping.get("historical_examples") or 0)
        agreement = float(mapping.get("agreement_rate") or 0.0)
        approved = mapping.get("review_status") == "approved"
        if approved or (support >= HISTORICAL_MIN_SUPPORT and agreement >= HISTORICAL_MIN_AGREEMENT):
            status = AUTO_APPROVED
            if approved:
                reasons.append("A reviewer approved this mapping for the current Seller Taxonomy.")
            else:
                reasons.append(
                    f"Historical mapping selected this category from {support} examples at {agreement:.1%} agreement."
                )
        else:
            status = REVIEW_REQUIRED
            reasons.append("The historical mapping does not have enough consistent examples for automatic use.")
    else:
        confidence = max(0.0, min(1.0, float(ai_confidence or 0.0)))
        agrees_with_local = local_top_taxonomy_id is not None and int(local_top_taxonomy_id) == int(selected_taxonomy_id)
        if confidence >= AI_AUTO_MIN_CONFIDENCE and agrees_with_local:
            status = AUTO_APPROVED
            reasons.append("AI and local Seller Taxonomy ranking agree with high confidence.")
        elif confidence >= AI_REVIEW_MIN_CONFIDENCE:
            status = REVIEW_REQUIRED
            reasons.append("A valid taxonomy was found, but it requires review before creation.")
        else:
            status = BLOCKED
            reasons.append("No sufficiently reliable taxonomy classification was found.")

    return {
        "status": status,
        "reason": reasons[0] if reasons else None,
        "reasons": reasons,
        "taxonomy_id": int(selected_taxonomy_id),
        "taxonomy_path": taxonomy.get("full_path"),
        "taxonomy_leaf": bool(taxonomy.get("leaf")),
        "source": source,
    }
