"""Read-only helpers for the eBay Christmas winners -> Etsy candidates notebook.

The module deliberately contains no marketplace write calls.  OAuth helpers may
refresh an expired token, but no listings, ads, titles, or inventory are changed.
"""

from __future__ import annotations

import asyncio
import html
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Iterable

import httpx


EBAY_ORDERS_URL = "https://api.ebay.com/sell/fulfillment/v1/order"
ETSY_BASE_URL = "https://openapi.etsy.com/v3/application"

ANALYSIS_CATEGORIES = (
    "Art & Prints",
    "Books & Manuscripts",
    "Boxes & Cases",
    "Cameras & Optical",
    "Clocks",
    "Coins & Currency",
    "Fashion & Accessories",
    "Figurines & Sculpture",
    "Furniture & Mirrors",
    "Glass & Crystal Decor",
    "Holiday Ornaments",
    "Jewelry",
    "Lamps & Lighting",
    "Militaria & Historical",
    "Music & Entertainment Memorabilia",
    "Pens & Desk Objects",
    "Religious & Spiritual",
    "Rocks, Minerals & Natural History",
    "Scarves & Textiles",
    "Smoking Collectibles",
    "Sports Memorabilia",
    "Tableware & Drinkware",
    "Tools & Scientific Instruments",
    "Toys & Models",
    "Vases, Urns & Pottery",
    "Watches",
    "Other Collectibles",
)

STOP_WORDS = {
    "a", "an", "and", "antique", "antiques", "as", "at", "by", "for",
    "from", "in", "is", "it", "lot", "of", "on", "or", "set", "the",
    "to", "vintage", "with", "rare", "old", "original", "collectible",
    "beautiful", "nice", "large", "small", "inch", "inches", "cm", "mm",
}


def money_value(value: Any) -> float:
    """Return a float from eBay/Etsy money objects or numeric values."""
    if isinstance(value, dict):
        if "value" in value:
            value = value.get("value")
        elif value.get("divisor"):
            value = float(value.get("amount", 0)) / float(value["divisor"])
        else:
            value = value.get("amount", 0)
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def normalize_text(value: Any) -> str:
    value = html.unescape(str(value or "")).lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def title_tokens(value: Any) -> set[str]:
    words = re.findall(r"[a-z0-9]+", normalize_text(value))
    normalized = []
    for word in words:
        if len(word) < 2 or word in STOP_WORDS or word.isdigit():
            continue
        if word.endswith("ies") and len(word) > 4:
            word = word[:-3] + "y"
        elif word.endswith("s") and not word.endswith("ss") and len(word) > 3:
            word = word[:-1]
        normalized.append(word)
    return set(normalized)


def compact_title_family(title: str, *, max_tokens: int = 6) -> str:
    """Stable title fallback when the eBay category is absent."""
    tokens = sorted(title_tokens(title))[:max_tokens]
    return " ".join(tokens) or "uncategorized"


def category_is_useful(category: Any) -> bool:
    value = normalize_text(category)
    return bool(value and value not in {"unknown", "uncategorized", "other", "none"})


def product_group_key(category: Any, title: Any) -> str:
    return str(category).strip() if category_is_useful(category) else compact_title_family(str(title or ""))


async def _get_json_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    attempts: int = 6,
) -> dict[str, Any]:
    for attempt in range(attempts):
        response = await client.get(url, headers=headers, params=params)
        if response.status_code == 429 and attempt < attempts - 1:
            try:
                delay = max(1.0, float(response.headers.get("Retry-After", 2 ** attempt)))
            except ValueError:
                delay = float(2 ** attempt)
            await asyncio.sleep(min(delay, 60.0))
            continue
        if response.status_code >= 500 and attempt < attempts - 1:
            await asyncio.sleep(min(2 ** attempt, 30))
            continue
        if response.status_code >= 400:
            try:
                payload = response.json()
                detail = json.dumps(payload, ensure_ascii=False)
            except (ValueError, TypeError):
                detail = response.text.strip() or "No response body"
            detail = detail[:2000]
            hint = ""
            if response.status_code == 403 and "/sell/fulfillment/" in url:
                hint = (
                    " The eBay token probably lacks the required "
                    "sell.fulfillment.readonly scope. Restart the app/kernel after updating "
                    "the scope list, then complete /auth/ebay/login again; refreshing the old "
                    "token cannot add a newly requested scope."
                )
            raise RuntimeError(
                f"GET {url} returned HTTP {response.status_code}: {detail}.{hint}"
            )
        return response.json()
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}")


async def fetch_ebay_orders(
    access_token: str,
    *,
    start: str,
    end: str,
    marketplace_id: str = "EBAY_US",
) -> list[dict[str, Any]]:
    """Fetch one UTC creation-date window from the eBay Fulfillment API."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
    }
    rows: list[dict[str, Any]] = []
    offset = 0
    limit = 200
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            payload = await _get_json_with_retries(
                client,
                EBAY_ORDERS_URL,
                headers=headers,
                params={
                    "filter": f"creationdate:[{start}..{end}]",
                    "limit": limit,
                    "offset": offset,
                },
            )
            batch = payload.get("orders") or []
            rows.extend(batch)
            offset += len(batch)
            total = int(payload.get("total") or len(rows))
            if not batch or len(rows) >= total:
                break
    return rows


def usable_ebay_order(order: dict[str, Any]) -> bool:
    cancel_state = normalize_text((order.get("cancelStatus") or {}).get("cancelState"))
    payment = normalize_text(order.get("orderPaymentStatus"))
    if cancel_state in {"canceled", "cancelled"}:
        return False
    return payment not in {"pending", "failed", "declined"}


def line_item_revenue(line_item: dict[str, Any]) -> float:
    """Gross item revenue before shipping, refunds, fees, and product cost."""
    for field in ("discountedLineItemCost", "lineItemCost", "total"):
        amount = money_value(line_item.get(field))
        if amount:
            return amount
    unit = money_value(line_item.get("unitPrice"))
    return unit * int(line_item.get("quantity") or 1)


def flatten_ebay_orders(
    orders: Iterable[dict[str, Any]],
    *,
    season: str,
    catalog_by_item_id: dict[str, dict[str, Any]] | None = None,
    catalog_by_sku: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Flatten orders to non-PII line-item rows and enrich them from Mongo."""
    by_item = catalog_by_item_id or {}
    by_sku = catalog_by_sku or {}
    rows: list[dict[str, Any]] = []
    for order in orders:
        if not usable_ebay_order(order):
            continue
        order_id = str(order.get("orderId") or "")
        created = str(order.get("creationDate") or "")
        for line in order.get("lineItems") or []:
            item_id = str(line.get("legacyItemId") or line.get("itemId") or "")
            sku = str(line.get("sku") or "").strip()
            catalog = by_item.get(item_id) or by_sku.get(sku) or {}
            title = str(line.get("title") or catalog.get("title") or "Untitled")
            category = (
                catalog.get("category_path")
                or catalog.get("category")
                or "Unknown"
            )
            quantity = int(line.get("quantity") or 1)
            rows.append(
                {
                    "season": season,
                    "order_id": order_id,
                    "created_at": created,
                    "line_item_id": str(line.get("lineItemId") or ""),
                    "ebay_item_id": item_id,
                    "sku": sku,
                    "title": title,
                    "ebay_category": category,
                    "ebay_category_id": catalog.get("category_id"),
                    "units": quantity,
                    "gross_item_revenue": line_item_revenue(line),
                    "currency": (
                        (line.get("lineItemCost") or {}).get("currency")
                        or (line.get("total") or {}).get("currency")
                        or "USD"
                    ),
                    "group_key": product_group_key(category, title),
                }
            )
    return rows


def taxonomy_category_index(nodes: Iterable[dict[str, Any]]) -> dict[int, dict[str, str]]:
    result: dict[int, dict[str, str]] = {}

    def visit(items: Iterable[dict[str, Any]], parents: list[str]) -> None:
        for node in items or []:
            name = str(node.get("name") or "").strip()
            path = [*parents, name] if name else list(parents)
            node_id = node.get("id")
            if node_id is not None and path:
                result[int(node_id)] = {
                    "category": path[0],
                    "category_path": " > ".join(path),
                }
            visit(node.get("children") or [], path)

    visit(nodes, [])
    return result


async def fetch_etsy_listings(
    access_token: str,
    api_key: str,
    *,
    shop_id: str,
    taxonomy_index: dict[int, dict[str, str]] | None = None,
    states: tuple[str, ...] = ("active",),
) -> list[dict[str, Any]]:
    """Read the seller's own Etsy catalog. No listing mutation is possible here."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": api_key,
        "Accept": "application/json",
    }
    taxonomy_index = taxonomy_index or {}
    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=60.0) as client:
        for state in states:
            offset = 0
            while True:
                payload = await _get_json_with_retries(
                    client,
                    f"{ETSY_BASE_URL}/shops/{shop_id}/listings",
                    headers=headers,
                    params={"state": state, "limit": 100, "offset": offset},
                )
                batch = payload.get("results") or []
                for listing in batch:
                    taxonomy_id = listing.get("taxonomy_id")
                    taxonomy = taxonomy_index.get(int(taxonomy_id)) if taxonomy_id else {}
                    rows.append(
                        {
                            "listing_id": str(listing.get("listing_id") or ""),
                            "state": state,
                            "title": str(listing.get("title") or ""),
                            "description": str(listing.get("description") or ""),
                            "tags": list(listing.get("tags") or []),
                            "taxonomy_id": taxonomy_id,
                            "etsy_category": taxonomy.get("category"),
                            "etsy_category_path": taxonomy.get("category_path"),
                            "quantity": int(listing.get("quantity") or 0),
                            "price": money_value(listing.get("price")),
                            "url": str(listing.get("url") or ""),
                        }
                    )
                offset += len(batch)
                if not batch or offset >= int(payload.get("count") or offset):
                    break
                await asyncio.sleep(0.25)
    return rows


def lexical_match_score(ebay: dict[str, Any], etsy: dict[str, Any]) -> dict[str, float]:
    """Transparent candidate-generation score; it is not a probability."""
    ebay_text = " ".join(
        str(ebay.get(key) or "") for key in ("canonical_product_type", "title", "ebay_category")
    )
    etsy_text = " ".join(
        [
            str(etsy.get("title") or ""),
            " ".join(etsy.get("tags") or []),
            str(etsy.get("etsy_category_path") or ""),
        ]
    )
    a = title_tokens(ebay_text)
    b = title_tokens(etsy_text)
    intersection = len(a & b)
    union = len(a | b)
    token_jaccard = intersection / union if union else 0.0
    title_similarity = SequenceMatcher(
        None,
        normalize_text(ebay.get("title")),
        normalize_text(etsy.get("title")),
    ).ratio()
    category_similarity = SequenceMatcher(
        None,
        normalize_text(ebay.get("ebay_category")),
        normalize_text(etsy.get("etsy_category_path")),
    ).ratio()
    containment = intersection / len(a) if a else 0.0
    score = 100 * (
        0.42 * token_jaccard
        + 0.28 * containment
        + 0.20 * title_similarity
        + 0.10 * category_similarity
    )
    return {
        "lexical_score": round(score, 2),
        "token_jaccard": round(token_jaccard, 4),
        "title_similarity": round(title_similarity, 4),
        "category_similarity": round(category_similarity, 4),
    }


def build_candidate_pairs(
    ebay_rows: Iterable[dict[str, Any]],
    etsy_rows: Iterable[dict[str, Any]],
    *,
    top_n: int = 5,
    minimum_score: float = 12.0,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    etsy = list(etsy_rows)
    for winner in ebay_rows:
        ranked = []
        for listing in etsy:
            scores = lexical_match_score(winner, listing)
            ranked.append(
                {
                    "ebay_item_id": winner.get("ebay_item_id"),
                    "ebay_sku": winner.get("sku"),
                    "ebay_title": winner.get("title"),
                    "ebay_category": winner.get("ebay_category"),
                    "performance_score": winner.get("performance_score"),
                    "units": winner.get("units"),
                    "gross_item_revenue": winner.get("gross_item_revenue"),
                    "etsy_listing_id": listing.get("listing_id"),
                    "etsy_title": listing.get("title"),
                    "etsy_category": listing.get("etsy_category_path"),
                    "etsy_state": listing.get("state"),
                    "etsy_price": listing.get("price"),
                    "etsy_url": listing.get("url"),
                    **scores,
                }
            )
        ranked.sort(key=lambda row: row["lexical_score"], reverse=True)
        kept = [row for row in ranked[:top_n] if row["lexical_score"] >= minimum_score]
        candidates.extend(kept)
    return candidates


def llm_match_review(
    client: Any,
    pairs: list[dict[str, Any]],
    *,
    model: str,
) -> list[dict[str, Any]]:
    """Review preselected pairs without sending customer/order information."""
    safe_pairs = [
        {
            "pair_id": index,
            "ebay_title": row.get("ebay_title"),
            "ebay_category": row.get("ebay_category"),
            "etsy_title": row.get("etsy_title"),
            "etsy_category": row.get("etsy_category"),
        }
        for index, row in enumerate(pairs)
    ]
    schema = {
        "type": "object",
        "properties": {
            "reviews": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pair_id": {"type": "integer"},
                        "same_product_type": {"type": "boolean"},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "canonical_product_type": {"type": "string"},
                        "christmas_relevance": {
                            "type": "string",
                            "enum": ["high", "medium", "low", "none"],
                        },
                        "suggested_holiday_title": {"type": ["string", "null"]},
                        "reason": {"type": "string"},
                    },
                    "required": [
                        "pair_id", "same_product_type", "confidence",
                        "canonical_product_type", "christmas_relevance",
                        "suggested_holiday_title", "reason",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["reviews"],
        "additionalProperties": False,
    }
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You compare seller-owned marketplace listings. Decide whether each eBay "
                    "winner and Etsy listing are the same practical product type, not merely "
                    "the same broad category. Be conservative. Suggest a readable Etsy holiday "
                    "title only when Christmas or gifting is genuinely relevant. Keep it under "
                    "15 words, do not repeat keywords, and do not invent materials, age, maker, "
                    "recipient, personalization, or condition. Return null otherwise."
                ),
            },
            {"role": "user", "content": json.dumps({"pairs": safe_pairs}, ensure_ascii=False)},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "marketplace_match_reviews",
                "schema": schema,
                "strict": True,
            }
        },
    )
    payload = json.loads(response.output_text)
    reviews = {int(row["pair_id"]): row for row in payload["reviews"]}
    output = []
    for index, pair in enumerate(pairs):
        output.append({**pair, **reviews.get(index, {})})
    return output


def llm_infer_product_categories(
    client: Any,
    products: list[dict[str, Any]],
    *,
    model: str,
    batch_size: int = 40,
) -> list[dict[str, Any]]:
    """Assign one controlled product-type category without sending order or buyer data."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    category_enum = list(ANALYSIS_CATEGORIES)
    schema = {
        "type": "object",
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "item_id": {"type": "string"},
                        "category": {"type": "string", "enum": category_enum},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "reason": {"type": "string"},
                    },
                    "required": ["item_id", "category", "confidence", "reason"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["classifications"],
        "additionalProperties": False,
    }

    safe_products = [
        {
            "item_id": str(row.get("ebay_item_id") or row.get("item_id") or ""),
            "title": str(row.get("title") or ""),
            "existing_ebay_category": str(row.get("ebay_category") or "Unknown"),
        }
        for row in products
    ]
    if any(not row["item_id"] for row in safe_products):
        raise ValueError("Every product must have an eBay item ID")
    if len({row["item_id"] for row in safe_products}) != len(safe_products):
        raise ValueError("eBay item IDs must be unique for category inference")

    inferred: dict[str, dict[str, Any]] = {}
    for offset in range(0, len(safe_products), batch_size):
        batch = safe_products[offset:offset + batch_size]
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "Classify each marketplace listing into exactly one supplied commercial "
                        "product-type category. Use the object being sold as the deciding factor, "
                        "not its maker, material, country, style, age, or condition. Prefer a "
                        "specific object category over Other Collectibles. Examples: a mantel "
                        "clock is Clocks even if porcelain; a cigarette case is Smoking "
                        "Collectibles; a Hermes scarf is Scarves & Textiles; a Bible is Books & "
                        "Manuscripts. Existing eBay category text is supporting evidence and may "
                        "be vague or wrong. Do not invent facts. Keep each reason under 14 words. "
                        "Return exactly one classification for every supplied item_id."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {"allowed_categories": category_enum, "products": batch},
                        ensure_ascii=False,
                    ),
                },
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "product_type_categories",
                    "schema": schema,
                    "strict": True,
                }
            },
        )
        payload = json.loads(response.output_text)
        returned = payload.get("classifications", [])
        returned_ids = {str(row.get("item_id")) for row in returned}
        expected_ids = {row["item_id"] for row in batch}
        if returned_ids != expected_ids:
            missing = sorted(expected_ids - returned_ids)
            extra = sorted(returned_ids - expected_ids)
            raise RuntimeError(f"LLM category response ID mismatch; missing={missing}, extra={extra}")
        for row in returned:
            inferred[str(row["item_id"])] = row

    return [inferred[row["item_id"]] for row in safe_products]


def compact_keyword_summary(titles: Iterable[str], *, limit: int = 12) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for title in titles:
        counts.update(title_tokens(title))
    return counts.most_common(limit)
