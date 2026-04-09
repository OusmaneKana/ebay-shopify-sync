from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Support ISO strings with trailing Z.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def _get_field(doc: dict[str, Any], key: str) -> Any:
    channels = doc.get("channels") if isinstance(doc, dict) else None
    if isinstance(channels, dict):
        shopify = channels.get("shopify")
        if isinstance(shopify, dict) and key in shopify and shopify.get(key) is not None:
            return shopify.get(key)
    return doc.get(key)


def resolve_shopify_variant_pricing(doc: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Resolve price and compare_at_price for Shopify variant updates.

    Priority:
    - `price` is the base selling price from normalized doc.
    - Sale is effective when sale_active is true and current time is in [sale_start, sale_end]
      when those bounds are present.
    - If sale is effective and compare_at_price is missing, compute from discount_percent.
    - compare_at_price is only sent when strictly greater than price.
    """

    now = now or datetime.now(timezone.utc)

    base_price = _to_float(_get_field(doc, "price"))
    if base_price is None or base_price < 0:
        base_price = 0.0

    sale_active = bool(_get_field(doc, "sale_active"))
    sale_start = _parse_dt(_get_field(doc, "sale_start"))
    sale_end = _parse_dt(_get_field(doc, "sale_end"))

    in_window = True
    if sale_start and now < sale_start:
        in_window = False
    if sale_end and now > sale_end:
        in_window = False

    effective_sale = sale_active and in_window

    compare_at = _to_float(_get_field(doc, "compare_at_price"))
    discount_percent = _to_float(_get_field(doc, "discount_percent"))

    if compare_at is None and effective_sale and discount_percent is not None and 0 < discount_percent < 100 and base_price > 0:
        compare_at = round(base_price / (1 - (discount_percent / 100.0)), 2)

    compare_at_str: str | None = None
    if effective_sale and compare_at is not None and compare_at > base_price:
        compare_at_str = f"{compare_at:.2f}"

    return {
        "price": f"{base_price:.2f}",
        "compare_at_price": compare_at_str,
        "effective_sale_active": effective_sale,
    }
