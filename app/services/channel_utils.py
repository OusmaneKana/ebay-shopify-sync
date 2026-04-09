from __future__ import annotations

from typing import Any


def get_channel(doc: dict[str, Any], channel: str) -> dict[str, Any]:
    channels = doc.get("channels")
    if not isinstance(channels, dict):
        return {}
    value = channels.get(channel)
    return value if isinstance(value, dict) else {}


def get_shopify_field(doc: dict[str, Any], key: str, default: Any = None) -> Any:
    shopify = get_channel(doc, "shopify")
    if key in shopify and shopify.get(key) is not None:
        return shopify.get(key)
    return doc.get(key, default)


def set_shopify_fields_set(update_data: dict[str, Any]) -> dict[str, Any]:
    """Build a $set payload with both legacy and channel paths for rollout safety."""
    out: dict[str, Any] = {}
    for key, value in update_data.items():
        out[key] = value
        out[f"channels.shopify.{key}"] = value
    return out