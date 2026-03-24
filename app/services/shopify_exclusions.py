from __future__ import annotations

from typing import Iterable

# Items carrying any of these tags will be excluded from Shopify.
# Primary use-case: compliance / platform policy exclusions.
BLOCKED_SHOPIFY_TAGS: set[str] = {
    "Category:Militaria",
    "SC:knives-bayonets",
    "Swords & Blades",
    "Daggers",
    "Category:bayonet",

}


def normalize_tag(tag: str) -> str:
    return tag.strip()


def has_blocked_shopify_tag(tags: Iterable[str] | None) -> bool:
    if not tags:
        return False

    # Some legacy docs may store tags as a single comma-separated string.
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]

    blocked = BLOCKED_SHOPIFY_TAGS
    for t in tags:
        if not isinstance(t, str):
            continue
        if normalize_tag(t) in blocked:
            return True
    return False


def is_shopify_excluded_doc(doc: dict) -> bool:
    return has_blocked_shopify_tag(doc.get("tags") or [])
