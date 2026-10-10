"""Shared marketplace performance metrics.

The project sells mostly single-stock products.  Category sell-through therefore
uses unique listings (not inventory quantities), while views per sale still uses
units so the calculation remains correct if a listing ever sells more than one.
"""

from collections import defaultdict
from statistics import median


def category_group(value: object) -> str:
    """Return a stable top-level category from an Etsy or eBay category path."""
    text = str(value or "").strip()
    if not text:
        return "Uncategorized"
    for separator in (":", " > "):
        if separator in text:
            return text.split(separator, 1)[0].strip() or "Uncategorized"
    return text


def build_category_performance(products: list[dict], days: int) -> list[dict]:
    """Aggregate comparable selling-speed metrics by top-level category.

    The inventory pool is current active inventory plus listings sold during the
    selected period.  This avoids counting every historical ended listing as
    inventory that was available during the period when its end date is unknown.
    """
    groups: dict[str, dict] = defaultdict(
        lambda: {
            "listing_ids": set(),
            "sold_listing_ids": set(),
            "sold_units": 0,
            "days_to_sale": [],
            "sold_views": 0.0,
            "view_units": 0,
        }
    )
    for product in products:
        units = max(0, int(product.get("units") or 0))
        state = str(product.get("state") or "").lower()
        # Historical ended listings with no selected-period sale are not known to
        # have been available during this period, so they stay out of the pool.
        if state != "active" and units <= 0:
            continue
        listing_id = str(product.get("listing_id") or "")
        if not listing_id:
            continue
        category = category_group(product.get("category_group") or product.get("category"))
        group = groups[category]
        group["listing_ids"].add(listing_id)
        if units <= 0:
            continue
        group["sold_listing_ids"].add(listing_id)
        group["sold_units"] += units
        days_to_sale = product.get("days_to_sale")
        if isinstance(days_to_sale, (int, float)) and days_to_sale >= 0:
            group["days_to_sale"].append(float(days_to_sale))
        views = product.get("views_for_sale")
        if isinstance(views, (int, float)) and views >= 0:
            group["sold_views"] += float(views)
            group["view_units"] += units

    rows = []
    for category, group in groups.items():
        listing_count = len(group["listing_ids"])
        sold_listings = len(group["sold_listing_ids"])
        ages = group["days_to_sale"]
        rows.append(
            {
                "category": category,
                "listing_count": listing_count,
                "sold_listings": sold_listings,
                "sold_units": group["sold_units"],
                "median_days_to_sale": round(median(ages), 1) if ages else None,
                "sell_through_rate": sold_listings / listing_count if listing_count else 0.0,
                "views_per_sale": (
                    round(group["sold_views"] / group["view_units"], 1)
                    if group["view_units"]
                    else None
                ),
                "view_sale_coverage": (
                    group["view_units"] / group["sold_units"]
                    if group["sold_units"]
                    else None
                ),
                "period_days": days,
            }
        )
    rows.sort(
        key=lambda row: (
            row["median_days_to_sale"] is None,
            row["median_days_to_sale"] if row["median_days_to_sale"] is not None else float("inf"),
            -row["sell_through_rate"],
            row["category"].lower(),
        )
    )
    return rows
