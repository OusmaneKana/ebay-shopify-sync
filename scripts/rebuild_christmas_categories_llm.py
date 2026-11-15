"""Infer controlled Christmas product categories and rebuild the performance table.

This reads existing notebook output and sends only item IDs, titles, and category text
to the language model. It does not call either marketplace and performs no listing edits.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from openai import OpenAI


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = REPO_ROOT / "notebooks"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(NOTEBOOK_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOK_DIR))

from app.config import settings  # noqa: E402
from christmas_marketplace_analysis import (  # noqa: E402
    category_is_useful,
    llm_infer_product_categories,
)


OUTPUT_DIR = REPO_ROOT / "outputs" / "christmas-ebay-to-etsy"
INPUT_PATH = OUTPUT_DIR / "ebay_christmas_product_performance.csv"
CLASSIFIED_PATH = OUTPUT_DIR / "ebay_christmas_product_performance_llm_categories.csv"
TABLE_PATH = OUTPUT_DIR / "ebay_christmas_category_performance_llm.csv"

WINDOWS = {
    2024: ("2024-11-01", "2024-12-31"),
    2025: ("2025-11-01", "2025-12-31"),
}


def main() -> None:
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Run the notebook first; missing {INPUT_PATH}")

    products = pd.read_csv(INPUT_PATH, dtype={"ebay_item_id": "string"})
    required = {"ebay_item_id", "title", "ebay_category", "units", "gross_item_revenue", "first_sale"}
    missing = required - set(products.columns)
    if missing:
        raise ValueError(f"Input is missing columns: {sorted(missing)}")

    products["ebay_item_id"] = products["ebay_item_id"].astype("string")
    products["sale_year"] = pd.to_datetime(products["first_sale"], utc=True).dt.year
    unexpected_years = sorted(set(products["sale_year"].dropna().astype(int)) - set(WINDOWS))
    if unexpected_years:
        raise ValueError(f"Sales fall outside configured Christmas windows: {unexpected_years}")

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    model = os.getenv("OPENAI_CATEGORY_MODEL", "gpt-4.1-mini")
    inferred = llm_infer_product_categories(
        client,
        products.to_dict("records"),
        model=model,
        batch_size=int(os.getenv("OPENAI_CATEGORY_BATCH_SIZE", "40")),
    )
    inferred_frame = pd.DataFrame(inferred).rename(
        columns={
            "item_id": "ebay_item_id",
            "category": "analysis_category",
            "confidence": "llm_category_confidence",
            "reason": "llm_category_reason",
        }
    )
    inferred_frame["ebay_item_id"] = inferred_frame["ebay_item_id"].astype("string")
    products = products.merge(inferred_frame, on="ebay_item_id", how="left", validate="one_to_one")
    products["category_source"] = products["ebay_category"].map(
        lambda value: "eBay category normalized by LLM" if category_is_useful(value) else "LLM inferred from title"
    )
    products["analysis_date_range"] = products["sale_year"].map(
        lambda year: f"{WINDOWS[int(year)][0]} to {WINDOWS[int(year)][1]}" if pd.notna(year) else ""
    )

    if products["analysis_category"].isna().any():
        raise RuntimeError("At least one product was not categorized")
    if len(products) != products["ebay_item_id"].nunique():
        raise RuntimeError("Product rows are no longer unique after categorization")

    by_category = products.groupby("analysis_category", as_index=False).agg(
        total_products=("ebay_item_id", "nunique"),
        total_units=("units", "sum"),
        total_revenue_usd=("gross_item_revenue", "sum"),
        average_llm_confidence=("llm_category_confidence", "mean"),
    )
    inferred_only = (
        products.loc[products["category_source"] == "LLM inferred from title"]
        .groupby("analysis_category", as_index=False)
        .agg(llm_inferred_products=("ebay_item_id", "nunique"))
    )
    by_category = by_category.merge(inferred_only, on="analysis_category", how="left")
    by_category["llm_inferred_products"] = by_category["llm_inferred_products"].fillna(0).astype(int)

    for year, (start, end) in WINDOWS.items():
        period = (
            products.loc[products["sale_year"] == year]
            .groupby("analysis_category", as_index=False)
            .agg(**{
                f"units_{year}": ("units", "sum"),
                f"revenue_{year}_usd": ("gross_item_revenue", "sum"),
            })
        )
        by_category = by_category.merge(period, on="analysis_category", how="left")
        by_category[f"data_range_{year}"] = f"{start} to {end}"

    numeric_fill = [
        column for column in by_category.columns
        if column.startswith("units_") or column.startswith("revenue_")
    ]
    by_category[numeric_fill] = by_category[numeric_fill].fillna(0)
    by_category["average_selling_price_usd"] = (
        by_category["total_revenue_usd"] / by_category["total_units"].clip(lower=1)
    )
    by_category["revenue_share_pct"] = (
        100 * by_category["total_revenue_usd"] / by_category["total_revenue_usd"].sum()
    )

    column_order = [
        "analysis_category",
        "data_range_2024",
        "units_2024",
        "revenue_2024_usd",
        "data_range_2025",
        "units_2025",
        "revenue_2025_usd",
        "total_products",
        "total_units",
        "total_revenue_usd",
        "revenue_share_pct",
        "average_selling_price_usd",
        "llm_inferred_products",
        "average_llm_confidence",
    ]
    by_category = by_category[column_order].sort_values(
        ["total_units", "total_revenue_usd"], ascending=False
    ).reset_index(drop=True)

    currency_columns = [
        "revenue_2024_usd", "revenue_2025_usd", "total_revenue_usd", "average_selling_price_usd"
    ]
    by_category[currency_columns] = by_category[currency_columns].round(2)
    by_category["revenue_share_pct"] = by_category["revenue_share_pct"].round(2)
    by_category["average_llm_confidence"] = by_category["average_llm_confidence"].round(3)

    products.to_csv(CLASSIFIED_PATH, index=False)
    by_category.to_csv(TABLE_PATH, index=False)

    print(f"Model: {model}")
    print(f"Products categorized: {len(products):,}")
    print(f"LLM inferred from title: {(products['category_source'] == 'LLM inferred from title').sum():,}")
    print(f"Categories: {len(by_category):,}")
    print(f"Units preserved: {products['units'].sum():,.0f}")
    print(f"Revenue preserved: ${products['gross_item_revenue'].sum():,.2f}")
    print(f"Wrote: {CLASSIFIED_PATH}")
    print(f"Wrote: {TABLE_PATH}")


if __name__ == "__main__":
    main()
