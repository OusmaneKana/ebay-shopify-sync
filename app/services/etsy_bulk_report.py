from __future__ import annotations

import json
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


HEADER_FILL = PatternFill("solid", fgColor="1F2937")
HEADER_FONT = Font(color="FFFFFF", bold=True)
TITLE_FONT = Font(size=16, bold=True, color="111827")
SUBTITLE_FONT = Font(size=10, italic=True, color="4B5563")
STATUS_FILLS = {
    "AUTO_APPROVED": PatternFill("solid", fgColor="DCFCE7"),
    "REVIEW_REQUIRED": PatternFill("solid", fgColor="FEF3C7"),
    "BLOCKED": PatternFill("solid", fgColor="FEE2E2"),
    "EXCLUDED": PatternFill("solid", fgColor="E5E7EB"),
    "created": PatternFill("solid", fgColor="DCFCE7"),
    "failed": PatternFill("solid", fgColor="FEE2E2"),
}


def _cell_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value[:30000] if isinstance(value, str) else value
    if hasattr(value, "tzinfo"):
        # Excel stores naive datetimes. All application timestamps are UTC.
        return value.replace(tzinfo=None) if value.tzinfo else value
    return json.dumps(value, ensure_ascii=False, default=str)[:30000]


def _serialized(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _write_table(
    ws,
    headers: list[str],
    rows: list[list[Any]],
    *,
    freeze_panes: str = "A2",
) -> None:
    ws.append(headers)
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for row in rows:
        ws.append([_cell_value(value) for value in row])

    ws.freeze_panes = freeze_panes
    ws.auto_filter.ref = ws.dimensions
    ws.sheet_view.showGridLines = False
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    for col_idx, header in enumerate(headers, start=1):
        max_len = len(header)
        for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2, max_row=min(ws.max_row, 200)):
            for value_cell in cell:
                value = value_cell.value
                if value is not None:
                    parts = str(value).splitlines() or [""]
                    max_len = max(max_len, max(len(part) for part in parts))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 11), 48)


def _taxonomy_rows(items: list[dict[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for item in items:
        inputs = item.get("classification_inputs") or {}
        attributes = inputs.get("attributes") or {}
        decision = item.get("taxonomy_decision") or {}
        mapping = item.get("historical_mapping") or {}
        candidates = item.get("taxonomy_candidates") or []
        alternatives = [
            f"{candidate.get('taxonomy_id')}: {candidate.get('full_path') or candidate.get('name')}"
            for candidate in candidates[:3]
        ]
        rows.append(
            [
                item.get("sku"),
                item.get("ebay_item_id"),
                inputs.get("original_title") or item.get("title"),
                (item.get("optimizations") or {}).get("base_price"),
                (item.get("optimizations") or {}).get("price_markup_percent"),
                (item.get("optimizations") or {}).get("price"),
                inputs.get("ebay_category_id"),
                inputs.get("ebay_category_path"),
                attributes.get("type"),
                attributes.get("material"),
                attributes.get("style"),
                attributes.get("subject") or attributes.get("theme"),
                attributes.get("era"),
                decision.get("taxonomy_id") or item.get("final_taxonomy_id"),
                decision.get("taxonomy_path") or item.get("final_taxonomy_path"),
                decision.get("source"),
                mapping.get("historical_examples"),
                mapping.get("agreement_rate"),
                item.get("ai_confidence"),
                "\n".join(alternatives),
                item.get("decision_status"),
                decision.get("reason") or item.get("validation_error"),
                item.get("reviewed_taxonomy_id"),
                item.get("review_note"),
            ]
        )
    return rows


TAXONOMY_HEADERS = [
    "SKU",
    "eBay item ID",
    "Original title",
    "Base price",
    "Price increase %",
    "Etsy upload price",
    "eBay category ID",
    "eBay category path",
    "Type",
    "Material",
    "Style",
    "Subject or theme",
    "Era",
    "Proposed taxonomy ID",
    "Proposed taxonomy path",
    "Selection source",
    "Historical examples",
    "Historical agreement",
    "AI confidence",
    "Top alternatives",
    "Decision",
    "Decision reason",
    "Reviewer taxonomy ID",
    "Reviewer note",
]


def build_etsy_run_workbook(
    run: dict[str, Any],
    items: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> bytes:
    """Create the downloadable audit workbook for one Etsy validation run."""
    wb = Workbook()
    summary = wb.active
    summary.title = "Summary"
    summary.sheet_view.showGridLines = False
    summary["A1"] = "Etsy batch validation"
    summary["A1"].font = TITLE_FONT
    summary["A2"] = "Validation snapshot and creation results. Times are UTC."
    summary["A2"].font = SUBTITLE_FONT

    counts = run.get("counts") or {}
    backtest = run.get("historical_backtest") or {}
    summary_rows = [
        ("Run ID", run.get("_id") or run.get("run_id")),
        ("Created at", run.get("created_at")),
        ("Updated at", run.get("updated_at")),
        ("Seller taxonomy fetched at", run.get("taxonomy_fetched_at")),
        ("Seller taxonomy version", run.get("taxonomy_tree_hash")),
        ("Historical known products", backtest.get("known_products", 0)),
        ("Historical auto predictions", backtest.get("auto_predictions", 0)),
        ("Historical exact accuracy", backtest.get("auto_exact_accuracy")),
        ("Historical coverage", backtest.get("coverage", 0)),
        ("Historical auto enabled", "Yes" if run.get("historical_auto_enabled") else "No"),
        ("Price increase %", run.get("price_markup_percent", 0)),
        ("Candidates", counts.get("total", len(items))),
        ("Queued", counts.get("queued", 0)),
        ("Processing", counts.get("processing", 0)),
        ("Auto approved", counts.get("auto_approved", 0)),
        ("Needs taxonomy review", counts.get("review_required", 0)),
        ("Blocked", counts.get("blocked", 0)),
        ("Excluded", counts.get("excluded", 0)),
        ("Creating now", counts.get("creation_processing", 0)),
        ("Test drafts attempted", counts.get("canary_attempted", 0)),
        ("Created", counts.get("created", 0)),
        ("Creation failures", counts.get("creation_failed", 0)),
        ("Complete error payloads", "See Raw Error Details; split into numbered chunks without truncation."),
    ]
    for row_index, (label, value) in enumerate(summary_rows, start=4):
        summary.cell(row=row_index, column=1, value=label).font = Font(bold=True, color="374151")
        summary.cell(row=row_index, column=2, value=_cell_value(value))
    summary.column_dimensions["A"].width = 30
    summary.column_dimensions["B"].width = 52

    source_row = len(summary_rows) + 6
    summary.cell(source_row, 1, "Sources").font = Font(bold=True, color="374151")
    summary.cell(source_row + 1, 1, "Etsy listings guidance")
    summary.cell(source_row + 1, 2, "https://developers.etsy.com/documentation/tutorials/listings/")
    summary.cell(source_row + 2, 1, "Etsy API reference")
    summary.cell(source_row + 2, 2, "https://developers.etsy.com/documentation/reference/")

    mapping_ws = wb.create_sheet("Historical Mapping Audit")
    mapping_headers = [
        "Mapping key",
        "eBay category ID",
        "eBay category path",
        "Item type",
        "Evidence taxonomy ID",
        "Evidence taxonomy path",
        "Selected taxonomy ID",
        "Selected taxonomy path",
        "Historical examples",
        "Dominant examples",
        "Agreement",
        "Strength",
        "Review status",
        "Alternatives",
        "Example products",
    ]
    mapping_rows = [
        [
            mapping.get("mapping_key"),
            mapping.get("ebay_category_id"),
            mapping.get("ebay_category_path"),
            mapping.get("item_type"),
            mapping.get("evidence_taxonomy_id") or mapping.get("taxonomy_id"),
            mapping.get("evidence_taxonomy_path") or mapping.get("taxonomy_path"),
            mapping.get("taxonomy_id"),
            mapping.get("taxonomy_path"),
            mapping.get("historical_examples"),
            mapping.get("dominant_examples"),
            mapping.get("agreement_rate"),
            mapping.get("strength"),
            mapping.get("review_status"),
            mapping.get("alternatives"),
            mapping.get("example_products"),
        ]
        for mapping in mappings
    ]
    _write_table(mapping_ws, mapping_headers, mapping_rows, freeze_panes="C2")

    review_items = [item for item in items if item.get("decision_status") == "REVIEW_REQUIRED"]
    review_ws = wb.create_sheet("Taxonomy Review")
    _write_table(review_ws, TAXONOMY_HEADERS, _taxonomy_rows(review_items), freeze_panes="C2")

    ready_items = [item for item in items if item.get("decision_status") == "AUTO_APPROVED"]
    ready_ws = wb.create_sheet("Ready to Create")
    _write_table(ready_ws, TAXONOMY_HEADERS, _taxonomy_rows(ready_items), freeze_panes="C2")

    validation_items = [
        item
        for item in items
        if item.get("decision_status") in {"BLOCKED", "EXCLUDED"}
        or item.get("validation_error")
    ]
    validation_ws = wb.create_sheet("Validation Errors")
    validation_headers = [
        "SKU",
        "eBay item ID",
        "Title",
        "Decision",
        "Error code",
        "Error detail",
        "Missing fields",
        "Taxonomy ID",
        "Taxonomy path",
        "Retryable",
        "Recommended action",
        "Updated at",
    ]
    validation_rows = []
    for item in validation_items:
        decision = item.get("taxonomy_decision") or {}
        validation_rows.append(
            [
                item.get("sku"),
                item.get("ebay_item_id"),
                item.get("title"),
                item.get("decision_status"),
                item.get("validation_error_code") or decision.get("reason"),
                item.get("validation_error") or decision.get("reasons"),
                item.get("missing_fields"),
                item.get("final_taxonomy_id") or decision.get("taxonomy_id"),
                item.get("final_taxonomy_path") or decision.get("taxonomy_path"),
                item.get("retryable", False),
                item.get("recommended_action"),
                item.get("updated_at"),
            ]
        )
    _write_table(validation_ws, validation_headers, validation_rows, freeze_panes="C2")

    created_ws = wb.create_sheet("Created Listings")
    created_headers = [
        "SKU",
        "eBay item ID",
        "Title",
        "Creation status",
        "Etsy listing ID",
        "Taxonomy ID",
        "Taxonomy path",
        "Inventory result",
        "Image result",
        "Created at",
    ]
    created_rows = []
    for item in items:
        if item.get("creation_status") != "created":
            continue
        result = item.get("creation_result") or {}
        created_rows.append(
            [
                item.get("sku"),
                item.get("ebay_item_id"),
                item.get("title"),
                item.get("creation_status"),
                item.get("etsy_listing_id") or result.get("linked_listing_id"),
                item.get("final_taxonomy_id"),
                item.get("final_taxonomy_path"),
                result.get("inventory_sync"),
                result.get("image_upload"),
                item.get("creation_attempted_at"),
            ]
        )
    _write_table(created_ws, created_headers, created_rows, freeze_panes="C2")

    creation_ws = wb.create_sheet("Creation Errors")
    creation_headers = [
        "SKU",
        "eBay item ID",
        "Title",
        "Creation status",
        "Etsy listing ID",
        "Etsy HTTP status",
        "Error code",
        "Error detail",
        "Inventory result",
        "Image result",
        "Retryable",
        "Attempted at",
    ]
    creation_rows = []
    for item in items:
        if item.get("creation_status") not in {"failed", "post_create_failed"}:
            continue
        result = item.get("creation_result") or {}
        creation_rows.append(
            [
                item.get("sku"),
                item.get("ebay_item_id"),
                item.get("title"),
                item.get("creation_status"),
                item.get("etsy_listing_id") or result.get("linked_listing_id"),
                result.get("etsy_status_code"),
                item.get("creation_error_code"),
                item.get("creation_error") or result.get("etsy_response"),
                result.get("inventory_sync"),
                result.get("image_upload"),
                item.get("creation_retryable", False),
                item.get("creation_attempted_at"),
            ]
        )
    _write_table(creation_ws, creation_headers, creation_rows, freeze_panes="C2")

    raw_error_ws = wb.create_sheet("Raw Error Details")
    raw_error_headers = [
        "Phase",
        "SKU",
        "Error code",
        "Chunk",
        "Total chunks",
        "Complete serialized detail",
    ]
    raw_error_rows: list[list[Any]] = []
    error_records: list[tuple[str, dict[str, Any], Any]] = []
    for item in validation_items:
        error_records.append(
            (
                "validation",
                item,
                {
                    "validation_error": item.get("validation_error"),
                    "missing_fields": item.get("missing_fields"),
                    "taxonomy_decision": item.get("taxonomy_decision"),
                    "taxonomy_technical_validation": item.get("taxonomy_technical_validation"),
                    "recommended_action": item.get("recommended_action"),
                },
            )
        )
    for item in items:
        if item.get("creation_status") in {"failed", "post_create_failed"}:
            error_records.append(
                (
                    "creation",
                    item,
                    {
                        "creation_error": item.get("creation_error"),
                        "creation_result": item.get("creation_result"),
                    },
                )
            )
    for phase, item, detail in error_records:
        serialized = _serialized(detail)
        chunks = [serialized[index:index + 30000] for index in range(0, len(serialized), 30000)] or [""]
        for chunk_number, chunk in enumerate(chunks, start=1):
            raw_error_rows.append(
                [
                    phase,
                    item.get("sku"),
                    item.get("validation_error_code") or item.get("creation_error_code"),
                    chunk_number,
                    len(chunks),
                    chunk,
                ]
            )
    _write_table(raw_error_ws, raw_error_headers, raw_error_rows, freeze_panes="C2")
    for cell in raw_error_ws["F"][1:]:
        cell.alignment = Alignment(vertical="top", wrap_text=False)

    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if cell.value in STATUS_FILLS:
                    cell.fill = STATUS_FILLS[cell.value]
        for col in ("Historical agreement", "AI confidence", "Agreement"):
            if ws.max_row < 2:
                continue
            for cell in ws[1]:
                if cell.value == col:
                    for data_cell in ws.iter_cols(
                        min_col=cell.column,
                        max_col=cell.column,
                        min_row=2,
                        max_row=ws.max_row,
                    ):
                        for value_cell in data_cell:
                            value_cell.number_format = "0.0%"

    for label in ("Historical exact accuracy", "Historical coverage"):
        for row in summary.iter_rows(min_row=4, max_col=2):
            if row[0].value == label:
                row[1].number_format = "0.0%"

    output = BytesIO()
    wb.save(output)
    return output.getvalue()
