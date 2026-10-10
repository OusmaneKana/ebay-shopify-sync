import unittest
from io import BytesIO

from openpyxl import load_workbook

from app.services.etsy_bulk_report import build_etsy_run_workbook
from app.services.etsy_taxonomy_validation import (
    AUTO_APPROVED,
    BLOCKED,
    REVIEW_REQUIRED,
    backtest_historical_mappings,
    build_historical_mapping_records,
    decide_taxonomy,
    extract_primary_classification_inputs,
    select_historical_mapping,
)


TREE_INDEX = {
    10: {
        "taxonomy_id": 10,
        "name": "Mantel Clocks",
        "full_path": "Home & Living > Home Decor > Clocks > Mantel Clocks",
        "leaf": True,
    },
    11: {
        "taxonomy_id": 11,
        "name": "Clocks",
        "full_path": "Home & Living > Home Decor > Clocks",
        "leaf": False,
    },
}


def _product(index: int, taxonomy_id: int = 10) -> tuple[dict, dict]:
    listing_id = 1000 + index
    product = {
        "_id": f"SKU-{index}",
        "sku": f"SKU-{index}",
        "title": f"Antique bronze mantel clock {index}",
        "description": "An antique clock with a bronze case.",
        "attributes": {"Type": "Mantel Clock", "Material": "Bronze", "Era": "1900s"},
        "ebay_category": {
            "id": "261636",
            "path": "Antiques > Decorative Arts > Clocks > Mantel Clocks",
        },
        "channels": {"etsy": {"listing_id": listing_id}},
    }
    listing = {"listing_id": listing_id, "taxonomy_id": taxonomy_id}
    return product, listing


class EtsyTaxonomyValidationTests(unittest.TestCase):
    def test_primary_inputs_use_original_ebay_facts(self):
        normalized, _listing = _product(1)
        raw = {
            "raw": {
                "Title": "Original eBay clock title",
                "Description": "<p>Original &amp; eBay description</p>",
                "PrimaryCategoryID": "261636",
                "PrimaryCategoryName": "Antiques > Clocks",
                "ItemSpecifics": {"Type": "Mantel Clock", "Material": "Brass"},
            }
        }

        result = extract_primary_classification_inputs(normalized, raw)

        self.assertEqual(result["original_title"], "Original eBay clock title")
        self.assertEqual(result["ebay_category_id"], "261636")
        self.assertEqual(result["ebay_category_path"], "Antiques > Clocks")
        self.assertEqual(result["item_type"], "Mantel Clock")
        self.assertEqual(result["attributes"]["material"], "Brass")
        self.assertEqual(result["original_description"], "Original & eBay description")

    def test_strong_historical_mapping_is_selected_before_ai(self):
        products = []
        listings = {}
        for index in range(6):
            product, listing = _product(index)
            products.append(product)
            listings[str(listing["listing_id"])] = listing

        records = build_historical_mapping_records(products, listings, TREE_INDEX)
        mappings = {record["mapping_key"]: record for record in records}
        inputs = extract_primary_classification_inputs(products[0])
        selected = select_historical_mapping(inputs, mappings)

        self.assertIsNotNone(selected)
        self.assertEqual(selected["taxonomy_id"], 10)
        self.assertEqual(selected["strength"], "strong")
        self.assertEqual(selected["agreement_rate"], 1.0)

    def test_historical_backtest_is_leave_one_out(self):
        products = []
        listings = {}
        for index in range(7):
            product, listing = _product(index)
            products.append(product)
            listings[str(listing["listing_id"])] = listing

        result = backtest_historical_mappings(products, listings, TREE_INDEX)

        self.assertEqual(result["known_products"], 7)
        self.assertEqual(result["auto_predictions"], 7)
        self.assertEqual(result["auto_exact_accuracy"], 1.0)

    def test_ai_only_auto_approves_when_local_ranking_agrees(self):
        agreed = decide_taxonomy(
            selected_taxonomy_id=10,
            taxonomy_index=TREE_INDEX,
            source="ai",
            ai_confidence=0.94,
            local_top_taxonomy_id=10,
        )
        disagreed = decide_taxonomy(
            selected_taxonomy_id=10,
            taxonomy_index=TREE_INDEX,
            source="ai",
            ai_confidence=0.94,
            local_top_taxonomy_id=11,
        )

        self.assertEqual(agreed["status"], AUTO_APPROVED)
        self.assertEqual(disagreed["status"], REVIEW_REQUIRED)

    def test_non_leaf_taxonomy_can_be_manually_approved(self):
        result = decide_taxonomy(
            selected_taxonomy_id=11,
            taxonomy_index=TREE_INDEX,
            source="manual",
        )
        self.assertEqual(result["status"], AUTO_APPROVED)
        self.assertFalse(result["taxonomy_leaf"])

    def test_non_leaf_taxonomy_still_uses_ai_confidence_and_agreement(self):
        for confidence, local_id, expected in [(0.94, 11, AUTO_APPROVED), (0.94, 10, REVIEW_REQUIRED), (0.75, 11, REVIEW_REQUIRED), (0.5, 11, BLOCKED)]:
            with self.subTest(confidence=confidence, local_id=local_id):
                result = decide_taxonomy(selected_taxonomy_id=11, taxonomy_index=TREE_INDEX, source="ai", ai_confidence=confidence, local_top_taxonomy_id=local_id)
                self.assertEqual(result["status"], expected)

    def test_non_leaf_historical_mapping_can_be_strong_and_backtested(self):
        pairs = [_product(index, taxonomy_id=11) for index in range(7)]
        products = [product for product, _ in pairs]
        listings = {str(listing["listing_id"]): listing for _, listing in pairs}
        records = build_historical_mapping_records(products, listings, TREE_INDEX)
        selected = select_historical_mapping(extract_primary_classification_inputs(products[0]), {r["mapping_key"]: r for r in records})
        self.assertEqual(selected["strength"], "strong")
        self.assertFalse(selected["taxonomy_leaf"])
        result = decide_taxonomy(selected_taxonomy_id=11, taxonomy_index=TREE_INDEX, source="historical", historical_mapping=selected)
        self.assertEqual(result["status"], AUTO_APPROVED)
        backtest = backtest_historical_mappings(products, listings, TREE_INDEX)
        self.assertEqual(backtest["auto_predictions"], 7)
        self.assertEqual(backtest["auto_exact_accuracy"], 1.0)

    def test_missing_taxonomy_and_failed_technical_validation_remain_blocked(self):
        for taxonomy_id, technical_ok in [(999, True), (11, False)]:
            with self.subTest(taxonomy_id=taxonomy_id):
                result = decide_taxonomy(selected_taxonomy_id=taxonomy_id, taxonomy_index=TREE_INDEX, source="manual", technical_validation_ok=technical_ok)
                self.assertEqual(result["status"], BLOCKED)


class EtsyWorkbookTests(unittest.TestCase):
    def test_workbook_contains_review_and_error_sheets(self):
        run = {
            "_id": "etsy-run-test",
            "counts": {
                "total": 4,
                "auto_approved": 3,
                "review_required": 1,
                "created": 1,
                "creation_failed": 1,
            },
            "taxonomy_tree_hash": "tree-v1",
            "historical_backtest": {"known_products": 7, "auto_exact_accuracy": 1.0, "coverage": 1.0},
            "historical_auto_enabled": True,
        }
        items = [
            {
                "sku": "READY-1",
                "title": "Clock",
                "decision_status": "AUTO_APPROVED",
                "classification_inputs": {"original_title": "Clock", "attributes": {}},
                "taxonomy_decision": {"taxonomy_id": 10, "taxonomy_path": TREE_INDEX[10]["full_path"], "source": "historical"},
            },
            {
                "sku": "REVIEW-1",
                "title": "Decorative object",
                "decision_status": "REVIEW_REQUIRED",
                "classification_inputs": {"original_title": "Decorative object", "attributes": {}},
                "taxonomy_decision": {"taxonomy_id": 10, "taxonomy_path": TREE_INDEX[10]["full_path"], "source": "ai"},
                "ai_confidence": 0.78,
            },
            {
                "sku": "CREATED-1",
                "title": "Created clock",
                "decision_status": "AUTO_APPROVED",
                "creation_status": "created",
                "etsy_listing_id": 123456789,
                "final_taxonomy_id": 10,
                "final_taxonomy_path": TREE_INDEX[10]["full_path"],
                "classification_inputs": {"original_title": "Created clock", "attributes": {}},
            },
            {
                "sku": "FAILED-1",
                "title": "Failed clock",
                "decision_status": "AUTO_APPROVED",
                "creation_status": "failed",
                "creation_error_code": "etsy_create_draft_failed",
                "creation_error": {
                    "error": "invalid taxonomy",
                    "field": "taxonomy_id",
                    "response_body": "x" * 35050,
                },
                "classification_inputs": {"original_title": "Failed clock", "attributes": {}},
            },
        ]

        output = build_etsy_run_workbook(run, items, [])
        workbook = load_workbook(BytesIO(output), data_only=False)

        self.assertEqual(
            workbook.sheetnames,
            [
                "Summary",
                "Historical Mapping Audit",
                "Taxonomy Review",
                "Ready to Create",
                "Validation Errors",
                "Created Listings",
                "Creation Errors",
                "Raw Error Details",
            ],
        )
        self.assertEqual(workbook["Taxonomy Review"]["A2"].value, "REVIEW-1")
        self.assertEqual(workbook["Ready to Create"]["A2"].value, "READY-1")
        self.assertEqual(workbook["Created Listings"]["A2"].value, "CREATED-1")
        self.assertEqual(workbook["Creation Errors"]["A2"].value, "FAILED-1")
        self.assertIn("invalid taxonomy", workbook["Creation Errors"]["H2"].value)
        raw_details = "".join(
            str(row[5].value or "")
            for row in workbook["Raw Error Details"].iter_rows(min_row=2)
        )
        self.assertIn("invalid taxonomy", raw_details)
        self.assertIn("x" * 35050, raw_details)


if __name__ == "__main__":
    unittest.main()
