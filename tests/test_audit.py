import unittest

from app.api.routes.audit import _collect_ebay_sale_evidence


class EbaySaleEvidenceTests(unittest.TestCase):
    def test_zero_quantity_without_sale_evidence_is_not_confirmed_sold(self):
        evidence, latest = _collect_ebay_sale_evidence(
            {"SKU-1"},
            [{"_id": "SKU-1", "raw": {"SKU": "SKU-1", "QuantityAvailable": 0, "QuantitySold": 0}}],
            [],
            [{"products": [{"sku": "SKU-1", "state": "active", "quantity": 1, "orders": 0, "units": 0}]}],
        )

        self.assertEqual(evidence["SKU-1"], set())
        self.assertEqual(latest["SKU-1"]["state"], "active")

    def test_quantity_sold_confirms_sale(self):
        evidence, _ = _collect_ebay_sale_evidence(
            {"SKU-1"},
            [{"_id": "SKU-1", "raw": {"SKU": "SKU-1", "QuantitySold": 1}}],
            [],
            [],
        )

        self.assertEqual(evidence["SKU-1"], {"ebay_quantity_sold"})

    def test_event_or_historical_order_confirms_sale(self):
        evidence, latest = _collect_ebay_sale_evidence(
            {"SKU-1", "SKU-2"},
            [],
            [{"sku": "SKU-1", "source_channel": "ebay", "type": "sale", "quantity_sold": 1}],
            [
                {"products": [{"sku": "SKU-2", "state": "ended", "units": 0}]},
                {"products": [{"sku": "SKU-2", "state": "active", "units": 1, "sold_at": "2026-10-01"}]},
            ],
        )

        self.assertEqual(evidence["SKU-1"], {"ebay_sale_event"})
        self.assertEqual(evidence["SKU-2"], {"ebay_orders"})
        self.assertEqual(latest["SKU-2"]["state"], "ended")


if __name__ == "__main__":
    unittest.main()
