import unittest
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.routes import audit
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


class SoldStillLiveBulkActionTests(unittest.TestCase):
    def test_deactivates_every_currently_eligible_listing_and_updates_snapshots(self):
        eligible = {
            "count": 2,
            "items": [
                {"listing_id": 101, "sku": "SKU-1"},
                {"listing_id": "102", "sku": "SKU-2"},
            ],
        }
        normalized = SimpleNamespace(
            find_one=AsyncMock(side_effect=[
                {"_id": "SKU-1", "quantity": 0, "channels": {"etsy": {"listing_id": 101}}},
                {"_id": "SKU-2", "quantity": 0, "channels": {"etsy": {"listing_id": "102"}}},
            ]),
            update_one=AsyncMock(),
        )
        investigation = SimpleNamespace(update_one=AsyncMock())
        fake_db = SimpleNamespace(
            product_normalized=normalized,
            etsy_listings_investigation=investigation,
        )

        with (
            patch.object(audit, "db", fake_db),
            patch.object(
                audit,
                "_audit_etsy_zero_quantity",
                AsyncMock(return_value=(eligible, {})),
            ) as current_audit,
            patch.object(
                audit,
                "_push_etsy_quantity",
                AsyncMock(side_effect=[(True, None), (False, "etsy_error")]),
            ) as deactivate,
        ):
            result = asyncio.run(audit._deactivate_all_sold_still_live())

        current_audit.assert_awaited_once_with(include_all_items=True)
        self.assertEqual(deactivate.await_count, 2)
        self.assertEqual(result["eligible"], 2)
        self.assertEqual(result["deactivated"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertFalse(result["ok"])
        self.assertEqual(normalized.update_one.await_count, 1)
        self.assertEqual(investigation.update_one.await_count, 1)


if __name__ == "__main__":
    unittest.main()
