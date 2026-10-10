import unittest

from app.services.etsy_price_adjustment import (
    apply_price_markup,
    build_adjusted_inventory_payload,
    normalize_price_markup_percent,
)


class EtsyPriceAdjustmentTests(unittest.TestCase):
    def test_apply_price_markup_rounds_half_up_to_cents(self):
        self.assertEqual(apply_price_markup(19.99, 5), 20.99)
        self.assertEqual(apply_price_markup("10.00", "7.5"), 10.75)

    def test_normalize_price_markup_percent_rejects_out_of_range(self):
        for value in (-1, 500.01, "not-a-number"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    normalize_price_markup_percent(value)

    def test_build_adjusted_inventory_payload_updates_every_offering_and_sanitizes_ids(self):
        inventory = {
            "products": [
                {
                    "product_id": 111,
                    "sku": "ABC-1",
                    "property_values": [
                        {
                            "property_id": 200,
                            "property_name": "Color",
                            "value_ids": [10],
                            "values": ["Blue"],
                            "scale_name": "remove me",
                        }
                    ],
                    "offerings": [
                        {
                            "offering_id": 222,
                            "price": {"amount": 1000, "divisor": 100, "currency_code": "USD"},
                            "quantity": 2,
                            "is_enabled": True,
                            "readiness_state_id": 3,
                        },
                        {
                            "offering_id": 223,
                            "price": {"amount": 1250, "divisor": 100, "currency_code": "USD"},
                            "quantity": 0,
                            "is_enabled": False,
                            "readiness_state_id": 3,
                        },
                    ],
                }
            ],
            "price_on_property": [200],
            "quantity_on_property": [],
            "sku_on_property": [200],
            "readiness_state_on_property": [],
        }

        payload, source_prices, target_prices = build_adjusted_inventory_payload(inventory, 5)

        self.assertEqual(source_prices, [10.0, 12.5])
        self.assertEqual(target_prices, [10.5, 13.13])
        self.assertEqual(
            [row["price"] for row in payload["products"][0]["offerings"]],
            [10.5, 13.13],
        )
        self.assertNotIn("product_id", payload["products"][0])
        self.assertNotIn("offering_id", payload["products"][0]["offerings"][0])
        self.assertNotIn("scale_name", payload["products"][0]["property_values"][0])
        self.assertEqual(payload["price_on_property"], [200])
