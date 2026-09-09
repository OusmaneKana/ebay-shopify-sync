import asyncio
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import scripts.fix_zero_quantity_mismatch as subject


class AsyncCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def __aiter__(self):
        self._iterator = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class RawCollection:
    def __init__(self, rows):
        self.rows = rows

    def find(self, query, projection):
        return AsyncCursor(self.rows)


class NormalizedCollection:
    def __init__(self, rows):
        self.rows = {row["_id"]: row for row in rows}

    async def find_one(self, query, projection):
        return self.rows.get(query["_id"])

    async def update_one(self, query, update):
        row = self.rows.get(query["_id"])
        if not row:
            return SimpleNamespace(modified_count=0)
        expected = query.get("quantity")
        if "quantity" in query and row.get("quantity") != expected:
            return SimpleNamespace(modified_count=0)
        for key, value in update["$set"].items():
            if key.startswith("channels.etsy."):
                row.setdefault("channels", {}).setdefault("etsy", {})[key.rsplit(".", 1)[-1]] = value
            else:
                row[key] = value
        return SimpleNamespace(modified_count=1)


class EtsyInvestigationCollection:
    def __init__(self, states):
        self.states = states

    async def find_one(self, query, projection):
        candidates = query["listing_id"]["$in"]
        for candidate in candidates:
            if candidate in self.states:
                return {"listing_state": self.states[candidate]}
        return None

    async def update_one(self, query, update):
        candidates = query["listing_id"]["$in"]
        for candidate in candidates:
            if candidate in self.states:
                self.states[candidate] = update["$set"]["listing_state"]
                return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)


def fake_db():
    return SimpleNamespace(
        product_raw=RawCollection([
            {"_id": "SKU-1", "raw": {"QuantityAvailable": 0, "QuantityTotal": 1}},
            {"_id": "SKU-2", "raw": {"QuantityAvailable": 0, "QuantityTotal": 1}},
        ]),
        product_normalized=NormalizedCollection([
            {
                "_id": "SKU-1",
                "quantity": 1,
                "shopify_variant_id": 101,
                "channels": {"etsy": {"listing_id": 201, "shop_id": 301}},
            },
            {
                "_id": "SKU-2",
                "quantity": 0,
                "shopify_variant_id": 102,
                "channels": {"etsy": {"listing_id": "202", "shop_id": 301}},
            },
        ]),
        etsy_listings_investigation=EtsyInvestigationCollection({201: "active", 202: "active"}),
    )


class ZeroQuantityMismatchTests(TestCase):
    def test_dry_run_includes_already_zero_etsy_listing(self):
        async def unexpected(*args, **kwargs):
            raise AssertionError("dry-run called a remote update")

        with (
            patch.object(subject, "db", fake_db()),
            patch.object(subject, "_make_shopify_client", side_effect=AssertionError),
            patch.object(subject, "set_inventory_quantity_by_variant", unexpected),
            patch.object(subject, "_push_etsy_quantity", unexpected),
        ):
            result = asyncio.run(subject.fix_zero_quantity_mismatches(dry_run=True))

        self.assertEqual(result["mismatched_candidates"], 2)
        self.assertEqual(result["normalized_already_zero"], 1)
        self.assertEqual(result["fixed_normalized"], 0)
        self.assertEqual(result["updated_shopify"], 0)
        self.assertEqual(result["updated_etsy"], 0)

    def test_live_run_updates_shopify_and_actionable_etsy_listings(self):
        database = fake_db()
        shopify_calls = []
        etsy_calls = []

        async def update_shopify(variant_id, quantity, client):
            shopify_calls.append((variant_id, quantity, client))
            return True

        async def update_etsy(doc, quantity):
            etsy_calls.append((doc["_id"], quantity))
            return True, None

        with (
            patch.object(subject, "db", database),
            patch.object(subject, "_make_shopify_client", return_value="shopify-client"),
            patch.object(subject, "set_inventory_quantity_by_variant", update_shopify),
            patch.object(subject, "_push_etsy_quantity", update_etsy),
        ):
            result = asyncio.run(subject.fix_zero_quantity_mismatches())

        self.assertEqual(database.product_normalized.rows["SKU-1"]["quantity"], 0)
        self.assertEqual(result["fixed_normalized"], 1)
        self.assertEqual(result["updated_shopify"], 1)
        self.assertEqual(result["skipped_shopify"], 1)
        self.assertEqual(result["updated_etsy"], 2)
        self.assertEqual(result["errors"], 0)
        self.assertEqual(shopify_calls, [(101, 0, "shopify-client")])
        self.assertEqual(etsy_calls, [("SKU-1", 0), ("SKU-2", 0)])
        self.assertEqual(
            database.product_normalized.rows["SKU-1"]["channels"]["etsy"]["listing_state"],
            "inactive",
        )
        self.assertEqual(database.etsy_listings_investigation.states[201], "inactive")
