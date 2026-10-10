import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from app.api.routes import reporting


class _Cursor:
    def __init__(self, documents):
        self.documents = documents

    async def to_list(self, length=None):
        return self.documents


class EtsyPublishOrderTests(unittest.IsolatedAsyncioTestCase):
    def test_recency_uses_ebay_listing_date_before_fallback_dates(self):
        raw_posted_at = datetime(2026, 10, 8, tzinfo=timezone.utc)
        normalized_posted_at = datetime(2026, 10, 7, tzinfo=timezone.utc)
        fields = reporting._etsy_publish_recency_fields(
            {"ebay_posted_at": raw_posted_at},
            {
                "ebay_posted_at": normalized_posted_at,
                "first_seen_at": datetime(2026, 10, 6, tzinfo=timezone.utc),
                "last_normalized_at": datetime(2026, 10, 9, tzinfo=timezone.utc),
            },
        )

        self.assertEqual(fields["source_published_at"], raw_posted_at)

    async def test_old_run_items_are_backfilled_from_source_dates(self):
        posted_at = datetime(2026, 10, 8, tzinfo=timezone.utc)
        item_collection = SimpleNamespace(
            find=MagicMock(return_value=_Cursor([{"sku": "SKU-1"}])),
            bulk_write=AsyncMock(),
        )
        normalized_collection = SimpleNamespace(
            find=MagicMock(return_value=_Cursor([{"_id": "SKU-1", "first_seen_at": posted_at}]))
        )
        raw_collection = SimpleNamespace(
            find=MagicMock(return_value=_Cursor([{"_id": "SKU-1", "ebay_posted_at": posted_at}]))
        )
        fake_db = MagicMock()
        fake_db.product_normalized = normalized_collection
        fake_db.product_raw = raw_collection
        fake_db.__getitem__.return_value = item_collection

        with patch.object(reporting, "db", fake_db):
            await reporting._backfill_etsy_publish_recency("run-1")

        operations = item_collection.bulk_write.await_args.args[0]
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0]._doc["$set"]["source_published_at"], posted_at)

    async def test_ready_quantity_selects_newest_products_first(self):
        run_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": "run-1", "taxonomy_tree_hash": "tree-1"}),
        )
        item_collection = SimpleNamespace(
            update_many=AsyncMock(return_value=SimpleNamespace(modified_count=0)),
            find_one_and_update=AsyncMock(
                return_value={
                    "_id": "run-1:NEW",
                    "sku": "NEW",
                    "optimizations": {"seo_title": "Newest", "tags": [], "taxonomy_id": 1},
                    "final_taxonomy_id": 1,
                }
            ),
            update_one=AsyncMock(),
        )
        fake_db = MagicMock()
        fake_db.__getitem__.side_effect = lambda name: {
            reporting.ETSY_PUBLISH_RUNS_COLLECTION: run_collection,
            reporting.ETSY_PUBLISH_RUN_ITEMS_COLLECTION: item_collection,
        }[name]

        counts = {
            "queued": 0,
            "processing": 0,
            "creation_processing": 0,
            "canary_attempted": 0,
        }
        with (
            patch.object(reporting, "db", fake_db),
            patch.object(reporting, "_seller_taxonomy_context", AsyncMock(return_value=([], {}, "tree-1"))),
            patch.object(reporting, "_refresh_publish_run_counts", AsyncMock(return_value=counts)),
            patch.object(reporting, "_backfill_etsy_publish_recency", AsyncMock()),
            patch.object(
                reporting,
                "etsy_publish_create_draft",
                AsyncMock(return_value={"ok": True, "linked_listing_id": "listing-1"}),
            ),
        ):
            result = await reporting.etsy_publish_create_run_drafts(
                "run-1",
                body={"confirmed": True, "limit": 1, "ready_only": True},
                _authorized=None,
            )

        self.assertEqual(result["processed"], 1)
        self.assertEqual(
            item_collection.find_one_and_update.await_args.kwargs["sort"],
            reporting.ETSY_PUBLISH_NEWEST_FIRST_SORT,
        )
        self.assertEqual(reporting.ETSY_PUBLISH_NEWEST_FIRST_SORT[0], ("source_published_at", -1))

    async def test_validation_also_processes_newest_products_first(self):
        run = {"_id": "run-1", "taxonomy_tree_hash": "tree-1"}
        run_collection = SimpleNamespace(find_one=AsyncMock(return_value=run))
        item_collection = SimpleNamespace(
            update_many=AsyncMock(return_value=SimpleNamespace(modified_count=0)),
            find_one_and_update=AsyncMock(return_value={"_id": "run-1:NEW", "sku": "NEW"}),
        )
        fake_db = MagicMock()
        fake_db.__getitem__.side_effect = lambda name: {
            reporting.ETSY_PUBLISH_RUNS_COLLECTION: run_collection,
            reporting.ETSY_PUBLISH_RUN_ITEMS_COLLECTION: item_collection,
        }[name]

        with (
            patch.object(reporting, "db", fake_db),
            patch.object(reporting, "_backfill_etsy_publish_recency", AsyncMock()),
            patch.object(reporting, "_seller_taxonomy_context", AsyncMock(return_value=([], {}, "tree-1"))),
            patch.object(
                reporting,
                "_process_etsy_validation_item",
                AsyncMock(return_value={"sku": "NEW", "processing_status": "completed"}),
            ),
            patch.object(reporting, "_refresh_publish_run_counts", AsyncMock(return_value={})),
        ):
            result = await reporting.etsy_publish_process_validation_run(
                "run-1", limit=1, _authorized=None
            )

        self.assertEqual(result["processed"], 1)
        self.assertEqual(
            item_collection.find_one_and_update.await_args.kwargs["sort"],
            reporting.ETSY_PUBLISH_NEWEST_FIRST_SORT,
        )


if __name__ == "__main__":
    unittest.main()
