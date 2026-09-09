import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.etsy_publish_queue import enqueue_etsy_publish_job


class EtsyPublishQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_sku_is_not_enqueued(self):
        result = await enqueue_etsy_publish_job("  ")

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "missing_sku")

    async def test_product_must_be_normalized(self):
        fake_db = SimpleNamespace(
            product_normalized=SimpleNamespace(find_one=AsyncMock(return_value=None))
        )

        with patch("app.services.etsy_publish_queue.db", fake_db):
            result = await enqueue_etsy_publish_job("SKU-1")

        self.assertFalse(result["queued"])
        self.assertEqual(result["reason"], "product_not_normalized")

    async def test_new_job_is_durably_enqueued(self):
        product_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": "SKU-1", "channels": {}})
        )
        queue_collection = SimpleNamespace(
            update_one=AsyncMock(return_value=SimpleNamespace(upserted_id="etsy-publish:SKU-1")),
            find_one=AsyncMock(return_value={"_id": "etsy-publish:SKU-1", "status": "queued", "attempts": 0}),
        )
        fake_db = MagicMock()
        fake_db.product_normalized = product_collection
        fake_db.__getitem__.return_value = queue_collection

        with patch("app.services.etsy_publish_queue.db", fake_db):
            result = await enqueue_etsy_publish_job(
                "SKU-1",
                source="ebay_itemlisted",
                source_job_id="source-1",
            )

        self.assertTrue(result["queued"])
        self.assertEqual(result["job_id"], "etsy-publish:SKU-1")
        update = queue_collection.update_one.await_args.args[1]
        self.assertEqual(update["$setOnInsert"]["status"], "queued")
        self.assertEqual(update["$set"]["source"], "ebay_itemlisted")


if __name__ == "__main__":
    unittest.main()
