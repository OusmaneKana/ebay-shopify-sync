import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from google.cloud import tasks_v2

from app.api.routes import reporting
from app.services import durable_operations


class DurableOperationServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_operation_expands_callback_and_persists_it(self):
        collection = SimpleNamespace(
            create_index=AsyncMock(),
            insert_one=AsyncMock(),
        )
        fake_db = MagicMock()
        fake_db.__getitem__.return_value = collection

        with patch.object(durable_operations, "db", fake_db):
            operation = await durable_operations.create_operation(
                "refresh",
                params={},
                callback_url="https://example.test/operations/{operation_id}/execute",
            )

        self.assertIn(operation["_id"], operation["callback_url"])
        self.assertEqual(operation["status"], "queued")
        collection.insert_one.assert_awaited_once_with(operation)

    async def test_dispatch_uses_configured_queue_callback_and_worker_secret(self):
        client = MagicMock()
        client.queue_path.return_value = "projects/p/locations/l/queues/q"
        client.create_task = AsyncMock(return_value=SimpleNamespace(name="tasks/task-1"))
        operation = {
            "_id": "op-1",
            "callback_url": "https://service.test/reporting/etsy-publish/operations/op-1/execute",
        }

        with (
            patch.object(tasks_v2, "CloudTasksAsyncClient", return_value=client),
            patch.object(durable_operations.settings, "GOOGLE_CLOUD_PROJECT", "p"),
            patch.object(durable_operations.settings, "CLOUD_TASKS_LOCATION", "l"),
            patch.object(durable_operations.settings, "GOOGLE_CLOUD_REGION", None),
            patch.object(durable_operations.settings, "CLOUD_TASKS_QUEUE", "q"),
            patch.object(durable_operations.settings, "CLOUD_TASKS_WORKER_SECRET", "secret\n"),
            patch.object(durable_operations.settings, "CLOUD_TASKS_SERVICE_ACCOUNT_EMAIL", None),
        ):
            task_name = await durable_operations.dispatch_operation(operation)

        self.assertEqual(task_name, "tasks/task-1")
        request = client.create_task.await_args.kwargs["task"]["http_request"]
        self.assertEqual(request["url"], operation["callback_url"])
        self.assertEqual(request["headers"]["X-Worker-Secret"], "secret")


class EtsyDurableWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_creates_deterministic_run_then_continues_validation(self):
        operation = {
            "_id": "op-1",
            "kind": "prepare",
            "phase": "queued",
            "params": {"all": True, "price_markup_percent": 5},
            "progress": {},
        }
        create_run = AsyncMock(
            return_value={"run_id": "etsy-run-op-1", "counts": {"queued": 12}}
        )

        with patch.object(reporting, "etsy_publish_create_validation_run", create_run):
            should_continue, updates = await reporting._advance_etsy_operation(operation)

        self.assertTrue(should_continue)
        self.assertEqual(updates["phase"], "validate")
        self.assertEqual(updates["run_id"], "etsy-run-op-1")
        self.assertEqual(
            create_run.await_args.kwargs["body"]["_run_id"],
            "etsy-run-op-1",
        )

    async def test_validation_dispatches_another_step_while_items_remain(self):
        operation = {
            "_id": "op-2",
            "kind": "validate",
            "phase": "validate",
            "params": {"run_id": "run-1", "batch_size": 3},
            "progress": {},
        }
        process = AsyncMock(return_value={"counts": {"queued": 9, "creation_pending": 3}})

        with patch.object(reporting, "etsy_publish_process_validation_run", process):
            should_continue, updates = await reporting._advance_etsy_operation(operation)

        self.assertTrue(should_continue)
        self.assertEqual(updates["progress"]["counts"]["queued"], 9)
        self.assertEqual(process.await_args.kwargs["limit"], 3)

    async def test_publish_checkpoints_requested_quantity_across_batches(self):
        operation = {
            "_id": "op-3",
            "kind": "publish",
            "phase": "publish",
            "params": {"run_id": "run-1", "count": 15, "batch_size": 10},
            "progress": {},
        }
        create = AsyncMock(return_value={"processed": 10, "counts": {"creation_pending": 5}})

        with patch.object(reporting, "etsy_publish_create_run_drafts", create):
            should_continue, updates = await reporting._advance_etsy_operation(operation)

        self.assertTrue(should_continue)
        self.assertEqual(updates["progress"]["attempted"], 10)
        self.assertEqual(updates["progress"]["remaining"], 5)
        self.assertEqual(create.await_args.kwargs["body"]["limit"], 10)

        operation.update(updates)
        create.return_value = {"processed": 5, "counts": {"creation_pending": 0}}
        with patch.object(reporting, "etsy_publish_create_run_drafts", create):
            should_continue, updates = await reporting._advance_etsy_operation(operation)

        self.assertFalse(should_continue)
        self.assertEqual(updates["phase"], "completed")
        self.assertEqual(updates["result"]["attempted"], 15)


if __name__ == "__main__":
    unittest.main()
