import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, patch

import app.services.multichannel_sync_service as subject


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class FakeAsyncClient:
    def __init__(self, response, calls, **kwargs):
        self.response = response
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def patch(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class EtsyDeactivationTests(TestCase):
    def _run(self, response):
        calls = []
        client_factory = lambda **kwargs: FakeAsyncClient(response, calls, **kwargs)
        doc = {
            "_id": "SKU-1",
            "channels": {"etsy": {"listing_id": 4458539428, "shop_id": 12345}},
        }
        with (
            patch.object(subject, "get_valid_etsy_token", AsyncMock(return_value="token")),
            patch.object(subject.settings, "ETSY_CLIENT_ID", "client"),
            patch.object(subject.settings, "ETSY_CLIENT_SECRET", "secret"),
            patch.object(subject.httpx, "AsyncClient", client_factory),
        ):
            result = asyncio.run(subject._push_etsy_quantity(doc, 0))
        return result, calls

    def test_zero_quantity_deactivates_listing(self):
        result, calls = self._run(FakeResponse(status_code=200))

        self.assertEqual(result, (True, None))
        self.assertEqual(len(calls), 1)
        url, kwargs = calls[0]
        self.assertEqual(
            url,
            "https://openapi.etsy.com/v3/application/shops/12345/listings/4458539428",
        )
        self.assertEqual(kwargs["data"], {"state": "inactive"})
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/x-www-form-urlencoded")

    def test_deactivation_error_includes_etsy_message(self):
        result, _ = self._run(
            FakeResponse(status_code=400, payload={"error": "listing cannot be deactivated"})
        )

        self.assertFalse(result[0])
        self.assertIn("etsy_listing_deactivate_failed:400", result[1])
        self.assertIn("listing cannot be deactivated", result[1])
