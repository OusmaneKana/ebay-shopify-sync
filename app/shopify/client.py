import logging
import asyncio
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientConnectorError
from aiolimiter import AsyncLimiter
from app.config import settings

logger = logging.getLogger(__name__)
API_VERSION = "2023-10"  # Reverted to ensure compatibility

class ShopifyClient:
    def __init__(self, api_key=None, password=None, store_url=None):
        # Use provided params or fall back to the production store settings.
        self.api_key = api_key or settings.SHOPIFY_API_KEY_PROD
        self.password = password or settings.SHOPIFY_PASSWORD_PROD
        self.store_url = store_url or settings.SHOPIFY_STORE_URL_PROD

        self.base_url = (
            f"https://{self.api_key}:{self.password}"
            f"@{self.store_url}/admin/api/{API_VERSION}"
        )
        
        # Rate limiter: 2 requests per second
        self.limiter = AsyncLimiter(2, 1)
        logger.debug(f"ShopifyClient initialized for store: {self.store_url}")

    def _url(self, endpoint: str) -> str:
        # endpoint examples: "products.json", "variants/123456789.json"
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        return f"{self.base_url}/{endpoint}"

    async def _request_with_retries(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> Dict[str, Any]:
        """Perform an HTTP request to Shopify with basic retry on transient connect errors.

        Retries on aiohttp.ClientConnectorError and generic OSErrors that indicate
        temporary network problems (e.g. TLS handshake / DNS glitches in Cloud Run).
        """

        url = self._url(endpoint)
        attempt = 0

        while True:
            attempt += 1
            try:
                async with self.limiter:
                    async with aiohttp.ClientSession() as session:
                        req_kwargs: Dict[str, Any] = {}
                        if params is not None:
                            req_kwargs["params"] = params
                        if json is not None:
                            req_kwargs["json"] = json

                        logger.debug(f"{method} request to {endpoint} (attempt {attempt})")
                        async with session.request(method, url, **req_kwargs) as resp:
                            self.last_response = resp  # Store the response
                            text = await resp.text()

                            if resp.status >= 400:
                                logger.error(
                                    "Shopify %s Error %s: %s | Endpoint: %s",
                                    method,
                                    resp.status,
                                    text,
                                    endpoint,
                                )
                                if params:
                                    logger.debug("Query params: %s", params)
                                if json is not None:
                                    logger.debug("Payload: %s", json)
                            else:
                                logger.debug("%s %s - Status: %s", method, endpoint, resp.status)

                            # Try to decode JSON; if it fails, return raw text
                            try:
                                return await resp.json()
                            except Exception:
                                return {"raw": text}

            except (ClientConnectorError, OSError) as e:
                # Network-level issue: consider retrying a few times
                logger.warning(
                    "Shopify %s %s failed on attempt %s due to network error: %s",
                    method,
                    endpoint,
                    attempt,
                    e,
                )
                if attempt >= max_retries:
                    logger.error(
                        "Giving up on Shopify %s %s after %s attempts (last error: %s)",
                        method,
                        endpoint,
                        attempt,
                        e,
                    )
                    raise

                delay = base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        return await self._request_with_retries("GET", endpoint, params=params)

    async def post(self, endpoint: str, payload: dict) -> dict:
        return await self._request_with_retries("POST", endpoint, json=payload)

    async def put(self, endpoint: str, payload: dict) -> dict:
        return await self._request_with_retries("PUT", endpoint, json=payload)

    async def delete(self, endpoint: str) -> dict:
        return await self._request_with_retries("DELETE", endpoint)
