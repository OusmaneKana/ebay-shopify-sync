import logging
import aiohttp
from aiolimiter import AsyncLimiter
from app.config import settings

logger = logging.getLogger(__name__)
API_VERSION = "2023-10"  # Reverted to ensure compatibility

class ShopifyClient:
    def __init__(self, api_key=None, password=None, store_url=None):
        # Use provided params or fall back to dev settings
        self.api_key = api_key or settings.SHOPIFY_API_KEY
        self.password = password or settings.SHOPIFY_PASSWORD
        self.store_url = store_url or settings.SHOPIFY_STORE_URL

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

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = self._url(endpoint)
        logger.debug(f"GET request to {endpoint}")
        async with self.limiter:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    self.last_response = resp  # Store the response
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error(f"Shopify GET Error {resp.status}: {text} | Endpoint: {endpoint}")
                        if params:
                            logger.debug(f"Query params: {params}")
                    else:
                        logger.debug(f"GET {endpoint} - Status: {resp.status}")
                    return await resp.json()

    async def post(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        logger.debug(f"POST request to {endpoint}")
        async with self.limiter:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    self.last_response = resp  # Store the response
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error(f"Shopify POST Error {resp.status}: {text} | Endpoint: {endpoint}")
                        logger.debug(f"Payload: {payload}")
                    else:
                        logger.debug(f"POST {endpoint} - Status: {resp.status}")
                    return await resp.json()

    async def put(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        logger.debug(f"PUT request to {endpoint}")
        async with self.limiter:
            async with aiohttp.ClientSession() as session:
                async with session.put(url, json=payload) as resp:
                    self.last_response = resp  # Store the response
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error(f"Shopify PUT Error {resp.status}: {text} | Endpoint: {endpoint}")
                        logger.debug(f"Payload: {payload}")
                    else:
                        logger.debug(f"PUT {endpoint} - Status: {resp.status}")
                    return await resp.json()

    async def delete(self, endpoint: str) -> dict:
        url = self._url(endpoint)
        logger.debug(f"DELETE request to {endpoint}")
        async with self.limiter:
            async with aiohttp.ClientSession() as session:
                async with session.delete(url) as resp:
                    self.last_response = resp  # Store the response
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error(f"Shopify DELETE Error {resp.status}: {text} | Endpoint: {endpoint}")
                    else:
                        logger.debug(f"DELETE {endpoint} - Status: {resp.status}")
                    return await resp.json()
