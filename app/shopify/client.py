import aiohttp
from app.config import settings

API_VERSION = "2023-10"  # or newer if you want

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

    def _url(self, endpoint: str) -> str:
        # endpoint examples: "products.json", "variants/123456789.json"
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        return f"{self.base_url}/{endpoint}"

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = self._url(endpoint)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                self.last_response = resp  # Store the response
                if resp.status >= 400:
                    text = await resp.text()
                    print(f"Shopify GET Error {resp.status}: {text}")
                return await resp.json()

    async def post(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                self.last_response = resp  # Store the response
                if resp.status >= 400:
                    text = await resp.text()
                    print(f"Shopify POST Error {resp.status}: {text}")
                return await resp.json()

    async def put(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=payload) as resp:
                self.last_response = resp  # Store the response
                if resp.status >= 400:
                    text = await resp.text()
                    print(f"Shopify PUT Error {resp.status}: {text}")
                return await resp.json()

    async def delete(self, endpoint: str) -> dict:
        url = self._url(endpoint)
        async with aiohttp.ClientSession() as session:
            async with session.delete(url) as resp:
                self.last_response = resp  # Store the response
                if resp.status >= 400:
                    text = await resp.text()
                    print(f"Shopify DELETE Error {resp.status}: {text}")
                return await resp.json()
