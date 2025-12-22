import requests
from app.config import settings

API_VERSION = "2023-10"  # or newer if you want

class ShopifyClient:
    def __init__(self):
        # For private app style auth: https://API_KEY:PASSWORD@STORE/admin/api/...
        self.base_url = (
            f"https://{settings.SHOPIFY_API_KEY}:{settings.SHOPIFY_PASSWORD}"
            f"@{settings.SHOPIFY_STORE_URL}/admin/api/{API_VERSION}"
        )

    def _url(self, endpoint: str) -> str:
        # endpoint examples: "products.json", "variants/123456789.json"
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        return f"{self.base_url}/{endpoint}"

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = self._url(endpoint)
        resp = requests.get(url, params=params)
        self.last_response = resp  # Store the response
        if resp.status_code >= 400:
            print(f"Shopify GET Error {resp.status_code}: {resp.text}")
        return resp.json()

    def post(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        resp = requests.post(url, json=payload)
        self.last_response = resp  # Store the response
        if resp.status_code >= 400:
            print(f"Shopify POST Error {resp.status_code}: {resp.text}")
        return resp.json()

    def put(self, endpoint: str, payload: dict) -> dict:
        url = self._url(endpoint)
        resp = requests.put(url, json=payload)
        self.last_response = resp  # Store the response
        if resp.status_code >= 400:
            print(f"Shopify PUT Error {resp.status_code}: {resp.text}")
        return resp.json()

    def delete(self, endpoint: str) -> dict:
        url = self._url(endpoint)
        resp = requests.delete(url)
        self.last_response = resp  # Store the response
        if resp.status_code >= 400:
            print(f"Shopify DELETE Error {resp.status_code}: {resp.text}")
        return resp.json()
