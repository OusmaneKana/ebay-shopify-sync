import logging
import requests
from app.config import settings

logger = logging.getLogger(__name__)

EBAY_BASE_URL = "https://api.ebay.com"
EBAY_TRADING_URL = "https://api.ebay.com/ws/api.dll"
EBAY_COMPAT_LEVEL = "1209"


class EbayClient:
    def __init__(self):
        # Start with the env var token as fallback; replaced by get_valid_token() at runtime
        self.token = settings.EBAY_OAUTH_TOKEN

    async def ensure_fresh_token(self):
        """Fetch a valid token from MongoDB (refreshing if expired). Call at the start of sync operations."""
        from app.services.ebay_auth_service import get_valid_token
        self.token = await get_valid_token()

    def get(self, endpoint, params=None):
        """
        REST APIs (Sell, Commerce, etc.)
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        url = f"{EBAY_BASE_URL}{endpoint}"
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error(f"eBay API Error (status {response.status_code}): {response.text}")

        return response.json()

    def trading_post(self, call_name: str, request_xml: str):
        headers = {
            "X-EBAY-API-CALL-NAME": call_name,
            "X-EBAY-API-COMPATIBILITY-LEVEL": EBAY_COMPAT_LEVEL,
            "X-EBAY-API-DEV-NAME": settings.EBAY_DEV_ID,
            "X-EBAY-API-APP-NAME": settings.EBAY_APP_ID,
            "X-EBAY-API-CERT-NAME": settings.EBAY_CERT_ID,
            "X-EBAY-API-SITEID": "0",
            "X-EBAY-API-IAF-TOKEN": self.token,
            "Content-Type": "text/xml",
        }
        resp = requests.post(EBAY_TRADING_URL, headers=headers, data=request_xml)
        resp.raise_for_status()
        return resp.text

