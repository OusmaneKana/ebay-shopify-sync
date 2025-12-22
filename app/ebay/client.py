import logging
import requests
from app.config import settings

logger = logging.getLogger(__name__)

EBAY_BASE_URL = "https://api.ebay.com"
EBAY_TRADING_URL = "https://api.ebay.com/ws/api.dll"
EBAY_COMPAT_LEVEL = "1209"


class EbayClient:
    def __init__(self):
        # OAuth user token you already use for Sell APIs
        self.token = settings.EBAY_OAUTH_TOKEN

        # Optional, but good to have in case you need them
        # Make sure these exist in your settings, or remove if unused
        # self.app_id = settings.EBAY_APP_ID
        # self.dev_id = settings.EBAY_DEV_ID
        # self.cert_id = settings.EBAY_CERT_ID

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
            # use OAuth user token here ⬇
            "X-EBAY-API-IAF-TOKEN": self.token,
            "Content-Type": "text/xml",
        }
        resp = requests.post(EBAY_TRADING_URL, headers=headers, data=request_xml)
        resp.raise_for_status()
        return resp.text
