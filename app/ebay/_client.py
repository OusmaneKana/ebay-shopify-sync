import requests
from app.config import settings

EBAY_BASE_URL = "https://api.ebay.com"

class EbayClient:
    def __init__(self):
        self.token = settings.EBAY_OAUTH_TOKEN

    def get(self, endpoint, params=None):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        url = f"{EBAY_BASE_URL}{endpoint}"
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print("eBay API Error:", response.text)

        return response.json()
