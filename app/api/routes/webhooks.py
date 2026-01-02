import hashlib
import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.shopify.client import ShopifyClient
from app.services.ebay_webhook_service import handle_ebay_order_webhook

router = APIRouter()

EBAY_VERIFICATION_TOKEN = "RWEnfkQjE6dEnRV3wcympNKnJUaxtfFpQVmBm8"  # store in env var in prod
ENDPOINT_URL = "https://ebay-shopify-sync-1096200401246.us-central1.run.app/webhooks/ebay/orders" 
# ENDPOINT_URL = "https://ace-samples-located-cook.trycloudflare.com/webhooks/ebay/orders" 

  # MUST match destination endpoint EXACTLY


@router.get("/ebay/orders")
async def ebay_challenge(challenge_code: str):
  raw = f"{challenge_code}{EBAY_VERIFICATION_TOKEN}{ENDPOINT_URL}"
  digest = hashlib.sha256(raw.encode("utf-8"))
  digest = digest.hexdigest()

  print("challenge_code:", challenge_code)
  print("verification_token:", EBAY_VERIFICATION_TOKEN)
  print("endpoint_url:", ENDPOINT_URL)
  print("raw:", raw)
  print("sha:", digest)
  return JSONResponse({"challengeResponse": digest})


@router.post("/ebay/orders")
async def ebay_order_webhook(request: Request, make_unavailable: bool = True):
  """
  Receive eBay order webhooks, enqueue processing, and return quickly.

  - Reads JSON payload from eBay
  - Starts background task to call `handle_ebay_order_webhook`
  - Returns a JSON response acknowledging receipt
  """
  payload = await request.json()
  print("Received eBay webhook payload:", payload)

  # Create a shopify client for processing
  shopify_client = ShopifyClient()

  # Run processing in background (non-blocking)
  try:
    asyncio.create_task(handle_ebay_order_webhook(payload, shopify_client, make_unavailable=make_unavailable))
  except Exception as e:
    print("Failed to schedule webhook processing:", e)
    return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

  return JSONResponse({"ok": True, "queued": True})
