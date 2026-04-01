import hashlib
import asyncio
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from app.shopify.client import ShopifyClient
from app.services.ebay_webhook_service import handle_ebay_order_webhook
from app.services.etsy_webhook_service import handle_etsy_event, verify_etsy_signature

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


@router.post("/etsy/events")
async def etsy_events_webhook(request: Request):
  """
  Receive Etsy webhook events.

  - Validates Etsy signature headers
  - Parses JSON payload
  - Persists event for downstream processing
  """
  raw_body = await request.body()
  webhook_id = request.headers.get("webhook-id")
  webhook_timestamp = request.headers.get("webhook-timestamp")
  webhook_signature = request.headers.get("webhook-signature")

  valid, reason = verify_etsy_signature(
    raw_body=raw_body,
    webhook_id=webhook_id,
    webhook_timestamp=webhook_timestamp,
    webhook_signature=webhook_signature,
  )

  if not valid:
    return JSONResponse({"ok": False, "error": "invalid_signature", "reason": reason}, status_code=401)

  try:
    payload = await request.json()
  except Exception:
    return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

  result = await handle_etsy_event(
    payload=payload,
    raw_body=raw_body.decode("utf-8", errors="replace"),
    webhook_id=webhook_id,
    webhook_timestamp=webhook_timestamp,
  )

  return JSONResponse(result, status_code=200)


@router.get("/ebay/callback")
async def ebay_auth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")
    error_desc = request.query_params.get("error_description")

    # eBay can redirect back with an error if user cancels/denies
    if error:
        return JSONResponse(
            {"ok": False, "error": error, "error_description": error_desc},
            status_code=400,
        )

    if not code:
        raise HTTPException(status_code=400, detail="Missing ?code in callback")

    print("Received eBay OAuth callback:", {"code": code[:20] + "...", "state": state})
    return JSONResponse({"ok": True, "code_received": True})