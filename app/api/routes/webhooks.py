import hashlib
import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.services.ebay_webhook_service import handle_ebay_order_webhook, handle_ebay_item_listed
from app.services.etsy_webhook_service import handle_etsy_event, verify_etsy_signature

router = APIRouter()
logger = logging.getLogger(__name__)

EBAY_VERIFICATION_TOKEN = "RWEnfkQjE6dEnRV3wcympNKnJUaxtfFpQVmBm8"  # store in env var in prod
ENDPOINT_URL = "https://my-service-578984177720.us-central1.run.app/webhooks/ebay/orders" 
# ENDPOINT_URL = "https://ace-samples-located-cook.trycloudflare.com/webhooks/ebay/orders" 

  # MUST match destination endpoint EXACTLY


def _xml_elem_to_dict(elem):
    """Recursively convert an ElementTree element to a plain dict."""
    result = {}
    for child in elem:
        tag = child.tag.split("}")[-1]  # strip xmlns
        val = _xml_elem_to_dict(child) if len(child) else (child.text or "").strip()
        if tag in result:
            existing = result[tag]
            if not isinstance(existing, list):
                result[tag] = [existing]
            result[tag].append(val)
        else:
            result[tag] = val
    return result


def _parse_ebay_xml_notification(raw: bytes) -> dict:
    """Parse an eBay platform notification XML body and normalize it."""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return {"_parse_error": True}

    # Unwrap SOAP envelope if present
    if root.tag.split("}")[-1].lower() == "envelope":
        for child in root:
            if child.tag.split("}")[-1].lower() == "body" and len(child):
                root = list(child)[0]
                break

    raw_tag = root.tag.split("}")[-1]
    event_type = raw_tag
    for suffix in ("Notification", "Response"):
        if event_type.endswith(suffix):
            event_type = event_type[: -len(suffix)]
            break

    data = _xml_elem_to_dict(root)
    data["_event_type"] = event_type
    data["_raw_event_tag"] = raw_tag

    # Normalize sale events into the shape handle_ebay_order_webhook expects
    if event_type in ("FixedPriceTransaction", "AuctionCheckoutComplete"):
        tx = (data.get("NotificationDetails") or {}).get("Transaction") or {}
        if isinstance(tx, list):
            tx = tx[0]
        item = tx.get("Item") or {}
        sku = item.get("SKU") or item.get("ApplicationData") or ""
        item_id = item.get("ItemID") or ""
        qty = tx.get("QuantityPurchased") or "1"
        order_id = (
            (tx.get("ContainingOrder") or {}).get("OrderID")
            or tx.get("TransactionID")
            or item_id
        )
        data["order"] = {
            "orderId": order_id,
            "lineItems": [{"sku": sku, "quantity": qty, "itemId": item_id}],
        }

    return data


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
  Receive eBay platform notifications (XML) or order webhooks (JSON).
  Parses the body, detects the event type, and routes accordingly.
  """
  raw_body = await request.body()
  content_type = request.headers.get("content-type", "")

  is_xml = "xml" in content_type or raw_body.lstrip()[:5] in (b"<?xml", b"<soap", b"<Soap", b"<SOAP")

  if is_xml:
    payload = _parse_ebay_xml_notification(raw_body)
  else:
    try:
      payload = json.loads(raw_body)
    except Exception:
      payload = {}

  event_type = payload.get("_event_type", "order")
  print(f"Received eBay webhook | event_type={event_type}")

  # Persist every incoming notification to MongoDB for auditing / replay
  try:
    await db.ebay_notification_events.insert_one({
      "event_type": event_type,
      "payload": payload,
      "raw_body": raw_body.decode("utf-8", errors="replace"),
      "content_type": content_type,
      "status": "received",
      "received_at": datetime.now(timezone.utc),
    })
  except Exception as log_err:
    print(f"Warning: failed to log eBay notification to Mongo: {log_err}")

  shopify_client = ShopifyClient()

  try:
    if event_type == "ItemListed":
      asyncio.create_task(handle_ebay_item_listed(payload))
    else:
      asyncio.create_task(handle_ebay_order_webhook(payload, shopify_client, make_unavailable=make_unavailable))
  except Exception as e:
    print("Failed to schedule webhook processing:", e)
    return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

  return JSONResponse({"ok": True, "queued": True, "event_type": event_type})


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

  event_doc = {
    "source": "etsy",
    "webhook": "etsy_event",
    "webhook_id": webhook_id,
    "webhook_timestamp": webhook_timestamp,
    "webhook_signature": webhook_signature,
    "content_type": request.headers.get("content-type"),
    "headers": dict(request.headers),
    "raw_body": raw_body.decode("utf-8", errors="replace"),
    "status": "received",
    "received_at": datetime.now(timezone.utc),
  }
  insert_result = await db.etsy_notification_events.insert_one(event_doc)

  # Temporary diagnostic logging to confirm Etsy signature input fields.
  logger.warning(
    "ETSY_WEBHOOK_DEBUG headers=%s body_prefix=%s",
    dict(request.headers),
    raw_body[:500].decode("utf-8", errors="replace"),
  )

  valid, reason = verify_etsy_signature(
    raw_body=raw_body,
    webhook_id=webhook_id,
    webhook_timestamp=webhook_timestamp,
    webhook_signature=webhook_signature,
  )

  if not valid:
    logger.warning(
      "ETSY_WEBHOOK_VERIFY_FAILED reason=%s webhook_id=%s webhook_timestamp=%s webhook_signature=%s",
      reason,
      webhook_id,
      webhook_timestamp,
      webhook_signature,
    )
    await db.etsy_notification_events.update_one(
      {"_id": insert_result.inserted_id},
      {
        "$set": {
          "status": "invalid_signature",
          "reason": reason,
          "processed_at": datetime.now(timezone.utc),
        }
      },
    )
    return JSONResponse({"ok": False, "error": "invalid_signature", "reason": reason}, status_code=401)

  try:
    payload = await request.json()
  except Exception:
    await db.etsy_notification_events.update_one(
      {"_id": insert_result.inserted_id},
      {
        "$set": {
          "status": "invalid_json",
          "processed_at": datetime.now(timezone.utc),
        }
      },
    )
    return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

  result = await handle_etsy_event(
    payload=payload,
    raw_body=raw_body.decode("utf-8", errors="replace"),
    webhook_id=webhook_id,
    webhook_timestamp=webhook_timestamp,
  )

  await db.etsy_notification_events.update_one(
    {"_id": insert_result.inserted_id},
    {
      "$set": {
        "status": "processed",
        "event_type": payload.get("event_type"),
        "shop_id": payload.get("shop_id"),
        "resource_url": payload.get("resource_url"),
        "processed_at": datetime.now(timezone.utc),
      }
    },
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