import base64
import hashlib
import hmac
import logging
import time
from typing import Any

from app.config import settings
from app.database.mongo import db
from app.services.multichannel_sync_service import (
    get_etsy_receipt_transactions_from_payload,
    ingest_sale_event,
    run_worker_batch,
)

logger = logging.getLogger(__name__)


def _decode_signing_secret(secret: str) -> bytes:
    """Decode Etsy signing secret (format: whsec_<base64>)."""
    if not secret:
        raise ValueError("Missing Etsy webhook signing secret")

    if secret.startswith("whsec_"):
        secret = secret[len("whsec_") :]

    try:
        return base64.b64decode(secret)
    except Exception as e:
        raise ValueError("Invalid Etsy webhook signing secret format") from e


def _extract_signatures(signature_header: str) -> list[str]:
    """Extract signature entries from Etsy webhook-signature header."""
    if not signature_header:
        return []

    entries = [part.strip() for part in signature_header.split(",") if part.strip()]
    signatures = []
    for entry in entries:
        # Etsy commonly sends: "v1,<base64-signature>".
        # Skip a bare version token like "v1".
        if entry.lower().startswith("v") and entry[1:].isdigit():
            continue

        # Also support key-value formats like "v1=<base64-signature>".
        if "=" in entry:
            key, value = entry.split("=", 1)
            key = key.strip().lower()
            value = value.strip()
            if key.startswith("v") and key[1:].isdigit() and value:
                signatures.append(value)
                continue

        # Keep the full token so base64 padding '=' is preserved.
        signatures.append(entry)
    return signatures


def verify_etsy_signature(
    raw_body: bytes,
    webhook_id: str | None,
    webhook_timestamp: str | None,
    webhook_signature: str | None,
) -> tuple[bool, str | None]:
    """Verify Etsy webhook signature and replay-window timestamp."""
    if not settings.ETSY_WEBHOOK_SIGNING_SECRET:
        return False, "ETSY_WEBHOOK_SIGNING_SECRET is not configured"

    if not webhook_id or not webhook_timestamp or not webhook_signature:
        return False, "Missing required Etsy webhook signature headers"

    try:
        ts = int(webhook_timestamp)
    except ValueError:
        return False, "Invalid webhook-timestamp header"

    now = int(time.time())
    tolerance = max(0, int(settings.ETSY_WEBHOOK_TOLERANCE_SECONDS))
    if abs(now - ts) > tolerance:
        return False, "Stale webhook timestamp"

    signed_content = f"{webhook_id}.{webhook_timestamp}.".encode("utf-8") + raw_body

    secret_bytes = _decode_signing_secret(settings.ETSY_WEBHOOK_SIGNING_SECRET)
    digest = hmac.new(secret_bytes, signed_content, hashlib.sha256).digest()
    expected_sig = base64.b64encode(digest).decode("utf-8")

    received_sigs = _extract_signatures(webhook_signature)
    if not received_sigs:
        return False, "Invalid webhook-signature header"

    valid = any(hmac.compare_digest(expected_sig, sig) for sig in received_sigs)
    if not valid:
        return False, "Signature mismatch"

    return True, None


async def handle_etsy_event(
    payload: dict[str, Any],
    raw_body: str,
    webhook_id: str | None,
    webhook_timestamp: str | None,
) -> dict[str, Any]:
    """Persist Etsy event for downstream processing."""
    event_type = payload.get("event_type")
    shop_id = payload.get("shop_id")
    resource_url = payload.get("resource_url")

    sync_result: dict[str, Any] | None = None
    event_type_normalized = str(event_type or "").lower()
    is_sale_like = any(token in event_type_normalized for token in ["sale", "sold", "transaction", "order"]) 

    if is_sale_like:
        base_event_id = f"etsy-event:{webhook_id or 'noid'}"
        receipt_transactions = await get_etsy_receipt_transactions_from_payload(payload)

        if receipt_transactions:
            per_tx_results: list[dict[str, Any]] = []
            for idx, tx in enumerate(receipt_transactions, start=1):
                tx_payload = dict(payload)
                tx_payload["transaction"] = tx
                if tx.get("listing_id") and not tx_payload.get("listing_id"):
                    tx_payload["listing_id"] = tx.get("listing_id")
                if tx.get("sku") and not tx_payload.get("sku"):
                    tx_payload["sku"] = tx.get("sku")

                tx_id = tx.get("transaction_id") or idx
                tx_result = await ingest_sale_event(
                    source_channel="etsy",
                    payload=tx_payload,
                    quantity_sold=max(1, int(tx.get("quantity") or 1)),
                    explicit_sku=None,
                    explicit_event_id=f"{base_event_id}:tx:{tx_id}",
                    enqueue_jobs_flag=True,
                )
                per_tx_results.append(tx_result)

            sync_result = {
                "event_id": base_event_id,
                "status": "receipt_transactions_ingested",
                "transactions": len(receipt_transactions),
                "results": per_tx_results,
            }
        else:
            # Etsy payload shape can vary; resolve SKU by listing link in sync service.
            sync_result = await ingest_sale_event(
                source_channel="etsy",
                payload=payload,
                quantity_sold=1,
                explicit_sku=None,
                explicit_event_id=base_event_id,
                enqueue_jobs_flag=True,
            )

        # Small worker pass for near-real-time propagation.
        worker_result = await run_worker_batch(limit=10)
        sync_result["worker"] = worker_result

    doc = {
        "webhook": "etsy_event",
        "source": "etsy",
        "event_type": event_type,
        "shop_id": shop_id,
        "resource_url": resource_url,
        "webhook_id": webhook_id,
        "webhook_timestamp": webhook_timestamp,
        "payload": payload,
        "raw_body": raw_body,
        "sync_result": sync_result,
        "received_at_unix": int(time.time()),
    }

    await db.sync_log.insert_one(doc)

    logger.info(
        "Processed Etsy webhook | event_type=%s | shop_id=%s | webhook_id=%s",
        event_type,
        shop_id,
        webhook_id,
    )

    return {
        "ok": True,
        "queued": True,
        "event_type": event_type,
        "shop_id": shop_id,
        "sync_result": sync_result,
    }
