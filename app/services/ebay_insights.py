"""Read-only eBay store intelligence collection and reporting helpers."""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import logging
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html import escape as xml_escape
from zoneinfo import ZoneInfo

import httpx
from bson import BSON
from pymongo import ReturnDocument

from app.config import settings
from app.database.mongo import db
from app.services.ebay_auth_service import get_valid_token
from app.services.insight_metrics import build_category_performance, category_group

API_BASE = "https://api.ebay.com"
TRADING_URL = "https://api.ebay.com/ws/api.dll"
TRADING_NAMESPACE = {"e": "urn:ebay:apis:eBLBaseComponents"}
PACIFIC = ZoneInfo("America/Los_Angeles")
LISTING_BATCH_SIZE = 200

TRAFFIC_METRICS = (
    "TOTAL_IMPRESSION_TOTAL",
    "LISTING_IMPRESSION_TOTAL",
    "LISTING_IMPRESSION_SEARCH_RESULTS_PAGE",
    "LISTING_IMPRESSION_STORE",
    "LISTING_VIEWS_TOTAL",
    "LISTING_VIEWS_SOURCE_SEARCH_RESULTS_PAGE",
    "LISTING_VIEWS_SOURCE_STORE",
    "LISTING_VIEWS_SOURCE_DIRECT",
    "LISTING_VIEWS_SOURCE_OFF_EBAY",
    "LISTING_VIEWS_SOURCE_OTHER_EBAY",
    "CLICK_THROUGH_RATE",
    "SALES_CONVERSION_RATE",
    "TRANSACTION",
)

CPS_METRICS = (
    "impressions",
    "clicks",
    "ad_fees",
    "sales",
    "sale_amount",
    "ctr",
    "avg_cost_per_sale",
)

CPC_METRICS = (
    "cpc_impressions",
    "cpc_clicks",
    "cpc_attributed_sales",
    "cpc_sale_amount_listingsite_currency",
    "cpc_ad_fees_listingsite_currency",
    "cost_per_click_listingsite_currency",
    "cpc_return_on_ad_spend",
    "cpc_conversion_rate",
    "cpc_avg_cost_per_sale_listingsite_currency",
)

MARKETPLACE_CURRENCIES = {
    "EBAY_US": "USD",
    "EBAY_GB": "GBP",
    "EBAY_AU": "AUD",
    "EBAY_CA": "CAD",
    "EBAY_DE": "EUR",
    "EBAY_ES": "EUR",
    "EBAY_FR": "EUR",
    "EBAY_IT": "EUR",
}

log = logging.getLogger(__name__)


class EbayApiError(RuntimeError):
    """Safe, user-facing eBay error without response secrets."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _number(value, default=0.0) -> float:
    if value is None or value == "":
        return default
    text = str(value).strip().replace(",", "").replace("%", "")
    text = re.sub(r"^[^0-9+\-.]+", "", text)
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return default


def _integer(value, default=0) -> int:
    try:
        return int(_number(value, default))
    except (TypeError, ValueError):
        return default


def _aware_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def period_bounds(days: int, now: datetime | None = None) -> tuple[date, date, datetime, datetime]:
    now = now or datetime.now(timezone.utc)
    local_now = now.astimezone(PACIFIC)
    end_date = local_now.date()
    start_date = end_date - timedelta(days=days - 1)
    start_at = datetime.combine(start_date, time.min, PACIFIC).astimezone(timezone.utc)
    return start_date, end_date, start_at, now


def _api_detail(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except Exception:
        return None
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if isinstance(errors, list) and errors:
        error = errors[0]
        if isinstance(error, dict):
            return error.get("longMessage") or error.get("message")
    return None


async def ebay_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    token: str,
    *,
    marketplace_id: str,
    params: dict | None = None,
    json: dict | None = None,
    accept: str = "application/json",
    attempts: int = 5,
) -> httpx.Response:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
    }
    for attempt in range(attempts):
        response = await client.request(
            method,
            API_BASE + path,
            headers=headers,
            params=params,
            json=json,
        )
        if response.status_code in (429, 500, 502, 503, 504) and attempt < attempts - 1:
            try:
                delay = max(1.0, float(response.headers.get("Retry-After", 2 ** attempt)))
            except ValueError:
                delay = float(2 ** attempt)
            await asyncio.sleep(min(delay, 30))
            continue
        if response.status_code in (401, 403):
            raise EbayApiError(
                "eBay Insights needs Analytics and Marketing permission. Reauthorize eBay, then refresh again.",
                response.status_code,
            )
        if response.status_code == 429:
            raise EbayApiError("eBay is rate-limiting Insights. Wait a minute and retry.", 429)
        if response.status_code >= 400:
            detail = _api_detail(response)
            suffix = f" {detail}" if detail else ""
            raise EbayApiError(f"eBay returned HTTP {response.status_code}.{suffix}", response.status_code)
        return response
    raise EbayApiError("eBay is temporarily unavailable. Retry later.")


def parse_traffic_report(payload: dict) -> dict[str, dict[str, float | None]]:
    """Map the ordered Analytics response arrays into listing keyed metrics."""
    header = payload.get("header") or {}
    metric_keys = [str(row.get("key")) for row in header.get("metrics", [])]
    result: dict[str, dict[str, float | None]] = {}
    for record in payload.get("records", []):
        dimensions = record.get("dimensionValues") or []
        if not dimensions:
            continue
        listing_id = str(dimensions[0].get("value") or "").strip()
        if not listing_id:
            continue
        values = record.get("metricValues") or []
        metrics: dict[str, float | None] = {}
        for position, key in enumerate(metric_keys):
            cell = values[position] if position < len(values) else {}
            metrics[key] = _number(cell.get("value")) if cell.get("applicable", True) else None
        result[listing_id] = metrics
    return result


def _traffic_day(value: str) -> str | None:
    value = str(value or "").strip()
    iso = re.search(r"\d{4}-\d{2}-\d{2}", value)
    if iso:
        return iso.group(0)
    match = re.search(r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\w{3})\s+(\d{1,2}).*?(\d{4})$", value)
    if match:
        try:
            return datetime.strptime(" ".join(match.groups()), "%b %d %Y").date().isoformat()
        except ValueError:
            return None
    return None


def parse_daily_traffic(payload: dict) -> dict[str, dict[str, float]]:
    header = payload.get("header") or {}
    keys = [str(row.get("key")) for row in header.get("metrics", [])]
    result = {}
    for record in payload.get("records", []):
        dimensions = record.get("dimensionValues") or []
        day = _traffic_day(dimensions[0].get("value")) if dimensions else None
        if not day:
            continue
        values = record.get("metricValues") or []
        result[day] = {
            key: _number(values[index].get("value"))
            if index < len(values) and values[index].get("applicable", True)
            else 0.0
            for index, key in enumerate(keys)
        }
    return result


async def fetch_listing_traffic(
    client: httpx.AsyncClient,
    token: str,
    listing_ids: list[str],
    start_date: date,
    end_date: date,
    marketplace_id: str,
) -> dict[str, dict[str, float | None]]:
    result = {}
    date_filter = f"date_range:[{start_date:%Y%m%d}..{end_date:%Y%m%d}]"
    for offset in range(0, len(listing_ids), LISTING_BATCH_SIZE):
        batch = listing_ids[offset : offset + LISTING_BATCH_SIZE]
        listing_filter = "listing_ids:{" + "|".join(batch) + "}"
        response = await ebay_request(
            client,
            "GET",
            "/sell/analytics/v1/traffic_report",
            token,
            marketplace_id=marketplace_id,
            params={
                "dimension": "LISTING",
                "filter": f"{listing_filter},{date_filter}",
                "metric": ",".join(TRAFFIC_METRICS),
            },
        )
        result.update(parse_traffic_report(response.json()))
    return result


async def fetch_daily_traffic(
    client: httpx.AsyncClient,
    token: str,
    start_date: date,
    end_date: date,
    marketplace_id: str,
) -> dict[str, dict[str, float]]:
    response = await ebay_request(
        client,
        "GET",
        "/sell/analytics/v1/traffic_report",
        token,
        marketplace_id=marketplace_id,
        params={
            "dimension": "DAY",
            "filter": (
                f"marketplace_ids:{{{marketplace_id}}},"
                f"date_range:[{start_date:%Y%m%d}..{end_date:%Y%m%d}]"
            ),
            "metric": "TOTAL_IMPRESSION_TOTAL,LISTING_VIEWS_TOTAL,TRANSACTION",
        },
    )
    return parse_daily_traffic(response.json())


def _trading_headers(token: str, call_name: str) -> dict[str, str]:
    return {
        "X-EBAY-API-CALL-NAME": call_name,
        "X-EBAY-API-COMPATIBILITY-LEVEL": "1209",
        "X-EBAY-API-DEV-NAME": settings.EBAY_DEV_ID,
        "X-EBAY-API-APP-NAME": settings.EBAY_APP_ID,
        "X-EBAY-API-CERT-NAME": settings.EBAY_CERT_ID,
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }


async def fetch_orders(
    client: httpx.AsyncClient,
    token: str,
    start_at: datetime,
    end_at: datetime,
) -> list[dict]:
    """Read paid, non-cancelled eBay orders through the existing Trading API access."""
    page = 1
    result = []
    token_xml = xml_escape(token)
    while True:
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetOrdersRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials><eBayAuthToken>{token_xml}</eBayAuthToken></RequesterCredentials>
  <DetailLevel>ReturnAll</DetailLevel>
  <CreateTimeFrom>{start_at.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')}</CreateTimeFrom>
  <CreateTimeTo>{end_at.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')}</CreateTimeTo>
  <OrderRole>Seller</OrderRole><OrderStatus>All</OrderStatus>
  <Pagination><EntriesPerPage>100</EntriesPerPage><PageNumber>{page}</PageNumber></Pagination>
</GetOrdersRequest>"""
        response = await client.post(
            TRADING_URL,
            headers=_trading_headers(token, "GetOrders"),
            content=body.encode(),
        )
        if response.status_code in (401, 403):
            raise EbayApiError("eBay order access needs reauthorization.", response.status_code)
        if response.status_code >= 400:
            raise EbayApiError(f"eBay GetOrders returned HTTP {response.status_code}.", response.status_code)
        root = ET.fromstring(response.text)
        ack = root.findtext(".//e:Ack", default="Failure", namespaces=TRADING_NAMESPACE)
        if ack not in ("Success", "Warning"):
            message = root.findtext(".//e:Errors/e:LongMessage", default="GetOrders failed.", namespaces=TRADING_NAMESPACE)
            raise EbayApiError(message)
        for order in root.findall(".//e:OrderArray/e:Order", TRADING_NAMESPACE):
            cancel_status = order.findtext("e:CancelStatus", default="", namespaces=TRADING_NAMESPACE)
            checkout_status = order.findtext("e:CheckoutStatus/e:Status", default="", namespaces=TRADING_NAMESPACE)
            paid_at = order.findtext("e:PaidTime", default="", namespaces=TRADING_NAMESPACE)
            if cancel_status and cancel_status not in ("NotApplicable", "Invalid"):
                continue
            if not paid_at and checkout_status not in ("Complete", "Completed"):
                continue
            order_id = order.findtext("e:OrderID", default="", namespaces=TRADING_NAMESPACE)
            created_at = order.findtext("e:CreatedTime", default=paid_at, namespaces=TRADING_NAMESPACE)
            transactions = []
            for transaction in order.findall("e:TransactionArray/e:Transaction", TRADING_NAMESPACE):
                price = transaction.find("e:TransactionPrice", TRADING_NAMESPACE)
                if price is None:
                    price = transaction.find("e:ConvertedTransactionPrice", TRADING_NAMESPACE)
                item = transaction.find("e:Item", TRADING_NAMESPACE)
                listing_id = item.findtext("e:ItemID", default="", namespaces=TRADING_NAMESPACE) if item is not None else ""
                if not listing_id:
                    continue
                transactions.append(
                    {
                        "listing_id": str(listing_id),
                        "sku": item.findtext("e:SKU", default="", namespaces=TRADING_NAMESPACE) if item is not None else "",
                        "title": item.findtext("e:Title", default="", namespaces=TRADING_NAMESPACE) if item is not None else "",
                        "quantity": _integer(transaction.findtext("e:QuantityPurchased", default="0", namespaces=TRADING_NAMESPACE)),
                        "price": _number(price.text if price is not None else 0),
                        "currency": price.get("currencyID", "USD") if price is not None else "USD",
                    }
                )
            if transactions:
                result.append(
                    {
                        "order_id": order_id,
                        "created_at": created_at,
                        "transactions": transactions,
                    }
                )
        total_pages = _integer(
            root.findtext(".//e:PaginationResult/e:TotalNumberOfPages", default="1", namespaces=TRADING_NAMESPACE),
            1,
        )
        if page >= max(1, total_pages):
            break
        page += 1
    return result


async def load_local_listings(marketplace_id: str, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    currency = MARKETPLACE_CURRENCIES.get(marketplace_id, "USD")
    projection = {
        "sku": 1,
        "ebay_posted_at": 1,
        "raw.ItemID": 1,
        "raw.SKU": 1,
        "raw.Title": 1,
        "raw.QuantityAvailable": 1,
        "raw.QuantitySold": 1,
        "raw.Price": 1,
        "raw.Images": 1,
        "raw.PrimaryCategoryName": 1,
        "raw.ListingStartTime": 1,
    }
    cursor = db.product_raw.find({"raw.ItemID": {"$exists": True, "$ne": None}}, projection)
    by_listing = {}
    async for document in cursor:
        raw = document.get("raw") or {}
        listing_id = str(raw.get("ItemID") or "").strip()
        if not listing_id:
            continue
        listed = _aware_datetime(document.get("ebay_posted_at") or raw.get("ListingStartTime"))
        quantity = _integer(raw.get("QuantityAvailable"))
        sold = _integer(raw.get("QuantitySold"))
        state = "active" if quantity > 0 else "sold_out" if sold > 0 else "ended"
        images = raw.get("Images") if isinstance(raw.get("Images"), list) else []
        row = {
            "listing_id": listing_id,
            "sku": str(raw.get("SKU") or document.get("sku") or document.get("_id") or ""),
            "title": str(raw.get("Title") or f"eBay listing {listing_id}"),
            "state": state,
            "quantity": quantity,
            "lifetime_sold": sold,
            "price": _number(raw.get("Price")),
            "currency": currency,
            "image": images[0] if images else None,
            "category": raw.get("PrimaryCategoryName"),
            "listed_at": listed.isoformat() if listed else None,
            "days_listed": max(0, (now.date() - listed.date()).days) if listed else None,
        }
        previous = by_listing.get(listing_id)
        if not previous or (row["state"] == "active" and previous["state"] != "active"):
            by_listing[listing_id] = row
    return list(by_listing.values())


def _report_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def parse_ad_report(content: bytes) -> list[dict[str, str]]:
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [
        {_report_key(key): value for key, value in row.items() if key is not None}
        for row in reader
    ]


def merge_ad_rows(rows: list[dict], membership: dict[str, dict] | None = None) -> dict[str, dict]:
    merged = {str(key): dict(value) for key, value in (membership or {}).items()}
    for row in rows:
        listing_id = str(row.get("listing_id") or "").strip()
        if not listing_id:
            continue
        item = merged.setdefault(listing_id, {})
        item["advertised"] = True
        item.setdefault("campaigns", [])
        campaign = {
            "campaign_id": row.get("campaign_id"),
            "campaign_name": row.get("campaign_name"),
            "campaign_status": row.get("campaign_status"),
            "ad_group_id": row.get("ad_group_id"),
            "ad_group_name": row.get("ad_group_name"),
        }
        if campaign["campaign_id"] and campaign not in item["campaigns"]:
            item["campaigns"].append(campaign)
        additions = {
            "impressions": _number(row.get("impressions")) + _number(row.get("cpc_impressions")),
            "clicks": _number(row.get("clicks")) + _number(row.get("cpc_clicks")),
            "fees": _number(row.get("ad_fees")) + _number(row.get("cpc_ad_fees_listingsite_currency")),
            "sales": _number(row.get("sales")) + _number(row.get("cpc_attributed_sales")),
            "sale_amount": _number(row.get("sale_amount")) + _number(row.get("cpc_sale_amount_listingsite_currency")),
        }
        for key, value in additions.items():
            item[key] = item.get(key, 0.0) + value
        if row.get("listing_title"):
            item["listing_title"] = row["listing_title"]
        if row.get("funding_model"):
            item.setdefault("funding_models", [])
            if row["funding_model"] not in item["funding_models"]:
                item["funding_models"].append(row["funding_model"])
    for item in merged.values():
        impressions = item.get("impressions", 0.0)
        clicks = item.get("clicks", 0.0)
        fees = item.get("fees", 0.0)
        sales = item.get("sales", 0.0)
        sale_amount = item.get("sale_amount", 0.0)
        item.update(
            ctr=clicks / impressions if impressions else 0.0,
            conversion_rate=sales / clicks if clicks else 0.0,
            cpc=fees / clicks if clicks else 0.0,
            roas=sale_amount / fees if fees else 0.0,
            acos=fees / sale_amount if sale_amount else None,
        )
    return merged


async def _campaigns(client, token: str, marketplace_id: str) -> list[dict]:
    rows = []
    page_size = 200
    for offset in range(0, 100000, page_size):
        response = await ebay_request(
            client,
            "GET",
            "/sell/marketing/v1/ad_campaign",
            token,
            marketplace_id=marketplace_id,
            params={"limit": page_size, "offset": offset},
        )
        payload = response.json()
        batch = payload.get("campaigns") or []
        rows.extend(batch)
        if len(batch) < page_size or len(rows) >= payload.get("total", float("inf")):
            return rows
    raise EbayApiError("eBay campaign pagination exceeded the safety limit.")


async def _campaign_ads(client, token: str, marketplace_id: str, campaign: dict) -> dict[str, dict]:
    campaign_id = str(campaign.get("campaignId") or "")
    if not campaign_id:
        return {}
    result = {}
    page_size = 200
    for offset in range(0, 50000, page_size):
        response = await ebay_request(
            client,
            "GET",
            f"/sell/marketing/v1/ad_campaign/{campaign_id}/ad",
            token,
            marketplace_id=marketplace_id,
            params={"limit": page_size, "offset": offset},
        )
        payload = response.json()
        batch = payload.get("ads") or []
        for ad in batch:
            listing_id = str(ad.get("listingId") or "")
            if not listing_id:
                continue
            target = result.setdefault(listing_id, {"advertised": True, "campaigns": []})
            target["ad_status"] = ad.get("status")
            target["bid_percentage"] = _number(ad.get("bidPercentage"), None)
            target["ad_id"] = ad.get("adId")
            target["campaigns"].append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign.get("campaignName"),
                    "campaign_status": campaign.get("campaignStatus"),
                    "ad_group_id": ad.get("adGroupId"),
                    "ad_group_name": None,
                }
            )
        if len(batch) < page_size or offset + len(batch) >= payload.get("total", float("inf")):
            break
    return result


async def _create_report(
    client: httpx.AsyncClient,
    token: str,
    marketplace_id: str,
    payload: dict,
) -> str:
    response = await ebay_request(
        client,
        "POST",
        "/sell/marketing/v1/ad_report_task",
        token,
        marketplace_id=marketplace_id,
        json=payload,
    )
    location = response.headers.get("Location") or response.headers.get("location") or ""
    task_id = location.rstrip("/").split("/")[-1]
    if not task_id:
        try:
            body = response.json()
        except Exception:
            body = {}
        task_id = str(body.get("reportTaskId") or body.get("taskId") or "")
    if not task_id:
        raise EbayApiError("eBay created an advertising report but did not return its task ID.")
    return task_id


async def _download_report_when_ready(
    client: httpx.AsyncClient,
    token: str,
    marketplace_id: str,
    task_id: str,
    *,
    max_polls: int = 90,
) -> list[dict[str, str]]:
    for _ in range(max_polls):
        response = await ebay_request(
            client,
            "GET",
            f"/sell/marketing/v1/ad_report_task/{task_id}",
            token,
            marketplace_id=marketplace_id,
        )
        payload = response.json()
        status = str(payload.get("reportTaskStatus") or "").upper()
        if status == "SUCCESS":
            report_id = payload.get("reportId")
            if not report_id:
                raise EbayApiError("The eBay advertising report completed without a report ID.")
            report = await ebay_request(
                client,
                "GET",
                f"/sell/marketing/v1/ad_report/{report_id}",
                token,
                marketplace_id=marketplace_id,
                accept="application/octet-stream",
            )
            return parse_ad_report(report.content)
        if status in ("FAILED", "CANCELLED"):
            raise EbayApiError("eBay could not generate the advertising performance report.")
        await asyncio.sleep(4)
    raise EbayApiError("The eBay advertising report is still processing. Refresh again later.")


async def fetch_ad_performance(
    client: httpx.AsyncClient,
    token: str,
    marketplace_id: str,
    start_at: datetime,
    end_at: datetime,
) -> tuple[dict[str, dict], list[str]]:
    campaigns = await _campaigns(client, token, marketplace_id)
    campaigns = [
        campaign
        for campaign in campaigns
        if not campaign.get("marketplaceId") or campaign.get("marketplaceId") == marketplace_id
    ]
    if not campaigns:
        return {}, []
    warnings = []
    membership: dict[str, dict] = {}
    active_campaigns = [c for c in campaigns if str(c.get("campaignStatus", "")).upper() not in ("ENDED", "DELETED")]
    ad_results = await asyncio.gather(
        *[_campaign_ads(client, token, marketplace_id, campaign) for campaign in active_campaigns],
        return_exceptions=True,
    )
    for result in ad_results:
        if isinstance(result, Exception):
            warnings.append("Some current promoted-listing memberships could not be read.")
            continue
        for listing_id, ad in result.items():
            target = membership.setdefault(listing_id, {"advertised": True, "campaigns": []})
            target.update({key: value for key, value in ad.items() if key != "campaigns"})
            target["campaigns"].extend(ad.get("campaigns", []))

    reports = []
    safe_end = min(end_at, datetime.now(timezone.utc) - timedelta(minutes=1))
    date_from = start_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    date_to = safe_end.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    for funding_model, metrics in (("COST_PER_SALE", CPS_METRICS), ("COST_PER_CLICK", CPC_METRICS)):
        selected = [
            str(c.get("campaignId"))
            for c in campaigns
            if (c.get("fundingStrategy") or {}).get("fundingModel") == funding_model and c.get("campaignId")
        ]
        if not selected:
            continue
        dimensions = [
            {
                "dimensionKey": "campaign_id",
                "annotationKeys": ["campaign_name", "campaign_status"],
            },
            {
                "dimensionKey": "listing_id",
                "annotationKeys": ["listing_title", "listing_status", "listing_start_date"],
            },
        ]
        if funding_model == "COST_PER_CLICK":
            dimensions.insert(1, {"dimensionKey": "ad_group_id", "annotationKeys": ["ad_group_name"]})
        for offset in range(0, len(selected), 1000):
            payload = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "marketplaceId": marketplace_id,
                "reportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "reportFormat": "TSV_GZIP",
                "fundingModels": [funding_model],
                "campaignIds": selected[offset : offset + 1000],
                "dimensions": dimensions,
                "metricKeys": list(metrics),
            }
            try:
                task_id = await _create_report(client, token, marketplace_id, payload)
                rows = await _download_report_when_ready(client, token, marketplace_id, task_id)
                for row in rows:
                    row["funding_model"] = funding_model
                reports.extend(rows)
            except EbayApiError as exc:
                warnings.append(f"{funding_model.replace('_', ' ').title()} advertising metrics are unavailable: {exc}")
    return merge_ad_rows(reports, membership), list(dict.fromkeys(warnings))


def _empty_product(listing_id: str, *, title: str | None = None, currency: str = "USD") -> dict:
    return {
        "listing_id": listing_id,
        "sku": "",
        "title": title or f"Archived eBay listing {listing_id}",
        "state": "archived",
        "quantity": 0,
        "lifetime_sold": 0,
        "price": 0.0,
        "currency": currency,
        "image": None,
        "category": None,
        "listed_at": None,
        "days_listed": None,
    }


def build_dashboard(
    listings: list[dict],
    traffic: dict[str, dict],
    daily_traffic: dict[str, dict],
    orders: list[dict],
    ads: dict[str, dict],
    days: int,
    *,
    now: datetime | None = None,
    marketplace_id: str = "EBAY_US",
    sources: dict | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    start_date, end_date, _, _ = period_bounds(days, now)
    currency = MARKETPLACE_CURRENCIES.get(marketplace_id, "USD")
    sales: dict[str, dict] = {}
    daily_sales: dict[str, dict] = {}
    valid_orders = set()
    for order in orders:
        order_id = str(order.get("order_id") or "")
        created = _aware_datetime(order.get("created_at"))
        day = created.astimezone(PACIFIC).date().isoformat() if created else None
        if order_id:
            valid_orders.add(order_id)
        if day:
            daily = daily_sales.setdefault(day, {"orders": set(), "units": 0, "revenue": defaultdict(float)})
            if order_id:
                daily["orders"].add(order_id)
        for transaction in order.get("transactions", []):
            listing_id = str(transaction.get("listing_id") or "")
            if not listing_id:
                continue
            item = sales.setdefault(listing_id, {"orders": set(), "units": 0, "revenue": defaultdict(float), "title": transaction.get("title"), "sold_at": None})
            quantity = _integer(transaction.get("quantity"))
            item_currency = str(transaction.get("currency") or currency)
            amount = round(_number(transaction.get("price")) * quantity, 2)
            if order_id:
                item["orders"].add(order_id)
            if created and (item["sold_at"] is None or created < item["sold_at"]):
                item["sold_at"] = created
            item["units"] += quantity
            item["revenue"][item_currency] += amount
            if day:
                daily_sales[day]["units"] += quantity
                daily_sales[day]["revenue"][item_currency] += amount

    listing_map = {str(row["listing_id"]): dict(row) for row in listings}
    all_ids = set(listing_map) | set(traffic) | set(sales) | set(ads)
    products = []
    totals = defaultdict(float)
    for listing_id in all_ids:
        sale = sales.get(listing_id, {"orders": set(), "units": 0, "revenue": {}, "title": None, "sold_at": None})
        ad = ads.get(listing_id, {})
        product = listing_map.get(listing_id) or _empty_product(
            listing_id,
            title=sale.get("title") or ad.get("listing_title"),
            currency=currency,
        )
        raw_traffic = traffic.get(listing_id, {})
        organic = {
            "available": bool((sources or {}).get("traffic", True)),
            "impressions": _number(raw_traffic.get("TOTAL_IMPRESSION_TOTAL")),
            "listing_impressions": _number(raw_traffic.get("LISTING_IMPRESSION_TOTAL")),
            "search_impressions": _number(raw_traffic.get("LISTING_IMPRESSION_SEARCH_RESULTS_PAGE")),
            "store_impressions": _number(raw_traffic.get("LISTING_IMPRESSION_STORE")),
            "views": _number(raw_traffic.get("LISTING_VIEWS_TOTAL")),
            "search_views": _number(raw_traffic.get("LISTING_VIEWS_SOURCE_SEARCH_RESULTS_PAGE")),
            "store_views": _number(raw_traffic.get("LISTING_VIEWS_SOURCE_STORE")),
            "direct_views": _number(raw_traffic.get("LISTING_VIEWS_SOURCE_DIRECT")),
            "off_ebay_views": _number(raw_traffic.get("LISTING_VIEWS_SOURCE_OFF_EBAY")),
            "other_ebay_views": _number(raw_traffic.get("LISTING_VIEWS_SOURCE_OTHER_EBAY")),
            "transactions": _number(raw_traffic.get("TRANSACTION")),
        }
        organic["ctr"] = organic["views"] / organic["impressions"] if organic["impressions"] else 0.0
        organic["conversion_rate"] = organic["transactions"] / organic["views"] if organic["views"] else 0.0
        ad_metrics = {
            "available": bool((sources or {}).get("ads", True)),
            "advertised": bool(ad.get("advertised")),
            "ad_status": ad.get("ad_status"),
            "bid_percentage": ad.get("bid_percentage"),
            "campaigns": ad.get("campaigns", []),
            "funding_models": ad.get("funding_models", []),
            "impressions": round(ad.get("impressions", 0.0), 4),
            "clicks": round(ad.get("clicks", 0.0), 4),
            "fees": round(ad.get("fees", 0.0), 2),
            "sales": round(ad.get("sales", 0.0), 4),
            "sale_amount": round(ad.get("sale_amount", 0.0), 2),
            "ctr": ad.get("ctr", 0.0),
            "conversion_rate": ad.get("conversion_rate", 0.0),
            "cpc": round(ad.get("cpc", 0.0), 2),
            "roas": round(ad.get("roas", 0.0), 3),
            "acos": round(ad["acos"], 4) if ad.get("acos") is not None else None,
        }
        revenue = {key: round(value, 2) for key, value in sale.get("revenue", {}).items()}
        for key, value in revenue.items():
            totals[key] += value
        product.update(
            orders=len(sale.get("orders", set())),
            units=sale.get("units", 0),
            revenue=revenue,
            organic=organic,
            ads=ad_metrics,
        )
        listed_at = _aware_datetime(product.get("listed_at"))
        sold_at = sale.get("sold_at")
        product["sold_at"] = sold_at.isoformat() if sold_at else None
        product["days_to_sale"] = (
            max(0, (sold_at.date() - listed_at.date()).days)
            if sold_at and listed_at
            else None
        )
        product["category_group"] = category_group(product.get("category"))
        product["views_for_sale"] = organic["views"] if product["units"] > 0 and organic["available"] else None
        product["views_per_sale"] = (
            round(product["views_for_sale"] / product["units"], 1)
            if product["views_for_sale"] is not None and product["units"]
            else None
        )
        product["signal"] = "steady"
        if ad_metrics["fees"] > 0 and ad_metrics["sale_amount"] <= 0:
            product["signal"] = "ad_waste"
        elif organic["views"] >= 20 and product["units"] == 0:
            product["signal"] = "conversion"
        elif product.get("days_listed") is not None and product["days_listed"] >= 120 and product["units"] == 0:
            product["signal"] = "stale"
        elif product["units"] >= 3 or ad_metrics["roas"] >= 4:
            product["signal"] = "performer"
        products.append(product)

    series = []
    for position in range(days):
        day = (start_date + timedelta(days=position)).isoformat()
        organic_day = daily_traffic.get(day, {})
        sales_day = daily_sales.get(day, {"orders": set(), "units": 0, "revenue": {}})
        series.append(
            {
                "date": day,
                "orders": len(sales_day["orders"]),
                "units": sales_day["units"],
                "revenue": {key: round(value, 2) for key, value in sales_day["revenue"].items()},
                "impressions": _number(organic_day.get("TOTAL_IMPRESSION_TOTAL")),
                "views": _number(organic_day.get("LISTING_VIEWS_TOTAL")),
                "transactions": _number(organic_day.get("TRANSACTION")),
            }
        )

    total_impressions = sum(p["organic"]["impressions"] for p in products)
    total_views = sum(p["organic"]["views"] for p in products)
    total_transactions = sum(p["organic"]["transactions"] for p in products)
    ad_fees = round(sum(p["ads"]["fees"] for p in products), 2)
    ad_sales = round(sum(p["ads"]["sale_amount"] for p in products), 2)
    products.sort(key=lambda row: (row["state"] != "active", -(row["revenue"].get(currency, 0))))
    return {
        "days": days,
        "marketplace_id": marketplace_id,
        "products": products,
        "series": series,
        "category_performance": build_category_performance(products, days),
        "summary": {
            "revenue": {key: round(value, 2) for key, value in totals.items()},
            "orders": len(valid_orders),
            "units": sum(p["units"] for p in products),
            "active": sum(p["state"] == "active" for p in products),
            "impressions": round(total_impressions),
            "views": round(total_views),
            "ctr": total_views / total_impressions if total_impressions else 0.0,
            "conversion_rate": total_transactions / total_views if total_views else 0.0,
            "ad_fees": {currency: ad_fees},
            "ad_sales": {currency: ad_sales},
            "ad_roas": ad_sales / ad_fees if ad_fees else 0.0,
            "advertised": sum(p["ads"]["advertised"] for p in products),
        },
        "sources": sources or {"traffic": True, "orders": True, "ads": True},
        "period_start": start_date.isoformat(),
        "period_end": end_date.isoformat(),
    }


async def collect(marketplace_id: str, days: int) -> dict:
    now = datetime.now(timezone.utc)
    start_date, end_date, start_at, end_at = period_bounds(days, now)
    status_id = f"{marketplace_id}:{days}"
    await db.ebay_insight_status.update_one(
        {"_id": status_id},
        {"$set": {"progress": "Reading local eBay listings"}},
    )
    listings = await load_local_listings(marketplace_id, now)
    listing_ids = [str(row["listing_id"]) for row in listings]
    token = await get_valid_token()
    await db.ebay_insight_status.update_one(
        {"_id": status_id},
        {"$set": {"progress": "Reading traffic, orders, and advertising in parallel"}},
    )
    warnings = []
    sources = {"traffic": True, "orders": True, "ads": True}
    timeout = httpx.Timeout(60, connect=20)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [
            fetch_listing_traffic(client, token, listing_ids, start_date, end_date, marketplace_id),
            fetch_daily_traffic(client, token, start_date, end_date, marketplace_id),
            fetch_orders(client, token, start_at, end_at),
            fetch_ad_performance(client, token, marketplace_id, start_at, end_at),
        ]
        traffic_result, daily_result, orders_result, ads_result = await asyncio.gather(*tasks, return_exceptions=True)

    if isinstance(traffic_result, Exception):
        sources["traffic"] = False
        warnings.append(str(traffic_result))
        traffic_result = {}
    if isinstance(daily_result, Exception):
        sources["traffic"] = False
        warnings.append(str(daily_result))
        daily_result = {}
    if isinstance(orders_result, Exception):
        sources["orders"] = False
        warnings.append(f"Order performance is unavailable: {orders_result}")
        orders_result = []
    if isinstance(ads_result, Exception):
        sources["ads"] = False
        warnings.append(str(ads_result))
        ads_result = ({}, [])
    ads, ad_warnings = ads_result
    warnings.extend(ad_warnings)
    warnings = list(dict.fromkeys(warnings))

    dashboard = build_dashboard(
        listings,
        traffic_result,
        daily_result,
        orders_result,
        ads,
        days,
        now=now,
        marketplace_id=marketplace_id,
        sources=sources,
    )
    snapshot = {
        "_id": f"{status_id}:{now.date().isoformat()}",
        "captured_at": now.isoformat(),
        "warnings": warnings,
        **dashboard,
    }
    if len(BSON.encode(snapshot)) > 15_000_000:
        raise RuntimeError("The eBay Insights snapshot exceeds MongoDB's document size limit.")
    await db.ebay_insight_snapshots.replace_one({"_id": snapshot["_id"]}, snapshot, upsert=True)
    return snapshot


async def refresh(marketplace_id: str, days: int) -> None:
    status_id = f"{marketplace_id}:{days}"
    try:
        snapshot = await asyncio.wait_for(collect(marketplace_id, days), timeout=2 * 60 * 60)
        await db.ebay_insight_status.update_one(
            {"_id": status_id},
            {
                "$set": {
                    "status": "ready",
                    "error": None,
                    "progress": "Refresh complete",
                    "warning_count": len(snapshot.get("warnings", [])),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                },
                "$unset": {"lease_until": ""},
            },
        )
    except Exception as exc:
        log.warning("eBay insight refresh failed (%s)", type(exc).__name__)
        message = str(exc) if isinstance(exc, (RuntimeError, EbayApiError)) else "Refresh failed. Check eBay authorization and database connectivity, then retry."
        await db.ebay_insight_status.update_one(
            {"_id": status_id},
            {
                "$set": {"status": "error", "error": message, "progress": "Refresh failed"},
                "$unset": {"lease_until": ""},
            },
        )


async def acquire_refresh(marketplace_id: str, days: int):
    status_id = f"{marketplace_id}:{days}"
    now = datetime.now(timezone.utc)
    await db.ebay_insight_status.update_one(
        {"_id": status_id}, {"$setOnInsert": {"status": "empty"}}, upsert=True
    )
    return await db.ebay_insight_status.find_one_and_update(
        {
            "_id": status_id,
            "$or": [
                {"status": {"$ne": "refreshing"}},
                {"lease_until": {"$lt": now}},
            ],
        },
        {
            "$set": {
                "status": "refreshing",
                "progress": "Connecting to eBay",
                "error": None,
                "warning_count": 0,
                "started_at": now.isoformat(),
                "lease_until": now + timedelta(hours=3),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
