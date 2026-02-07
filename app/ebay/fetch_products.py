import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from app.ebay.client import EbayClient
from app.config import settings

logger = logging.getLogger(__name__)

client = EbayClient()

async def fetch_all_ebay_products():
    """
    Fetch ALL active products from eBay using Trading API (GetMyeBaySelling),
    with clear logging.
    """

    logger.info("▶ Starting eBay product fetch...")

    call_name = "GetMyeBaySelling"
    page_number = 1
    products = []
    total_items_found = 0

    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

    # Limit concurrent GetItem calls so we don't hammer the Trading API
    detail_semaphore = asyncio.Semaphore(5)

    async def _fetch_details_with_fallback(meta: dict) -> dict:
        """Fetch item details with bounded concurrency and safe fallback."""

        item_id = meta.get("item_id")
        fallback_images = meta.get("images") or []

        if not item_id:
            return {
                "description": "",
                "images": fallback_images,
                "item_specifics": {},
                "category_id": meta.get("category_id"),
                "category_name": None,
                "shipping": {},
            }

        try:
            async with detail_semaphore:
                # Run blocking Trading API call in a thread so we don't block the event loop
                return await asyncio.to_thread(get_item_details, item_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Error fetching details for ItemID %s: %s", item_id, exc, exc_info=True)
            return {
                "description": "",
                "images": fallback_images,
                "item_specifics": {},
                "category_id": meta.get("category_id"),
                "category_name": None,
                "shipping": {},
            }

    while True:
        print(f"📄 Fetching Page {page_number} ...")

        request_xml = f"""<?xml version="1.0" encoding="utf-8"?>
        <{call_name}Request xmlns="urn:ebay:apis:eBLBaseComponents">
          <RequesterCredentials>
            <eBayAuthToken>{settings.EBAY_OAUTH_TOKEN}</eBayAuthToken>
          </RequesterCredentials>
          <Version>1209</Version>
          <DetailLevel>ReturnAll</DetailLevel>
          <ActiveList>
            <Include>true</Include>
            <Pagination>
              <EntriesPerPage>200</EntriesPerPage>
              <PageNumber>{page_number}</PageNumber>
            </Pagination>
          </ActiveList>
        </{call_name}Request>
        """

        response_xml = client.trading_post(call_name, request_xml)
        root = ET.fromstring(response_xml)

        # Ack status
        ack = root.findtext(".//e:Ack", namespaces=ns)
        print(f"   ➝ Ack: {ack}")

        if ack != "Success":
            print("⚠ Trading API returned an error:")
            errors = root.findall(".//e:Errors", namespaces=ns)
            for err in errors:
                print("   →", err.findtext("e:LongMessage", namespaces=ns))
            break

        # Extract items
        items = root.findall(".//e:ActiveList/e:ItemArray/e:Item", namespaces=ns)
        page_count = len(items)
        print(f"   ➝ Items on this page: {page_count}")

        if not items:
            print("⭕ No more items on this page. Stopping.\n")
            break

        # First, build lightweight metadata for all items on this page
        page_items_meta = []

        for idx, item in enumerate(items, start=1):
            item_id = item.findtext("e:ItemID", default=None, namespaces=ns)
            print(f"      ▹ Processing item {idx}/{page_count} (ItemID: {item_id})")

            sku = item.findtext("e:SKU", default=None, namespaces=ns) or item_id
            title = item.findtext("e:Title", default="", namespaces=ns)
            category_id = item.findtext("e:PrimaryCategory/e:CategoryID", default=None, namespaces=ns)

            picture_urls = item.findall("e:PictureDetails/e:PictureURL", namespaces=ns)
            images: list[str] = []
            for p in picture_urls:
                if p is not None and p.text:
                    url = p.text
                    # Convert to full-resolution image (_32.JPG)
                    url = url.replace('_0.JPG', '_32.JPG')
                    url = url.replace('_12.JPG', '_32.JPG')
                    url = url.replace('_14.JPG', '_32.JPG')
                    if '_32.JPG' not in url and '_' in url and '.JPG' in url:
                        # If already has a size token but not _32, replace it
                        url = re.sub(r'_\d+\.JPG', '_32.JPG', url)
                    images.append(url)

            quantity_total = int(item.findtext("e:Quantity", default="0", namespaces=ns) or 0)
            quantity_sold = int(item.findtext("e:SellingStatus/e:QuantitySold", default="0", namespaces=ns) or 0)
            quantity_available = max(quantity_total - quantity_sold, 0)

            current_price_elem = item.find("e:SellingStatus/e:CurrentPrice", namespaces=ns)
            start_price_elem = item.find("e:StartPrice", namespaces=ns)

            if current_price_elem is not None and current_price_elem.text:
                price_text = current_price_elem.text
            elif start_price_elem is not None and start_price_elem.text:
                price_text = start_price_elem.text
            else:
                price_text = None

            page_items_meta.append(
                {
                    "item_id": item_id,
                    "sku": sku,
                    "title": title,
                    "category_id": category_id,
                    "images": images,
                    "quantity_total": quantity_total,
                    "quantity_sold": quantity_sold,
                    "quantity_available": quantity_available,
                    "price_text": price_text,
                }
            )

        # Then, fetch detailed info for all items concurrently with bounded concurrency
        detail_tasks = [
            asyncio.create_task(_fetch_details_with_fallback(meta))
            for meta in page_items_meta
        ]
        details_list = await asyncio.gather(*detail_tasks)

        # Merge metadata and details into final product records
        for meta, details in zip(page_items_meta, details_list):
            raw = {
                "ItemID": meta["item_id"],
                "SKU": meta["sku"],
                "Title": meta["title"],
                "QuantityTotal": meta["quantity_total"],
                "QuantitySold": meta["quantity_sold"],
                "QuantityAvailable": meta["quantity_available"],
                "Price": meta["price_text"],
                "Description": details["description"],
                "Images": details["images"],
                "ItemSpecifics": details["item_specifics"],
                "PrimaryCategoryID": details["category_id"],
                "PrimaryCategoryName": details["category_name"],
                "Shipping": details.get("shipping"),
                "LastSyncAt": datetime.now(timezone.utc).isoformat(),
            }

            products.append(
                {
                    "sku": meta["sku"],
                    "title": meta["title"],
                    "categoryId": meta["category_id"],
                    "images": details["images"],
                    "quantity": meta["quantity_available"],
                    "price": meta["price_text"],
                    "raw": raw,
                }
            )

            total_items_found += 1

        # Pagination
        total_pages_text = root.findtext(
            ".//e:ActiveList/e:PaginationResult/e:TotalNumberOfPages",
            default="1",
            namespaces=ns,
        )
        try:
            total_pages = int(total_pages_text)
        except ValueError:
            total_pages = 1

        print(f"   ➝ Total Pages: {total_pages}")

        if page_number >= total_pages:
            print("\n✔️ Completed all pages.\n")
            break

        page_number += 1

    print(f"🏁 Fetch complete. Total items processed: {total_items_found}\n")
    return products



def get_item_details(item_id: str):
    request_xml = f"""<?xml version="1.0" encoding="utf-8"?>
    <GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
      <RequesterCredentials>
        <eBayAuthToken>{settings.EBAY_OAUTH_TOKEN}</eBayAuthToken>
      </RequesterCredentials>
      <ItemID>{item_id}</ItemID>
      <DetailLevel>ReturnAll</DetailLevel>
      <IncludeItemSpecifics>true</IncludeItemSpecifics>
    </GetItemRequest>"""

    xml_str = client.trading_post("GetItem", request_xml)
    root = ET.fromstring(xml_str)

    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

    # Description
    desc = root.findtext(".//e:Description", default="", namespaces=ns)

    # Images - normalize to full-resolution variant (_32.JPG)
    pics = []
    for p in root.findall(".//e:PictureDetails/e:PictureURL", namespaces=ns):
        if p is not None and p.text:
            url = p.text
            # Convert common thumbnail sizes to full-resolution _32.JPG
            url = url.replace('_0.JPG', '_32.JPG')
            url = url.replace('_12.JPG', '_32.JPG')
            url = url.replace('_14.JPG', '_32.JPG')
            if '_32.JPG' not in url and '_' in url and '.JPG' in url:
                # If URL has another size token, normalize it to _32.JPG
                url = re.sub(r'_\d+\.JPG', '_32.JPG', url)
            pics.append(url)

    # Item Specifics
    item_specifics = {}
    for nvl in root.findall(".//e:ItemSpecifics/e:NameValueList", namespaces=ns):
        name = nvl.findtext("e:Name", default=None, namespaces=ns)
        values = [v.text for v in nvl.findall("e:Value", namespaces=ns) if v is not None and v.text]
        if name and values:
            item_specifics[name] = values if len(values) > 1 else values[0]

    # Category
    category_id = root.findtext(".//e:PrimaryCategory/e:CategoryID", default=None, namespaces=ns)
    category_name = root.findtext(".//e:PrimaryCategory/e:CategoryName", default=None, namespaces=ns)

    # Shipping details
    shipping = {}
    shipping["shipping_type"] = root.findtext(".//e:ShippingDetails/e:ShippingType", default=None, namespaces=ns)
    shipping["global_shipping"] = root.findtext(".//e:ShippingDetails/e:GlobalShipping", default=None, namespaces=ns)
    ships = [el.text for el in root.findall(".//e:ShippingDetails/e:ShipToLocations", namespaces=ns) if el is not None and el.text]
    shipping["ships_to_locations"] = ships if ships else None

    # ShippingServiceOptions
    service_options = []
    for s in root.findall(".//e:ShippingDetails/e:ShippingServiceOptions", namespaces=ns):
        opt = {
            "service": s.findtext("e:ShippingService", default=None, namespaces=ns),
            "priority": s.findtext("e:ShippingServicePriority", default=None, namespaces=ns),
            "cost": s.findtext("e:ShippingServiceCost", default=None, namespaces=ns),
            "additional_cost": s.findtext("e:ShippingServiceAdditionalCost", default=None, namespaces=ns),
            "free_shipping": s.findtext("e:FreeShipping", default=None, namespaces=ns),
        }
        service_options.append(opt)

    # InternationalShippingServiceOptions
    intl_options = []
    for s in root.findall(".//e:ShippingDetails/e:InternationalShippingServiceOption", namespaces=ns):
        opt = {
            "service": s.findtext("e:ShippingService", default=None, namespaces=ns),
            "priority": s.findtext("e:ShippingServicePriority", default=None, namespaces=ns),
            "cost": s.findtext("e:ShippingServiceCost", default=None, namespaces=ns),
            "additional_cost": s.findtext("e:ShippingServiceAdditionalCost", default=None, namespaces=ns),
            "ship_to_locations": [el.text for el in s.findall("e:ShipToLocation", namespaces=ns) if el is not None and el.text],
        }
        intl_options.append(opt)

    shipping["service_options"] = service_options if service_options else None
    shipping["international_service_options"] = intl_options if intl_options else None

    return {
        "description": desc,
        "images": pics,
        "item_specifics": item_specifics,
        "category_id": category_id,
        "category_name": category_name,
        "shipping": shipping,
    }

