import xml.etree.ElementTree as ET
from app.ebay.client import EbayClient
from app.config import settings

client = EbayClient()

async def fetch_all_ebay_products():
    """
    Fetch ALL active products from eBay using Trading API (GetMyeBaySelling),
    with proper pagination over all pages.
    """
    call_name = "GetMyeBaySelling"
    page_number = 1

    products = []

    # Trading API namespace
    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

    while True:
        request_xml = f"""<?xml version="1.0" encoding="utf-8"?>
        <{call_name}Request xmlns="urn:ebay:apis:eBLBaseComponents">
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

        # Get the items for this page
        items = root.findall(".//e:ActiveList/e:ItemArray/e:Item", namespaces=ns)

        # If no items on this page, we are done
        if not items:
            break

        for item in items:
            # SKU (may be missing if seller never set SKU)
            sku = item.findtext("e:SKU", default=None, namespaces=ns)
            if not sku:
                sku = item.findtext("e:ItemID", default=None, namespaces=ns)

            title = item.findtext("e:Title", default="", namespaces=ns)

            category_id = item.findtext(
                "e:PrimaryCategory/e:CategoryID",
                default=None,
                namespaces=ns,
            )

            # Images
            picture_urls = item.findall(
                "e:PictureDetails/e:PictureURL",
                namespaces=ns,
            )
            images = [p.text for p in picture_urls if p is not None and p.text]

            # Quantity
            quantity_total_text = item.findtext("e:Quantity", default="0", namespaces=ns)
            quantity_sold_text = item.findtext(
                "e:SellingStatus/e:QuantitySold",
                default="0",
                namespaces=ns,
            )

            try:
                quantity_total = int(quantity_total_text)
            except ValueError:
                quantity_total = 0

            try:
                quantity_sold = int(quantity_sold_text)
            except ValueError:
                quantity_sold = 0

            quantity_available = max(quantity_total - quantity_sold, 0)

            # Price – CurrentPrice if available, fallback to StartPrice
            current_price_elem = item.find("e:SellingStatus/e:CurrentPrice", namespaces=ns)
            start_price_elem = item.find("e:StartPrice", namespaces=ns)

            if current_price_elem is not None and current_price_elem.text:
                price_text = current_price_elem.text
            elif start_price_elem is not None and start_price_elem.text:
                price_text = start_price_elem.text
            else:
                price_text = None

            details = get_item_details(item["ItemID"])

            raw = {
                "ItemID": item.findtext("e:ItemID", default=None, namespaces=ns),
                "SKU": sku,
                "Title": title,
                "PrimaryCategoryID": category_id,
                "Images": images,
                "QuantityTotal": quantity_total,
                "QuantitySold": quantity_sold,
                "QuantityAvailable": quantity_available,
                "Price": price_text,
                "Description": details["description"],
                "Images": details["images"],
            }

            products.append(
                {
                    "sku": sku,
                    "title": title,
                    "categoryId": category_id,
                    "images": images,
                    "quantity": quantity_available,
                    "price": price_text,
                    "raw": raw,
                }
            )

        # Optional: also respect TotalNumberOfPages if you want
        total_pages_text = root.findtext(
            ".//e:ActiveList/e:PaginationResult/e:TotalNumberOfPages",
            default="1",
            namespaces=ns,
        )
        try:
            total_pages = int(total_pages_text)
        except ValueError:
            total_pages = page_number

        if page_number >= total_pages:
            break

        page_number += 1

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
    desc = root.findtext(".//e:Description", default="", namespaces=ns)
    pics = [p.text for p in root.findall(".//e:PictureDetails/e:PictureURL", ns)]

    return {
        "description": desc,
        "images": pics,
    }