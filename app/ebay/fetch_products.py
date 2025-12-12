import xml.etree.ElementTree as ET
from app.ebay.client import EbayClient
from app.config import settings

client = EbayClient()

async def fetch_all_ebay_products():
    """
    Fetch ALL active products from eBay using Trading API (GetMyeBaySelling),
    with clear logging.
    """

    print("▶ Starting eBay product fetch...\n")

    call_name = "GetMyeBaySelling"
    page_number = 1
    products = []
    total_items_found = 0

    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

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

        for idx, item in enumerate(items, start=1):
            item_id = item.findtext("e:ItemID", default=None, namespaces=ns)
            print(f"      ▹ Processing item {idx}/{page_count} (ItemID: {item_id})")

            # --- extract data ---
            sku = item.findtext("e:SKU", default=None, namespaces=ns) or item_id
            title = item.findtext("e:Title", default="", namespaces=ns)
            category_id = item.findtext("e:PrimaryCategory/e:CategoryID", default=None, namespaces=ns)

            picture_urls = item.findall("e:PictureDetails/e:PictureURL", namespaces=ns)
            images = [p.text for p in picture_urls if p is not None and p.text]

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

            # Fetch detailed description + images
            details = get_item_details(item_id) if item_id else {"description": "", "images": images}

            raw = {
                "ItemID": item_id,
                "SKU": sku,
                "Title": title,
                "PrimaryCategoryID": category_id,
                "QuantityTotal": quantity_total,
                "QuantitySold": quantity_sold,
                "QuantityAvailable": quantity_available,
                "Price": price_text,
                "Description": details["description"],
                "Images": details["images"],
            }

            products.append({
                "sku": sku,
                "title": title,
                "categoryId": category_id,
                "images": details["images"],
                "quantity": quantity_available,
                "price": price_text,
                "raw": raw,
            })

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
    desc = root.findtext(".//e:Description", default="", namespaces=ns)
    pics = [p.text for p in root.findall(".//e:PictureDetails/e:PictureURL", namespaces=ns)]

    return {
        "description": desc,
        "images": pics,
    }
