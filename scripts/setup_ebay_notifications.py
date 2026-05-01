import sys, os, asyncio
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from app.ebay.client import EbayClient

ENDPOINT_URL = "https://my-service-578984177720.us-central1.run.app/webhooks/ebay/orders"

EVENTS = [
    "FixedPriceTransaction",      # fixed-price sale — order details per transaction
    "AuctionCheckoutComplete",     # auction sale — order details on checkout
    "ItemListed",                  # new listing created
    "ItemRevised",                 # listing price/title/quantity updated
    "ItemSold",                    # all units sold, listing closed — inventory safety net
]

async def main():
    client = EbayClient()
    await client.ensure_fresh_token()

    enable_blocks = "\n".join(
        f"""    <NotificationEnable>
      <EventType>{evt}</EventType>
      <EventEnable>Enable</EventEnable>
    </NotificationEnable>"""
        for evt in EVENTS
    )

    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<SetNotificationPreferencesRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ApplicationDeliveryPreferences>
    <ApplicationEnable>Enable</ApplicationEnable>
    <ApplicationURL>{ENDPOINT_URL}</ApplicationURL>
  </ApplicationDeliveryPreferences>
  <UserDeliveryPreferenceArray>
{enable_blocks}
  </UserDeliveryPreferenceArray>
</SetNotificationPreferencesRequest>"""

    print("Sending SetNotificationPreferences...")
    response_text = client.trading_post("SetNotificationPreferences", xml)
    print("Raw response:\n", response_text)

    try:
        root = ET.fromstring(response_text)
        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
        ack = root.findtext("e:Ack", namespaces=ns) or root.findtext("Ack")
        errors = root.findall("e:Errors", ns) or root.findall("Errors")
        print(f"\nAck: {ack}")
        for err in errors:
            code = err.findtext("e:ErrorCode", namespaces=ns) or err.findtext("ErrorCode")
            msg = err.findtext("e:LongMessage", namespaces=ns) or err.findtext("LongMessage")
            print(f"  Error {code}: {msg}")
        if ack in ("Success", "Warning"):
            print("\n✓ Notification preferences set.")
        else:
            print("\n✗ Failed. Check errors above.")
    except ET.ParseError:
        print("Could not parse XML response.")

if __name__ == "__main__":
    asyncio.run(main())