"""
Audit Etsy item availability discrepancies (ACTIVE LISTINGS ONLY).

Compares quantities between:
1. Etsy ACTIVE listings (from etsy_listings_investigation collection)
2. Linked normalized products (product_normalized with channels.etsy.listing_id)

Reports mismatches and unlinked items for ACTIVE listings only.
Archived, draft, and sold-out listings are excluded.
"""

import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

ROOT = __import__("os").path.dirname(__import__("os").path.dirname(__import__("os").path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.config import settings
from app.database.mongo import db, close_mongo_client


async def fetch_etsy_listings(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch ACTIVE Etsy listings only from investigation collection."""
    query = {
        "listing_id": {"$exists": True, "$ne": None},
        "quantity": {"$exists": True},
        # Only ACTIVE listings to exclude archived/delisted/draft
        "listing_state": "active",
    }
    
    cursor = db.etsy_listings_investigation.find(query)
    docs: List[Dict[str, Any]] = []
    
    async for doc in cursor:
        docs.append({
            "listing_id": doc.get("listing_id"),
            "title": doc.get("title"),
            "quantity": doc.get("quantity"),
            "listing_state": doc.get("listing_state"),
            "url": doc.get("url"),
            "fetched_at": doc.get("fetched_at"),
        })
        if limit and len(docs) >= limit:
            break
    
    return docs


async def fetch_linked_normalized_products() -> Dict[int, Dict[str, Any]]:
    """Fetch normalized products linked to Etsy, keyed by listing_id."""
    query = {
        "channels.etsy.listing_id": {"$exists": True, "$ne": None},
        "quantity": {"$exists": True},
    }
    
    projection = {
        "_id": 1,
        "channels.etsy.listing_id": 1,
        "quantity": 1,
        "title": 1,
        "shopify_id": 1,
    }
    
    linked_by_listing_id: Dict[int, Dict[str, Any]] = {}
    
    cursor = db.product_normalized.find(query, projection)
    async for doc in cursor:
        listing_id = doc.get("channels", {}).get("etsy", {}).get("listing_id")
        if listing_id:
            try:
                listing_id_int = int(listing_id)
                linked_by_listing_id[listing_id_int] = {
                    "sku": doc.get("_id"),
                    "quantity": doc.get("quantity"),
                    "title": doc.get("title"),
                    "shopify_id": doc.get("shopify_id"),
                }
            except (TypeError, ValueError):
                pass
    
    return linked_by_listing_id


async def audit(limit: Optional[int] = None) -> None:
    """Perform availability audit (ACTIVE listings only)."""
    print("🔍 Fetching ACTIVE Etsy listings...")
    etsy_listings = await fetch_etsy_listings(limit)
    
    print("🔗 Fetching linked normalized products...")
    linked_products = await fetch_linked_normalized_products()
    
    # Check data freshness
    if etsy_listings:
        fetched_ts_str = etsy_listings[0].get("fetched_at")
        if fetched_ts_str:
            try:
                if isinstance(fetched_ts_str, str):
                    fetched_ts = datetime.fromisoformat(fetched_ts_str.replace('Z', '+00:00'))
                else:
                    fetched_ts = fetched_ts_str
                    if not fetched_ts.tzinfo:
                        fetched_ts = fetched_ts.replace(tzinfo=timezone.utc)
                
                age_days = (datetime.now(timezone.utc) - fetched_ts).days
                if age_days > 7:
                    print(f"⚠️  WARNING: Etsy data is {age_days} days old (fetched {fetched_ts})")
                    print("   Consider refreshing with: python -m scripts.sync_etsy_listings_investigation --shop-id <ID>\n")
                else:
                    print(f"✓ Etsy data is current ({age_days} days old)\n")
            except Exception as e:
                print(f"⚠️  Could not determine data age: {e}\n")
    
    print(f"✓ ACTIVE Etsy listings: {len(etsy_listings)}")
    print(f"✓ Linked products: {len(linked_products)}")
    
    # Matching analysis
    matched_and_linked = 0
    quantity_mismatches = 0
    unlinked_etsy = []
    unlinked_normalized = []
    mismatches = []
    
    seen_normalized_ids = set()
    
    # Check each ACTIVE Etsy listing
    for etsy in etsy_listings:
        listing_id = etsy.get("listing_id")
        etsy_qty = etsy.get("quantity")
        
        if listing_id in linked_products:
            matched_and_linked += 1
            norm = linked_products[listing_id]
            seen_normalized_ids.add(norm["sku"])
            
            try:
                etsy_qty_int = int(etsy_qty) if etsy_qty is not None else 0
                norm_qty_int = int(norm.get("quantity")) if norm.get("quantity") is not None else 0
            except (TypeError, ValueError):
                continue
            
            if etsy_qty_int != norm_qty_int:
                quantity_mismatches += 1
                mismatches.append({
                    "listing_id": listing_id,
                    "etsy_title": etsy.get("title"),
                    "etsy_qty": etsy_qty_int,
                    "normalized_qty": norm_qty_int,
                    "delta": etsy_qty_int - norm_qty_int,
                    "sku": norm["sku"],
                    "shopify_id": norm.get("shopify_id"),
                    "url": etsy.get("url"),
                })
        else:
            # Unlinked ACTIVE Etsy listing
            unlinked_etsy.append({
                "listing_id": listing_id,
                "title": etsy.get("title"),
                "quantity": etsy_qty,
                "url": etsy.get("url"),
            })
    
    # Check for normalized products with Etsy links but not found in ACTIVE Etsy
    for listing_id, norm in linked_products.items():
        if norm["sku"] not in seen_normalized_ids:
            unlinked_normalized.append({
                "sku": norm["sku"],
                "listing_id": listing_id,
                "quantity": norm.get("quantity"),
                "title": norm.get("title"),
            })
    
    # Print summary
    print("\n" + "="*70)
    print("📈 AUDIT SUMMARY (ACTIVE LISTINGS ONLY)")
    print("="*70)
    print(f"Total ACTIVE Etsy listings: {len(etsy_listings)}")
    print(f"(Archived, draft, and sold-out listings excluded)")
    print(f"\nTotal linked to normalized: {len(linked_products)}")
    print(f"Matched pairs: {matched_and_linked}")
    print(f"Quantity mismatches: {quantity_mismatches}")
    print(f"Unlinked ACTIVE listings: {len(unlinked_etsy)}")
    print(f"Normalized with missing Etsy link: {len(unlinked_normalized)}")
    
    # Display quantity mismatches
    if mismatches:
        print("\n" + "="*70)
        print("⚠️  QUANTITY MISMATCHES (ACTIVE ETSY LISTINGS)")
        print("="*70)
        
        # Sort by delta (largest first)
        mismatches_sorted = sorted(mismatches, key=lambda x: abs(x["delta"]), reverse=True)
        
        for i, m in enumerate(mismatches_sorted[:20], start=1):
            print(f"\n{i}. Listing {m['listing_id']}")
            print(f"   Title: {m['etsy_title'][:60]}")
            print(f"   SKU: {m['sku']}")
            print(f"   Etsy: {m['etsy_qty']} | Normalized: {m['normalized_qty']} | Delta: {m['delta']:+d}")
            if m['shopify_id']:
                print(f"   Shopify ID: {m['shopify_id']}")
            print(f"   URL: {m['url']}")
        
        if len(mismatches) > 20:
            print(f"\n... and {len(mismatches) - 20} more mismatches")
    
    # Display unlinked Etsy listings (sample)
    if unlinked_etsy:
        print("\n" + "="*70)
        print(f"🔗 UNLINKED ACTIVE ETSY LISTINGS ({len(unlinked_etsy)} total)")
        print("="*70)
        print("\nThese ACTIVE listings exist on Etsy but have no normalized product link:\n")
        
        for i, item in enumerate(unlinked_etsy[:10], start=1):
            print(f"{i}. Listing {item['listing_id']}")
            print(f"   Title: {item['title'][:60]}")
            print(f"   Quantity: {item['quantity']}")
            print(f"   URL: {item['url']}")
        
        if len(unlinked_etsy) > 10:
            print(f"\n... and {len(unlinked_etsy) - 10} more unlinked listings")
    
    # Display orphaned normalized links
    if unlinked_normalized:
        print("\n" + "="*70)
        print(f"🔗 ORPHANED NORMALIZED ETSY LINKS ({len(unlinked_normalized)} total)")
        print("="*70)
        print("These are normalized products with Etsy links but listing not in ACTIVE Etsy batch:\n")
        
        for i, item in enumerate(unlinked_normalized[:10], start=1):
            print(f"{i}. SKU: {item['sku']} | Listing ID: {item['listing_id']}")
            print(f"   Title: {item['title'][:60]}")
            print(f"   Quantity: {item['quantity']}")
    
    # Export detailed report
    report = {
        "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        "audit_scope": "ACTIVE Etsy listings only",
        "note": "Archived, draft, and sold-out listings are excluded from all counts",
        "summary": {
            "total_active_etsy_listings": len(etsy_listings),
            "total_linked_normalized": len(linked_products),
            "matched_pairs": matched_and_linked,
            "quantity_mismatches": quantity_mismatches,
            "unlinked_active_etsy": len(unlinked_etsy),
            "orphaned_normalized_links": len(unlinked_normalized),
        },
        "quantity_mismatches_sample": mismatches[:50],
        "unlinked_etsy_sample": unlinked_etsy[:20],
        "orphaned_links_sample": unlinked_normalized[:20],
    }
    
    report_file = "etsy_availability_audit_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n✅ Detailed report saved to: {report_file}")
    
    close_mongo_client()


def main():
    parser = argparse.ArgumentParser(
        description="Audit ACTIVE Etsy availability discrepancies against normalized products"
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of Etsy listings to scan")
    args = parser.parse_args()
    
    asyncio.run(audit(limit=args.limit))


if __name__ == "__main__":
    main()
