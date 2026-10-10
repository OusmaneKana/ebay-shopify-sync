import gzip
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.ebay_auth_service import EBAY_SCOPES
from app.services.ebay_insights import (
    build_dashboard,
    merge_ad_rows,
    parse_ad_report,
    parse_daily_traffic,
    parse_traffic_report,
)

NOW = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)


class ParsingTests(unittest.TestCase):
    def test_traffic_metrics_follow_header_order(self):
        payload = {
            "header": {
                "metrics": [
                    {"key": "LISTING_VIEWS_TOTAL"},
                    {"key": "TOTAL_IMPRESSION_TOTAL"},
                    {"key": "TRANSACTION"},
                ]
            },
            "records": [
                {
                    "dimensionValues": [{"value": "123"}],
                    "metricValues": [
                        {"value": 20, "applicable": True},
                        {"value": 500, "applicable": True},
                        {"value": 0, "applicable": False},
                    ],
                }
            ],
        }
        result = parse_traffic_report(payload)["123"]
        self.assertEqual(result["LISTING_VIEWS_TOTAL"], 20)
        self.assertEqual(result["TOTAL_IMPRESSION_TOTAL"], 500)
        self.assertIsNone(result["TRANSACTION"])

    def test_daily_traffic_understands_ebay_date_format(self):
        payload = {
            "header": {"metrics": [{"key": "LISTING_VIEWS_TOTAL"}]},
            "records": [
                {
                    "dimensionValues": [{"value": "Fri Oct 02 00:00:00 GMT-07:00 2026"}],
                    "metricValues": [{"value": 44, "applicable": True}],
                }
            ],
        }
        self.assertEqual(parse_daily_traffic(payload)["2026-10-02"]["LISTING_VIEWS_TOTAL"], 44)

    def test_gzipped_ad_report_is_merged_per_listing(self):
        raw = (
            "listing_id\tcampaign_id\tcampaign_name\timpressions\tclicks\tad_fees\tsales\tsale_amount\n"
            "123\tc1\tAlways on\t1000\t25\t12.50\t2\t160\n"
            "123\tc2\tSeasonal\t500\t10\t5\t1\t70\n"
        ).encode()
        rows = parse_ad_report(gzip.compress(raw))
        merged = merge_ad_rows(rows)["123"]
        self.assertEqual(merged["impressions"], 1500)
        self.assertEqual(merged["clicks"], 35)
        self.assertEqual(merged["fees"], 17.5)
        self.assertEqual(merged["sale_amount"], 230)
        self.assertAlmostEqual(merged["roas"], 230 / 17.5)
        self.assertEqual(len(merged["campaigns"]), 2)


class DashboardTests(unittest.TestCase):
    def test_dashboard_combines_orders_traffic_ads_and_listing_age(self):
        listings = [
            {
                "listing_id": "123",
                "sku": "SKU-1",
                "title": "Old listing",
                "state": "active",
                "quantity": 1,
                "lifetime_sold": 0,
                "price": 50,
                "currency": "USD",
                "image": None,
                "category": "Antiques",
                "listed_at": (NOW - timedelta(days=180)).isoformat(),
                "days_listed": 180,
            },
            {
                "listing_id": "456",
                "sku": "SKU-2",
                "title": "Seller",
                "state": "active",
                "quantity": 1,
                "lifetime_sold": 3,
                "price": 20,
                "currency": "USD",
                "image": None,
                "category": "Decor",
                "listed_at": (NOW - timedelta(days=20)).isoformat(),
                "days_listed": 20,
            },
        ]
        traffic = {
            "123": {"TOTAL_IMPRESSION_TOTAL": 500, "LISTING_VIEWS_TOTAL": 30, "TRANSACTION": 0},
            "456": {"TOTAL_IMPRESSION_TOTAL": 300, "LISTING_VIEWS_TOTAL": 20, "TRANSACTION": 2},
        }
        orders = [
            {
                "order_id": "O-1",
                "created_at": (NOW - timedelta(days=1)).isoformat(),
                "transactions": [
                    {"listing_id": "456", "quantity": 2, "price": 20, "currency": "USD", "title": "Seller"}
                ],
            }
        ]
        ads = {
            "123": {
                "advertised": True,
                "campaigns": [],
                "fees": 8,
                "sale_amount": 0,
                "impressions": 100,
                "clicks": 4,
                "sales": 0,
                "ctr": 0.04,
                "conversion_rate": 0,
                "cpc": 2,
                "roas": 0,
                "acos": None,
            }
        }
        dashboard = build_dashboard(
            listings,
            traffic,
            {},
            orders,
            ads,
            30,
            now=NOW,
            sources={"traffic": True, "orders": True, "ads": True},
        )
        products = {p["listing_id"]: p for p in dashboard["products"]}
        self.assertEqual(products["123"]["signal"], "ad_waste")
        self.assertEqual(products["456"]["revenue"], {"USD": 40})
        self.assertEqual(products["456"]["orders"], 1)
        self.assertEqual(products["456"]["organic"]["conversion_rate"], 0.1)
        self.assertEqual(products["456"]["days_to_sale"], 19)
        self.assertEqual(products["456"]["views_per_sale"], 10)
        decor = next(row for row in dashboard["category_performance"] if row["category"] == "Decor")
        self.assertEqual(decor["median_days_to_sale"], 19)
        self.assertEqual(decor["sell_through_rate"], 1)
        self.assertEqual(decor["views_per_sale"], 10)
        self.assertEqual(dashboard["summary"]["revenue"], {"USD": 40})
        self.assertEqual(dashboard["summary"]["orders"], 1)
        self.assertEqual(dashboard["summary"]["ad_fees"], {"USD": 8})

    def test_missing_sources_are_explicit(self):
        dashboard = build_dashboard([], {}, {}, [], {}, 7, now=NOW, sources={"traffic": False, "orders": False, "ads": False})
        self.assertFalse(dashboard["sources"]["traffic"])
        self.assertEqual(len(dashboard["series"]), 7)


class AuthorizationTests(unittest.TestCase):
    def test_read_only_insight_scopes_are_requested(self):
        self.assertIn("https://api.ebay.com/oauth/api_scope/sell.analytics.readonly", EBAY_SCOPES)
        self.assertIn("https://api.ebay.com/oauth/api_scope/sell.marketing.readonly", EBAY_SCOPES)
        self.assertNotIn("https://api.ebay.com/oauth/api_scope/sell.marketing", EBAY_SCOPES)


class ApiTests(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.routes.ebay_insights import router
        from app.security.passkey import require_authorized

        self.app = FastAPI()
        self.app.include_router(router, prefix="/ebay-insights/api")
        self.app.dependency_overrides[require_authorized] = lambda: None
        self.client = TestClient(self.app)

    def test_dashboard_empty_and_validation(self):
        fake_db = MagicMock()
        fake_db.ebay_insight_snapshots.find_one = AsyncMock(return_value=None)
        fake_db.ebay_insight_status.find_one = AsyncMock(return_value=None)
        with patch("app.api.routes.ebay_insights.db", fake_db):
            response = self.client.get("/ebay-insights/api/dashboard?marketplace_id=EBAY_US&days=30")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["empty"])
        self.assertEqual(self.client.get("/ebay-insights/api/dashboard?marketplace_id=EBAY_JP").status_code, 422)
        self.assertEqual(self.client.get("/ebay-insights/api/dashboard?days=91").status_code, 422)

    def test_refresh_dispatch_and_duplicate_guard(self):
        task = AsyncMock()
        with patch("app.api.routes.ebay_insights.acquire_refresh", AsyncMock(return_value={"status": "refreshing"})), patch("app.api.routes.ebay_insights.refresh", task):
            response = self.client.post("/ebay-insights/api/refresh?marketplace_id=EBAY_US&days=7")
        self.assertEqual(response.status_code, 202)
        task.assert_awaited_once_with("EBAY_US", 7)
        with patch("app.api.routes.ebay_insights.acquire_refresh", AsyncMock(return_value=None)):
            response = self.client.post("/ebay-insights/api/refresh?marketplace_id=EBAY_US&days=7")
        self.assertEqual(response.status_code, 409)


if __name__ == "__main__":
    unittest.main()
