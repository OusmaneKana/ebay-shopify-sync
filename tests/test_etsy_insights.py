import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.etsy_insights import build_dashboard, money, pages, collect, request_json, taxonomy_category_index

NOW = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)


def product(id=1, **values):
    return dict(listing_id=id, title='A product', state='active', quantity=10, favorites=10,
                views=100, price=12, currency='USD', rating=None, review_count=0, **values)


def receipt(id=1, **values):
    result = dict(receipt_id=id, timestamp=int((NOW - timedelta(days=1)).timestamp()), is_paid=True, is_canceled=False,
                  transactions=[dict(listing_id=1, quantity=2, price=12.5, currency='USD')])
    result.update(values)
    return result


class MetricsTests(unittest.TestCase):
    def test_money_uses_divisor(self):
        self.assertEqual(money({'amount': 12345, 'divisor': 1000}), 12.345)
        self.assertEqual(money(None), 0)

    def test_paid_sales_only_and_distinct_product_orders(self):
        r = receipt(); r['transactions'].append(dict(listing_id=1, quantity=1, price=7, currency='USD'))
        current = {'products': [product()], 'receipts': [r, receipt(2, is_paid=False), receipt(3, is_canceled=True), receipt(4, timestamp=1)]}
        data = build_dashboard(current, None, 30, NOW)
        self.assertEqual(data['summary']['revenue'], {'USD': 32})
        self.assertEqual(data['summary']['orders'], 1)
        self.assertEqual(data['products'][0]['orders'], 1)
        self.assertEqual(data['summary']['units'], 3)
        self.assertIsNone(data['products'][0]['favorite_change'])

    def test_currencies_and_deleted_listings_are_preserved(self):
        r = receipt(); r['transactions'].append(dict(listing_id=99, quantity=1, price=8.5, currency='EUR'))
        data = build_dashboard({'products':[product()], 'receipts':[r]}, None, 30, NOW)
        self.assertEqual(data['summary']['revenue'], {'USD':25, 'EUR':8.5})
        self.assertEqual(data['products'][1]['state'], 'archived')

    def test_favorites_can_decrease_and_views_must_be_valid(self):
        baseline = {'products':[product()]}
        p = product(); p.update(favorites=7, views=0)
        data = build_dashboard({'products':[p]}, baseline, 30, NOW)
        self.assertEqual(data['products'][0]['favorite_change'], -3)
        self.assertIsNone(data['products'][0]['view_change'])
        p.update(views=80)
        self.assertIsNone(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['view_change'])
        p.update(views=130)
        self.assertEqual(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['view_change'], 30)
        p.update(state='inactive')
        self.assertIsNone(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['view_change'])

    def test_chart_and_totals_share_calendar_boundaries(self):
        start = NOW.replace(hour=0) - timedelta(days=6)
        rows = [receipt(timestamp=int(start.timestamp())), receipt(2, timestamp=int(start.timestamp())-1)]
        data = build_dashboard({'products':[product()], 'receipts':rows}, None, 7, NOW)
        self.assertEqual(data['summary']['revenue']['USD'], 25)
        self.assertEqual(sum(day['revenue'].get('USD',0) for day in data['series']), 25)
        self.assertEqual(len(data['series']), 7)

    def test_signals(self):
        baseline = {'products':[product()]}
        p = product(); p.update(favorites=15)
        self.assertEqual(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['signal'], 'interest')
        p.update(state='sold_out')
        self.assertEqual(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['signal'], 'restock')
        p.update(state='active',rating=3.5,review_count=3)
        self.assertEqual(build_dashboard({'products':[p]}, baseline, 30, NOW)['products'][0]['signal'], 'review')

    def test_listing_age_uses_original_timestamp(self):
        listed = int((NOW - timedelta(days=45, hours=3)).timestamp())
        row = product(listed_timestamp=listed)
        result = build_dashboard({'products': [row], 'receipts': []}, None, 30, NOW)
        self.assertEqual(result['products'][0]['days_listed'], 45)
        self.assertTrue(result['products'][0]['listed_at'].startswith('2026-07-31'))

    def test_selling_speed_and_views_per_sale(self):
        listed = int((NOW - timedelta(days=40)).timestamp())
        row = product(listed_timestamp=listed, category='Home & Living')
        data = build_dashboard({'products': [row], 'receipts': [receipt()]}, None, 30, NOW)
        sold = data['products'][0]
        self.assertEqual(sold['days_to_sale'], 39)
        self.assertEqual(sold['views_per_sale'], 50)
        category = data['category_performance'][0]
        self.assertEqual(category['category'], 'Home & Living')
        self.assertEqual(category['median_days_to_sale'], 39)
        self.assertEqual(category['sell_through_rate'], 1)

    def test_taxonomy_categories_use_root_and_path(self):
        index = taxonomy_category_index([
            {'id': 1, 'name': 'Accessories', 'children': [
                {'id': 2, 'name': 'Scarves', 'children': []}
            ]}
        ])
        self.assertEqual(index[2], {'category': 'Accessories', 'category_path': 'Accessories > Scarves'})


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_pagination_reads_all_pages(self):
        fetch = AsyncMock(side_effect=[{'count':101,'results':[{'id':i} for i in range(100)]},{'count':101,'results':[{'id':100}]}])
        with patch('app.services.etsy_insights.request_json', fetch):
            rows = await pages(None,'/test',{})
        self.assertEqual(len(rows),101)
        self.assertEqual(fetch.await_args_list[1].args[3]['offset'],100)

    async def test_failed_orders_never_replace_snapshot(self):
        fake_db=MagicMock(); fake_db.etsy_insight_status.update_one=AsyncMock();fake_db.etsy_insight_snapshots.replace_one=AsyncMock()
        fetch_pages=AsyncMock(side_effect=[[],[],[],[],[],RuntimeError('Orders unavailable')])
        with patch('app.services.etsy_insights.db',fake_db), patch('app.services.etsy_insights.get_valid_token',AsyncMock(return_value='test')), patch('app.services.etsy_insights.settings',ETSY_CLIENT_ID='key',ETSY_CLIENT_SECRET=None), patch('app.services.etsy_insights.pages',fetch_pages):
            with self.assertRaises(RuntimeError):
                await collect('123')
        fake_db.etsy_insight_snapshots.replace_one.assert_not_awaited()

    async def test_rate_limit_error_is_not_reported_as_oauth_failure(self):
        response = MagicMock(status_code=429, headers={"x-remaining-today": "12"})
        client = MagicMock()
        client.get = AsyncMock(return_value=response)
        with patch("app.services.etsy_insights.asyncio.sleep", AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "temporarily rate-limiting"):
                await request_json(client, "/test", {})
        self.assertEqual(client.get.await_count, 6)

    async def test_daily_rate_limit_has_specific_message(self):
        response = MagicMock(status_code=429, headers={"x-remaining-today": "0"})
        client = MagicMock()
        client.get = AsyncMock(return_value=response)
        with patch("app.services.etsy_insights.asyncio.sleep", AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "daily API request limit"):
                await request_json(client, "/test", {})
        client.get.assert_awaited_once()

class ApiTests(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.api.routes.etsy_insights import router
        from app.security.passkey import require_authorized
        self.app = FastAPI()
        self.app.include_router(router, prefix='/etsy-insights/api')
        self.app.dependency_overrides[require_authorized] = lambda: None
        self.client = TestClient(self.app)

    def test_validation_rejects_non_numeric_shops(self):
        self.assertEqual(self.client.get('/etsy-insights/api/dashboard?shop_id=bad').status_code, 422)
        self.assertEqual(self.client.get('/etsy-insights/api/dashboard?shop_id=123&days=91').status_code, 422)

    def test_empty_shop_and_database_errors(self):
        fake_db = MagicMock()
        fake_db.etsy_insight_snapshots.find_one = AsyncMock(return_value=None)
        fake_db.etsy_insight_status.find_one = AsyncMock(return_value=None)
        with patch('app.api.routes.etsy_insights.db',fake_db):
            response=self.client.get('/etsy-insights/api/dashboard?shop_id=123')
        self.assertTrue(response.json()['empty'])
        fake_db.etsy_insight_snapshots.find_one.side_effect=RuntimeError('private connection details')
        with patch('app.api.routes.etsy_insights.db',fake_db):
            response=self.client.get('/etsy-insights/api/dashboard?shop_id=123')
        self.assertEqual(response.status_code,503)
        self.assertNotIn('private',response.text)

    def test_refresh_is_dispatched_once(self):
        task=AsyncMock()
        with patch('app.api.routes.etsy_insights.acquire_refresh',AsyncMock(return_value={'status':'refreshing'})), patch('app.api.routes.etsy_insights.refresh',task):
            response=self.client.post('/etsy-insights/api/refresh?shop_id=123')
        self.assertEqual(response.status_code,202)
        task.assert_awaited_once_with('123')
        with patch('app.api.routes.etsy_insights.acquire_refresh',AsyncMock(return_value=None)):
            response=self.client.post('/etsy-insights/api/refresh?shop_id=123')
        self.assertEqual(response.status_code,409)

    def test_existing_passkey_protects_insight_api(self):
        self.app.dependency_overrides.clear()
        with patch('app.security.passkey.settings', ADMIN_PASSKEY='test-required'):
            response=self.client.get('/etsy-insights/api/shops')
        self.assertEqual(response.status_code,401)


if __name__ == '__main__':
    unittest.main()
