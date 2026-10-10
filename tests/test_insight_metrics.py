import unittest

from app.services.insight_metrics import build_category_performance, category_group


class CategoryPerformanceTests(unittest.TestCase):
    def test_category_paths_are_normalized(self):
        self.assertEqual(category_group("Collectibles:Decor:Figures"), "Collectibles")
        self.assertEqual(category_group("Home & Living > Decor"), "Home & Living")
        self.assertEqual(category_group(None), "Uncategorized")

    def test_historical_unsold_endings_do_not_distort_sell_through(self):
        rows = build_category_performance(
            [
                {"listing_id": "1", "category": "Collectibles:Figures", "state": "active", "units": 1,
                 "days_to_sale": 10, "views_for_sale": 20},
                {"listing_id": "2", "category": "Collectibles:Books", "state": "sold_out", "units": 1,
                 "days_to_sale": 30, "views_for_sale": 40},
                {"listing_id": "3", "category": "Collectibles:Glass", "state": "active", "units": 0},
                {"listing_id": "4", "category": "Collectibles:Other", "state": "ended", "units": 0},
            ],
            90,
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["listing_count"], 3)
        self.assertEqual(row["sold_listings"], 2)
        self.assertEqual(row["median_days_to_sale"], 20)
        self.assertAlmostEqual(row["sell_through_rate"], 2 / 3)
        self.assertEqual(row["views_per_sale"], 30)


if __name__ == "__main__":
    unittest.main()
