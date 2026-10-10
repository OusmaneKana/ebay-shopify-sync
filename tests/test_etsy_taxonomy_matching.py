import unittest
from unittest.mock import AsyncMock, patch

from app.services.etsy_taxonomy_matching import product_anchors, score_candidate
from app.api.routes.reporting import _suggest_etsy_taxonomy


class EtsyObjectMatchingTests(unittest.TestCase):
    def test_antique_plate_rejects_dresses_despite_description_words(self):
        query = {"title": "vintage Chinese hand painted CPC MADE IN MACAU Plate. Minor Chip",
                 "category": "Antiques:Asian Antiques:China:Plates",
                 "description": "A gift for the mother of the bride, suitable for weddings."}
        dress = {"name": "Mother of the Bride Dresses", "full_path": "Weddings > Clothing > Dresses > Mother of the Bride Dresses", "leaf": True}
        plate = {"name": "Plates", "full_path": "Home & Living > Kitchen & Dining > Dining & Serving > Plates", "leaf": True}
        self.assertEqual(score_candidate(query, dress), 0)
        self.assertGreater(score_candidate(query, plate), 0)

    def test_numbered_engraving_plate_uses_print_type(self):
        query = {"title": "1754 George Edwards Copper Engraving Plate 235 Parrot",
                 "category": "Art:Art Prints", "item_type": "Print"}
        self.assertEqual(product_anchors(query), {"print"})
        print_node = {"name": "Etchings & Engravings", "full_path": "Art & Collectibles > Prints > Etchings & Engravings"}
        dinner_node = {"name": "Plates", "full_path": "Home & Living > Kitchen & Dining > Dining & Serving > Plates"}
        self.assertGreater(score_candidate(query, print_node), score_candidate(query, dinner_node))
        self.assertEqual(score_candidate(query, dinner_node), 0)

    def test_camera_body_is_ranked_above_film_stock(self):
        query = {"title": "Vintage Nikon 35mm Film Camera", "category": "Cameras & Photo:Film Photography:Film Cameras"}
        camera = {"name": "Cameras", "full_path": "Electronics & Accessories > Cameras & Equipment > Cameras"}
        film = {"name": "Film", "full_path": "Electronics & Accessories > Cameras & Equipment > Film"}
        self.assertGreater(score_candidate(query, camera), score_candidate(query, film))

    def test_antique_porcelain_plate_rejects_disposable_party_tableware(self):
        query = {"title": "Antique porcelain plate", "category": "Antiques:China:Plates", "description": "A unique piece of antique porcelain."}
        party = {"name": "Plates", "full_path": "Paper & Party Supplies > Party Supplies > Party Decor > Tableware > Plates"}
        self.assertEqual(score_candidate(query, party), 0)

    def test_dinner_plate_rejects_other_meanings_of_plate(self):
        query = {"title": "Antique porcelain plate", "category": "Antiques:China:Plates"}
        for path in ["Electronics & Accessories > Decorative License Plates", "Craft Supplies & Tools > Printing Plates", "Home & Living > Bakeware > Pie Plates", "Home & Living > Office > Desk Name Plates"]:
            with self.subTest(path=path):
                self.assertEqual(score_candidate(query, {"name": path.split(' > ')[-1], "full_path": path}), 0)

    def test_unknown_product_does_not_receive_leaf_specificity_points(self):
        self.assertEqual(score_candidate({"title": "Unidentified antique object", "category": "Other Antiques"}, {"name": "Dresses", "full_path": "Clothing > Dresses", "leaf": True}), 0)

    def test_maker_overlap_cannot_make_a_ring_into_a_necklace(self):
        query = {"title": "James Avery Sterling Silver Ring", "category": "Jewelry & Watches:Fine Jewelry:Rings", "item_type": "Ring"}
        ring = {"name": "Rings", "full_path": "Jewelry > Rings"}
        necklace = {"name": "Beaded Necklaces", "full_path": "Jewelry > Necklaces > Beaded Necklaces"}
        self.assertGreater(score_candidate(query, ring), 0)
        self.assertEqual(score_candidate(query, necklace), 0)


class EtsySuggestionOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_ai_choice_outside_display_limit_is_ranked_first(self):
        nodes = [{"id": 1, "name": "Objects"}]
        candidates = [{"taxonomy_id": i, "name": f"Object {i}", "full_path": f"Objects > Object {i}", "local_score": 20-i, "leaf": True} for i in range(1, 11)]
        with patch("app.api.routes.reporting._fetch_seller_taxonomy_nodes", AsyncMock(return_value=nodes)), \
             patch("app.api.routes.reporting._prefilter_etsy_taxonomy_candidates", return_value=candidates), \
             patch("app.api.routes.reporting._rank_etsy_taxonomy_candidates_with_openai", return_value={"taxonomy_id": 10, "confidence": .95, "reason": "Correct object."}), \
             patch("app.api.routes.reporting.OPENAI_REQUEST_DELAY_SECONDS", 0):
            result = await _suggest_etsy_taxonomy({"title": "Object"}, limit=3)
        self.assertEqual(len(result["suggestions"]), 3)
        self.assertEqual(result["suggestions"][0]["taxonomy_id"], 10)
        self.assertEqual(result["best_match"]["taxonomy_id"], 10)
        self.assertEqual(result["suggestions"][0]["ai_confidence"], .95)
