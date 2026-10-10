"""Object-first retrieval for Etsy category suggestions.

Scores rank candidates; they are not calibrated probabilities of correctness.
"""
import html
import re

MATCHING_VERSION = "object-first-v1"
STOP_WORDS = set("a an and are as at be by for from in is it of on or the to was were with made antique antiques vintage rare original collectible collection good condition minor chip hand painted signed large small inch inches century circa very clean untested working works read new old lot set pair other unknown miscellaneous assorted united states china chinese german germany french france japan japanese silver gold brass bronze porcelain glass wood wooden crystal sterling plated material style art nouveau deco handmade black white red blue green brown".split())


def object_tokens(value):
    words = re.findall(r"[a-z]+", html.unescape(str(value or "")).lower())
    plurals = {"watches": "watch", "wristwatches": "watch", "wristwatch": "watch",
               "dishes": "dish", "boxes": "box", "dresses": "dress",
               "photographs": "photo", "photograph": "photo"}
    return {plurals.get(w, w[:-1] if w.endswith("s") and not w.endswith("ss") and len(w) > 3 else w)
            for w in words if len(w) > 2 and w not in STOP_WORDS}


# Resolve common product names before descriptive text enters retrieval.
OBJECT_NAMES = (
    "salt pepper shaker", "watch box", "cigarette case", "perfume bottle",
    "picture frame", "table lamp", "pocket watch", "wrist watch", "music box",
    "pendant necklace", "tea cup", "fountain pen", "ballpoint pen", "rollerball pen",
    "plate", "vase", "bowl", "basket", "clock", "watch", "camera", "necklace",
    "bracelet", "earring", "ring", "figurine", "sculpture", "statue", "doll",
    "lighter", "pipe", "book", "comic", "scarf", "sunglass", "jacket", "purse",
    "handbag", "wallet", "mask", "ashtray", "pitcher", "teapot", "decanter",
    "bookend", "candlestick", "candelabra", "lantern", "lamp", "telescope",
    "binocular", "compass", "pen", "pencil", "kaleidoscope", "fossil", "mineral",
    "engraving", "lithograph", "serigraph", "print", "photograph", "photo",
    "ticket", "memorabilia", "advertisement", "ad", "shackle", "knife", "sword",
)


def product_anchors(query):
    explicit = object_tokens(query.get("item_type"))
    category = str(query.get("category") or "")
    leaf = re.split(r"\s*(?:>|:)\s*", category)[-1]
    category_tokens = object_tokens(leaf)
    title_tokens = object_tokens(query.get("title"))
    # Prefer the type shared by title and source category, so an engraved
    # print numbered 'Plate 235' cannot become dinnerware.
    shared = title_tokens & category_tokens
    known = {word for phrase in OBJECT_NAMES for word in object_tokens(phrase)}
    explicit_objects = explicit & known
    primary = explicit_objects or (shared & known) or shared or category_tokens
    if primary:
        return primary
    for phrase in OBJECT_NAMES:
        words = object_tokens(phrase)
        if words and words <= title_tokens:
            return words
    return title_tokens


def score_candidate(query, candidate):
    anchors = product_anchors(query)
    name = object_tokens(candidate.get("name"))
    path = object_tokens(candidate.get("full_path"))
    primary_overlap = anchors & name
    path_overlap = anchors & path
    title = object_tokens(query.get("title"))
    category = object_tokens(query.get("category"))
    known = {word for phrase in OBJECT_NAMES for word in object_tokens(phrase)}
    if anchors & known and not (primary_overlap or path_overlap):
        return 0.0
    source = str(query.get("title") or "") + " " + str(query.get("description") or "")
    full_path = str(candidate.get("full_path") or "").lower()
    # Shared words can name different objects: a ceramic plate is not a
    # license plate, pie pan, printing plate, or desk nameplate.
    if anchors == {"plate"}:
        if re.search(r"license|printing|desk name|pie plates", full_path) and not re.search(r"\b(license|printing|nameplate|pie)\b", source, re.I):
            return 0.0
    if anchors == {"watch"} and re.search(r"watch bands|watch necklaces|watch rings|watch straps", full_path):
        if not re.search(r"watch band|watch necklace|watch ring|watch strap", source, re.I):
            return 0.0
    if "camera" in anchors and str(candidate.get("name") or "").lower() != "cameras":
        if re.search(r"film$|camera bags|camera cases|camera straps", full_path):
            return 0.0
    if "digital" in full_path and re.search(r"\b(framed|leather.bound|antique|serigraph|lithograph|engraving|etching)\b", source, re.I) and not re.search(r"\b(digital|download)\b", source, re.I):
        return 0.0
    if "party supplies" in full_path and re.search(r"\b(porcelain|ironstone|silverplate|antique)\b", source, re.I) and not re.search(r"\b(disposable|paper plates|party plates)\b", source, re.I):
        return 0.0
    # Never award specificity points to a category with no product evidence.
    if not (primary_overlap or path_overlap or title & name or category & name):
        return 0.0
    score = 30 * len(primary_overlap) + 8 * len(path_overlap)
    score += 6 * len(title & name) + 5 * len(category & name)
    score += len(object_tokens(query.get("description")) & name) * 0.25
    # Descendant paths without an object-name match must not outrank the
    # category naming the object merely because they are deeper or leaves.
    score -= 0.2 * len(name - title - category - anchors)
    return round(score, 3)
