import asyncio
from app.database.mongo import db

PRODUCT_IDS = [
    "156696619841",
    "EB-156790341919",
    "156696716826",
    "156459123888",
    "156459127814",
    "156459497697",
    "156708788635",
    "157326766326",
    "156899837326",
    "EB-157176486671",
    "EB-156172341258",
    "156999098398",
    "156999174261",
    "EB-157399225699",
    "156715057012",
    "156715085545",
    "156529007490",
    "EB-157254715727",
    "EB-157401461867",
    "EB-157401465609",
    "156364567726",
    "156645820608",
    "EB-156645833894",
    "EB-157259423116",
    "156721634965",
    "EB-157336952360",
    "EB-157009132542",
    "157259474838",
    "157336977857",
    "EB-157259499690",
    "EB-156912768110",
    "EB-156177506003",
    "EB-156177509583",
    "156649112344",
    "EB-157261781857",
    "157489183190",
    "157119538302",
    "157119552377",
    "157119560203",
    "157489320340",
    "157340828765",
    "157121473782",
    "156650723921",
    "156650744495",
    "EB-156422796233",
    "156422839777",
    "157264421805",
    "156822803691",
    "156423814761",
    "156423817773",
    "156423825743",
    "156473330270",
    "156922398761",
    "156922504557",
    "157495446734",
    "156270433461",
    "157495455084",
    "156270444367",
    "156922569286",
    "156371268476",
    "EB-156654219546",
    "EB-156734150934",
    "156541821608",
    "EB-156654255495",
    "157416563846",
    "156831475080",
    "157348552758",
    "EB-157272134017",
    "157348596692",
    "156929078319",
    "156929081197",
    "157418928287",
    "157418943829",
    "156477646543",
    "156656054531",
    "155972005220",
    "155972005771",
    "157029935211",
    "156740368098",
    "EB-157029940359",
    "156740374358",
    "EB-157029947127",
    "156227876401",
    "EB-157351769254",
    "157503943601",
    "155973823637",
    "155973827897",
    "155974724233",
    "157423245509",
    "156377835441",
    "156480877403",
    "EB-156659281707",
    "157279963565",
    "156938085968",
    "156938091454",
    "157427782243",
    "157510140391",
    "156382584783",
    "EB-157044633244",
    "156382594691",
    "157358903554",
    "EB-157358911010",
    "157358913976",
    "157430096603",
    "157430113018",
    "156611439832",
    "EB-156146649041",
    "156146690114",
]


def _numeric_ids(ids):
    out = []
    for value in ids:
        if isinstance(value, str) and value.isdigit():
            try:
                out.append(int(value))
            except Exception:
                pass
    return out


async def main():
    string_ids = PRODUCT_IDS
    number_ids = _numeric_ids(PRODUCT_IDS)

    filter_query = {
        "$or": [
            {"_id": {"$in": string_ids}},
            {"sku": {"$in": string_ids}},
            {"_id": {"$in": number_ids}},
            {"sku": {"$in": number_ids}},
        ]
    }

    result = await db.product_normalized.update_many(
        filter_query,
        {"$addToSet": {"tags": "discount_15"}},
    )

    cursor = db.product_normalized.find(filter_query, {"_id": 1, "sku": 1})
    found_docs = await cursor.to_list(length=None)

    found_ids = set()
    for doc in found_docs:
        _id = doc.get("_id")
        sku = doc.get("sku")
        if _id is not None:
            found_ids.add(str(_id))
        if sku is not None:
            found_ids.add(str(sku))

    requested = set(string_ids)
    missing = sorted(requested - found_ids)

    print("REQUESTED", len(string_ids))
    print("MATCHED", result.matched_count)
    print("MODIFIED", result.modified_count)
    print("FOUND_UNIQUE_IDS", len(found_ids & requested))
    print("MISSING", len(missing))
    if missing:
        print("MISSING_IDS", missing)


if __name__ == "__main__":
    asyncio.run(main())
