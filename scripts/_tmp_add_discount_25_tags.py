import asyncio
from app.database.mongo import db

PRODUCT_IDS = [
    "156790343990",
    "156696694894",
    "157167357337",
    "EB-157389852161",
    "156166904423",
    "EB-156459502977",
    "156167989459",
    "157098017490",
    "EB-156796964542",
    "156583287576",
    "156583354741",
    "157098116099",
    "156008380775",
    "157468215017",
    "EB-157394671550",
    "157475490171",
    "157475519199",
    "156715175591",
    "157002311443",
    "EB-157401469215",
    "EB-157401494855",
    "156264625362",
    "157009175164",
    "156177493383",
    "156915967207",
    "156649158407",
    "157119578664",
    "156538302086",
    "156371213777",
    "156180381523",
    "156371252697",
    "157416578692",
    "EB-157023115566",
    "157023142637",
    "156928994974",
    "157348620695",
    "156929040589",
    "157026564045",
    "156477621461",
    "155972006023",
    "155972009862",
    "155972012384",
    "155972012727",
    "155972026813",
    "157421014141",
    "157421016525",
    "157421049651",
    "156740361019",
    "EB-156833754565",
    "157505150896",
    "157505203518",
    "156377811616",
    "157423229655",
    "EB-156659267231",
    "157505324792",
    "EB-157355195568",
    "157355207620",
    "157037367635",
    "EB-156143895121",
    "EB-157355265573",
    "156938165711",
    "156941049089",
    "156381021694",
    "156941116798",
    "155978730968",
    "157510117008",
    "156034098288",
    "156609903986",
    "156382599498",
    "EB-157284840721",
    "EB-157284852014",
    "157284908020",
    "157284913398",
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
        {"$addToSet": {"tags": "discount_25"}},
    )

    cursor = db.product_normalized.find(filter_query, {"_id": 1, "sku": 1, "tags": 1})
    found_docs = await cursor.to_list(length=None)

    found_ids = set()
    with_tag = 0
    for doc in found_docs:
        _id = doc.get("_id")
        sku = doc.get("sku")
        if _id is not None:
            found_ids.add(str(_id))
        if sku is not None:
            found_ids.add(str(sku))
        tags = doc.get("tags") or []
        if "discount_25" in tags:
            with_tag += 1

    requested = set(string_ids)
    missing = sorted(requested - found_ids)

    print("REQUESTED", len(string_ids))
    print("MATCHED", result.matched_count)
    print("MODIFIED", result.modified_count)
    print("FOUND_UNIQUE_IDS", len(found_ids & requested))
    print("WITH_TAG", with_tag)
    print("MISSING", len(missing))
    if missing:
        print("MISSING_IDS", missing)


if __name__ == "__main__":
    asyncio.run(main())
