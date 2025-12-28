import json
import sys
import requests
from typing import Dict, List, Tuple, Optional, Union
from app.config import settings

SHOP_DOMAIN = settings.SHOPIFY_STORE_URL_PROD
ADMIN_TOKEN = settings.SHOPIFY_PASSWORD_PROD
API_VERSION = "2024-07"

GRAPHQL_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"


def gql(query: str, variables: dict) -> dict:
    if not SHOP_DOMAIN or not ADMIN_TOKEN:
        raise RuntimeError("Missing SHOP_DOMAIN / ADMIN_TOKEN (settings)")

    resp = requests.post(
        GRAPHQL_URL,
        headers={
            "X-Shopify-Access-Token": ADMIN_TOKEN,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables},
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL top-level errors: {data['errors']}")
    return data["data"]


def normalize_tags(value: Union[str, List[str], None]) -> List[str]:
    """
    Mapping values can be:
      - "SC:..." (string)
      - ["tag1", "tag2"] (list)  [backward compatible]
      - None
    Returns a clean list of tags.
    """
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    seen = set()
    out: List[str] = []
    for t in value:
        tt = (t or "").strip()
        if tt and tt not in seen:
            out.append(tt)
            seen.add(tt)
    return out


def find_collection_id_by_title(title: str) -> Optional[str]:
    query = """
    query FindCollections($q: String!) {
      collections(first: 50, query: $q) {
        nodes { id title }
      }
    }
    """
    q = f'title:"{title}"'
    data = gql(query, {"q": q})
    for n in (data["collections"]["nodes"] or []):
        if n.get("title") == title:
            return n["id"]
    return None


def create_smart_collection(
    title: str,
    tags: List[str],
    use_or_logic: bool = True,
    sort_order: str = "CREATED_DESC",
) -> Tuple[Optional[str], List[str]]:
    mutation = """
    mutation CreateCollection($input: CollectionInput!) {
      collectionCreate(input: $input) {
        collection { id title }
        userErrors { field message }
      }
    }
    """

    rules = [{"column": "TAG", "relation": "EQUALS", "condition": t} for t in tags]

    input_payload = {
        "title": title,
        "sortOrder": sort_order,
        "ruleSet": {
            "appliedDisjunctively": bool(use_or_logic),
            "rules": rules,
        },
    }

    data = gql(mutation, {"input": input_payload})
    errs = data["collectionCreate"]["userErrors"] or []
    if errs:
        return None, [e["message"] for e in errs]

    col = data["collectionCreate"]["collection"]
    return (col["id"] if col else None), []


def update_smart_collection_rules(
    collection_id: str,
    tags: List[str],
    use_or_logic: bool = True,
    sort_order: str = "CREATED_DESC",
) -> Tuple[bool, List[str]]:
    mutation = """
    mutation UpdateCollection($input: CollectionInput!) {
      collectionUpdate(input: $input) {
        collection { id title }
        userErrors { field message }
      }
    }
    """

    rules = [{"column": "TAG", "relation": "EQUALS", "condition": t} for t in tags]

    input_payload = {
        "id": collection_id,
        "sortOrder": sort_order,
        "ruleSet": {
            "appliedDisjunctively": bool(use_or_logic),
            "rules": rules,
        },
    }

    data = gql(mutation, {"input": input_payload})
    errs = data["collectionUpdate"]["userErrors"] or []
    if errs:
        return False, [e["message"] for e in errs]
    return True, []


# NEW: mapping values can be string or list
MappingType = Dict[str, Dict[str, Union[str, List[str]]]]


def load_mapping(json_path: str) -> MappingType:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("Usage: python udpate_collections.py mapping.json [--dry-run]")
        sys.exit(1)

    json_path = sys.argv[1]
    dry_run = "--dry-run" in sys.argv
    mapping = load_mapping(json_path)

    total = 0
    updated = 0
    created = 0
    failed = 0
    skipped = 0

    for main_title, collections in mapping.items():
        if not isinstance(collections, dict):
            print(f"[SKIP] '{main_title}' value is not a dict of collections.")
            continue

        print(f"\n=== {main_title} ===")
        for collection_title, raw_value in collections.items():
            total += 1

            tags = normalize_tags(raw_value)

            # For SC mapping you expect exactly 1 tag, but we don't hard-fail.
            if not tags:
                skipped += 1
                print(f"[SKIP] {collection_title}: empty tag(s)")
                continue

            cid = find_collection_id_by_title(collection_title)

            if not cid:
                print(f"[MISSING] {collection_title}: will create")
                print(f"        tags: {tags}")

                if dry_run:
                    print("        (dry-run) would create")
                    continue

                new_id, errors = create_smart_collection(
                    title=collection_title,
                    tags=tags,
                    use_or_logic=True,
                    sort_order="CREATED_DESC",
                )
                if not new_id:
                    failed += 1
                    print("        ❌ create failed:", "; ".join(errors))
                    continue

                created += 1
                print(f"[CREATED] {collection_title} -> {new_id}")
                continue  # created with correct rules

            print(f"[FOUND] {collection_title} -> {cid}")
            print(f"        tags: {tags}")

            if dry_run:
                print("        (dry-run) would update rules")
                continue

            ok, errors = update_smart_collection_rules(
                collection_id=cid,
                tags=tags,
                use_or_logic=True,
                sort_order="CREATED_DESC",
            )
            if ok:
                updated += 1
                print("        ✅ updated")
            else:
                failed += 1
                print("        ❌ update failed:", "; ".join(errors))

    print("\n--- Summary ---")
    print(f"Total processed: {total}")
    print(f"Created:         {created}")
    print(f"Updated:         {updated}")
    print(f"Skipped:         {skipped}")
    print(f"Failed:          {failed}")


if __name__ == "__main__":
    main()
