import os
import sys
import asyncio
import argparse
import logging
import re
from typing import Any, Dict, Iterable, Optional


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient
from app.services.shopify_exclusions import BLOCKED_SHOPIFY_TAGS


logger = logging.getLogger(__name__)


def _make_shopify_client(env: str) -> ShopifyClient:
    if env == "prod":
        logger.info("Using Shopify PROD credentials")
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    logger.info("Using Shopify DEV credentials")
    return ShopifyClient()


def _is_http_ok(status: Optional[int]) -> bool:
    return status is not None and 200 <= int(status) < 300


def _coerce_tags(value: Any) -> list[str]:
    """Coerce tags from Mongo into a normalized list[str].

    Supports both:
      - list[str]
      - comma-separated string (legacy)
    """

    if value is None:
        return []
    if isinstance(value, list):
        return [str(t).strip() for t in value if str(t).strip()]
    if isinstance(value, str):
        return [t.strip() for t in value.split(",") if t.strip()]
    s = str(value).strip()
    return [s] if s else []


def _pick_first_matched_tag(doc_tags: Any, purge_tags: Iterable[str]) -> Optional[str]:
    doc_set = set(_coerce_tags(doc_tags))
    for t in purge_tags:
        if t in doc_set:
            return t
    return None


async def purge_blocked_tag_from_shopify(
    *,
    env: str,
    tags: list[str],
    dry_run: bool,
    limit: Optional[int],
    max_concurrency: int,
) -> Dict[str, Any]:
    """Delete Shopify products linked to Mongo docs that contain any `tags` in `tags`.

    - Finds docs in `db.product_normalized` where tags contains `tag` and shopify_id exists
    - Deletes `products/{shopify_id}.json` in Shopify
    - Clears Shopify link fields in Mongo (so they won't be treated as already-synced)

    Safe defaults:
    - dry_run=True by default in CLI
    """

    shopify_client = _make_shopify_client(env)

    purge_tags = [t.strip() for t in (tags or []) if isinstance(t, str) and t.strip()]
    if not purge_tags:
        raise ValueError("No tags provided to purge")

    # Support both representations:
    # - tags as a list[str] (preferred)
    # - tags as a single comma-separated string (legacy)
    alt = "|".join(re.escape(t) for t in purge_tags)
    query: Dict[str, Any] = {
        "$and": [
            {
                "$or": [
                    {"tags": {"$in": purge_tags}},
                    {"tags": {"$regex": rf"(^|,\s*)(?:{alt})(,|$)"}},
                ]
            },
            {"shopify_id": {"$exists": True, "$ne": None}},
        ]
    }

    projection = {
        "_id": 1,
        "tags": 1,
        "shopify_id": 1,
        "shopify_variant_id": 1,
        "inventory_item_id": 1,
        "location_id": 1,
    }

    cursor = db.product_normalized.find(query, projection).sort("_id", 1)

    docs: list[Dict[str, Any]] = []
    async for doc in cursor:
        docs.append(doc)
        if limit is not None and len(docs) >= limit:
            break

    total = len(docs)
    deleted_ok = 0
    already_gone = 0
    failed = 0
    cleared_links = 0

    sem = asyncio.Semaphore(max_concurrency)

    async def process_doc(doc: Dict[str, Any]) -> None:
        nonlocal deleted_ok, already_gone, failed, cleared_links

        sku = doc.get("_id")
        pid = doc.get("shopify_id")
        matched_tag = _pick_first_matched_tag(doc.get("tags"), purge_tags)

        if not pid:
            return

        logger.info("[PURGE] sku=%s shopify_id=%s matched_tag=%s", sku, pid, matched_tag)

        if dry_run:
            return

        async with sem:
            try:
                resp = await shopify_client.delete(f"products/{int(pid)}.json")
                status = getattr(getattr(shopify_client, "last_response", None), "status", None)

                if _is_http_ok(status):
                    deleted_ok += 1
                elif int(status or 0) == 404:
                    # Treat as success: already deleted in Shopify.
                    already_gone += 1
                else:
                    failed += 1
                    logger.error(
                        "[PURGE] Delete failed | sku=%s | shopify_id=%s | status=%s | resp=%r",
                        sku,
                        pid,
                        status,
                        resp,
                    )
                    return

                # Clear Shopify linkage fields in Mongo.
                reason = (
                    f"blocked_tag:{matched_tag}"
                    if matched_tag
                    else f"blocked_tag:any_of:{','.join(purge_tags)}"
                )
                await db.product_normalized.update_one(
                    {"_id": sku},
                    {
                        "$set": {
                            "excluded_from_shopify": True,
                            "excluded_reason": reason,
                        },
                        "$unset": {
                            "shopify_id": "",
                            "shopify_variant_id": "",
                            "inventory_item_id": "",
                            "location_id": "",
                            "last_synced_hash": "",
                        },
                    },
                )
                cleared_links += 1

            except Exception as e:
                failed += 1
                logger.exception("[PURGE] Exception | sku=%s | shopify_id=%s | err=%s", sku, pid, e)

    tasks = [asyncio.create_task(process_doc(d)) for d in docs]
    if tasks:
        await asyncio.gather(*tasks)

    return {
        "env": env,
        "tags": purge_tags,
        "dry_run": dry_run,
        "matched_docs": total,
        "deleted_ok": deleted_ok,
        "already_gone": already_gone,
        "failed": failed,
        "cleared_links": cleared_links,
        "max_concurrency": max_concurrency,
        "limit": limit,
    }


async def _async_main(args: argparse.Namespace) -> None:
    summary = await purge_blocked_tag_from_shopify(
        env=args.env,
        tags=args.tags,
        dry_run=args.dry_run,
        limit=args.limit,
        max_concurrency=args.max_concurrency,
    )

    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Delete Shopify products linked to Mongo docs that contain a blocked tag (default: Category:Militaria). "
            "Also clears Shopify linkage fields in Mongo so the item won't be treated as already-synced."
        )
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Which Shopify environment to use (default: dev)",
    )
    parser.add_argument(
        "--tag",
        dest="tags",
        action="append",
        default=None,
        help=(
            "Tag to purge (repeatable). "
            "If omitted, defaults to all tags in BLOCKED_SHOPIFY_TAGS. "
            f"Known blocked tags: {sorted(BLOCKED_SHOPIFY_TAGS)}"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of docs to process",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete from Shopify (default is dry-run)",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=5,
        help="Maximum concurrent delete operations (still rate-limited by ShopifyClient)",
    )

    args = parser.parse_args()

    dry_run = not bool(args.execute)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(name)s: %(message)s",
    )

    # Patch the parsed args for downstream helpers
    args.dry_run = dry_run
    args.tags = args.tags or sorted(BLOCKED_SHOPIFY_TAGS)

    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
