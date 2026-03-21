import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Optional

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

import aiohttp
from aiolimiter import AsyncLimiter

# Allow running as either:
# - python -m scripts.populate_baserow_from_normalized
# - python scripts/populate_baserow_from_normalized.py
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from app.config import settings
from app.database.mongo import db
from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)


def _require_setting(name: str, value: Any) -> Any:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise RuntimeError(
            f"Missing required setting {name}. Put it in .env (see BASEROW_* variables)."
        )
    return value


def _join_url(base_url: str, path: str) -> str:
    base = (base_url or "").rstrip("/")
    p = (path or "").lstrip("/")
    return f"{base}/{p}"


def _first_image_url(images: Any) -> Optional[str]:
    if not images:
        return None
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, str):
            return first.strip() or None
        if isinstance(first, dict):
            for k in ("url", "src", "source", "link"):
                v = first.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None


def _tags_join(tags: Any, prefix: str) -> str:
    if not tags or not isinstance(tags, list):
        return ""
    out: list[str] = []
    for t in tags:
        if not isinstance(t, str):
            continue
        if t.startswith(prefix):
            out.append(t.replace(prefix, "", 1).strip())
    out = [x for x in out if x]
    return ", ".join(out)


def _money_2dp(value: object) -> float | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value).strip())
        d = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return None


class BaserowClient:
    def __init__(self, *, base_url: str, api_token: str, rate_limiter: AsyncLimiter | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token
        # Default: keep it gentle; Baserow is usually fast.
        self.limiter = rate_limiter or AsyncLimiter(5, 1)

    def _headers(self) -> dict:
        return {
            "Authorization": f"Token {self.api_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, *, params: dict | None = None, json: Any = None) -> tuple[int, Any]:
        url = _join_url(self.base_url, path)
        async with self.limiter:
            async with aiohttp.ClientSession(headers=self._headers()) as session:
                async with session.request(method, url, params=params, json=json) as resp:
                    status = resp.status
                    try:
                        data = await resp.json()
                    except Exception:
                        data = await resp.text()
                    return status, data

    async def iter_rows(self, *, table_id: int, page_size: int = 200) -> Any:
        page = 1
        while True:
            status, data = await self._request(
                "GET",
                f"/api/database/rows/table/{table_id}/",
                params={
                    "user_field_names": "true",
                    "page": str(page),
                    "size": str(page_size),
                },
            )
            if status >= 400:
                raise RuntimeError(f"Baserow list rows failed HTTP {status}: {data}")

            results = (data or {}).get("results") or []
            if not results:
                return

            for row in results:
                yield row

            # Baserow uses a cursor-style 'next' URL.
            if not (data or {}).get("next"):
                return
            page += 1

    async def batch_create_rows(self, *, table_id: int, items: list[dict]) -> dict:
        status, data = await self._request(
            "POST",
            f"/api/database/rows/table/{table_id}/batch/",
            params={"user_field_names": "true"},
            json={"items": items},
        )
        if status >= 400:
            raise RuntimeError(f"Baserow batch create failed HTTP {status}: {data}")
        return data


def _make_shopify_client(env: str) -> ShopifyClient | None:
    if env == "none":
        return None
    if env == "prod":
        logger.info("Using Shopify PROD credentials (for handle lookup)")
        return ShopifyClient(
            api_key=settings.SHOPIFY_API_KEY_PROD,
            password=settings.SHOPIFY_PASSWORD_PROD,
            store_url=settings.SHOPIFY_STORE_URL_PROD,
        )
    logger.info("Using Shopify DEV credentials (for handle lookup)")
    return ShopifyClient()


def _shopify_last_status(client: ShopifyClient) -> Optional[int]:
    return getattr(getattr(client, "last_response", None), "status", None)


async def _shopify_get_handle(shopify: ShopifyClient, product_id: int) -> Optional[str]:
    res = await shopify.get(
        f"products/{int(product_id)}.json",
        params={"fields": "handle"},
    )
    status = _shopify_last_status(shopify)
    if status is not None and status >= 400:
        logger.warning("Shopify handle lookup failed | id=%s | http=%s | res=%s", product_id, status, res)
        return None
    product = (res or {}).get("product") or {}
    handle = product.get("handle")
    if isinstance(handle, str) and handle.strip():
        return handle.strip()
    return None


async def populate_baserow(
    *,
    baserow_base_url: str | None,
    baserow_api_token: str | None,
    baserow_table_id: int | None,
    limit: int | None,
    batch_size: int,
    shopify_env: str,
    dry_run: bool,
) -> dict:
    base_url = _require_setting(
        "BASEROW_BASE_URL",
        baserow_base_url if baserow_base_url is not None else settings.BASEROW_BASE_URL,
    )
    api_token = _require_setting(
        "BASEROW_API_TOKEN",
        baserow_api_token if baserow_api_token is not None else settings.BASEROW_API_TOKEN,
    )
    table_id = int(
        _require_setting(
            "BASEROW_TABLE_ID",
            baserow_table_id if baserow_table_id is not None else settings.BASEROW_TABLE_ID,
        )
    )

    baserow = BaserowClient(base_url=base_url, api_token=api_token)
    shopify = _make_shopify_client(shopify_env)

    # 1) Load existing shopify_product_id values to avoid duplicates.
    existing_ids: set[str] = set()
    async for row in baserow.iter_rows(table_id=table_id):
        v = row.get("shopify_product_id")
        if v is None:
            continue
        s = str(v).strip()
        if s:
            existing_ids.add(s)

    logger.info("Baserow: loaded %s existing rows (by shopify_product_id)", len(existing_ids))

    # 2) Iterate normalized products from Mongo.
    query = {"shopify_id": {"$exists": True, "$ne": None}}
    projection = {
        "title": 1,
        "description": 1,
        "price": 1,
        "images": 1,
        "tags": 1,
        "shopify_id": 1,
    }

    cursor = db.product_normalized.find(query, projection).sort("_id", 1)

    created = 0
    skipped_existing = 0
    skipped_missing_shopify_id = 0
    errors = 0
    processed = 0

    handle_cache: dict[int, Optional[str]] = {}

    async def build_item(doc: dict) -> Optional[dict]:
        nonlocal skipped_existing, skipped_missing_shopify_id

        shopify_id = doc.get("shopify_id")
        if shopify_id is None:
            skipped_missing_shopify_id += 1
            return None

        try:
            shopify_id_int = int(shopify_id)
        except Exception:
            skipped_missing_shopify_id += 1
            return None

        if str(shopify_id_int) in existing_ids:
            skipped_existing += 1
            return None

        handle: Optional[str] = None
        if shopify is not None:
            if shopify_id_int not in handle_cache:
                handle_cache[shopify_id_int] = await _shopify_get_handle(shopify, shopify_id_int)
            handle = handle_cache.get(shopify_id_int)

        product_url = f"https://gallery1880.com/products/{handle}" if handle else ""

        tags = doc.get("tags") or []
        return {
            "title": (doc.get("title") or "").strip(),
            "shopify_product_id": shopify_id_int,
            "description": (doc.get("description") or "").strip(),
            "price": _money_2dp(doc.get("price")),
            "product_url": product_url,
            "primary_image_url": _first_image_url(doc.get("images")),
            "category": _tags_join(tags, "Category:"),
            "era": _tags_join(tags, "Era:"),
        }

    pending: list[dict] = []

    async for doc in cursor:
        processed += 1

        try:
            item = await build_item(doc)
            if item is None:
                continue

            pending.append(item)

            # Respect limit as "max new rows created".
            if limit is not None and (created + len(pending)) >= int(limit):
                pending = pending[: max(0, int(limit) - created)]

            if len(pending) >= batch_size or (limit is not None and (created + len(pending)) >= int(limit)):
                if dry_run:
                    logger.info("Dry-run: would create %s rows", len(pending))
                    created += len(pending)
                else:
                    await baserow.batch_create_rows(table_id=table_id, items=pending)
                    created += len(pending)
                    logger.info("Created %s rows (total=%s)", len(pending), created)

                # Mark these as existing so we don't create duplicates within the same run.
                for it in pending:
                    existing_ids.add(str(it.get("shopify_product_id")))

                pending = []

                if limit is not None and created >= int(limit):
                    break

        except Exception as e:
            errors += 1
            logger.exception("Error processing doc _id=%s: %s", doc.get("_id"), e)

    if pending:
        if dry_run:
            logger.info("Dry-run: would create %s rows", len(pending))
            created += len(pending)
        else:
            await baserow.batch_create_rows(table_id=table_id, items=pending)
            created += len(pending)
            logger.info("Created %s rows (total=%s)", len(pending), created)

    return {
        "processed_mongo_docs": processed,
        "created_rows": created,
        "skipped_existing": skipped_existing,
        "skipped_missing_shopify_id": skipped_missing_shopify_id,
        "errors": errors,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate a Baserow table from MongoDB product_normalized.")
    p.add_argument(
        "--baserow-base-url",
        default=None,
        help="Override BASEROW_BASE_URL from .env (e.g. https://api.baserow.io or your self-hosted URL).",
    )
    p.add_argument(
        "--baserow-api-token",
        default=None,
        help="Override BASEROW_API_TOKEN from .env.",
    )
    p.add_argument(
        "--baserow-table-id",
        type=int,
        default=None,
        help="Override BASEROW_TABLE_ID from .env.",
    )
    p.add_argument("--limit", type=int, default=None, help="Max number of NEW rows to create (for testing).")
    p.add_argument("--batch-size", type=int, default=25, help="Baserow batch size.")
    p.add_argument(
        "--shopify-env",
        choices=["prod", "dev", "none"],
        default="prod",
        help="Which Shopify credentials to use for handle lookup (product_url).",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not write to Baserow; just log.")
    p.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING...")
    return p.parse_args()


async def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    res = await populate_baserow(
        baserow_base_url=args.baserow_base_url,
        baserow_api_token=args.baserow_api_token,
        baserow_table_id=args.baserow_table_id,
        limit=args.limit,
        batch_size=int(args.batch_size),
        shopify_env=str(args.shopify_env),
        dry_run=bool(args.dry_run),
    )
    logger.info("Done: %s", res)


if __name__ == "__main__":
    asyncio.run(main())
