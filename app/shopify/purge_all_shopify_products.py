import logging
import time
from app.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)


def purge_all_shopify_products(client=None):
    """
    Delete all products from Shopify store.
    Returns the number of products deleted.
    """
    if client is None:
        client = ShopifyClient()

    logger.info("Starting purge of all Shopify products")

    products = []
    endpoint = "products.json?limit=250"
    page_count = 0

    while endpoint:
        page_count += 1
        logger.info(f"Fetching products page {page_count}...")
        try:
            res = client.get(endpoint)
            batch = res.get("products", [])
            products.extend(batch)
            logger.info(f"Retrieved {len(batch)} products from page {page_count}")

            # Shopify pagination via Link header
            link_header = client.last_response.headers.get("Link")
            next_link = None

            if link_header:
                links = link_header.split(",")
                for link in links:
                    if 'rel="next"' in link:
                        next_link = link.split(";")[0].strip("<>")

            endpoint = next_link.replace(client.base_url + "/", "") if next_link else None

        except Exception as e:
            logger.error(f"Failed to fetch products page {page_count}: {e}")
            raise

    logger.info(f"Total products to delete: {len(products)}")

    deleted = 0
    failed = 0

    for i, p in enumerate(products, 1):
        pid = p["id"]
        title = p.get("title", f"Product {pid}")

        # Retry logic for delete operations
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                logger.info(f"Deleting product {i}/{len(products)}: {title} (ID: {pid}) - attempt {attempt + 1}")
                client.delete(f"products/{pid}.json")
                deleted += 1
                logger.info(f"Successfully deleted product: {title}")
                break  # Success, exit retry loop

            except Exception as e:
                logger.warning(f"Failed to delete product {title} (ID: {pid}) on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to delete product {title} (ID: {pid}) after {max_retries} attempts")
                    failed += 1

    logger.info(f"Purge completed. Deleted: {deleted}, Failed: {failed}")
    return deleted