# eBay to Shopify Sync - AI Agent Instructions

## Architecture Overview
This is a FastAPI-based middleware service that synchronizes eBay product listings to Shopify stores. The data flow is: eBay API → MongoDB (raw) → Normalization Pipeline → MongoDB (normalized) → Shopify API.

Key components:
- **app/**: FastAPI application with API routes, services, and clients
- **scripts/**: Standalone Python scripts for manual operations and testing
- **normalizer/**: Product data transformation pipeline with LLM enrichment
- **ebay/** & **shopify/**: Platform-specific API clients and operations

## Critical Workflows
- **Local Development**: Run `uvicorn app.main:app --host 0.0.0.0 --port 8080` to start the API server
- **Full Sync Process**: Execute `python -m scripts.run_full_sync` (normalizes raw data and syncs to Shopify)
- **Individual Steps**:
  - Fetch eBay: `python -m scripts.test_ebay_fetch`
  - Normalize: Call `/sync/dev/normalize-raw` API endpoint
  - Sync to Shopify: Call `/sync/dev/sync-shopify` API endpoint
- **Environment Switching**: Use `/sync/dev/*` routes for development Shopify store, `/sync/prod/*` for production
- **Database Reset**: `python -m scripts.reset_shopify_links` to clear Shopify product links

## Project Conventions
- **Async Everywhere**: All I/O operations use async/await (Motor for MongoDB, aiohttp implied)
- **Dev/Prod Separation**: Shopify clients instantiated with different credentials for dev vs prod environments
- **Metafield Mapping**: Extensive domain-specific metafield mappings in `app/services/normalizer_service.py` (e.g., antiques, books, blades)
- **Configuration**: Environment variables loaded via Pydantic Settings from `.env` file
- **Error Handling**: API clients store last response for debugging (`client.last_response`)
- **Normalization Pipeline**: `app/normalizer/pipeline.py` orchestrates title cleaning, category mapping, and hashing
- **LLM Enrichment**: OpenAI integration in `app/normalizer/enrich_llm.py` for product description enhancement (optional)

## Integration Patterns
- **eBay API**: Uses OAuth token and app credentials for product fetching
- **Shopify API**: Basic auth with API key/password, versioned endpoints (2023-10)
- **MongoDB**: Async operations with Motor, collections for raw/normalized/sync_log data
- **APScheduler**: Background job scheduling (commented out in main.py, runs every 30 minutes)
- **External Dependencies**: Requires `.env` with EBAY_*, SHOPIFY_*, MONGO_*, OPENAI_* variables

## Key Files to Reference
- `app/config.py`: All configuration settings and environment loading
- `app/services/normalizer_service.py`: Core normalization logic and metafield mappings
- `app/api/routes/sync.py`: Dev/prod API endpoint definitions
- `scripts/run_full_sync.py`: Orchestrates the complete sync workflow
- `app/shopify/client.py`: Handles Shopify API authentication and requests</content>
<parameter name="filePath">/Users/serigneciss/Desktop/Dev/ebay-shopify-sync/.github/copilot-instructions.md