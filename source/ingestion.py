"""
Ingestion orchestration module for CoinGecko market data.

This module implements the control flow for:
- Initial runs (fetch top 350, backfill historical data)
- Incremental runs (update existing assets, add new top assets)
- Resumable execution after failures or rate limits
"""

import time
import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

try:
    from .duckdb_store import DuckDBStore
    from .coingecko_api import CoinGeckoAPI, CoinGeckoSecrets
except ImportError:
    from duckdb_store import DuckDBStore
    from coingecko_api import CoinGeckoAPI, CoinGeckoSecrets

logger = logging.getLogger(__name__)


class IngestionOrchestrator:
    """
    Orchestrates the CoinGecko data ingestion process.
    
    Implements a restartable, idempotent ingestion pipeline that:
    1. Fetches top 350 assets by market cap
    2. Backfills ~1 year of historical data
    3. Continues incrementally forward
    """
    
    # Target number of top tokens to collect by market cap
    TARGET_TOP_TOKENS = 350
    
    # CoinGecko API max results per page
    MAX_PER_PAGE = 250
    
    # Number of tokens to get from page 2 (TARGET_TOP_TOKENS - MAX_PER_PAGE)
    PAGE_2_TOKEN_COUNT = 100
    
    # Default backfill period: ~2 years
    DEFAULT_BACKFILL_DAYS = 729
    
    # Skip assets that were updated within this threshold (seconds)
    UPDATE_THRESHOLD_SECONDS = 3600  # 1 hour
    
    def __init__(
        self,
        db_path: str = "market_data.duckdb",
        secrets: Optional[Dict] = None
    ):
        """
        Initialize the ingestion orchestrator.
        
        :param db_path: Path to DuckDB database file
        :param secrets: API configuration dict (base_url, parameters)
        """
        self.store = DuckDBStore(db_path)
        self.secrets = secrets or self._default_secrets()
        
    def _default_secrets(self) -> Dict:
        """Return default API configuration."""
        current_ts = int(time.time())
        backfill_start = current_ts - (self.DEFAULT_BACKFILL_DAYS * 24 * 3600)
        
        return {
            "base_url": "https://pro-api.coingecko.com/api/v3/",
            "parameters": {
                "coinmarkets": {
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": "250",
                    "page": None  # Will be set per page fetch
                },
                "marketchart": {
                    "vs_currency": "usd",
                    "initial_query_from": backfill_start,
                    "query_to": current_ts
                }
            }
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the main ingestion flow.
        
        Automatically determines whether to run initial or incremental mode
        based on database state.
        
        :return: Summary of ingestion results
        """
        try:
            self.store.update_ingestion_state(run_status=DuckDBStore.RUN_STATUS_RUNNING)
            
            if self.store.is_initial_run():
                logger.info("Starting initial run - fetching top assets")
                self._initial_run()
            else:
                logger.info("Starting incremental run")
                self._incremental_run()
            
            # Run market chart ingestion
            self._run_market_chart_ingestion()
            
            self.store.update_ingestion_state(run_status=DuckDBStore.RUN_STATUS_IDLE)
            
            return self.store.get_ingestion_summary()
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            self.store.update_ingestion_state(run_status=DuckDBStore.RUN_STATUS_ERROR)
            raise
    
    def _initial_run(self):
        """
        Execute initial run: fetch top 350 assets and initialize state.
        """
        # Fetch top 350 coins by market cap
        assets = self._fetch_coin_markets()
        
        if not assets:
            logger.warning("No assets returned from coins/markets endpoint")
            return
        
        # Persist asset metadata for all fetched assets
        logger.info(f"Persisting metadata for {len(assets)} assets")
        self.store.upsert_asset_metadata(assets)
        
        # Initialize ingestion state for each asset
        for asset in assets:
            self.store.initialize_asset_ingestion_state(asset["asset_id"])
        
        logger.info(f"Initialized {len(assets)} assets for ingestion")
    
    def _incremental_run(self):
        """
        Execute incremental run: refresh top 350 and merge with existing.
        
        This method:
        1. Fetches the current top assets from the API
        2. Updates metadata for assets in the current top list
        3. Marks assets that dropped out of the top list (sets market_cap_rank to NULL)
        4. Initializes ingestion state for any newly added assets
        """
        # Refresh top 350 list
        current_top_assets = self._fetch_coin_markets()
        
        if current_top_assets:
            # Update metadata for current top assets (with new rankings)
            logger.info(f"Updating metadata for {len(current_top_assets)} assets")
            self.store.upsert_asset_metadata(current_top_assets)
            
            # Mark assets that dropped out of the top list
            # This sets their market_cap_rank to NULL and updates last_updated_ts
            current_top_ids = [asset["asset_id"] for asset in current_top_assets]
            self.store.mark_assets_dropped_from_top_list(current_top_ids)
            
            # Initialize state for any new assets
            # Note: We check against asset_ingestion_state (not asset_metadata) because
            # new tokens were just added to asset_metadata above. This ensures we correctly
            # identify assets that need their ingestion state initialized for market chart data.
            assets_with_state = set(self.store.get_assets_with_ingestion_state())
            new_assets_count = 0
            for asset in current_top_assets:
                if asset["asset_id"] not in assets_with_state:
                    self.store.initialize_asset_ingestion_state(asset["asset_id"])
                    logger.info(f"Added new asset to tracking: {asset['asset_id']}")
                    new_assets_count += 1
            if new_assets_count > 0:
                logger.info(f"Initialized {new_assets_count} new assets for market data collection")
    
    def _fetch_coin_markets(self) -> List[Dict[str, Any]]:
        """
        Fetch top TARGET_TOP_TOKENS assets from coins/markets endpoint with resumable pagination support.
        
        Fetches page 1 (MAX_PER_PAGE tokens) and page 2 (MAX_PER_PAGE tokens, sliced to PAGE_2_TOKEN_COUNT) 
        and combines results.
        Always re-fetches both pages to ensure current metadata.
        
        Note: CoinGecko pagination formula is (page-1)*per_page+1 to page*per_page.
        To get tokens 251-350, we must use page=2 with per_page=250 (returns 251-500),
        then slice to get only the first 100 (tokens 251-350).
        
        :return: List of asset dictionaries (up to TARGET_TOP_TOKENS)
        """
        all_assets = []
        
        # Always fetch page 1 (for current metadata)
        logger.info(f"Fetching page 1 (top {self.MAX_PER_PAGE} tokens)")
        page1_assets = self._fetch_coin_markets_page(page=1, per_page=self.MAX_PER_PAGE)
        if page1_assets:
            all_assets.extend(page1_assets)
            logger.info(f"Fetched page 1: {len(page1_assets)} assets")
        else:
            logger.error("No assets returned from page 1 - cannot proceed")
            return []
        
        # Always fetch page 2 (for current metadata, even if previously completed)
        # Use per_page=MAX_PER_PAGE to get tokens 251-500, then slice to get only 251-350
        logger.info(f"Fetching page 2 (tokens {self.MAX_PER_PAGE + 1}-{self.TARGET_TOP_TOKENS})")
        page2_assets = self._fetch_coin_markets_page(page=2, per_page=self.MAX_PER_PAGE)
        if page2_assets:
            # Slice to get only first PAGE_2_TOKEN_COUNT tokens (ranks 251-350)
            page2_assets = page2_assets[:self.PAGE_2_TOKEN_COUNT]
            all_assets.extend(page2_assets)
            logger.info(f"Fetched page 2: {len(page2_assets)} assets (sliced to {self.PAGE_2_TOKEN_COUNT})")
        else:
            # Retry page 2 once if it fails
            logger.warning("Page 2 returned no assets, retrying...")
            page2_assets = self._fetch_coin_markets_page(page=2, per_page=self.MAX_PER_PAGE)
            if page2_assets:
                # Slice to get only first PAGE_2_TOKEN_COUNT tokens (ranks 251-350)
                page2_assets = page2_assets[:self.PAGE_2_TOKEN_COUNT]
                all_assets.extend(page2_assets)
                logger.info(f"Page 2 retry successful: {len(page2_assets)} assets (sliced to {self.PAGE_2_TOKEN_COUNT})")
            else:
                logger.error(f"Page 2 failed after retry - only {self.MAX_PER_PAGE} tokens collected")
        
        # Validate we got close to TARGET_TOP_TOKENS tokens
        total_assets = len(all_assets)
        if total_assets < self.TARGET_TOP_TOKENS - 50:
            logger.warning(f"Only collected {total_assets} tokens, expected ~{self.TARGET_TOP_TOKENS}")
        elif total_assets == self.TARGET_TOP_TOKENS:
            logger.info(f"Successfully collected all {self.TARGET_TOP_TOKENS} tokens")
        else:
            logger.info(f"Collected {total_assets} tokens (expected {self.TARGET_TOP_TOKENS})")
        
        # Update pagination state to mark both pages as completed
        self.store.update_ingestion_state(
            coinmarkets_completed_pages=json.dumps([1, 2]),
            coinmarkets_current_page=None,
            coinmarkets_total_pages=None
        )

        # Cleanup pagination state after successful fetch to avoid stale values
        self.store.clear_pagination_state()
        logger.info("Cleared coinmarkets pagination state after fetch")
        
        return all_assets
    
    def _fetch_coin_markets_page(self, page: int, per_page: int) -> List[Dict[str, Any]]:
        """
        Helper to fetch a single page of coin markets.
        
        :param page: Page number to fetch (1 or 2)
        :param per_page: Number of results per page (250 for both pages)
        :return: List of asset dictionaries
        """
        # Build secrets with specific page
        secrets = self._default_secrets()
        secrets["parameters"]["coinmarkets"]["per_page"] = str(per_page)
        secrets["parameters"]["coinmarkets"]["page"] = str(page)
        
        # Build state and make request
        state = {
            "api": None,  # None triggers coinmarkets endpoint
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        api = CoinGeckoAPI(state, secrets)
        endpoint = api.build_api()
        
        logger.info(f"Fetching coins/markets page {page}: {endpoint}")
        
        response = api.make_request(endpoint)
        parsed = api.build_response(response)
        
        if parsed and parsed.get("type") == "coinmarkets":
            return parsed.get("assets", [])
        
        return []
    
    def _run_market_chart_ingestion(self):
        """
        Execute market chart ingestion for all assets needing data.
        
        Processes each asset independently, allowing resumption on failure.
        """
        current_ts = int(time.time())
        
        # Get assets that need data
        assets_to_query = self.store.get_assets_to_query(
            current_ts, 
            self.UPDATE_THRESHOLD_SECONDS
        )
        
        if not assets_to_query:
            logger.info("All assets are up to date")
            return
        
        # Validate that we have assets from the top 350 list
        total_tracked_assets = len(self.store.get_all_asset_ids())
        logger.info(f"Processing {len(assets_to_query)} assets (out of {total_tracked_assets} total tracked)")
        
        self.store.update_ingestion_state(current_endpoint="marketchart")
        
        processed = 0
        errors = 0
        
        for asset_id in assets_to_query:
            try:
                success = self._fetch_asset_market_chart(asset_id, current_ts)
                if success:
                    processed += 1
                else:
                    errors += 1
            except Exception as e:
                logger.error(f"Error processing {asset_id}: {e}")
                errors += 1
                # Continue with next asset - don't fail entire run
                continue
        
        logger.info(f"Market chart ingestion complete: {processed} processed, {errors} errors")
    
    def _fetch_asset_market_chart(self, asset_id: str, current_ts: int) -> bool:
        """
        Fetch and store market chart data for a single asset.
        
        :param asset_id: Asset identifier
        :param current_ts: Current unix timestamp
        :return: True if successful, False otherwise
        """
        # Get asset's current ingestion state
        asset_state = self.store.get_asset_ingestion_state(asset_id)
        
        if not asset_state:
            logger.warning(f"No ingestion state found for {asset_id}")
            return False
        
        # Determine query range
        if asset_state.last_collected_unix_ts is None:
            # New asset - full backfill
            query_from = self._get_backfill_start_ts()
            logger.info(f"Starting backfill for {asset_id}")
        else:
            # Incremental - start from last collected
            query_from = asset_state.last_collected_unix_ts
        
        query_to = current_ts
        
        # Skip if already current
        if query_from >= query_to:
            logger.debug(f"Skipping {asset_id} - already current")
            return True
        
        # Build state for market chart request
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {
                    asset_id: {"last_fetched": query_from}
                } if asset_state.last_collected_unix_ts else {},
                "to_query_asset_id": [asset_id]
            }
        }
        
        # Update secrets with current query parameters
        secrets = self._build_secrets_for_range(query_from, query_to)
        
        api = CoinGeckoAPI(state, secrets)
        endpoint = api.build_api()
        
        logger.info(f"Fetching market chart for {asset_id}: {query_from} -> {query_to}")
        
        response = api.make_request(endpoint)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch {asset_id}: status {response.status_code}")
            return False
        
        parsed = api.build_response(response)
        
        if not parsed or parsed.get("type") != "marketchart":
            logger.error(f"Invalid response for {asset_id}")
            return False
        
        data_points = parsed.get("data_points", [])
        
        if not data_points:
            logger.warning(f"No data points returned for {asset_id}")
            return True  # Not an error, just no data
        
        # Persist data transactionally
        with self.store.transaction():
            self.store.insert_market_data(asset_id, data_points)
            
            # Update progress
            max_ts = max(dp["timestamp_unix"] for dp in data_points)
            min_ts = min(dp["timestamp_unix"] for dp in data_points)
            
            # Determine if backfill is complete
            backfill_start = self._get_backfill_start_ts()
            is_backfill_complete = min_ts <= backfill_start + 3600  # Within 1 hour of target
            
            self.store.update_asset_progress(
                asset_id=asset_id,
                last_collected_unix_ts=max_ts,
                first_collected_unix_ts=min_ts if asset_state.first_collected_unix_ts is None else None,
                is_backfill_complete=is_backfill_complete if not asset_state.is_backfill_complete else None
            )
        
        logger.info(f"Stored {len(data_points)} data points for {asset_id}")
        return True
    
    def _get_backfill_start_ts(self) -> int:
        """Get unix timestamp for backfill start (~1 year ago)."""
        return int(time.time()) - (self.DEFAULT_BACKFILL_DAYS * 24 * 3600)
    
    def _build_secrets_for_range(self, query_from: int, query_to: int) -> Dict:
        """
        Build secrets dict with specific query range.
        
        :param query_from: Start timestamp
        :param query_to: End timestamp
        :return: Secrets dictionary
        """
        secrets = self._default_secrets()
        secrets["parameters"]["marketchart"]["initial_query_from"] = query_from
        secrets["parameters"]["marketchart"]["query_to"] = query_to
        return secrets
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current ingestion status.
        
        :return: Status summary dictionary
        """
        return self.store.get_ingestion_summary()
    
    def close(self):
        """Close database connection."""
        self.store.close()


def run_ingestion(
    db_path: str = "market_data.duckdb",
    secrets: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Convenience function to run a full ingestion cycle.
    
    :param db_path: Path to DuckDB database
    :param secrets: Optional API configuration
    :return: Ingestion summary
    """
    orchestrator = IngestionOrchestrator(db_path=db_path, secrets=secrets)
    try:
        return orchestrator.run()
    finally:
        orchestrator.close()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run ingestion
    print("Starting CoinGecko market data ingestion...")
    result = run_ingestion()
    print(f"\nIngestion complete!")
    print(f"  Total assets: {result['total_assets']}")
    print(f"  Assets with data: {result['assets_with_data']}")
    print(f"  Assets pending: {result['assets_pending']}")
    print(f"  Total data points: {result['total_data_points']}")

