"""
Ingestion orchestration module for CoinGecko market data.

This module implements the control flow for:
- Initial runs (fetch top 300, backfill historical data)
- Incremental runs (update existing assets, add new top assets)
- Resumable execution after failures or rate limits
"""

import time
import logging
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
    1. Fetches top 300 assets by market cap
    2. Backfills ~1 year of historical data
    3. Continues incrementally forward
    """
    
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
                    "per_page": "250"
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
        Execute initial run: fetch top 300 assets and initialize state.
        """
        # Fetch top 300 coins by market cap
        assets = self._fetch_coin_markets()
        
        if not assets:
            logger.warning("No assets returned from coins/markets endpoint")
            return
        
        # Persist asset metadata
        self.store.upsert_asset_metadata(assets)
        
        # Initialize ingestion state for each asset
        for asset in assets:
            self.store.initialize_asset_ingestion_state(asset["asset_id"])
        
        logger.info(f"Initialized {len(assets)} assets for ingestion")
    
    def _incremental_run(self):
        """
        Execute incremental run: refresh top 300 and merge with existing.
        """
        # Refresh top 300 list
        current_top_assets = self._fetch_coin_markets()
        
        if current_top_assets:
            self.store.upsert_asset_metadata(current_top_assets)
            
            # Initialize state for any new assets
            existing_assets = set(self.store.get_all_asset_ids())
            for asset in current_top_assets:
                if asset["asset_id"] not in existing_assets:
                    self.store.initialize_asset_ingestion_state(asset["asset_id"])
                    logger.info(f"Added new asset to tracking: {asset['asset_id']}")
    
    def _fetch_coin_markets(self) -> List[Dict[str, Any]]:
        """
        Fetch top assets from coins/markets endpoint.
        
        :return: List of asset dictionaries
        """
        # Build state for coins/markets request
        state = {
            "api": None,  # None triggers coinmarkets endpoint
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        api = CoinGeckoAPI(state, self.secrets)
        endpoint = api.build_api()
        
        self.store.update_ingestion_state(current_endpoint="coinmarkets")
        logger.info(f"Fetching coins/markets: {endpoint}")
        
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
        
        logger.info(f"Processing {len(assets_to_query)} assets")
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

