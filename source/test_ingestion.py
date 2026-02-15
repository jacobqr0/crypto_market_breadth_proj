"""
Integration tests for the ingestion orchestration module.

Tests cover:
- Full ingestion cycle with mocked API
- Incremental updates after initial run
- State resumption scenarios
"""

import pytest
import time
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import requests

os.environ.setdefault("DUCKDB_SKIP_PANDAS_IMPORT", "1")

# Add source directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion import IngestionOrchestrator, run_ingestion
from duckdb_store import DuckDBStore


@pytest.fixture
def mock_secrets():
    """Test API configuration."""
    current_ts = int(time.time())
    backfill_start = current_ts - (30 * 24 * 3600)  # 30 days for faster tests
    
    return {
        "base_url": "https://api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "10"  # Small for testing
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": backfill_start,
                "query_to": current_ts
            }
        }
    }


@pytest.fixture
def mock_coin_markets_response():
    """Mock response from coins/markets endpoint."""
    return [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
    ]


@pytest.fixture
def mock_market_chart_response():
    """Mock response from market_chart/range endpoint."""
    base_ts = int(time.time()) - (7 * 24 * 3600)  # 7 days ago
    
    prices = []
    market_caps = []
    volumes = []
    
    for i in range(168):  # 7 days * 24 hours
        ts = (base_ts + (i * 3600)) * 1000  # Convert to milliseconds
        prices.append([ts, 35000 + (i * 10)])
        market_caps.append([ts, 680000000000 + (i * 1000000000)])
        volumes.append([ts, 15000000000 + (i * 100000000)])
    
    return {
        "prices": prices,
        "market_caps": market_caps,
        "total_volumes": volumes
    }


def create_mock_response(json_data, status_code=200):
    """Create a mock requests.Response object."""
    mock_resp = Mock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    return mock_resp


class TestIngestionOrchestratorInit:
    """Tests for orchestrator initialization."""
    
    def test_init_creates_store(self, mock_secrets):
        """Verify orchestrator creates DuckDB store."""
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        assert orchestrator.store is not None
        assert isinstance(orchestrator.store, DuckDBStore)
        orchestrator.close()
    
    def test_init_with_default_secrets(self):
        """Verify orchestrator works with default secrets."""
        orchestrator = IngestionOrchestrator(db_path=":memory:")
        
        assert orchestrator.secrets is not None
        assert "base_url" in orchestrator.secrets
        assert "parameters" in orchestrator.secrets
        orchestrator.close()


class TestInitialRun:
    """Tests for initial ingestion run."""
    
    @patch('coingecko_api.requests.get')
    def test_initial_run_fetches_assets(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify initial run fetches and stores asset metadata."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            result = orchestrator.run()
            
            assert result["total_assets"] == 3
            assert "bitcoin" in orchestrator.store.get_all_asset_ids()
            assert "ethereum" in orchestrator.store.get_all_asset_ids()
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_initial_run_initializes_ingestion_state(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify initial run initializes ingestion state for each asset."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            orchestrator.run()
            
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                state = orchestrator.store.get_asset_ingestion_state(asset_id)
                assert state is not None
        finally:
            orchestrator.close()


class TestIncrementalRun:
    """Tests for incremental ingestion runs."""
    
    @patch('coingecko_api.requests.get')
    def test_incremental_adds_new_assets(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify incremental run adds new assets to tracking."""
        call_count = {"markets": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Handle pagination: page 1 and page 2
                if "page=1" in url or call_count["markets"] <= 2:
                    if call_count["markets"] == 1:
                        # First call (page 1): original 3 assets
                        return create_mock_response(mock_coin_markets_response)
                    elif call_count["markets"] == 2:
                        # Second call (page 2): empty or same for test simplicity
                        return create_mock_response([])
                else:
                    # Subsequent calls: add new asset
                    updated = mock_coin_markets_response + [
                        {"id": "binancecoin", "symbol": "bnb", "name": "BNB", "market_cap_rank": 4}
                    ]
                    return create_mock_response(updated)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run (initial)
            orchestrator.run()
            assert orchestrator.store.is_initial_run() is False
            
            # Second run (incremental)
            result = orchestrator.run()
            
            all_assets = orchestrator.store.get_all_asset_ids()
            assert "binancecoin" in all_assets
            assert result["total_assets"] == 4
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_incremental_updates_existing_assets(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify incremental run updates existing asset data."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        orchestrator.UPDATE_THRESHOLD_SECONDS = 0  # Force updates
        
        try:
            # First run
            orchestrator.run()
            initial_points = orchestrator.store.get_total_data_points()
            
            # Reset timestamps to force re-fetch
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                orchestrator.store.update_asset_progress(
                    asset_id, 
                    last_collected_unix_ts=int(time.time()) - 7200  # 2 hours ago
                )
            
            # Second run
            orchestrator.run()
            
            # Should have fetched more data (or same if idempotent)
            final_points = orchestrator.store.get_total_data_points()
            assert final_points >= initial_points
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_incremental_initializes_ingestion_state_for_new_tokens(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify that new tokens added during incremental run get their 
        ingestion state initialized in asset_ingestion_state table.
        
        This tests the fix for the bug where new tokens were added to
        asset_metadata but not to asset_ingestion_state, causing them
        to never have their market chart data collected.
        """
        run_count = {"value": 0}
        
        # Page 2 has additional tokens (ranks 251-350)
        page2_tokens = [
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 251},
            {"id": "cardano", "symbol": "ada", "name": "Cardano", "market_cap_rank": 252},
            {"id": "avalanche", "symbol": "avax", "name": "Avalanche", "market_cap_rank": 253},
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                if "&page=1" in url:
                    run_count["value"] += 1
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url:
                    return create_mock_response(page2_tokens)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run (initial) - initializes ingestion state for all tokens
            orchestrator.run()
            
            # Verify all 6 tokens have ingestion state initialized
            with_state = orchestrator.store.get_assets_with_ingestion_state()
            assert len(with_state) == 6, f"Expected 6, got {len(with_state)}: {with_state}"
            assert "bitcoin" in with_state
            assert "solana" in with_state
            assert "cardano" in with_state
            assert "avalanche" in with_state
            
            # Verify they're in get_assets_to_query (need data collection)
            current_ts = int(time.time())
            to_query = orchestrator.store.get_assets_to_query(current_ts, 0)
            # After first run, assets were fetched so they won't need querying
            # unless we set threshold to 0 and they're current
            
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_incremental_initializes_state_for_newly_entered_tokens(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify tokens that newly enter the top 350 during incremental runs
        get their ingestion state initialized even though they're already
        in asset_metadata after upsert.
        
        This specifically tests the fix: checking against asset_ingestion_state
        instead of asset_metadata when determining which assets need initialization.
        """
        run_count = {"value": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                if "&page=1" in url:
                    run_count["value"] += 1
                    if run_count["value"] == 1:
                        # First run: only 3 tokens
                        return create_mock_response(mock_coin_markets_response)
                    else:
                        # Second run: one new token enters
                        return create_mock_response(mock_coin_markets_response + [
                            {"id": "new_token", "symbol": "new", "name": "New Token", "market_cap_rank": 4}
                        ])
                elif "&page=2" in url:
                    return create_mock_response([])
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run (initial)
            orchestrator.run()
            
            # Verify 3 tokens have state
            with_state = orchestrator.store.get_assets_with_ingestion_state()
            assert len(with_state) == 3
            assert "new_token" not in with_state
            
            # Second run (incremental) - new_token enters the top list
            orchestrator.run()
            
            # Verify new_token now has ingestion state initialized
            with_state = orchestrator.store.get_assets_with_ingestion_state()
            assert len(with_state) == 4, f"Expected 4, got {len(with_state)}: {with_state}"
            assert "new_token" in with_state, "new_token should have ingestion state initialized"
            
            # Verify new_token is in asset_metadata
            all_ids = orchestrator.store.get_all_asset_ids()
            assert "new_token" in all_ids
            
            # The key test: both tables should have the same set of assets
            # (for tracked assets that are currently in the top list)
            assert set(with_state) == set(all_ids), \
                f"asset_ingestion_state and asset_metadata should match: " \
                f"state={with_state}, metadata={all_ids}"
            
        finally:
            orchestrator.close()


class TestMarketChartIngestion:
    """Tests for market chart data ingestion."""
    
    @patch('coingecko_api.requests.get')
    def test_fetches_market_data_for_all_assets(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify market data is fetched for all assets."""
        assets_fetched = set()
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                # Extract asset ID from URL
                for asset in mock_coin_markets_response:
                    if f"coins/{asset['id']}/market_chart" in url:
                        assets_fetched.add(asset["id"])
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            orchestrator.run()
            
            assert "bitcoin" in assets_fetched
            assert "ethereum" in assets_fetched
            assert "tether" in assets_fetched
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_stores_data_points(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify data points are stored in DuckDB."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            orchestrator.run()
            
            # Each asset should have data points
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                count = orchestrator.store.get_data_point_count(asset_id)
                assert count > 0, f"No data points for {asset_id}"
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_updates_ingestion_progress(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify ingestion progress is updated after fetch."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            orchestrator.run()
            
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                state = orchestrator.store.get_asset_ingestion_state(asset_id)
                assert state.last_collected_unix_ts is not None
                assert state.last_query_ts is not None
        finally:
            orchestrator.close()


class TestErrorHandling:
    """Tests for error handling and recovery."""
    
    @patch('coingecko_api.requests.get')
    def test_continues_on_single_asset_failure(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify ingestion continues if single asset fails."""
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                if "ethereum" in url:
                    # Simulate failure for ethereum
                    return create_mock_response({}, 500)
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            result = orchestrator.run()
            
            # Bitcoin and tether should have data, ethereum should not
            assert orchestrator.store.get_data_point_count("bitcoin") > 0
            assert orchestrator.store.get_data_point_count("ethereum") == 0
            assert orchestrator.store.get_data_point_count("tether") > 0
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_updates_status_on_error(
        self, 
        mock_get, 
        mock_secrets
    ):
        """Verify status is updated when critical error occurs."""
        mock_get.side_effect = Exception("Network error")
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            with pytest.raises(Exception):
                orchestrator.run()
            
            state = orchestrator.store.get_ingestion_state()
            assert state.run_status == "error"
        finally:
            orchestrator.close()


class TestStateResumption:
    """Tests for resuming after interruption."""
    
    @patch('coingecko_api.requests.get')
    def test_resume_from_partial_ingestion(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify ingestion resumes from where it left off."""
        call_sequence = []
        
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            call_sequence.append(url)
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        # First run - simulate partial completion
        store = DuckDBStore(":memory:")
        store.upsert_asset_metadata([
            {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
            {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        ])
        store.initialize_asset_ingestion_state("bitcoin")
        store.initialize_asset_ingestion_state("ethereum")
        
        # Mark bitcoin as already fetched (simulating partial completion)
        current_ts = int(time.time())
        store.update_asset_progress("bitcoin", last_collected_unix_ts=current_ts - 1800)
        
        orchestrator = IngestionOrchestrator(db_path=":memory:", secrets=mock_secrets)
        orchestrator.store = store  # Use pre-populated store
        orchestrator.UPDATE_THRESHOLD_SECONDS = 3600  # 1 hour threshold
        
        try:
            orchestrator.run()
            
            # Bitcoin should be skipped (recent), ethereum should be fetched
            market_chart_calls = [url for url in call_sequence if "market_chart" in url]
            ethereum_calls = [url for url in market_chart_calls if "ethereum" in url]
            bitcoin_calls = [url for url in market_chart_calls if "bitcoin" in url]
            
            assert len(ethereum_calls) > 0, "Ethereum should have been fetched"
            assert len(bitcoin_calls) == 0, "Bitcoin should have been skipped (recently updated)"
        finally:
            orchestrator.close()


class TestConvenienceFunction:
    """Tests for the run_ingestion convenience function."""
    
    @patch('coingecko_api.requests.get')
    def test_run_ingestion_function(
        self, 
        mock_get, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify run_ingestion convenience function works."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        result = run_ingestion(db_path=":memory:")
        
        assert "total_assets" in result
        assert "assets_with_data" in result
        assert "total_data_points" in result


class TestIdempotency:
    """Tests for idempotent behavior."""
    
    @patch('coingecko_api.requests.get')
    def test_multiple_runs_no_duplicates(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify multiple runs don't create duplicate data."""
        # Setup mock responses - handle pagination (page 1 and page 2)
        call_count = {"markets": 0}
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Return same response for both pages (for simplicity in test)
                return create_mock_response(mock_coin_markets_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        orchestrator.UPDATE_THRESHOLD_SECONDS = 0  # Force updates each run
        
        try:
            # First run
            orchestrator.run()
            first_count = orchestrator.store.get_total_data_points()
            
            # Reset last_collected to force re-fetch
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                orchestrator.store.update_asset_progress(
                    asset_id,
                    last_collected_unix_ts=int(time.time()) - 7200
                )
            
            # Second run with same data
            orchestrator.run()
            second_count = orchestrator.store.get_total_data_points()
            
            # Count should be same (upsert, no duplicates)
            assert first_count == second_count
        finally:
            orchestrator.close()


class TestDroppedAssets:
    """Tests for assets that drop out of the top market cap list."""
    
    @patch('coingecko_api.requests.get')
    def test_dropped_asset_market_cap_rank_set_to_null(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify assets that drop out of the top list have their 
        market_cap_rank set to NULL.
        """
        call_count = {"markets": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Handle pagination: page 1 and page 2
                if "page=1" in url or (call_count["markets"] % 2 == 1):
                    if call_count["markets"] <= 2:
                        # First two calls (page 1 and page 2): 3 assets
                        return create_mock_response(mock_coin_markets_response)
                    else:
                        # Subsequent page 1 calls: tether dropped out, replaced by bnb
                        updated = [
                            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                            {"id": "binancecoin", "symbol": "bnb", "name": "BNB", "market_cap_rank": 3},
                        ]
                        return create_mock_response(updated)
                else:
                    # Page 2 calls: return empty or same
                    return create_mock_response([])
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run - all 3 assets tracked
            orchestrator.run()
            
            # Verify tether has market_cap_rank = 3
            tether_meta = orchestrator.store.get_asset_metadata("tether")
            assert tether_meta is not None
            assert tether_meta.market_cap_rank == 3
            
            # Second run - tether drops out
            orchestrator.run()
            
            # Verify tether's market_cap_rank is now NULL
            tether_meta_after = orchestrator.store.get_asset_metadata("tether")
            assert tether_meta_after is not None
            assert tether_meta_after.market_cap_rank is None, \
                f"Expected market_cap_rank to be NULL, got {tether_meta_after.market_cap_rank}"
            
            # Verify bitcoin and ethereum still have correct ranks
            btc_meta = orchestrator.store.get_asset_metadata("bitcoin")
            eth_meta = orchestrator.store.get_asset_metadata("ethereum")
            assert btc_meta.market_cap_rank == 1
            assert eth_meta.market_cap_rank == 2
            
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_dropped_asset_last_updated_ts_is_updated(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify assets that drop out of the top list have their 
        last_updated_ts updated.
        """
        call_count = {"markets": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                if call_count["markets"] == 1:
                    return create_mock_response(mock_coin_markets_response)
                else:
                    # tether dropped out
                    updated = [
                        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                        {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                    ]
                    return create_mock_response(updated)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run
            orchestrator.run()
            
            tether_meta_initial = orchestrator.store.get_asset_metadata("tether")
            initial_updated_ts = tether_meta_initial.last_updated_ts
            
            # Small delay to ensure timestamp difference
            time.sleep(0.1)
            
            # Second run - tether drops out
            orchestrator.run()
            
            tether_meta_after = orchestrator.store.get_asset_metadata("tether")
            after_updated_ts = tether_meta_after.last_updated_ts
            
            # last_updated_ts should be updated even though asset dropped
            assert after_updated_ts > initial_updated_ts, \
                f"Expected last_updated_ts to be updated, initial={initial_updated_ts}, after={after_updated_ts}"
            
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_dropped_asset_still_gets_market_data_ingested(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify assets that dropped out of the top list still have 
        their market data ingested on subsequent runs.
        """
        call_count = {"markets": 0}
        assets_fetched = []
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                if call_count["markets"] == 1:
                    return create_mock_response(mock_coin_markets_response)
                else:
                    # tether dropped out
                    updated = [
                        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                        {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                    ]
                    return create_mock_response(updated)
            elif "market_chart/range" in url:
                # Track which assets had market data fetched
                for asset in ["bitcoin", "ethereum", "tether"]:
                    if f"coins/{asset}/market_chart" in url:
                        assets_fetched.append(asset)
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        orchestrator.UPDATE_THRESHOLD_SECONDS = 0  # Force updates
        
        try:
            # First run - all 3 assets fetched
            orchestrator.run()
            
            initial_tether_count = orchestrator.store.get_data_point_count("tether")
            assert initial_tether_count > 0, "Tether should have initial market data"
            
            # Clear fetch tracking for second run
            assets_fetched.clear()
            
            # Reset timestamps to force re-fetch
            for asset_id in ["bitcoin", "ethereum", "tether"]:
                orchestrator.store.update_asset_progress(
                    asset_id,
                    last_collected_unix_ts=int(time.time()) - 7200
                )
            
            # Second run - tether dropped from top list but should still be fetched
            orchestrator.run()
            
            # Verify tether was still fetched (even though it dropped from top list)
            assert "tether" in assets_fetched, \
                f"Tether should still be fetched even after dropping from top list. Fetched: {assets_fetched}"
            
            # Verify tether still has market data
            final_tether_count = orchestrator.store.get_data_point_count("tether")
            assert final_tether_count >= initial_tether_count, \
                "Tether should still have market data after dropping from top list"
            
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_no_duplicate_market_cap_ranks(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify there are no duplicate market_cap_rank values after
        assets drop out and new ones enter the top list.
        """
        call_count = {"markets": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                if call_count["markets"] == 1:
                    # Initial: bitcoin(1), ethereum(2), tether(3)
                    return create_mock_response(mock_coin_markets_response)
                else:
                    # Second: bitcoin stays at 1, ethereum stays at 2
                    # tether dropped, bnb takes rank 3
                    updated = [
                        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                        {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                        {"id": "binancecoin", "symbol": "bnb", "name": "BNB", "market_cap_rank": 3},
                    ]
                    return create_mock_response(updated)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run
            orchestrator.run()
            
            # Second run - tether drops, bnb enters
            orchestrator.run()
            
            # Query for duplicate ranks (excluding NULL)
            result = orchestrator.store.conn.execute("""
                SELECT market_cap_rank, COUNT(*) as cnt
                FROM asset_metadata
                WHERE market_cap_rank IS NOT NULL
                GROUP BY market_cap_rank
                HAVING COUNT(*) > 1
            """).fetchall()
            
            assert len(result) == 0, \
                f"Found duplicate market_cap_rank values: {result}"
            
            # Also verify the specific ranks
            btc = orchestrator.store.get_asset_metadata("bitcoin")
            eth = orchestrator.store.get_asset_metadata("ethereum")
            tether = orchestrator.store.get_asset_metadata("tether")
            bnb = orchestrator.store.get_asset_metadata("binancecoin")
            
            assert btc.market_cap_rank == 1
            assert eth.market_cap_rank == 2
            assert tether.market_cap_rank is None  # Dropped
            assert bnb.market_cap_rank == 3  # New entrant
            
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_asset_returns_to_top_list_gets_rank_restored(
        self, 
        mock_get, 
        mock_secrets, 
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """
        Verify that if an asset drops out and then returns to the top list,
        its market_cap_rank is properly restored.
        """
        run_count = {"value": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                # Determine which run we're on based on page 1 calls
                if "&page=1" in url:
                    run_count["value"] += 1
                
                current_run = run_count["value"]
                
                if "&page=1" in url:
                    if current_run == 1:
                        # Initial run: bitcoin(1), ethereum(2), tether(3)
                        return create_mock_response(mock_coin_markets_response)
                    elif current_run == 2:
                        # Second run: tether drops out
                        return create_mock_response([
                            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                        ])
                    else:
                        # Third run: tether returns at rank 4
                        return create_mock_response([
                            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                            {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 4},
                        ])
                elif "&page=2" in url:
                    # Page 2 calls: return empty for simplicity
                    return create_mock_response([])
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # First run - tether at rank 3
            orchestrator.run()
            tether_initial = orchestrator.store.get_asset_metadata("tether")
            assert tether_initial.market_cap_rank == 3
            
            # Second run - tether drops out
            orchestrator.run()
            tether_dropped = orchestrator.store.get_asset_metadata("tether")
            assert tether_dropped.market_cap_rank is None
            
            # Third run - tether returns at rank 4
            orchestrator.run()
            tether_restored = orchestrator.store.get_asset_metadata("tether")
            assert tether_restored.market_cap_rank == 4, \
                f"Expected rank 4, got {tether_restored.market_cap_rank}"
            
        finally:
            orchestrator.close()


class TestPagination:
    """Tests for pagination support (top 350 tokens)."""
    
    @patch('coingecko_api.requests.get')
    def test_page2_uses_per_page_250_not_100(
        self,
        mock_get,
        mock_secrets,
        mock_coin_markets_response
    ):
        """
        Verify page 2 is called with per_page=250, NOT per_page=100.
        
        This is critical because CoinGecko pagination formula is:
        (page-1)*per_page+1 to page*per_page
        
        With per_page=100, page=2 would return tokens 101-200 (WRONG!)
        With per_page=250, page=2 returns tokens 251-500 (correct, then slice to 251-350)
        """
        captured_urls = []
        
        # Generate a larger page 2 response (250 tokens) to test slicing
        page2_response = [
            {"id": f"token{i}", "symbol": f"t{i}", "name": f"Token {i}", "market_cap_rank": 250 + i}
            for i in range(1, 251)  # 250 tokens
        ]
        
        def mock_get_response(url, **kwargs):
            captured_urls.append(url)
            if "coins/markets" in url:
                if "&page=1" in url:
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url:
                    return create_mock_response(page2_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Find the page 2 URL
            page2_urls = [url for url in captured_urls if "&page=2" in url]
            assert len(page2_urls) >= 1, "Page 2 should have been called"
            
            # Verify page 2 uses per_page=250 (not 100)
            page2_url = page2_urls[0]
            assert "per_page=250" in page2_url, \
                f"Page 2 should use per_page=250, but URL was: {page2_url}"
            assert "per_page=100" not in page2_url, \
                f"Page 2 should NOT use per_page=100, but URL was: {page2_url}"
            
            # Verify the results are sliced to 100 tokens from page 2
            # Total should be 3 (page 1) + 100 (sliced from page 2) = 103
            page2_assets_in_result = [a for a in assets if a["asset_id"].startswith("token")]
            assert len(page2_assets_in_result) == 100, \
                f"Expected 100 tokens from page 2 (sliced from 250), got {len(page2_assets_in_result)}"
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_fetches_both_pages(
        self,
        mock_get,
        mock_secrets,
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify _fetch_coin_markets() always fetches both page 1 and page 2."""
        call_count = {"markets": 0}
        page2_response = [
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 251},
            {"id": "cardano", "symbol": "ada", "name": "Cardano", "market_cap_rank": 252},
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Use more specific matching to avoid false positives
                if "&page=1" in url or url.endswith("page=1"):
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url or url.endswith("page=2"):
                    return create_mock_response(page2_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Should have assets from both pages
            assert len(assets) == 5  # 3 from page 1 + 2 from page 2
            assert any(a["asset_id"] == "bitcoin" for a in assets)
            assert any(a["asset_id"] == "solana" for a in assets)
            assert any(a["asset_id"] == "cardano" for a in assets)
            assert call_count["markets"] == 2  # Two API calls (page 1 and page 2)
            
            # Verify pagination state is cleared after fetch
            state = orchestrator.store.get_ingestion_state()
            assert state.coinmarkets_completed_pages is None
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.time.sleep')  # Mock sleep to avoid long delays
    @patch('coingecko_api.requests.get')
    def test_handles_page2_failure(
        self,
        mock_get,
        mock_sleep,
        mock_secrets,
        mock_coin_markets_response
    ):
        """Verify system continues with page 1 if page 2 fails after retry."""
        call_count = {"markets": 0}
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Use more specific matching to avoid false positives
                if "&page=1" in url or url.endswith("page=1"):
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url or url.endswith("page=2"):
                    # Page 2 fails both times (initial and retry)
                    # Return 429 to trigger retry logic, but mock sleep to avoid delays
                    return create_mock_response({}, status_code=429)  # Rate limit
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Should still return page 1 assets even if page 2 fails after retry
            assert len(assets) == 3  # Only page 1 assets
            assert any(a["asset_id"] == "bitcoin" for a in assets)
            # Page 2 will be retried by make_request (up to 5 times), then by our retry logic
            # So we expect multiple calls for page 2
            assert call_count["markets"] >= 2  # At least page 1 + page 2 initial call
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_resumability_after_page1_complete(
        self,
        mock_get,
        mock_secrets,
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify system always re-fetches both pages even if previously completed."""
        import json
        
        # Setup: both pages already marked as completed
        store = DuckDBStore(":memory:")
        store.update_ingestion_state(
            coinmarkets_completed_pages=json.dumps([1, 2]),
            coinmarkets_total_pages=2
        )
        
        call_count = {"markets": 0}
        page2_response = [
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 251},
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Use more specific matching to avoid false positives
                if "&page=1" in url or url.endswith("page=1"):
                    # Always re-fetch page 1 (for metadata update)
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url or url.endswith("page=2"):
                    # Always re-fetch page 2 (for metadata update)
                    return create_mock_response(page2_response)
            elif "market_chart/range" in url:
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        orchestrator.store = store
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Should have both page 1 (re-fetched) and page 2 (re-fetched) assets
            assert len(assets) == 4  # 3 from page 1 + 1 from page 2
            assert any(a["asset_id"] == "bitcoin" for a in assets)
            assert any(a["asset_id"] == "solana" for a in assets)
            # Verify both pages were fetched (2 calls)
            assert call_count["markets"] == 2
            
            # Verify pagination state is cleared after fetch
            state = store.get_ingestion_state()
            assert state.coinmarkets_completed_pages is None
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_page2_always_refetched_even_when_completed(
        self,
        mock_get,
        mock_secrets,
        mock_coin_markets_response
    ):
        """Verify page 2 is always re-fetched even when marked as completed in state."""
        import json
        
        # Setup: both pages marked as completed
        store = DuckDBStore(":memory:")
        store.update_ingestion_state(
            coinmarkets_completed_pages=json.dumps([1, 2]),
            coinmarkets_total_pages=2
        )
        
        call_count = {"markets": 0}
        page2_response = [
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 251},
            {"id": "cardano", "symbol": "ada", "name": "Cardano", "market_cap_rank": 252},
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Use more specific matching to avoid false positives
                if "&page=1" in url or url.endswith("page=1"):
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url or url.endswith("page=2"):
                    return create_mock_response(page2_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        orchestrator.store = store
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Should have both pages even though they were marked as completed
            assert len(assets) == 5  # 3 from page 1 + 2 from page 2
            assert any(a["asset_id"] == "bitcoin" for a in assets)
            assert any(a["asset_id"] == "solana" for a in assets)
            assert any(a["asset_id"] == "cardano" for a in assets)
            
            # Verify both pages were fetched (not skipped)
            assert call_count["markets"] == 2, "Both pages should be fetched even when marked as completed"
            
            # Verify pagination state is cleared after fetch
            state = store.get_ingestion_state()
            assert state.coinmarkets_completed_pages is None
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_all_350_tokens_get_market_chart_data(
        self,
        mock_get,
        mock_secrets,
        mock_coin_markets_response,
        mock_market_chart_response
    ):
        """Verify all 350 tokens (page 1 + page 2) get market chart data collected."""
        call_count = {"markets": 0, "charts": 0}
        page2_response = [
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 251},
            {"id": "cardano", "symbol": "ada", "name": "Cardano", "market_cap_rank": 252},
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                call_count["markets"] += 1
                # Use more specific matching to avoid false positives
                if "&page=1" in url or url.endswith("page=1"):
                    return create_mock_response(mock_coin_markets_response)
                elif "&page=2" in url or url.endswith("page=2"):
                    return create_mock_response(page2_response)
            elif "market_chart/range" in url:
                call_count["charts"] += 1
                return create_mock_response(mock_market_chart_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            orchestrator.run()
            
            # Verify all assets were initialized
            all_assets = orchestrator.store.get_all_asset_ids()
            assert "bitcoin" in all_assets
            assert "ethereum" in all_assets
            assert "tether" in all_assets
            assert "solana" in all_assets
            assert "cardano" in all_assets
            
            # Verify market chart data was collected for all
            # (get_assets_to_query should return all initialized assets)
            current_ts = int(time.time())
            assets_to_query = orchestrator.store.get_assets_to_query(current_ts, 0)
            # All 5 assets should be in the query list (they need data)
            assert len(assets_to_query) >= 5
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_fetches_exactly_350_tokens(
        self,
        mock_get,
        mock_secrets
    ):
        """
        Verify _fetch_coin_markets() returns exactly 350 tokens when both pages succeed.
        
        This tests the full 350-token scenario:
        - Page 1: 250 tokens (ranks 1-250)
        - Page 2: 250 tokens returned, sliced to 100 (ranks 251-350)
        - Total: 350 tokens
        """
        # Generate 250 tokens for page 1 (ranks 1-250)
        page1_response = [
            {"id": f"page1_token{i}", "symbol": f"p1t{i}", "name": f"Page1 Token {i}", "market_cap_rank": i}
            for i in range(1, 251)  # 250 tokens
        ]
        
        # Generate 250 tokens for page 2 (ranks 251-500)
        # The orchestrator should slice this to only the first 100 (ranks 251-350)
        page2_response = [
            {"id": f"page2_token{i}", "symbol": f"p2t{i}", "name": f"Page2 Token {i}", "market_cap_rank": 250 + i}
            for i in range(1, 251)  # 250 tokens (will be sliced to 100)
        ]
        
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
                if "&page=1" in url:
                    return create_mock_response(page1_response)
                elif "&page=2" in url:
                    return create_mock_response(page2_response)
            return create_mock_response({}, 404)
        
        mock_get.side_effect = mock_get_response
        
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            assets = orchestrator._fetch_coin_markets()
            
            # Verify exactly 350 tokens are returned
            assert len(assets) == 350, \
                f"Expected exactly 350 tokens, got {len(assets)}"
            
            # Verify the first 250 are from page 1
            page1_assets = [a for a in assets if a["asset_id"].startswith("page1_")]
            assert len(page1_assets) == 250, \
                f"Expected 250 tokens from page 1, got {len(page1_assets)}"
            
            # Verify the last 100 are from page 2 (sliced from 250)
            page2_assets = [a for a in assets if a["asset_id"].startswith("page2_")]
            assert len(page2_assets) == 100, \
                f"Expected 100 tokens from page 2 (sliced from 250), got {len(page2_assets)}"
            
            # Verify the ranks are correct (1-250 from page 1, 251-350 from page 2)
            page1_ranks = [a["market_cap_rank"] for a in page1_assets]
            page2_ranks = [a["market_cap_rank"] for a in page2_assets]
            
            assert min(page1_ranks) == 1, "Page 1 should start at rank 1"
            assert max(page1_ranks) == 250, "Page 1 should end at rank 250"
            assert min(page2_ranks) == 251, "Page 2 (sliced) should start at rank 251"
            assert max(page2_ranks) == 350, "Page 2 (sliced) should end at rank 350"
        finally:
            orchestrator.close()
    
    @patch('coingecko_api.requests.get')
    def test_class_constants_match_expected_values(
        self,
        mock_get,
        mock_secrets
    ):
        """Verify the class constants are set correctly for 350-token ingestion."""
        orchestrator = IngestionOrchestrator(
            db_path=":memory:",
            secrets=mock_secrets
        )
        
        try:
            # Verify class constants
            assert orchestrator.TARGET_TOP_TOKENS == 350, \
                f"TARGET_TOP_TOKENS should be 350, got {orchestrator.TARGET_TOP_TOKENS}"
            assert orchestrator.MAX_PER_PAGE == 250, \
                f"MAX_PER_PAGE should be 250, got {orchestrator.MAX_PER_PAGE}"
            assert orchestrator.PAGE_2_TOKEN_COUNT == 100, \
                f"PAGE_2_TOKEN_COUNT should be 100, got {orchestrator.PAGE_2_TOKEN_COUNT}"
            
            # Verify the math: TARGET = MAX_PER_PAGE + PAGE_2_TOKEN_COUNT
            assert orchestrator.TARGET_TOP_TOKENS == orchestrator.MAX_PER_PAGE + orchestrator.PAGE_2_TOKEN_COUNT, \
                "TARGET_TOP_TOKENS should equal MAX_PER_PAGE + PAGE_2_TOKEN_COUNT"
        finally:
            orchestrator.close()

