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
        # Setup mock responses
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
                if call_count["markets"] == 1:
                    # First call: original 3 assets
                    return create_mock_response(mock_coin_markets_response)
                else:
                    # Second call: add new asset
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
        
        def mock_get_response(url, **kwargs):
            call_sequence.append(url)
            if "coins/markets" in url:
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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
        def mock_get_response(url, **kwargs):
            if "coins/markets" in url:
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

