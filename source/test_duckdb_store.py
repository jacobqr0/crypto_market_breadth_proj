"""
Tests for DuckDB persistence layer.

Tests cover:
- Schema initialization
- CRUD operations for all tables
- Idempotency guarantees
- State resumption after interruption
"""

import pytest
import time
import sys
import os

# Add source directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from duckdb_store import DuckDBStore, AssetIngestionState, IngestionState


@pytest.fixture
def store():
    """Create an in-memory DuckDB store for testing."""
    store = DuckDBStore(db_path=":memory:")
    yield store
    store.close()


@pytest.fixture
def sample_assets():
    """Sample asset metadata for testing."""
    return [
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
    ]


@pytest.fixture
def sample_data_points():
    """Sample market data points for testing."""
    base_ts = 1700000000
    return [
        {"timestamp_unix": base_ts, "price_usd": 35000.0, "market_cap_usd": 680000000000.0, "volume_usd": 15000000000.0},
        {"timestamp_unix": base_ts + 3600, "price_usd": 35100.0, "market_cap_usd": 682000000000.0, "volume_usd": 16000000000.0},
        {"timestamp_unix": base_ts + 7200, "price_usd": 35200.0, "market_cap_usd": 684000000000.0, "volume_usd": 14000000000.0},
    ]


class TestSchemaInitialization:
    """Tests for schema creation and initialization."""
    
    def test_initialize_schema_creates_tables(self, store):
        """Verify all required tables are created."""
        # Query information about tables
        result = store.conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        
        table_names = [row[0] for row in result]
        
        assert "ingestion_state" in table_names
        assert "asset_metadata" in table_names
        assert "asset_ingestion_state" in table_names
        assert "market_data" in table_names
    
    def test_initialize_schema_creates_indexes(self, store):
        """Verify indexes are created."""
        result = store.conn.execute("""
            SELECT index_name FROM duckdb_indexes()
        """).fetchall()
        
        index_names = [row[0] for row in result]
        
        assert "idx_market_data_asset" in index_names
        assert "idx_market_data_ts" in index_names
        assert "idx_asset_meta_rank" in index_names
    
    def test_ingestion_state_singleton_initialized(self, store):
        """Verify ingestion_state has single row initialized."""
        result = store.conn.execute("""
            SELECT COUNT(*) FROM ingestion_state
        """).fetchone()
        
        assert result[0] == 1
    
    def test_initial_run_detection(self, store):
        """Verify is_initial_run returns True when no assets."""
        assert store.is_initial_run() is True
        
        # Add an asset
        store.upsert_asset_metadata([{"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1}])
        
        assert store.is_initial_run() is False


class TestIngestionStateOperations:
    """Tests for global ingestion state management."""
    
    def test_get_ingestion_state_default(self, store):
        """Verify default ingestion state."""
        state = store.get_ingestion_state()
        
        assert isinstance(state, IngestionState)
        assert state.current_endpoint is None
        assert state.run_status == "idle"
    
    def test_update_ingestion_state(self, store):
        """Verify ingestion state updates."""
        store.update_ingestion_state(
            current_endpoint="coinmarkets",
            run_status="running"
        )
        
        state = store.get_ingestion_state()
        
        assert state.current_endpoint == "coinmarkets"
        assert state.run_status == "running"
        assert state.last_updated_ts is not None
    
    def test_update_ingestion_state_partial(self, store):
        """Verify partial updates preserve other fields."""
        store.update_ingestion_state(current_endpoint="coinmarkets", run_status="running")
        store.update_ingestion_state(run_status="rate_limited")
        
        state = store.get_ingestion_state()
        
        assert state.current_endpoint == "coinmarkets"  # Unchanged
        assert state.run_status == "rate_limited"  # Updated


class TestAssetMetadataOperations:
    """Tests for asset metadata CRUD operations."""
    
    def test_upsert_asset_metadata_insert(self, store, sample_assets):
        """Verify new assets are inserted."""
        store.upsert_asset_metadata(sample_assets)
        
        result = store.conn.execute("""
            SELECT asset_id, symbol, name, market_cap_rank
            FROM asset_metadata
            ORDER BY market_cap_rank
        """).fetchall()
        
        assert len(result) == 3
        assert result[0][0] == "bitcoin"
        assert result[1][0] == "ethereum"
        assert result[2][0] == "tether"
    
    def test_upsert_asset_metadata_update(self, store, sample_assets):
        """Verify existing assets are updated."""
        store.upsert_asset_metadata(sample_assets)
        
        # Update bitcoin's rank
        updated = [{"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 5}]
        store.upsert_asset_metadata(updated)
        
        result = store.conn.execute("""
            SELECT market_cap_rank FROM asset_metadata
            WHERE asset_id = 'bitcoin'
        """).fetchone()
        
        assert result[0] == 5
    
    def test_get_top_assets(self, store, sample_assets):
        """Verify top assets retrieval by rank."""
        store.upsert_asset_metadata(sample_assets)
        
        top_2 = store.get_top_assets(limit=2)
        
        assert len(top_2) == 2
        assert top_2[0] == "bitcoin"
        assert top_2[1] == "ethereum"
    
    def test_get_all_asset_ids(self, store, sample_assets):
        """Verify all asset IDs retrieval."""
        store.upsert_asset_metadata(sample_assets)
        
        all_ids = store.get_all_asset_ids()
        
        assert set(all_ids) == {"bitcoin", "ethereum", "tether"}


class TestAssetIngestionStateOperations:
    """Tests for per-asset ingestion state management."""
    
    def test_initialize_asset_ingestion_state(self, store, sample_assets):
        """Verify asset ingestion state initialization."""
        store.upsert_asset_metadata(sample_assets)
        store.initialize_asset_ingestion_state("bitcoin")
        
        state = store.get_asset_ingestion_state("bitcoin")
        
        assert state is not None
        assert state.asset_id == "bitcoin"
        assert state.last_collected_unix_ts is None
        assert state.is_backfill_complete is False
    
    def test_initialize_asset_ingestion_state_idempotent(self, store, sample_assets):
        """Verify initialization is idempotent (no duplicate)."""
        store.upsert_asset_metadata(sample_assets)
        store.initialize_asset_ingestion_state("bitcoin")
        store.initialize_asset_ingestion_state("bitcoin")  # Second call
        
        result = store.conn.execute("""
            SELECT COUNT(*) FROM asset_ingestion_state
            WHERE asset_id = 'bitcoin'
        """).fetchone()
        
        assert result[0] == 1
    
    def test_update_asset_progress(self, store, sample_assets):
        """Verify asset progress updates."""
        store.upsert_asset_metadata(sample_assets)
        store.initialize_asset_ingestion_state("bitcoin")
        
        store.update_asset_progress(
            asset_id="bitcoin",
            last_collected_unix_ts=1700000000,
            first_collected_unix_ts=1690000000,
            is_backfill_complete=True
        )
        
        state = store.get_asset_ingestion_state("bitcoin")
        
        assert state.last_collected_unix_ts == 1700000000
        assert state.first_collected_unix_ts == 1690000000
        assert state.is_backfill_complete is True
        assert state.last_query_ts is not None
    
    def test_get_assets_needing_data(self, store, sample_assets):
        """Verify assets needing data are correctly identified."""
        store.upsert_asset_metadata(sample_assets)
        
        for asset in sample_assets:
            store.initialize_asset_ingestion_state(asset["asset_id"])
        
        current_ts = int(time.time())
        
        # All should need data (never fetched)
        needing_data = store.get_assets_needing_data(current_ts)
        assert len(needing_data) == 3
        
        # Update bitcoin to be current
        store.update_asset_progress("bitcoin", last_collected_unix_ts=current_ts - 1800)  # 30 min ago
        
        needing_data = store.get_assets_needing_data(current_ts, threshold_seconds=3600)
        assert len(needing_data) == 2
        assert "bitcoin" not in needing_data
    
    def test_get_assets_with_data(self, store, sample_assets):
        """Verify assets with data are correctly identified."""
        store.upsert_asset_metadata(sample_assets)
        
        for asset in sample_assets:
            store.initialize_asset_ingestion_state(asset["asset_id"])
        
        # Initially none have data
        with_data = store.get_assets_with_data()
        assert len(with_data) == 0
        
        # Update bitcoin
        store.update_asset_progress("bitcoin", last_collected_unix_ts=1700000000)
        
        with_data = store.get_assets_with_data()
        assert with_data == ["bitcoin"]


class TestMarketDataOperations:
    """Tests for market data storage operations."""
    
    def test_insert_market_data(self, store, sample_assets, sample_data_points):
        """Verify market data insertion."""
        store.upsert_asset_metadata(sample_assets)
        store.insert_market_data("bitcoin", sample_data_points)
        
        count = store.get_data_point_count("bitcoin")
        assert count == 3
    
    def test_insert_market_data_idempotent(self, store, sample_assets, sample_data_points):
        """Verify same data inserted twice produces no duplicates."""
        store.upsert_asset_metadata(sample_assets)
        
        store.insert_market_data("bitcoin", sample_data_points)
        store.insert_market_data("bitcoin", sample_data_points)  # Duplicate insert
        
        count = store.get_data_point_count("bitcoin")
        assert count == 3  # Still only 3
    
    def test_insert_market_data_updates_existing(self, store, sample_assets, sample_data_points):
        """Verify upsert updates existing data points."""
        store.upsert_asset_metadata(sample_assets)
        store.insert_market_data("bitcoin", sample_data_points)
        
        # Update price for first data point
        updated = [{"timestamp_unix": 1700000000, "price_usd": 40000.0, "market_cap_usd": 780000000000.0, "volume_usd": 20000000000.0}]
        store.insert_market_data("bitcoin", updated)
        
        result = store.conn.execute("""
            SELECT price_usd FROM market_data
            WHERE asset_id = 'bitcoin' AND timestamp_unix = 1700000000
        """).fetchone()
        
        assert result[0] == 40000.0
    
    def test_get_max_timestamp(self, store, sample_assets, sample_data_points):
        """Verify max timestamp retrieval."""
        store.upsert_asset_metadata(sample_assets)
        store.insert_market_data("bitcoin", sample_data_points)
        
        max_ts = store.get_max_timestamp("bitcoin")
        assert max_ts == 1700000000 + 7200
    
    def test_get_min_timestamp(self, store, sample_assets, sample_data_points):
        """Verify min timestamp retrieval."""
        store.upsert_asset_metadata(sample_assets)
        store.insert_market_data("bitcoin", sample_data_points)
        
        min_ts = store.get_min_timestamp("bitcoin")
        assert min_ts == 1700000000
    
    def test_get_total_data_points(self, store, sample_assets, sample_data_points):
        """Verify total data point count."""
        store.upsert_asset_metadata(sample_assets)
        store.insert_market_data("bitcoin", sample_data_points)
        store.insert_market_data("ethereum", sample_data_points)
        
        total = store.get_total_data_points()
        assert total == 6


class TestTransactions:
    """Tests for transactional operations."""
    
    def test_transaction_commit(self, store, sample_assets, sample_data_points):
        """Verify successful transaction commits."""
        store.upsert_asset_metadata(sample_assets)
        store.initialize_asset_ingestion_state("bitcoin")
        
        with store.transaction():
            store.insert_market_data("bitcoin", sample_data_points)
            store.update_asset_progress("bitcoin", last_collected_unix_ts=1700007200)
        
        count = store.get_data_point_count("bitcoin")
        state = store.get_asset_ingestion_state("bitcoin")
        
        assert count == 3
        assert state.last_collected_unix_ts == 1700007200
    
    def test_transaction_rollback(self, store, sample_assets, sample_data_points):
        """Verify failed transaction rollback."""
        store.upsert_asset_metadata(sample_assets)
        store.initialize_asset_ingestion_state("bitcoin")
        
        try:
            with store.transaction():
                store.insert_market_data("bitcoin", sample_data_points)
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # Data should be rolled back
        count = store.get_data_point_count("bitcoin")
        assert count == 0


class TestStateResumption:
    """Tests for state resumption after interruption."""
    
    def test_resume_after_interruption(self, store, sample_assets, sample_data_points):
        """Simulate crash mid-ingestion, verify restart continues correctly."""
        store.upsert_asset_metadata(sample_assets)
        
        for asset in sample_assets:
            store.initialize_asset_ingestion_state(asset["asset_id"])
        
        # Simulate partial ingestion for bitcoin
        partial_data = sample_data_points[:2]  # Only 2 of 3 points
        store.insert_market_data("bitcoin", partial_data)
        store.update_asset_progress(
            "bitcoin", 
            last_collected_unix_ts=1700000000 + 3600  # Only up to second point
        )
        
        # "Restart" - check state
        state = store.get_asset_ingestion_state("bitcoin")
        
        assert state.last_collected_unix_ts == 1700000000 + 3600
        
        # Assets to query should include ethereum and tether (never fetched)
        # but bitcoin should continue from where it left off
        current_ts = int(time.time())
        to_query = store.get_assets_to_query(current_ts, threshold_seconds=0)
        
        assert "ethereum" in to_query
        assert "tether" in to_query
        # bitcoin would be in to_query since last_collected_ts is old
        assert "bitcoin" in to_query
    
    def test_resume_after_rate_limit(self, store, sample_assets):
        """Verify state persisted when rate limited."""
        store.upsert_asset_metadata(sample_assets)
        
        # Set rate limited state
        store.update_ingestion_state(
            current_endpoint="marketchart",
            run_status="rate_limited"
        )
        
        # Verify state persisted
        state = store.get_ingestion_state()
        
        assert state.current_endpoint == "marketchart"
        assert state.run_status == "rate_limited"
        
        # Resume should pick up from same endpoint
        assert state.current_endpoint == "marketchart"


class TestIdempotency:
    """Tests for idempotency guarantees."""
    
    def test_duplicate_api_response_no_duplicates(self, store, sample_assets, sample_data_points):
        """Process same API response twice, verify single data set."""
        store.upsert_asset_metadata(sample_assets)
        
        # Simulate processing same response twice
        for _ in range(3):
            store.insert_market_data("bitcoin", sample_data_points)
        
        count = store.get_data_point_count("bitcoin")
        assert count == 3  # No duplicates
    
    def test_overlapping_time_ranges(self, store, sample_assets):
        """Fetch overlapping ranges, verify no duplicate timestamps."""
        store.upsert_asset_metadata(sample_assets)
        
        base_ts = 1700000000
        
        # First range: 0-4 hours
        range1 = [
            {"timestamp_unix": base_ts, "price_usd": 35000.0, "market_cap_usd": 680e9, "volume_usd": 15e9},
            {"timestamp_unix": base_ts + 3600, "price_usd": 35100.0, "market_cap_usd": 681e9, "volume_usd": 16e9},
            {"timestamp_unix": base_ts + 7200, "price_usd": 35200.0, "market_cap_usd": 682e9, "volume_usd": 14e9},
            {"timestamp_unix": base_ts + 10800, "price_usd": 35300.0, "market_cap_usd": 683e9, "volume_usd": 17e9},
        ]
        
        # Second range: 2-6 hours (overlaps 2 hours with first)
        range2 = [
            {"timestamp_unix": base_ts + 7200, "price_usd": 35250.0, "market_cap_usd": 682.5e9, "volume_usd": 14.5e9},  # Overlap
            {"timestamp_unix": base_ts + 10800, "price_usd": 35350.0, "market_cap_usd": 683.5e9, "volume_usd": 17.5e9},  # Overlap
            {"timestamp_unix": base_ts + 14400, "price_usd": 35400.0, "market_cap_usd": 684e9, "volume_usd": 18e9},
            {"timestamp_unix": base_ts + 18000, "price_usd": 35500.0, "market_cap_usd": 685e9, "volume_usd": 19e9},
        ]
        
        store.insert_market_data("bitcoin", range1)
        store.insert_market_data("bitcoin", range2)
        
        # Should have 6 unique timestamps (4 + 4 - 2 overlap)
        count = store.get_data_point_count("bitcoin")
        assert count == 6
        
        # Overlapping points should have updated values
        result = store.conn.execute("""
            SELECT price_usd FROM market_data
            WHERE asset_id = 'bitcoin' AND timestamp_unix = ?
        """, [base_ts + 7200]).fetchone()
        
        assert result[0] == 35250.0  # Updated value from range2


class TestIngestionSummary:
    """Tests for ingestion summary reporting."""
    
    def test_get_ingestion_summary(self, store, sample_assets, sample_data_points):
        """Verify summary statistics."""
        store.upsert_asset_metadata(sample_assets)
        
        for asset in sample_assets:
            store.initialize_asset_ingestion_state(asset["asset_id"])
        
        store.insert_market_data("bitcoin", sample_data_points)
        store.update_asset_progress("bitcoin", last_collected_unix_ts=1700007200)
        
        summary = store.get_ingestion_summary()
        
        assert summary["total_assets"] == 3
        assert summary["assets_with_data"] == 1
        assert summary["assets_pending"] == 2
        assert summary["total_data_points"] == 3
        assert summary["run_status"] == "idle"

