#!/usr/bin/env python3
"""
Simple verification script for the dropped assets fix.
This isolates the DuckDB test from the rest of the codebase to avoid pandas issues.
"""

import sys
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

# Inline the minimal DuckDB code needed for testing
try:
    import duckdb
    print("✓ DuckDB import successful")
except ImportError as e:
    print(f"✗ DuckDB import failed: {e}")
    print("  Try: pip install duckdb")
    sys.exit(1)


@dataclass
class AssetMetadata:
    """Asset metadata from coins/markets endpoint."""
    asset_id: str
    symbol: str
    name: str
    market_cap_rank: Optional[int]
    first_seen_ts: Optional[datetime] = None
    last_updated_ts: Optional[datetime] = None


class TestDuckDBStore:
    """Minimal DuckDB store for testing the fix."""
    
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()
    
    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_metadata (
                asset_id VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                name VARCHAR,
                market_cap_rank INTEGER,
                first_seen_ts TIMESTAMP,
                last_updated_ts TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_ingestion_state (
                asset_id VARCHAR PRIMARY KEY,
                last_collected_unix_ts BIGINT,
                first_collected_unix_ts BIGINT,
                is_backfill_complete BOOLEAN DEFAULT FALSE,
                last_query_ts TIMESTAMP
            )
        """)
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def upsert_asset_metadata(self, assets: List[Dict[str, Any]]):
        now = datetime.now()
        for asset in assets:
            self.conn.execute("""
                INSERT INTO asset_metadata (
                    asset_id, symbol, name, market_cap_rank,
                    first_seen_ts, last_updated_ts
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (asset_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    last_updated_ts = ?
            """, [
                asset.get("asset_id"),
                asset.get("symbol"),
                asset.get("name"),
                asset.get("market_cap_rank"),
                now,
                now,
                now
            ])
    
    def initialize_asset_ingestion_state(self, asset_id: str):
        self.conn.execute("""
            INSERT INTO asset_ingestion_state (asset_id, is_backfill_complete)
            VALUES (?, FALSE)
            ON CONFLICT (asset_id) DO NOTHING
        """, [asset_id])
    
    def get_asset_metadata(self, asset_id: str) -> Optional[AssetMetadata]:
        result = self.conn.execute("""
            SELECT asset_id, symbol, name, market_cap_rank, first_seen_ts, last_updated_ts
            FROM asset_metadata
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        if result:
            return AssetMetadata(
                asset_id=result[0],
                symbol=result[1],
                name=result[2],
                market_cap_rank=result[3],
                first_seen_ts=result[4],
                last_updated_ts=result[5]
            )
        return None
    
    def mark_assets_dropped_from_top_list(self, current_top_asset_ids: List[str]):
        """
        Mark assets that are no longer in the current top list.
        Sets market_cap_rank to NULL and updates last_updated_ts.
        """
        if not current_top_asset_ids:
            return
        
        now = datetime.now()
        
        # Get all tracked assets (those with ingestion state)
        all_tracked = self.conn.execute("""
            SELECT DISTINCT asset_id FROM asset_ingestion_state
        """).fetchall()
        all_tracked_ids = set(row[0] for row in all_tracked)
        
        current_top_set = set(current_top_asset_ids)
        dropped_assets = all_tracked_ids - current_top_set
        
        if not dropped_assets:
            return
        
        # Update dropped assets: set market_cap_rank to NULL and update timestamp
        for asset_id in dropped_assets:
            self.conn.execute("""
                UPDATE asset_metadata
                SET market_cap_rank = NULL,
                    last_updated_ts = ?
                WHERE asset_id = ?
            """, [now, asset_id])
        
        print(f"  [DEBUG] Marked {len(dropped_assets)} assets as dropped: {dropped_assets}")


def test_mark_assets_dropped_from_top_list():
    """Test that dropped assets have their market_cap_rank set to NULL."""
    print("\n--- Test: mark_assets_dropped_from_top_list ---")
    
    store = TestDuckDBStore(":memory:")
    
    # Setup: Create 3 assets
    sample_assets = [
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
    ]
    store.upsert_asset_metadata(sample_assets)
    
    # Initialize ingestion state (marks them as tracked)
    for asset in sample_assets:
        store.initialize_asset_ingestion_state(asset["asset_id"])
    
    # Verify initial state
    tether_meta = store.get_asset_metadata("tether")
    assert tether_meta is not None, "Tether metadata should exist"
    assert tether_meta.market_cap_rank == 3, f"Expected rank 3, got {tether_meta.market_cap_rank}"
    print(f"  Initial: tether rank = {tether_meta.market_cap_rank}")
    
    # Simulate tether dropping out - only bitcoin and ethereum in top list
    current_top_ids = ["bitcoin", "ethereum"]
    store.mark_assets_dropped_from_top_list(current_top_ids)
    
    # Verify tether's rank is now NULL
    tether_after = store.get_asset_metadata("tether")
    assert tether_after.market_cap_rank is None, f"Expected NULL rank, got {tether_after.market_cap_rank}"
    print(f"  After drop: tether rank = {tether_after.market_cap_rank}")
    
    # Verify bitcoin and ethereum ranks unchanged
    btc = store.get_asset_metadata("bitcoin")
    eth = store.get_asset_metadata("ethereum")
    assert btc.market_cap_rank == 1, f"Bitcoin rank should be 1, got {btc.market_cap_rank}"
    assert eth.market_cap_rank == 2, f"Ethereum rank should be 2, got {eth.market_cap_rank}"
    print(f"  Bitcoin rank = {btc.market_cap_rank}, Ethereum rank = {eth.market_cap_rank}")
    
    store.close()
    print("✓ Test passed!")


def test_no_duplicate_ranks():
    """Test that no duplicate market_cap_rank values exist after drop."""
    print("\n--- Test: no_duplicate_ranks ---")
    
    store = TestDuckDBStore(":memory:")
    
    # Setup: Create 3 assets
    sample_assets = [
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
    ]
    store.upsert_asset_metadata(sample_assets)
    
    for asset in sample_assets:
        store.initialize_asset_ingestion_state(asset["asset_id"])
    
    # Add new asset that takes rank 3 (replacing tether)
    new_asset = {"asset_id": "binancecoin", "symbol": "bnb", "name": "BNB", "market_cap_rank": 3}
    store.upsert_asset_metadata([new_asset])
    store.initialize_asset_ingestion_state("binancecoin")
    
    # Check for duplicates BEFORE fix (tether and bnb both have rank 3)
    result_before = store.conn.execute("""
        SELECT market_cap_rank, COUNT(*) as cnt
        FROM asset_metadata
        WHERE market_cap_rank IS NOT NULL
        GROUP BY market_cap_rank
        HAVING COUNT(*) > 1
    """).fetchall()
    print(f"  Before fix - duplicate ranks: {result_before}")
    
    # Apply fix: tether dropped, bnb at rank 3
    current_top_ids = ["bitcoin", "ethereum", "binancecoin"]
    store.mark_assets_dropped_from_top_list(current_top_ids)
    
    # Check for duplicates AFTER fix
    result_after = store.conn.execute("""
        SELECT market_cap_rank, COUNT(*) as cnt
        FROM asset_metadata
        WHERE market_cap_rank IS NOT NULL
        GROUP BY market_cap_rank
        HAVING COUNT(*) > 1
    """).fetchall()
    
    assert len(result_after) == 0, f"Found duplicate ranks after fix: {result_after}"
    print(f"  After fix - duplicate ranks: {result_after}")
    
    # Verify the specific ranks
    btc = store.get_asset_metadata("bitcoin")
    eth = store.get_asset_metadata("ethereum")
    tether = store.get_asset_metadata("tether")
    bnb = store.get_asset_metadata("binancecoin")
    
    print(f"  BTC rank: {btc.market_cap_rank}")
    print(f"  ETH rank: {eth.market_cap_rank}")
    print(f"  Tether rank: {tether.market_cap_rank}")
    print(f"  BNB rank: {bnb.market_cap_rank}")
    
    assert btc.market_cap_rank == 1
    assert eth.market_cap_rank == 2
    assert tether.market_cap_rank is None  # Dropped
    assert bnb.market_cap_rank == 3  # New entrant
    
    store.close()
    print("✓ Test passed!")


def test_get_asset_metadata():
    """Test get_asset_metadata method."""
    print("\n--- Test: get_asset_metadata ---")
    
    store = TestDuckDBStore(":memory:")
    
    # Test nonexistent asset
    meta = store.get_asset_metadata("nonexistent")
    assert meta is None, "Should return None for nonexistent asset"
    print("  Nonexistent asset returns None: ✓")
    
    # Add asset and retrieve
    store.upsert_asset_metadata([
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1}
    ])
    
    meta = store.get_asset_metadata("bitcoin")
    assert meta is not None
    assert meta.asset_id == "bitcoin"
    assert meta.symbol == "btc"
    assert meta.market_cap_rank == 1
    print(f"  Retrieved: {meta.asset_id}, rank={meta.market_cap_rank}: ✓")
    
    store.close()
    print("✓ Test passed!")


def test_last_updated_ts_updated():
    """Test that last_updated_ts is updated for dropped assets."""
    print("\n--- Test: last_updated_ts_updated ---")
    
    store = TestDuckDBStore(":memory:")
    
    sample_assets = [
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 2},
    ]
    store.upsert_asset_metadata(sample_assets)
    
    for asset in sample_assets:
        store.initialize_asset_ingestion_state(asset["asset_id"])
    
    initial_ts = store.get_asset_metadata("tether").last_updated_ts
    print(f"  Initial last_updated_ts: {initial_ts}")
    
    # Small delay
    time.sleep(0.1)
    
    # Tether drops out
    store.mark_assets_dropped_from_top_list(["bitcoin"])
    
    after_ts = store.get_asset_metadata("tether").last_updated_ts
    print(f"  After drop last_updated_ts: {after_ts}")
    
    assert after_ts > initial_ts, f"Expected timestamp to be updated, initial={initial_ts}, after={after_ts}"
    
    store.close()
    print("✓ Test passed!")


def test_asset_returns_to_top_list():
    """Test that assets can return to the top list and get their rank restored."""
    print("\n--- Test: asset_returns_to_top_list ---")
    
    store = TestDuckDBStore(":memory:")
    
    sample_assets = [
        {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
    ]
    store.upsert_asset_metadata(sample_assets)
    
    for asset in sample_assets:
        store.initialize_asset_ingestion_state(asset["asset_id"])
    
    # Initial: tether at rank 3
    tether = store.get_asset_metadata("tether")
    assert tether.market_cap_rank == 3
    print(f"  Initial: tether rank = {tether.market_cap_rank}")
    
    # Tether drops out
    store.mark_assets_dropped_from_top_list(["bitcoin", "ethereum"])
    tether = store.get_asset_metadata("tether")
    assert tether.market_cap_rank is None
    print(f"  After drop: tether rank = {tether.market_cap_rank}")
    
    # Tether returns at rank 4
    store.upsert_asset_metadata([
        {"asset_id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 4}
    ])
    tether = store.get_asset_metadata("tether")
    assert tether.market_cap_rank == 4
    print(f"  After return: tether rank = {tether.market_cap_rank}")
    
    store.close()
    print("✓ Test passed!")


if __name__ == "__main__":
    print("=" * 60)
    print("VERIFICATION: Dropped Assets Fix")
    print("=" * 60)
    
    try:
        test_get_asset_metadata()
        test_mark_assets_dropped_from_top_list()
        test_no_duplicate_ranks()
        test_last_updated_ts_updated()
        test_asset_returns_to_top_list()
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED! ✓")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
