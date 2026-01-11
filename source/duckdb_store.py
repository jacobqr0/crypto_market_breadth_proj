"""
DuckDB persistence layer for CoinGecko market data ingestion.

This module provides a DuckDBStore class that handles all database operations
including schema initialization, state management, and market data storage.
"""

import duckdb
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class AssetMetadata:
    """Asset metadata from coins/markets endpoint."""
    asset_id: str
    symbol: str
    name: str
    market_cap_rank: Optional[int]
    first_seen_ts: Optional[datetime] = None
    last_updated_ts: Optional[datetime] = None


@dataclass
class AssetIngestionState:
    """Per-asset ingestion progress tracking."""
    asset_id: str
    last_collected_unix_ts: Optional[int]
    first_collected_unix_ts: Optional[int]
    is_backfill_complete: bool
    last_query_ts: Optional[datetime]


@dataclass
class IngestionState:
    """Global ingestion process state."""
    current_endpoint: Optional[str]
    last_updated_ts: Optional[datetime]
    run_status: str  # 'idle' | 'running' | 'rate_limited' | 'error'


class DuckDBStore:
    """
    DuckDB-backed storage for crypto market data ingestion.
    
    Handles schema initialization, state management, and market data persistence
    with transactional guarantees for restartable ingestion.
    """
    
    RUN_STATUS_IDLE = "idle"
    RUN_STATUS_RUNNING = "running"
    RUN_STATUS_RATE_LIMITED = "rate_limited"
    RUN_STATUS_ERROR = "error"
    
    def __init__(self, db_path: str = "market_data.duckdb"):
        """
        Initialize DuckDB connection.
        
        :param db_path: Path to DuckDB database file. Use ':memory:' for in-memory DB.
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Create all required tables and indexes if they don't exist."""
        
        # Ingestion state table (singleton row)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                current_endpoint VARCHAR,
                last_updated_ts TIMESTAMP,
                run_status VARCHAR DEFAULT 'idle'
            )
        """)
        
        # Initialize singleton row if not exists
        self.conn.execute("""
            INSERT INTO ingestion_state (id, run_status)
            SELECT 1, 'idle'
            WHERE NOT EXISTS (SELECT 1 FROM ingestion_state WHERE id = 1)
        """)
        
        # Asset metadata table
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
        
        # Asset ingestion state table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_ingestion_state (
                asset_id VARCHAR PRIMARY KEY,
                last_collected_unix_ts BIGINT,
                first_collected_unix_ts BIGINT,
                is_backfill_complete BOOLEAN DEFAULT FALSE,
                last_query_ts TIMESTAMP
            )
        """)
        
        # Market data table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                asset_id VARCHAR,
                timestamp_unix BIGINT,
                price_usd DOUBLE,
                market_cap_usd DOUBLE,
                volume_usd DOUBLE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (asset_id, timestamp_unix)
            )
        """)
        
        # Create indexes for common query patterns
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_asset 
            ON market_data(asset_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_ts 
            ON market_data(timestamp_unix)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_asset_meta_rank 
            ON asset_metadata(market_cap_rank)
        """)
        
        logger.info("DuckDB schema initialized successfully")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactional operations.
        
        Usage:
            with store.transaction():
                store.insert_market_data(...)
                store.update_asset_progress(...)
        """
        try:
            self.conn.execute("BEGIN TRANSACTION")
            yield
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
    
    # ==================== Ingestion State Operations ====================
    
    def get_ingestion_state(self) -> IngestionState:
        """Get current ingestion state."""
        result = self.conn.execute("""
            SELECT current_endpoint, last_updated_ts, run_status
            FROM ingestion_state
            WHERE id = 1
        """).fetchone()
        
        if result:
            return IngestionState(
                current_endpoint=result[0],
                last_updated_ts=result[1],
                run_status=result[2] or self.RUN_STATUS_IDLE
            )
        return IngestionState(None, None, self.RUN_STATUS_IDLE)
    
    def update_ingestion_state(
        self,
        current_endpoint: Optional[str] = None,
        run_status: Optional[str] = None
    ):
        """
        Update global ingestion state.
        
        :param current_endpoint: Current API endpoint being processed
        :param run_status: Current run status
        """
        updates = ["last_updated_ts = ?"]
        params = [datetime.now()]
        
        if current_endpoint is not None:
            updates.append("current_endpoint = ?")
            params.append(current_endpoint)
        
        if run_status is not None:
            updates.append("run_status = ?")
            params.append(run_status)
        
        query = f"UPDATE ingestion_state SET {', '.join(updates)} WHERE id = 1"
        self.conn.execute(query, params)
    
    def is_initial_run(self) -> bool:
        """Check if this is the first run (no assets in database)."""
        result = self.conn.execute("""
            SELECT COUNT(*) FROM asset_metadata
        """).fetchone()
        return result[0] == 0
    
    # ==================== Asset Metadata Operations ====================
    
    def upsert_asset_metadata(self, assets: List[Dict[str, Any]]):
        """
        Insert or update asset metadata.
        
        :param assets: List of asset dictionaries with keys:
                      asset_id, symbol, name, market_cap_rank
        """
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
        
        logger.info(f"Upserted {len(assets)} asset metadata records")
    
    def get_top_assets(self, limit: int = 300) -> List[str]:
        """
        Get top assets by market cap rank.
        
        :param limit: Maximum number of assets to return
        :return: List of asset IDs
        """
        result = self.conn.execute("""
            SELECT asset_id FROM asset_metadata
            WHERE market_cap_rank IS NOT NULL
            ORDER BY market_cap_rank ASC
            LIMIT ?
        """, [limit]).fetchall()
        return [row[0] for row in result]
    
    def get_all_asset_ids(self) -> List[str]:
        """Get all asset IDs in the database."""
        result = self.conn.execute("""
            SELECT asset_id FROM asset_metadata
        """).fetchall()
        return [row[0] for row in result]
    
    # ==================== Asset Ingestion State Operations ====================
    
    def initialize_asset_ingestion_state(self, asset_id: str):
        """
        Initialize ingestion state for a new asset.
        
        :param asset_id: Asset identifier
        """
        self.conn.execute("""
            INSERT INTO asset_ingestion_state (asset_id, is_backfill_complete)
            VALUES (?, FALSE)
            ON CONFLICT (asset_id) DO NOTHING
        """, [asset_id])
    
    def get_asset_ingestion_state(self, asset_id: str) -> Optional[AssetIngestionState]:
        """
        Get ingestion state for a specific asset.
        
        :param asset_id: Asset identifier
        :return: AssetIngestionState or None if not found
        """
        result = self.conn.execute("""
            SELECT asset_id, last_collected_unix_ts, first_collected_unix_ts,
                   is_backfill_complete, last_query_ts
            FROM asset_ingestion_state
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        if result:
            return AssetIngestionState(
                asset_id=result[0],
                last_collected_unix_ts=result[1],
                first_collected_unix_ts=result[2],
                is_backfill_complete=result[3],
                last_query_ts=result[4]
            )
        return None
    
    def update_asset_progress(
        self,
        asset_id: str,
        last_collected_unix_ts: Optional[int] = None,
        first_collected_unix_ts: Optional[int] = None,
        is_backfill_complete: Optional[bool] = None
    ):
        """
        Update ingestion progress for an asset.
        
        :param asset_id: Asset identifier
        :param last_collected_unix_ts: Latest timestamp collected
        :param first_collected_unix_ts: Earliest timestamp collected
        :param is_backfill_complete: Whether backfill is complete
        """
        updates = ["last_query_ts = ?"]
        params = [datetime.now()]
        
        if last_collected_unix_ts is not None:
            updates.append("last_collected_unix_ts = ?")
            params.append(last_collected_unix_ts)
        
        if first_collected_unix_ts is not None:
            updates.append("first_collected_unix_ts = ?")
            params.append(first_collected_unix_ts)
        
        if is_backfill_complete is not None:
            updates.append("is_backfill_complete = ?")
            params.append(is_backfill_complete)
        
        params.append(asset_id)
        query = f"""
            UPDATE asset_ingestion_state 
            SET {', '.join(updates)} 
            WHERE asset_id = ?
        """
        self.conn.execute(query, params)
    
    def get_assets_needing_data(self, current_ts: int, threshold_seconds: int = 3600) -> List[str]:
        """
        Get assets that need data fetching.
        
        Returns assets where:
        - Never fetched (last_collected_unix_ts is NULL)
        - Last fetch is older than threshold
        
        :param current_ts: Current unix timestamp
        :param threshold_seconds: Minimum age in seconds before refetch (default 1 hour)
        :return: List of asset IDs needing data
        """
        result = self.conn.execute("""
            SELECT ais.asset_id
            FROM asset_ingestion_state ais
            JOIN asset_metadata am ON ais.asset_id = am.asset_id
            WHERE ais.last_collected_unix_ts IS NULL
               OR (? - ais.last_collected_unix_ts) > ?
            ORDER BY am.market_cap_rank ASC NULLS LAST
        """, [current_ts, threshold_seconds]).fetchall()
        return [row[0] for row in result]
    
    def get_assets_with_data(self) -> List[str]:
        """Get asset IDs that have been fetched at least once."""
        result = self.conn.execute("""
            SELECT asset_id FROM asset_ingestion_state
            WHERE last_collected_unix_ts IS NOT NULL
        """).fetchall()
        return [row[0] for row in result]
    
    def get_assets_to_query(self, current_ts: int, threshold_seconds: int = 3600) -> List[str]:
        """
        Get union of top assets and already-fetched assets that need updates.
        
        This implements the control flow requirement:
        Union(already_fetched, current_top_300) -> filter by needs update
        
        :param current_ts: Current unix timestamp
        :param threshold_seconds: Minimum age before refetch
        :return: List of asset IDs to query
        """
        # Get all assets with ingestion state that need updates
        result = self.conn.execute("""
            SELECT DISTINCT ais.asset_id
            FROM asset_ingestion_state ais
            JOIN asset_metadata am ON ais.asset_id = am.asset_id
            WHERE ais.last_collected_unix_ts IS NULL
               OR (? - ais.last_collected_unix_ts) > ?
            ORDER BY am.market_cap_rank ASC NULLS LAST
        """, [current_ts, threshold_seconds]).fetchall()
        return [row[0] for row in result]
    
    # ==================== Market Data Operations ====================
    
    def insert_market_data(self, asset_id: str, data_points: List[Dict[str, Any]]):
        """
        Insert market data points with upsert semantics (idempotent).
        
        :param asset_id: Asset identifier
        :param data_points: List of data point dicts with keys:
                          timestamp_unix, price_usd, market_cap_usd, volume_usd
        """
        if not data_points:
            return
        
        now = datetime.now()
        for dp in data_points:
            self.conn.execute("""
                INSERT INTO market_data (
                    asset_id, timestamp_unix, price_usd, 
                    market_cap_usd, volume_usd, ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (asset_id, timestamp_unix) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    volume_usd = EXCLUDED.volume_usd,
                    ingested_at = ?
            """, [
                asset_id,
                dp.get("timestamp_unix"),
                dp.get("price_usd"),
                dp.get("market_cap_usd"),
                dp.get("volume_usd"),
                now,
                now
            ])
        
        logger.info(f"Inserted {len(data_points)} data points for {asset_id}")
    
    def get_max_timestamp(self, asset_id: str) -> Optional[int]:
        """
        Get the maximum timestamp for an asset's market data.
        
        :param asset_id: Asset identifier
        :return: Max unix timestamp or None if no data
        """
        result = self.conn.execute("""
            SELECT MAX(timestamp_unix) FROM market_data
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        return result[0] if result and result[0] else None
    
    def get_min_timestamp(self, asset_id: str) -> Optional[int]:
        """
        Get the minimum timestamp for an asset's market data.
        
        :param asset_id: Asset identifier
        :return: Min unix timestamp or None if no data
        """
        result = self.conn.execute("""
            SELECT MIN(timestamp_unix) FROM market_data
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        return result[0] if result and result[0] else None
    
    def get_data_point_count(self, asset_id: str) -> int:
        """
        Get count of data points for an asset.
        
        :param asset_id: Asset identifier
        :return: Number of data points
        """
        result = self.conn.execute("""
            SELECT COUNT(*) FROM market_data
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        return result[0] if result else 0
    
    def get_total_data_points(self) -> int:
        """Get total count of all data points in database."""
        result = self.conn.execute("""
            SELECT COUNT(*) FROM market_data
        """).fetchone()
        return result[0] if result else 0
    
    # ==================== Utility Methods ====================
    
    def get_ingestion_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics about ingestion progress.
        
        :return: Dictionary with summary statistics
        """
        total_assets = self.conn.execute(
            "SELECT COUNT(*) FROM asset_metadata"
        ).fetchone()[0]
        
        assets_with_data = self.conn.execute("""
            SELECT COUNT(*) FROM asset_ingestion_state
            WHERE last_collected_unix_ts IS NOT NULL
        """).fetchone()[0]
        
        total_data_points = self.get_total_data_points()
        
        state = self.get_ingestion_state()
        
        return {
            "total_assets": total_assets,
            "assets_with_data": assets_with_data,
            "assets_pending": total_assets - assets_with_data,
            "total_data_points": total_data_points,
            "current_endpoint": state.current_endpoint,
            "run_status": state.run_status,
            "last_updated": state.last_updated_ts
        }

