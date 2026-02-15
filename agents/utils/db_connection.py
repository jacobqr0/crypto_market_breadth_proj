"""
Centralized DuckDB connection manager for all agent tools.

This module provides a singleton connection manager to ensure all tools
use the same database connection with consistent configuration, avoiding
DuckDB's "different configuration" connection errors.

Usage:
    from agents.utils.db_connection import get_db_connection, close_db_connection
    
    # Get the shared connection
    conn = get_db_connection()
    result = conn.execute("SELECT * FROM market_data LIMIT 10").fetchall()
    
    # Close when done (typically at end of crew execution)
    close_db_connection()
"""

import os
import duckdb
from typing import Optional
import threading
import logging

logger = logging.getLogger(__name__)


class DBConnectionManager:
    """
    Thread-safe singleton connection manager for DuckDB.
    
    Ensures all components share a single connection with consistent
    configuration to avoid DuckDB connection conflicts.
    """
    _instance: Optional['DBConnectionManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._conn = None
                cls._instance._db_path = None
            return cls._instance
    
    def get_connection(self, db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        """
        Get or create the shared DuckDB connection.
        
        Args:
            db_path: Optional path to database. If not provided, uses
                     DUCKDB_PATH environment variable or defaults to
                     "market_data.duckdb"
        
        Returns:
            Shared DuckDB connection
            
        Note:
            All connections use read_only=False for consistency.
            The technical_tools module needs write access for caching
            indicator values, so we use read_only=False across all tools.
        """
        with self._lock:
            if db_path is None:
                db_path = os.environ.get("DUCKDB_PATH", "market_data.duckdb")
            
            # If connection exists but for different db_path, close and reconnect
            if self._conn is not None and self._db_path != db_path:
                logger.info(f"Closing existing connection to {self._db_path}, reconnecting to {db_path}")
                self._conn.close()
                self._conn = None
            
            if self._conn is None:
                logger.info(f"Opening DuckDB connection to {db_path}")
                # Use read_only=False consistently for all connections
                # This allows technical_tools to cache indicator values
                self._conn = duckdb.connect(db_path, read_only=False)
                self._db_path = db_path
            
            return self._conn
    
    def close(self):
        """
        Close the shared connection.
        
        Call this at the end of crew execution to release database resources.
        """
        with self._lock:
            if self._conn is not None:
                logger.info(f"Closing DuckDB connection to {self._db_path}")
                self._conn.close()
                self._conn = None
                self._db_path = None
    
    @property
    def is_connected(self) -> bool:
        """Check if a connection is currently open."""
        return self._conn is not None


# Module-level convenience functions

def get_db_connection(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """
    Get the shared DuckDB connection.
    
    Args:
        db_path: Optional path to database file
    
    Returns:
        Shared DuckDB connection
    """
    return DBConnectionManager().get_connection(db_path)


def close_db_connection():
    """Close the shared DuckDB connection."""
    DBConnectionManager().close()


def is_db_connected() -> bool:
    """Check if a database connection is currently open."""
    return DBConnectionManager().is_connected
