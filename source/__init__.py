"""
CoinGecko market data ingestion package.

This package provides tools for collecting and storing cryptocurrency
market data from the CoinGecko API.

Modules:
    coingecko_api: API client for CoinGecko endpoints
    duckdb_store: DuckDB persistence layer
    ingestion: Orchestration and control flow
"""

try:
    from .coingecko_api import CoinGeckoAPI
    from .duckdb_store import DuckDBStore
    from .ingestion import IngestionOrchestrator, run_ingestion
except ImportError:
    # When running tests directly from source directory
    pass

__all__ = [
    "CoinGeckoAPI",
    "DuckDBStore", 
    "IngestionOrchestrator",
    "run_ingestion"
]

