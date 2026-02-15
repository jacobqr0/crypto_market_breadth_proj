"""
Pytest configuration and shared fixtures for agent tests.
"""

import os
import sys
import types

# Only stub pandas/duckdb if they can't actually be imported
# This allows tests that need real pandas (like market_breadth) to work
def _try_import_real_pandas():
    """Try to import real pandas, return True if successful."""
    try:
        import pandas as _pd
        # Verify it's a real pandas with expected attributes
        if hasattr(_pd, 'date_range') and hasattr(_pd, 'DataFrame'):
            return True
    except ImportError:
        pass
    return False

def _try_import_real_duckdb():
    """Try to import real duckdb, return True if successful."""
    try:
        import duckdb as _duckdb
        if hasattr(_duckdb, 'connect'):
            return True
    except ImportError:
        pass
    return False

# Only stub if real module is not available
if "pandas" not in sys.modules and not _try_import_real_pandas():
    pandas_stub = types.ModuleType("pandas")
    pandas_stub.__version__ = "0.0.0"
    pandas_stub.DataFrame = type("DataFrame", (), {})
    pandas_stub.Series = type("Series", (), {})
    sys.modules["pandas"] = pandas_stub

# Stub duckdb only if not available
if "duckdb" not in sys.modules and not _try_import_real_duckdb():
    duckdb_stub = types.ModuleType("duckdb")
    
    # Create a mock connection class
    class MockDuckDBConnection:
        def execute(self, *args, **kwargs):
            return self
        def fetchall(self):
            return []
        def fetchone(self):
            return None
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    
    duckdb_stub.DuckDBPyConnection = MockDuckDBConnection
    duckdb_stub.connect = lambda *args, **kwargs: MockDuckDBConnection()
    sys.modules["duckdb"] = duckdb_stub

import pytest
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def setup_project_path():
    """Ensure project is in path for all tests."""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def mock_env_vars():
    """
    Fixture to set mock environment variables for testing.
    Restores original values after test.
    """
    original = {}
    
    def _set_vars(**kwargs):
        for key, value in kwargs.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value
    
    yield _set_vars
    
    # Restore original values
    for key, value in original.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
