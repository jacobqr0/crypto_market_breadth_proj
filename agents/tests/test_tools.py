"""
Tests for the agent tools.

Note: Many tools require database connections and external APIs.
These tests focus on the tool function signatures and basic behavior.
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

import duckdb


class TestPortfolioTools:
    """Tests for portfolio tools."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with test data."""
        temp_dir = Path(tempfile.mkdtemp())
        db_path = temp_dir / "test.duckdb"
        
        conn = duckdb.connect(str(db_path))
        
        # Create schema
        conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                asset_id VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                quantity DOUBLE NOT NULL DEFAULT 0,
                avg_cost_basis_usd DOUBLE NOT NULL DEFAULT 0,
                market_value_usd DOUBLE,
                unrealized_pnl_usd DOUBLE,
                opened_at TIMESTAMP NOT NULL,
                last_updated_at TIMESTAMP NOT NULL
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                side VARCHAR NOT NULL,
                quantity DOUBLE NOT NULL,
                price_usd DOUBLE NOT NULL,
                trade_value_usd DOUBLE NOT NULL,
                executed_at TIMESTAMP NOT NULL,
                fees_usd DOUBLE DEFAULT 0,
                realized_pnl_usd DOUBLE,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # Insert test data
        conn.execute("""
            INSERT INTO positions VALUES 
            ('bitcoin', 'btc', 0.5, 45000.0, NULL, NULL, '2026-01-01', '2026-01-10')
        """)
        
        conn.execute("""
            INSERT INTO trades VALUES 
            ('trade-1', 'bitcoin', 'btc', 'BUY', 0.5, 45000.0, 22500.0, '2026-01-01', 10.0, NULL, '2026-01-01')
        """)
        
        conn.close()
        
        # Set environment variable for tools
        old_env = os.environ.get("DUCKDB_PATH")
        os.environ["DUCKDB_PATH"] = str(db_path)
        
        yield str(db_path)
        
        # Cleanup
        if old_env:
            os.environ["DUCKDB_PATH"] = old_env
        else:
            os.environ.pop("DUCKDB_PATH", None)
        shutil.rmtree(temp_dir)
    
    def test_portfolio_tools_import(self):
        """Test that portfolio tools can be imported."""
        from agents.tools.portfolio_tools import (
            get_open_positions,
            get_position,
            get_trade_history,
            get_realized_pnl_summary,
            get_portfolio_summary,
        )
        
        # Check they are Tool objects (crewai @tool decorator returns Tool instances)
        assert hasattr(get_open_positions, 'func') or callable(get_open_positions)
        assert hasattr(get_position, 'func') or callable(get_position)
        assert hasattr(get_trade_history, 'func') or callable(get_trade_history)
        assert hasattr(get_realized_pnl_summary, 'func') or callable(get_realized_pnl_summary)
        assert hasattr(get_portfolio_summary, 'func') or callable(get_portfolio_summary)


class TestMarketDataTools:
    """Tests for market data tools."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with market data."""
        temp_dir = Path(tempfile.mkdtemp())
        db_path = temp_dir / "test.duckdb"
        
        conn = duckdb.connect(str(db_path))
        
        # Create schema
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                asset_id VARCHAR,
                timestamp_unix BIGINT,
                price_usd DOUBLE,
                market_cap_usd DOUBLE,
                volume_usd DOUBLE,
                ingested_at TIMESTAMP,
                PRIMARY KEY (asset_id, timestamp_unix)
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_metadata (
                asset_id VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                name VARCHAR,
                market_cap_rank INTEGER,
                first_seen_ts TIMESTAMP,
                last_updated_ts TIMESTAMP
            )
        """)
        
        # Insert test data
        base_ts = int(datetime(2026, 1, 1).timestamp())
        for i in range(10):
            conn.execute("""
                INSERT INTO market_data VALUES 
                ('bitcoin', ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [base_ts + i * 3600, 45000 + i * 100, 800e9, 50e9])
        
        conn.execute("""
            INSERT INTO asset_metadata VALUES
            ('bitcoin', 'btc', 'Bitcoin', 1, '2026-01-01', '2026-01-10')
        """)
        
        conn.close()
        
        old_env = os.environ.get("DUCKDB_PATH")
        os.environ["DUCKDB_PATH"] = str(db_path)
        
        yield str(db_path)
        
        if old_env:
            os.environ["DUCKDB_PATH"] = old_env
        else:
            os.environ.pop("DUCKDB_PATH", None)
        shutil.rmtree(temp_dir)
    
    def test_market_data_tools_import(self):
        """Test that market data tools can be imported."""
        from agents.tools.market_data_tools import (
            get_price_history,
            get_btc_relative_price,
            get_market_cap_rankings,
            get_price_change,
        )
        
        # Check they are Tool objects (crewai @tool decorator returns Tool instances)
        assert hasattr(get_price_history, 'func') or callable(get_price_history)
        assert hasattr(get_btc_relative_price, 'func') or callable(get_btc_relative_price)
        assert hasattr(get_market_cap_rankings, 'func') or callable(get_market_cap_rankings)
        assert hasattr(get_price_change, 'func') or callable(get_price_change)


class TestTechnicalTools:
    """Tests for technical analysis tools."""
    
    def test_technical_tools_import(self):
        """Test that technical tools can be imported."""
        from agents.tools.technical_tools import (
            get_sma,
            get_rsi,
            get_price_correlation,
            get_momentum_summary,
        )
        
        # Check they are Tool objects (crewai @tool decorator returns Tool instances)
        assert hasattr(get_sma, 'func') or callable(get_sma)
        assert hasattr(get_rsi, 'func') or callable(get_rsi)
        assert hasattr(get_price_correlation, 'func') or callable(get_price_correlation)
        assert hasattr(get_momentum_summary, 'func') or callable(get_momentum_summary)


class TestSerperTools:
    """Tests for Serper search tools."""
    
    def test_serper_tools_import(self):
        """Test that serper tools can be imported."""
        from agents.tools.serper_tools import (
            search_web,
            search_crypto_news,
            search_market_metrics,
            search_macro_conditions,
            search_asset_fundamentals,
        )
        
        # Check they are Tool objects (crewai @tool decorator returns Tool instances)
        assert hasattr(search_web, 'func') or callable(search_web)
        assert hasattr(search_crypto_news, 'func') or callable(search_crypto_news)
        assert hasattr(search_market_metrics, 'func') or callable(search_market_metrics)
        assert hasattr(search_macro_conditions, 'func') or callable(search_macro_conditions)
        assert hasattr(search_asset_fundamentals, 'func') or callable(search_asset_fundamentals)
    
    def test_serper_without_api_key(self):
        """Test serper tools error when API key not set."""
        # Remove API key if set
        old_key = os.environ.pop("SERPER_API_KEY", None)
        
        try:
            from agents.tools.serper_tools import _get_serper_api_key
            
            with pytest.raises(ValueError, match="SERPER_API_KEY"):
                _get_serper_api_key()
        finally:
            if old_key:
                os.environ["SERPER_API_KEY"] = old_key


class TestCrewImports:
    """Tests for crew module imports."""
    
    @pytest.mark.skipif(
        os.environ.get("SKIP_CREWAI_INIT_TESTS") == "1",
        reason="Skipping tests that require crewai initialization (permission issues in sandbox)"
    )
    def test_crew_imports(self):
        """Test that crew module can be imported."""
        from agents.crew import (
            create_token_research_agent,  # Legacy, kept for backward compatibility
            create_token_screener_agent,
            create_fundamentals_analyst_agent,
            create_research_synthesizer_agent,
            create_technical_analyst_agent,
            create_macro_cycle_agent,
            create_portfolio_context_agent,
            create_orchestrator_agent,
            create_qa_risk_agent,
            create_investment_crew,
            run_investment_crew,
        )
        
        # Test legacy agent (backward compatibility)
        assert callable(create_token_research_agent)
        # Test new token research chain agents
        assert callable(create_token_screener_agent)
        assert callable(create_fundamentals_analyst_agent)
        assert callable(create_research_synthesizer_agent)
        # Test crew functions
        assert callable(create_investment_crew)
        assert callable(run_investment_crew)
    
    @pytest.mark.skipif(
        os.environ.get("SKIP_CREWAI_INIT_TESTS") == "1",
        reason="Skipping tests that require crewai initialization (permission issues in sandbox)"
    )
    def test_post_mortem_imports(self):
        """Test that post-mortem module can be imported."""
        from agents.post_mortem import (
            create_post_mortem_agent,
            run_meta_learning,
            get_performance_summary,
        )
        
        assert callable(create_post_mortem_agent)
        assert callable(run_meta_learning)
        assert callable(get_performance_summary)
