"""
Tests for the deterministic portfolio snapshot tool.

These tests verify that get_portfolio_snapshot returns correct compliance
checks computed in code, eliminating LLM-based compliance that could
produce contradictions.

Test Cases:
1. BTC-only portfolio (100% BTC) - btc_within_target should be false
2. BTC 50% / ETH 50% - btc_within_target should be true
3. Multiple positions summing to 100%
4. Missing price for one asset - data_quality should be "partial"
5. Contradiction injection - data_quality should be "invalid"
"""

import json
import pytest
import os
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

import duckdb


class TestPortfolioSnapshot:
    """Tests for the deterministic portfolio snapshot tool."""
    
    @pytest.fixture
    def temp_db_factory(self):
        """Factory fixture that creates temporary databases with custom data."""
        created_dirs = []
        created_envs = []
        
        def _create_db(positions=None, trades=None, market_data=None, asset_metadata=None):
            temp_dir = Path(tempfile.mkdtemp())
            created_dirs.append(temp_dir)
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
            
            # Insert positions
            if positions:
                for pos in positions:
                    conn.execute("""
                        INSERT INTO positions 
                        (asset_id, symbol, quantity, avg_cost_basis_usd, 
                         market_value_usd, unrealized_pnl_usd, opened_at, last_updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        pos.get("asset_id"),
                        pos.get("symbol"),
                        pos.get("quantity"),
                        pos.get("avg_cost_basis_usd"),
                        pos.get("market_value_usd"),
                        pos.get("unrealized_pnl_usd"),
                        pos.get("opened_at", datetime.now()),
                        pos.get("last_updated_at", datetime.now()),
                    ])
            
            # Insert trades
            if trades:
                for trade in trades:
                    conn.execute("""
                        INSERT INTO trades 
                        (trade_id, asset_id, symbol, side, quantity, price_usd,
                         trade_value_usd, executed_at, fees_usd, realized_pnl_usd, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        trade.get("trade_id"),
                        trade.get("asset_id"),
                        trade.get("symbol"),
                        trade.get("side"),
                        trade.get("quantity"),
                        trade.get("price_usd"),
                        trade.get("trade_value_usd"),
                        trade.get("executed_at", datetime.now()),
                        trade.get("fees_usd", 0),
                        trade.get("realized_pnl_usd"),
                        trade.get("created_at", datetime.now()),
                    ])
            
            # Insert market data
            if market_data:
                base_ts = int(datetime.now().timestamp())
                for md in market_data:
                    conn.execute("""
                        INSERT INTO market_data 
                        (asset_id, timestamp_unix, price_usd, market_cap_usd, volume_usd, ingested_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        md.get("asset_id"),
                        md.get("timestamp_unix", base_ts),
                        md.get("price_usd"),
                        md.get("market_cap_usd"),
                        md.get("volume_usd"),
                    ])
            
            # Insert asset metadata
            if asset_metadata:
                for am in asset_metadata:
                    conn.execute("""
                        INSERT INTO asset_metadata 
                        (asset_id, symbol, name, market_cap_rank, first_seen_ts, last_updated_ts)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, [
                        am.get("asset_id"),
                        am.get("symbol"),
                        am.get("name"),
                        am.get("market_cap_rank"),
                    ])
            
            conn.close()
            
            # Save old env and set new
            old_env = os.environ.get("DUCKDB_PATH")
            created_envs.append(old_env)
            os.environ["DUCKDB_PATH"] = str(db_path)
            
            # Reset the portfolio store to pick up the new connection
            from agents.tools import portfolio_tools
            portfolio_tools._portfolio_store = None
            
            return str(db_path)
        
        yield _create_db
        
        # Cleanup
        from agents.tools import portfolio_tools
        portfolio_tools._portfolio_store = None
        
        for i, temp_dir in enumerate(created_dirs):
            old_env = created_envs[i] if i < len(created_envs) else None
            if old_env:
                os.environ["DUCKDB_PATH"] = old_env
            else:
                os.environ.pop("DUCKDB_PATH", None)
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_btc_only_portfolio(self, temp_db_factory):
        """
        Test Case 1: BTC-only portfolio (100% BTC)
        
        Expected:
        - btc_allocation_pct_by_value = 100.0
        - btc_within_target = false (100 > 60)
        - contradictions_detected = false
        """
        # Setup: 1 BTC position at $50,000 cost, current price $100,000
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.5, "avg_cost_basis_usd": 50000.0}
            ],
            market_data=[
                {"asset_id": "bitcoin", "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9}
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1}
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # Check derived metrics
        assert snapshot["derived"]["btc_allocation_pct_by_value"] == 100.0
        assert snapshot["derived"]["btc_quantity"] == 0.5
        
        # Check compliance
        checks = snapshot["framework"]["checks"]
        assert checks["btc_within_target"] is False  # 100% > 60% max
        assert checks["pricing_complete"] is True
        assert checks["contradictions_detected"] is False
        assert checks["total_allocations_sum_to_100"] is True
        
        # Check data quality
        assert snapshot["meta"]["data_quality"] == "ok"
    
    def test_btc_50_eth_50_portfolio(self, temp_db_factory):
        """
        Test Case 2: BTC 50% / ETH 50%
        
        Expected:
        - btc_within_target = true (50% is within 40-60%)
        - any_position_over_limit = true (ETH at 50% exceeds 20% single asset limit)
        """
        # Setup: BTC and ETH each worth $50,000 current value
        base_ts = int(datetime.now().timestamp())
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.5, "avg_cost_basis_usd": 50000.0},
                {"asset_id": "ethereum", "symbol": "eth", "quantity": 20, "avg_cost_basis_usd": 2000.0},
            ],
            market_data=[
                {"asset_id": "bitcoin", "timestamp_unix": base_ts, "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9},
                {"asset_id": "ethereum", "timestamp_unix": base_ts, "price_usd": 2500.0, "market_cap_usd": 300e9, "volume_usd": 20e9},
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # BTC: 0.5 * 100000 = 50000
        # ETH: 20 * 2500 = 50000
        # Total: 100000
        # BTC allocation: 50%
        # ETH allocation: 50%
        
        assert snapshot["derived"]["btc_allocation_pct_by_value"] == 50.0
        
        checks = snapshot["framework"]["checks"]
        assert checks["btc_within_target"] is True  # 50% is within 40-60%
        assert checks["any_position_over_limit"] is True  # ETH at 50% > 20%
        assert checks["pricing_complete"] is True
        
        # Check positions over limit
        over_limit = checks["positions_over_limit"]
        assert len(over_limit) == 1
        assert over_limit[0]["symbol"] == "eth"
        assert over_limit[0]["allocation_pct"] == 50.0
    
    def test_multiple_positions_sum_to_100(self, temp_db_factory):
        """
        Test Case 3: Multiple positions summing to 100%
        
        Expected:
        - total_allocations_sum_to_100 = true
        """
        # Setup: 5 positions with equal value
        base_ts = int(datetime.now().timestamp())
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.1, "avg_cost_basis_usd": 50000.0},
                {"asset_id": "ethereum", "symbol": "eth", "quantity": 4, "avg_cost_basis_usd": 2000.0},
                {"asset_id": "solana", "symbol": "sol", "quantity": 100, "avg_cost_basis_usd": 80.0},
                {"asset_id": "cardano", "symbol": "ada", "quantity": 20000, "avg_cost_basis_usd": 0.4},
                {"asset_id": "avalanche-2", "symbol": "avax", "quantity": 400, "avg_cost_basis_usd": 20.0},
            ],
            market_data=[
                {"asset_id": "bitcoin", "timestamp_unix": base_ts, "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9},
                {"asset_id": "ethereum", "timestamp_unix": base_ts, "price_usd": 2500.0, "market_cap_usd": 300e9, "volume_usd": 20e9},
                {"asset_id": "solana", "timestamp_unix": base_ts, "price_usd": 100.0, "market_cap_usd": 50e9, "volume_usd": 5e9},
                {"asset_id": "cardano", "timestamp_unix": base_ts, "price_usd": 0.5, "market_cap_usd": 20e9, "volume_usd": 1e9},
                {"asset_id": "avalanche-2", "timestamp_unix": base_ts, "price_usd": 25.0, "market_cap_usd": 10e9, "volume_usd": 500e6},
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                {"asset_id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 5},
                {"asset_id": "cardano", "symbol": "ada", "name": "Cardano", "market_cap_rank": 10},
                {"asset_id": "avalanche-2", "symbol": "avax", "name": "Avalanche", "market_cap_rank": 15},
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # BTC: 0.1 * 100000 = 10000
        # ETH: 4 * 2500 = 10000
        # SOL: 100 * 100 = 10000
        # ADA: 20000 * 0.5 = 10000
        # AVAX: 400 * 25 = 10000
        # Total: 50000
        
        total_allocation = sum(
            p["allocation_pct_by_value"] for p in snapshot["positions"]
            if p["allocation_pct_by_value"] is not None
        )
        
        assert abs(total_allocation - 100.0) < 0.5  # Within tolerance
        
        checks = snapshot["framework"]["checks"]
        assert checks["total_allocations_sum_to_100"] is True
        assert checks["pricing_complete"] is True
    
    def test_missing_price_for_one_asset(self, temp_db_factory):
        """
        Test Case 4: Missing price for one asset
        
        Expected:
        - That position's current_price_usd = null
        - allocation_pct_by_value = null for that position
        - meta.data_quality = "partial"
        - framework.checks.pricing_complete = false
        """
        # Setup: BTC has price, ETH does not
        base_ts = int(datetime.now().timestamp())
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.5, "avg_cost_basis_usd": 50000.0},
                {"asset_id": "ethereum", "symbol": "eth", "quantity": 10, "avg_cost_basis_usd": 2000.0},
            ],
            market_data=[
                # Only BTC has market data
                {"asset_id": "bitcoin", "timestamp_unix": base_ts, "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9},
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # Check data quality
        assert snapshot["meta"]["data_quality"] == "partial"
        assert "current_price_usd for ethereum" in snapshot["meta"]["missing_fields"]
        
        # Check ETH position has null price
        eth_pos = next(p for p in snapshot["positions"] if p["asset_id"] == "ethereum")
        assert eth_pos["current_price_usd"] is None
        assert eth_pos["current_value_usd"] is None
        assert eth_pos["allocation_pct_by_value"] is None
        
        # BTC should still have values
        btc_pos = next(p for p in snapshot["positions"] if p["asset_id"] == "bitcoin")
        assert btc_pos["current_price_usd"] == 100000.0
        assert btc_pos["current_value_usd"] == 50000.0
        
        # Check compliance flags
        checks = snapshot["framework"]["checks"]
        assert checks["pricing_complete"] is False
        # Most compliance checks should be null since we can't compute allocations
        assert checks["btc_within_target"] is None
    
    def test_empty_portfolio(self, temp_db_factory):
        """
        Test empty portfolio returns valid structure with warnings.
        """
        temp_db_factory(
            positions=[],  # No positions
            market_data=[],
            asset_metadata=[],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # Should have valid structure
        assert "meta" in snapshot
        assert "portfolio_totals" in snapshot
        assert "positions" in snapshot
        assert "derived" in snapshot
        assert "framework" in snapshot
        
        # Should indicate no positions
        assert len(snapshot["positions"]) == 0
        assert "No open positions" in str(snapshot["meta"].get("warnings", []))
        
        # Totals should be zero
        assert snapshot["portfolio_totals"]["total_cost_basis_usd"] == 0.0
        assert snapshot["portfolio_totals"]["total_current_value_usd"] == 0.0
    
    def test_tier_assignment(self, temp_db_factory):
        """
        Test that tiers are correctly assigned based on market cap rank.
        
        Tier 0: Bitcoin
        Tier 1: rank <= 20
        Tier 2: rank 21-100
        Tier 3: rank > 100
        """
        base_ts = int(datetime.now().timestamp())
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.1, "avg_cost_basis_usd": 50000.0},
                {"asset_id": "ethereum", "symbol": "eth", "quantity": 2, "avg_cost_basis_usd": 2000.0},
                {"asset_id": "arbitrum", "symbol": "arb", "quantity": 1000, "avg_cost_basis_usd": 1.0},
                {"asset_id": "some-token", "symbol": "some", "quantity": 5000, "avg_cost_basis_usd": 0.5},
            ],
            market_data=[
                {"asset_id": "bitcoin", "timestamp_unix": base_ts, "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9},
                {"asset_id": "ethereum", "timestamp_unix": base_ts, "price_usd": 2500.0, "market_cap_usd": 300e9, "volume_usd": 20e9},
                {"asset_id": "arbitrum", "timestamp_unix": base_ts, "price_usd": 1.2, "market_cap_usd": 5e9, "volume_usd": 500e6},
                {"asset_id": "some-token", "timestamp_unix": base_ts, "price_usd": 0.6, "market_cap_usd": 100e6, "volume_usd": 10e6},
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},  # Tier 1 (rank <= 20)
                {"asset_id": "arbitrum", "symbol": "arb", "name": "Arbitrum", "market_cap_rank": 50},  # Tier 2 (21-100)
                {"asset_id": "some-token", "symbol": "some", "name": "Some Token", "market_cap_rank": 150},  # Tier 3 (> 100)
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # Check tier assignments
        tiers = {p["asset_id"]: p["tier"] for p in snapshot["positions"]}
        
        assert tiers["bitcoin"] == 0  # Always Tier 0
        assert tiers["ethereum"] == 1  # Rank 2 -> Tier 1
        assert tiers["arbitrum"] == 2  # Rank 50 -> Tier 2
        assert tiers["some-token"] == 3  # Rank 150 -> Tier 3
    
    def test_tier2_3_allocation_computation(self, temp_db_factory):
        """
        Test that tier2_3_allocation_pct_by_value is correctly computed.
        """
        base_ts = int(datetime.now().timestamp())
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.5, "avg_cost_basis_usd": 50000.0},  # Tier 0
                {"asset_id": "ethereum", "symbol": "eth", "quantity": 10, "avg_cost_basis_usd": 2000.0},  # Tier 1
                {"asset_id": "arbitrum", "symbol": "arb", "quantity": 5000, "avg_cost_basis_usd": 1.0},  # Tier 2
            ],
            market_data=[
                {"asset_id": "bitcoin", "timestamp_unix": base_ts, "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9},
                {"asset_id": "ethereum", "timestamp_unix": base_ts, "price_usd": 2500.0, "market_cap_usd": 300e9, "volume_usd": 20e9},
                {"asset_id": "arbitrum", "timestamp_unix": base_ts, "price_usd": 1.0, "market_cap_usd": 5e9, "volume_usd": 500e6},
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
                {"asset_id": "arbitrum", "symbol": "arb", "name": "Arbitrum", "market_cap_rank": 50},  # Tier 2
            ],
        )
        
        from agents.tools.portfolio_tools import _build_portfolio_snapshot
        
        snapshot = _build_portfolio_snapshot()
        
        # BTC: 0.5 * 100000 = 50000 (Tier 0)
        # ETH: 10 * 2500 = 25000 (Tier 1)
        # ARB: 5000 * 1.0 = 5000 (Tier 2)
        # Total: 80000
        # ARB allocation: 5000 / 80000 * 100 = 6.25%
        
        tier2_3_alloc = snapshot["derived"]["tier2_3_allocation_pct_by_value"]
        assert abs(tier2_3_alloc - 6.25) < 0.1  # Allow small rounding difference
        
        checks = snapshot["framework"]["checks"]
        assert checks["tier2_3_within_limit"] is True  # 6.25% < 35%
    
    def test_get_portfolio_snapshot_returns_json_string(self, temp_db_factory):
        """
        Test that the tool returns a valid JSON string.
        """
        temp_db_factory(
            positions=[
                {"asset_id": "bitcoin", "symbol": "btc", "quantity": 0.5, "avg_cost_basis_usd": 50000.0}
            ],
            market_data=[
                {"asset_id": "bitcoin", "price_usd": 100000.0, "market_cap_usd": 2e12, "volume_usd": 50e9}
            ],
            asset_metadata=[
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1}
            ],
        )
        
        from agents.tools.portfolio_tools import get_portfolio_snapshot
        
        # The tool is decorated, so we need to call it correctly
        result = get_portfolio_snapshot.run()
        
        # Should be valid JSON
        parsed = json.loads(result)
        
        # Should have all required keys
        assert "meta" in parsed
        assert "portfolio_totals" in parsed
        assert "positions" in parsed
        assert "derived" in parsed
        assert "framework" in parsed


class TestPortfolioJsonValidation:
    """Tests for the JSON validation helper in crew.py."""
    
    def test_validate_portfolio_json_success(self):
        """Test successful validation of valid JSON."""
        from agents.crew import validate_portfolio_json
        
        valid_json = json.dumps({
            "meta": {"data_quality": "ok"},
            "portfolio_totals": {"total_cost_basis_usd": 1000},
            "positions": [],
            "derived": {"btc_allocation_pct_by_value": 0},
            "framework": {
                "config": {},
                "checks": {"btc_within_target": None}
            }
        })
        
        result = validate_portfolio_json(valid_json)
        assert result["meta"]["data_quality"] == "ok"
    
    def test_validate_portfolio_json_strips_markdown(self):
        """Test that markdown code fences are stripped."""
        from agents.crew import validate_portfolio_json
        
        json_with_fences = """```json
{
    "meta": {"data_quality": "ok"},
    "portfolio_totals": {},
    "positions": [],
    "derived": {},
    "framework": {"checks": {}}
}
```"""
        
        result = validate_portfolio_json(json_with_fences)
        assert result["meta"]["data_quality"] == "ok"
    
    def test_validate_portfolio_json_invalid(self):
        """Test that invalid JSON raises ValueError."""
        from agents.crew import validate_portfolio_json
        
        with pytest.raises(ValueError, match="not valid JSON"):
            validate_portfolio_json("this is not json")
    
    def test_validate_portfolio_json_missing_keys(self):
        """Test that missing required keys raises ValueError."""
        from agents.crew import validate_portfolio_json
        
        incomplete_json = json.dumps({
            "meta": {},
            "positions": [],
            # Missing: portfolio_totals, derived, framework
        })
        
        with pytest.raises(ValueError, match="missing required keys"):
            validate_portfolio_json(incomplete_json)
