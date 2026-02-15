"""
Portfolio tools for CrewAI agents.

Wraps PortfolioStore read-only functions as CrewAI tools for safe agent access
to portfolio state, positions, trade history, and P&L data.
"""

import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from crewai.tools import tool

from source.portfolio_store import PortfolioStore
from agents.utils.db_connection import get_db_connection


# =============================================================================
# Framework Configuration Constants
# =============================================================================

FRAMEWORK_CONFIG = {
    "btc_target_min_pct": 40,
    "btc_target_max_pct": 60,
    "single_asset_limit_pct": 20,
    "tier2_3_max_pct": 35,
    "allow_100pct_btc_if_no_alts": False,
}

# Tolerance for floating point comparisons (0.5%)
ALLOCATION_SUM_TOLERANCE = 0.5


# Global store instance - initialized lazily
_portfolio_store: Optional[PortfolioStore] = None


def _get_store() -> PortfolioStore:
    """Get or create the portfolio store instance using the shared connection."""
    global _portfolio_store
    if _portfolio_store is None:
        # Use the shared connection to avoid connection conflicts
        conn = get_db_connection()
        _portfolio_store = PortfolioStore(connection=conn)
    return _portfolio_store


def close_store():
    """Close the portfolio store (connection managed centrally)."""
    global _portfolio_store
    if _portfolio_store is not None:
        # Note: close() won't close the shared connection since we passed it externally
        _portfolio_store.close()
        _portfolio_store = None


@tool
def get_open_positions() -> str:
    """
    Get all current open positions in the portfolio.
    
    Returns a list of positions with quantity > 0, ordered by quantity descending.
    Each position includes: asset_id, symbol, quantity, avg_cost_basis_usd,
    market_value_usd, unrealized_pnl_usd, opened_at, last_updated_at.
    
    Use this to understand what assets are currently held in the portfolio.
    """
    store = _get_store()
    positions = store.get_open_positions()
    
    if not positions:
        return "No open positions in the portfolio."
    
    result_lines = ["Current Open Positions:", "=" * 50]
    for pos in positions:
        result_lines.append(
            f"- {pos['symbol'].upper()} ({pos['asset_id']}): "
            f"{pos['quantity']:.6f} units @ avg cost ${pos['avg_cost_basis_usd']:.2f}"
        )
    
    return "\n".join(result_lines)


@tool
def get_position(asset_id: str) -> str:
    """
    Get details for a specific position by asset ID.
    
    Args:
        asset_id: The CoinGecko asset identifier (e.g., "bitcoin", "ethereum")
    
    Returns position details including quantity, average cost basis, and P&L info.
    Returns "No position found" if the asset is not in the portfolio.
    """
    store = _get_store()
    position = store.get_position(asset_id)
    
    if position is None:
        return f"No position found for asset_id '{asset_id}'"
    
    return (
        f"Position for {position['symbol'].upper()} ({asset_id}):\n"
        f"  Quantity: {position['quantity']:.6f}\n"
        f"  Avg Cost Basis: ${position['avg_cost_basis_usd']:.2f}\n"
        f"  Market Value: {position['market_value_usd'] or 'Not calculated'}\n"
        f"  Unrealized P&L: {position['unrealized_pnl_usd'] or 'Not calculated'}\n"
        f"  Opened: {position['opened_at']}\n"
        f"  Last Updated: {position['last_updated_at']}"
    )


@tool
def get_trade_history(asset_id: Optional[str] = None) -> str:
    """
    Get trade history, optionally filtered by asset.
    
    Args:
        asset_id: Optional asset filter. If None, returns all trades.
    
    Returns the trade ledger ordered by execution time (most recent first).
    Each trade includes: side (BUY/SELL), quantity, price, value, fees, and realized P&L.
    """
    store = _get_store()
    trades = store.get_trade_history(asset_id)
    
    if not trades:
        filter_msg = f" for {asset_id}" if asset_id else ""
        return f"No trades found{filter_msg}."
    
    result_lines = [f"Trade History{' for ' + asset_id if asset_id else ''}:", "=" * 60]
    
    for trade in trades[:20]:  # Limit to 20 most recent
        pnl_str = f", P&L: ${trade['realized_pnl_usd']:.2f}" if trade['realized_pnl_usd'] else ""
        result_lines.append(
            f"- [{trade['executed_at']}] {trade['side']} {trade['quantity']:.6f} "
            f"{trade['symbol'].upper()} @ ${trade['price_usd']:.2f} "
            f"(Value: ${trade['trade_value_usd']:.2f}{pnl_str})"
        )
    
    if len(trades) > 20:
        result_lines.append(f"... and {len(trades) - 20} more trades")
    
    return "\n".join(result_lines)


@tool
def get_realized_pnl_summary() -> str:
    """
    Get aggregated realized P&L statistics across all trades.
    
    Returns total realized P&L, trade counts (buys/sells), total fees,
    and a per-asset breakdown of realized P&L.
    
    Use this to understand the portfolio's historical trading performance.
    """
    store = _get_store()
    summary = store.get_realized_pnl_summary()
    
    result_lines = [
        "Realized P&L Summary",
        "=" * 50,
        f"Total Realized P&L: ${summary['total_realized_pnl_usd']:.2f}",
        f"Total Trades: {summary['total_trades']} ({summary['total_buys']} buys, {summary['total_sells']} sells)",
        f"Total Fees Paid: ${summary['total_fees_usd']:.2f}",
        "",
        "Per-Asset Breakdown:"
    ]
    
    for asset_id, stats in summary.get('by_asset', {}).items():
        result_lines.append(
            f"  {asset_id}: P&L ${stats['realized_pnl_usd']:.2f} "
            f"({stats['trade_count']} trades)"
        )
    
    return "\n".join(result_lines)


@tool
def get_portfolio_summary() -> str:
    """
    Get high-level portfolio summary statistics.
    
    Returns total number of positions, total cost basis, total realized P&L,
    and total trade count. Use this for a quick overview of portfolio state.
    """
    store = _get_store()
    summary = store.get_portfolio_summary()
    
    return (
        "Portfolio Summary\n"
        "=" * 50 + "\n"
        f"Total Open Positions: {summary['total_positions']}\n"
        f"Total Cost Basis: ${summary['total_cost_basis_usd']:.2f}\n"
        f"Total Realized P&L: ${summary['total_realized_pnl_usd']:.2f}\n"
        f"Total Trades Recorded: {summary['total_trades']}"
    )


# =============================================================================
# Deterministic Portfolio Snapshot Tool
# =============================================================================

def _get_current_price(asset_id: str) -> Optional[float]:
    """
    Get the most recent price for an asset from the market_data table.
    
    Args:
        asset_id: CoinGecko asset identifier
    
    Returns:
        Current price in USD, or None if not available
    """
    conn = get_db_connection()
    try:
        result = conn.execute("""
            SELECT price_usd
            FROM market_data
            WHERE asset_id = ?
            ORDER BY timestamp_unix DESC
            LIMIT 1
        """, [asset_id]).fetchone()
        
        if result and result[0] is not None:
            return float(result[0])
        return None
    except Exception:
        return None


def _get_market_cap_rank(asset_id: str) -> Optional[int]:
    """
    Get the market cap rank for an asset from asset_metadata table.
    
    Args:
        asset_id: CoinGecko asset identifier
    
    Returns:
        Market cap rank, or None if not available
    """
    conn = get_db_connection()
    try:
        result = conn.execute("""
            SELECT market_cap_rank
            FROM asset_metadata
            WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        if result and result[0] is not None:
            return int(result[0])
        return None
    except Exception:
        return None


def _get_tier_for_asset(asset_id: str, market_cap_rank: Optional[int] = None) -> Optional[int]:
    """
    Determine the tier classification for an asset.
    
    Tier 0: Bitcoin (baseline)
    Tier 1: Large-cap infrastructure (market_cap_rank <= 20)
    Tier 2: Emerging themes (market_cap_rank 21-100)
    Tier 3: Tactical opportunities (market_cap_rank > 100)
    
    Args:
        asset_id: CoinGecko asset identifier
        market_cap_rank: Optional market cap rank (fetched if not provided)
    
    Returns:
        Tier (0, 1, 2, or 3), or None if cannot be determined
    """
    # Bitcoin is always Tier 0
    if asset_id == "bitcoin":
        return 0
    
    # Fetch market cap rank if not provided
    if market_cap_rank is None:
        market_cap_rank = _get_market_cap_rank(asset_id)
    
    if market_cap_rank is None:
        return None  # Cannot determine tier without rank
    
    if market_cap_rank <= 20:
        return 1
    elif market_cap_rank <= 100:
        return 2
    else:
        return 3


def _compute_framework_checks(
    positions: List[Dict[str, Any]],
    derived: Dict[str, Any],
    config: Dict[str, Any],
    pricing_complete: bool,
) -> Dict[str, Any]:
    """
    Compute all framework compliance checks deterministically.
    
    Args:
        positions: List of position dictionaries with allocation data
        derived: Derived metrics dictionary
        config: Framework configuration
        pricing_complete: Whether all positions have prices
    
    Returns:
        Dictionary with all compliance check results
    """
    checks = {
        "btc_within_target": None,
        "any_position_over_limit": None,
        "positions_over_limit": [],
        "tier2_3_within_limit": None,
        "total_allocations_sum_to_100": None,
        "pricing_complete": pricing_complete,
        "contradictions_detected": False,
        "contradictions": [],
    }
    
    # Cannot compute allocation-based checks without complete pricing
    if not pricing_complete:
        return checks
    
    btc_pct = derived.get("btc_allocation_pct_by_value", 0) or 0
    tier2_3_pct = derived.get("tier2_3_allocation_pct_by_value", 0) or 0
    
    # Check BTC allocation
    btc_min = config["btc_target_min_pct"]
    btc_max = config["btc_target_max_pct"]
    
    # Special case: 100% BTC when no alts
    if config.get("allow_100pct_btc_if_no_alts", False):
        non_btc_positions = [p for p in positions if p["asset_id"] != "bitcoin"]
        if not non_btc_positions and btc_pct == 100:
            checks["btc_within_target"] = True
        else:
            checks["btc_within_target"] = btc_min <= btc_pct <= btc_max
    else:
        checks["btc_within_target"] = btc_min <= btc_pct <= btc_max
    
    # Check single asset limit (applies to non-BTC assets)
    single_limit = config["single_asset_limit_pct"]
    positions_over = []
    
    for pos in positions:
        alloc = pos.get("allocation_pct_by_value")
        if alloc is not None and pos["asset_id"] != "bitcoin":
            if alloc > single_limit:
                positions_over.append({
                    "symbol": pos["symbol"],
                    "allocation_pct": round(alloc, 2),
                })
    
    checks["any_position_over_limit"] = len(positions_over) > 0
    checks["positions_over_limit"] = positions_over
    
    # Check Tier 2+3 allocation
    tier2_3_max = config["tier2_3_max_pct"]
    checks["tier2_3_within_limit"] = tier2_3_pct <= tier2_3_max
    
    # Check allocations sum to 100%
    total_alloc = sum(
        p.get("allocation_pct_by_value", 0) or 0
        for p in positions
    )
    checks["total_allocations_sum_to_100"] = abs(total_alloc - 100) < ALLOCATION_SUM_TOLERANCE
    
    return checks


def _detect_contradictions(
    positions: List[Dict[str, Any]],
    totals: Dict[str, Any],
    derived: Dict[str, Any],
) -> List[str]:
    """
    Detect data contradictions that indicate computation errors.
    
    Args:
        positions: List of position dictionaries
        totals: Portfolio totals dictionary
        derived: Derived metrics dictionary
    
    Returns:
        List of contradiction descriptions (empty if none found)
    """
    contradictions = []
    tolerance = 0.01  # $0.01 tolerance for floating point
    
    # Check 1: Sum of position cost basis vs total
    sum_cost_basis = sum(
        p.get("total_cost_basis_usd", 0) or 0
        for p in positions
    )
    total_cost = totals.get("total_cost_basis_usd", 0) or 0
    
    if abs(sum_cost_basis - total_cost) > tolerance:
        contradictions.append(
            f"Cost basis mismatch: sum of positions (${sum_cost_basis:.2f}) "
            f"!= total (${total_cost:.2f})"
        )
    
    # Check 2: Sum of position current values vs total (if pricing complete)
    sum_current_value = sum(
        p.get("current_value_usd", 0) or 0
        for p in positions
        if p.get("current_value_usd") is not None
    )
    total_value = totals.get("total_current_value_usd")
    
    if total_value is not None and sum_current_value > 0:
        if abs(sum_current_value - total_value) > tolerance:
            contradictions.append(
                f"Current value mismatch: sum of positions (${sum_current_value:.2f}) "
                f"!= total (${total_value:.2f})"
            )
    
    # Check 3: BTC position allocation vs derived btc_allocation
    btc_positions = [p for p in positions if p["asset_id"] == "bitcoin"]
    if btc_positions:
        btc_pos = btc_positions[0]
        pos_btc_alloc = btc_pos.get("allocation_pct_by_value")
        derived_btc_alloc = derived.get("btc_allocation_pct_by_value")
        
        if pos_btc_alloc is not None and derived_btc_alloc is not None:
            if abs(pos_btc_alloc - derived_btc_alloc) > 0.1:  # 0.1% tolerance
                contradictions.append(
                    f"BTC allocation mismatch: position says {pos_btc_alloc:.2f}% "
                    f"but derived says {derived_btc_alloc:.2f}%"
                )
    
    return contradictions


def _build_portfolio_snapshot() -> Dict[str, Any]:
    """
    Build the complete portfolio snapshot with all computed fields.
    
    This is the internal implementation that returns a dictionary.
    
    Returns:
        Complete portfolio snapshot dictionary
    """
    store = _get_store()
    
    # Initialize meta with required schema fields
    meta = {
        "agent_name": "portfolio_context",
        "schema_version": "1.0",
        "as_of_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "pricing_source": "market_data_table",
        "data_quality": "ok",
        "missing_fields": [],
        "warnings": [],
    }
    
    # Get all open positions
    raw_positions = store.get_open_positions()
    
    if not raw_positions:
        # Empty portfolio - include all required meta fields
        return {
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": meta["as_of_timestamp_utc"],
                "data_quality": "ok",
                "warnings": ["No open positions in portfolio"],
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 0.0,
                "total_current_value_usd": 0.0,
                "total_realized_pnl_usd": 0.0,
                "drawdown_from_peak_pct": None,
            },
            "positions": [],
            "derived": {
                "btc_quantity": 0.0,
                "btc_allocation_pct_by_value": 0.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": None,
            },
            "framework": {
                "config": FRAMEWORK_CONFIG.copy(),
                "checks": {
                    "btc_within_target": None,
                    "any_position_over_limit": None,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": None,
                    "total_allocations_sum_to_100": None,
                    "pricing_complete": False,
                    "contradictions_detected": False,
                    "contradictions": [],
                },
            },
        }
    
    # Build enriched positions with current prices
    positions = []
    total_cost_basis = 0.0
    total_current_value = 0.0
    pricing_complete = True
    missing_prices = []
    
    for raw_pos in raw_positions:
        asset_id = raw_pos["asset_id"]
        symbol = raw_pos["symbol"]
        quantity = raw_pos["quantity"]
        avg_cost = raw_pos["avg_cost_basis_usd"]
        cost_basis = quantity * avg_cost
        
        # Fetch current price
        current_price = _get_current_price(asset_id)
        
        # Fetch market cap rank and determine tier
        market_cap_rank = _get_market_cap_rank(asset_id)
        tier = _get_tier_for_asset(asset_id, market_cap_rank)
        
        # Compute current value and P&L
        current_value = None
        unrealized_pnl = None
        unrealized_pnl_pct = None
        
        if current_price is not None:
            current_value = quantity * current_price
            total_current_value += current_value
            unrealized_pnl = current_value - cost_basis
            if cost_basis > 0:
                unrealized_pnl_pct = (unrealized_pnl / cost_basis) * 100
        else:
            pricing_complete = False
            missing_prices.append(asset_id)
        
        total_cost_basis += cost_basis
        
        positions.append({
            "symbol": symbol,
            "asset_id": asset_id,
            "quantity": quantity,
            "avg_cost_usd": avg_cost,
            "total_cost_basis_usd": round(cost_basis, 2),
            "current_price_usd": round(current_price, 2) if current_price else None,
            "current_value_usd": round(current_value, 2) if current_value else None,
            "allocation_pct_by_value": None,  # Computed below
            "unrealized_pnl_usd": round(unrealized_pnl, 2) if unrealized_pnl else None,
            "unrealized_pnl_pct": round(unrealized_pnl_pct, 2) if unrealized_pnl_pct else None,
            "tier": tier,
        })
    
    # Compute allocation percentages (if we have total value)
    if total_current_value > 0:
        for pos in positions:
            if pos["current_value_usd"] is not None:
                pos["allocation_pct_by_value"] = round(
                    (pos["current_value_usd"] / total_current_value) * 100, 2
                )
    
    # Compute derived metrics
    btc_quantity = 0.0
    btc_allocation = 0.0
    tier2_3_allocation = 0.0
    max_allocation = 0.0
    max_allocation_symbol = None
    
    for pos in positions:
        if pos["asset_id"] == "bitcoin":
            btc_quantity = pos["quantity"]
            btc_allocation = pos["allocation_pct_by_value"] or 0
        
        tier = pos.get("tier")
        alloc = pos.get("allocation_pct_by_value") or 0
        
        if tier is not None and tier >= 2:
            tier2_3_allocation += alloc
        
        if alloc > max_allocation:
            max_allocation = alloc
            max_allocation_symbol = pos["symbol"]
    
    derived = {
        "btc_quantity": btc_quantity,
        "btc_allocation_pct_by_value": round(btc_allocation, 2),
        "tier2_3_allocation_pct_by_value": round(tier2_3_allocation, 2),
        "max_single_asset_allocation_pct_by_value": round(max_allocation, 2),
        "max_single_asset_symbol": max_allocation_symbol,
    }
    
    # Get realized P&L
    pnl_summary = store.get_realized_pnl_summary()
    total_realized_pnl = pnl_summary.get("total_realized_pnl_usd", 0.0)
    
    # Build totals
    portfolio_totals = {
        "total_cost_basis_usd": round(total_cost_basis, 2),
        "total_current_value_usd": round(total_current_value, 2) if pricing_complete else None,
        "total_realized_pnl_usd": round(total_realized_pnl, 2),
        "drawdown_from_peak_pct": None,  # Would need historical tracking
    }
    
    # Compute framework checks
    checks = _compute_framework_checks(
        positions=positions,
        derived=derived,
        config=FRAMEWORK_CONFIG,
        pricing_complete=pricing_complete,
    )
    
    # Detect contradictions
    contradictions = _detect_contradictions(positions, portfolio_totals, derived)
    
    if contradictions:
        checks["contradictions_detected"] = True
        checks["contradictions"] = contradictions
        meta["data_quality"] = "invalid"
    elif not pricing_complete:
        meta["data_quality"] = "partial"
        meta["missing_fields"] = [f"current_price_usd for {aid}" for aid in missing_prices]
    
    return {
        "meta": meta,
        "portfolio_totals": portfolio_totals,
        "positions": positions,
        "derived": derived,
        "framework": {
            "config": FRAMEWORK_CONFIG.copy(),
            "checks": checks,
        },
    }


@tool
def get_portfolio_snapshot() -> str:
    """
    Get a complete, deterministic portfolio snapshot with compliance checks.
    
    Returns a JSON object containing:
    - meta: Timestamp, data quality status, warnings
    - portfolio_totals: Total cost basis, current value, realized P&L
    - positions: Array of positions with prices, values, allocations, tiers
    - derived: BTC allocation %, Tier 2+3 allocation %, max single asset
    - framework: Config values and compliance check booleans
    
    All compliance checks are computed deterministically in code.
    The output is strict JSON suitable for downstream processing.
    
    Use this tool when you need the complete portfolio state with
    framework compliance validation.
    """
    snapshot = _build_portfolio_snapshot()
    return json.dumps(snapshot, indent=2)
