"""
Market data tools for CrewAI agents.

Provides read-only access to historical price data, market caps, and volumes
from the DuckDB market_data table.

DATA GRANULARITY:
- Historical data (Jan 2024 - Jan 2026): DAILY intervals (one data point per day)
- Recent/ongoing data (Jan 2026 onward): 5-MINUTE intervals (high frequency)

Functions in this module detect and report the actual data granularity to help
agents interpret the data correctly. When querying across time periods, you may
see mixed granularities in the same result set.
"""

from typing import Tuple, List
from datetime import datetime, timedelta
import duckdb
from crewai.tools import tool

from agents.utils.db_connection import get_db_connection


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Get the shared DuckDB connection."""
    return get_db_connection()


def _resolve_asset_id(identifier: str) -> Tuple[str, str]:
    """
    Resolve a symbol or asset_id to the canonical CoinGecko asset_id.
    
    The database stores:
    - asset_id: CoinGecko ID (e.g., "ripple", "bitcoin", "ethereum")
    - symbol: Trading symbol (e.g., "xrp", "btc", "eth")
    
    This function accepts either and returns the correct asset_id.
    
    Args:
        identifier: Either a symbol (like "XRP") or asset_id (like "ripple")
    
    Returns:
        Tuple of (resolved_asset_id, error_message)
        If successful, error_message is empty string
        If failed, resolved_asset_id is the original identifier
    """
    conn = _get_conn()
    identifier_lower = identifier.lower().strip()
    
    # First, try exact match on asset_id
    result = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(asset_id) = ?
        LIMIT 1
    """, [identifier_lower]).fetchone()
    
    if result:
        return result[0], ""
    
    # Try exact match on symbol
    result = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(symbol) = ?
        LIMIT 1
    """, [identifier_lower]).fetchone()
    
    if result:
        return result[0], ""
    
    # Try partial match on name (for cases like "Bitcoin" -> "bitcoin")
    result = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(name) = ?
        LIMIT 1
    """, [identifier_lower]).fetchone()
    
    if result:
        return result[0], ""
    
    # Return original identifier with error message listing available assets
    similar = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(symbol) LIKE ? OR LOWER(asset_id) LIKE ? OR LOWER(name) LIKE ?
        ORDER BY market_cap_rank ASC NULLS LAST
        LIMIT 5
    """, [f"%{identifier_lower}%", f"%{identifier_lower}%", f"%{identifier_lower}%"]).fetchall()
    
    if similar:
        suggestions = [f"{r[1].upper()} (asset_id: {r[0]})" for r in similar]
        error_msg = f"Could not find asset '{identifier}'. Did you mean: {', '.join(suggestions)}?"
    else:
        error_msg = f"Could not find asset '{identifier}' in database. Use get_market_cap_rankings() to see available assets."
    
    return identifier, error_msg


def _detect_time_granularity(timestamps: List[int]) -> Tuple[str, int]:
    """
    Detect the predominant time interval in a series of timestamps.
    
    Args:
        timestamps: List of unix timestamps (should be sorted ascending)
    
    Returns:
        Tuple of (granularity_description, median_interval_seconds)
        granularity_description is one of: "5-minute", "hourly", "daily", "mixed"
    """
    if len(timestamps) < 2:
        return ("unknown", 0)
    
    # Calculate intervals between consecutive timestamps
    intervals = []
    sorted_ts = sorted(timestamps)
    for i in range(1, len(sorted_ts)):
        intervals.append(sorted_ts[i] - sorted_ts[i-1])
    
    if not intervals:
        return ("unknown", 0)
    
    # Get median interval to be robust against gaps
    intervals.sort()
    median_interval = intervals[len(intervals) // 2]
    
    # Classify based on median interval
    # 5-minute = 300 seconds, hourly = 3600, daily = 86400
    if median_interval < 600:  # < 10 minutes
        return ("5-minute", median_interval)
    elif median_interval < 7200:  # < 2 hours
        return ("hourly", median_interval)
    elif median_interval < 172800:  # < 2 days
        return ("daily", median_interval)
    else:
        return ("sparse", median_interval)


def _format_granularity_note(granularity: str, interval_secs: int, data_points: int) -> str:
    """Format a note explaining the data granularity for agent consumption."""
    if granularity == "5-minute":
        return (f"DATA GRANULARITY: 5-minute intervals ({data_points} data points). "
                f"This is recent high-frequency data collected from Jan 2026 onward.")
    elif granularity == "hourly":
        return (f"DATA GRANULARITY: ~hourly intervals ({data_points} data points). "
                f"This may indicate a transition zone between daily historical and 5-minute recent data.")
    elif granularity == "daily":
        return (f"DATA GRANULARITY: Daily intervals ({data_points} data points). "
                f"This is historical data from Jan 2024 - Jan 2026.")
    elif granularity == "sparse":
        return (f"DATA GRANULARITY: Sparse data with gaps ({data_points} data points, "
                f"~{interval_secs//86400:.1f} days between points on average).")
    else:
        return f"DATA GRANULARITY: Could not be determined ({data_points} data points)."


@tool
def get_price_history(asset_id: str, days: int = 30) -> str:
    """
    Get historical price data for an asset.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "bitcoin", "ripple") 
                  OR trading symbol (e.g., "BTC", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
        days: Number of days of history to retrieve (default 30, max 365)
    
    Returns price history with timestamps, prices, volumes, and market caps.
    
    DATA GRANULARITY: The database contains daily data for Jan 2024 - Jan 2026,
    and 5-minute data from Jan 2026 onward. The response indicates which
    granularity is present in the returned data.
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    conn = _get_conn()
    days = min(days, 365)  # Cap at 365 days
    
    # Calculate cutoff timestamp
    cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    
    result = conn.execute("""
        SELECT timestamp_unix, price_usd, volume_usd, market_cap_usd
        FROM market_data
        WHERE asset_id = ? AND timestamp_unix >= ?
        ORDER BY timestamp_unix DESC
        LIMIT 100
    """, [asset_id, cutoff_ts]).fetchall()
    
    if not result:
        return f"No price data found for {asset_id} in the last {days} days."
    
    # Detect time granularity
    timestamps = [r[0] for r in result]
    granularity, interval_secs = _detect_time_granularity(timestamps)
    granularity_note = _format_granularity_note(granularity, interval_secs, len(result))
    
    lines = [f"Price History for {asset_id} (last {days} days):", "=" * 60]
    
    # Add granularity information prominently
    lines.append(granularity_note)
    lines.append("")
    
    # Show summary stats
    prices = [r[1] for r in result if r[1]]
    if prices:
        lines.append(f"Current Price: ${prices[0]:.2f}")
        lines.append(f"High (in this sample): ${max(prices):.2f}")
        lines.append(f"Low (in this sample): ${min(prices):.2f}")
        lines.append(f"Avg (in this sample): ${sum(prices)/len(prices):.2f}")
        lines.append(f"Data Points Retrieved: {len(result)}")
        lines.append("")
    
    # Show recent data points with clearer time formatting
    lines.append("Recent Data (last 10 points):")
    for row in result[:10]:
        ts = datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d %H:%M")
        price = row[1] or 0
        volume = row[2] or 0
        lines.append(f"  {ts}: ${price:.2f} (Vol: ${volume:,.0f})")
    
    return "\n".join(lines)


@tool
def get_btc_relative_price(asset_id: str, days: int = 30) -> str:
    """
    Get price performance of an asset relative to Bitcoin.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "ethereum", "ripple") 
                  OR trading symbol (e.g., "ETH", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
        days: Number of days of history to analyze (default 30)
    
    Returns the asset's price in BTC terms (ASSET/BTC ratio) over time,
    showing whether the asset is outperforming or underperforming Bitcoin.
    
    This analysis uses data points where both the asset and BTC have prices
    at the SAME timestamp. Historical data (pre-2026) is daily; recent data
    is at 5-minute intervals.
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    conn = _get_conn()
    days = min(days, 365)
    cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    
    # Get both asset and BTC prices
    result = conn.execute("""
        SELECT 
            a.timestamp_unix,
            a.price_usd as asset_price,
            b.price_usd as btc_price
        FROM market_data a
        JOIN market_data b ON a.timestamp_unix = b.timestamp_unix AND b.asset_id = 'bitcoin'
        WHERE a.asset_id = ? AND a.timestamp_unix >= ?
        ORDER BY a.timestamp_unix DESC
        LIMIT 100
    """, [asset_id, cutoff_ts]).fetchall()
    
    if not result:
        return f"No data found for {asset_id} relative to BTC. Note: This requires matching timestamps between {asset_id} and bitcoin data."
    
    # Detect time granularity
    timestamps = [r[0] for r in result]
    granularity, interval_secs = _detect_time_granularity(timestamps)
    
    lines = [f"BTC-Relative Performance for {asset_id} ({days} days):", "=" * 60]
    
    # Add data interval note
    if granularity == "5-minute":
        lines.append(f"Data Interval: ~5 minutes ({len(result)} matched data points)")
    elif granularity == "hourly":
        lines.append(f"Data Interval: ~hourly ({len(result)} matched data points)")
    elif granularity == "daily":
        lines.append(f"Data Interval: ~daily ({len(result)} matched data points)")
    else:
        lines.append(f"Data Interval: {granularity} ({len(result)} matched data points)")
    lines.append("")
    
    # Calculate BTC ratios
    ratios = []
    for row in result:
        if row[1] and row[2] and row[2] > 0:
            ratio = row[1] / row[2]
            ratios.append((row[0], ratio))
    
    if not ratios:
        return f"Unable to calculate BTC ratio for {asset_id}."
    
    current_ratio = ratios[0][1]
    oldest_ratio = ratios[-1][1]
    pct_change = ((current_ratio - oldest_ratio) / oldest_ratio) * 100 if oldest_ratio else 0
    
    lines.append(f"Current {asset_id}/BTC: {current_ratio:.8f}")
    lines.append(f"Period Start {asset_id}/BTC: {oldest_ratio:.8f}")
    lines.append(f"Relative Change: {pct_change:+.2f}%")
    lines.append("")
    
    if pct_change > 5:
        lines.append(f"Assessment: {asset_id} is OUTPERFORMING Bitcoin over this period.")
    elif pct_change < -5:
        lines.append(f"Assessment: {asset_id} is UNDERPERFORMING Bitcoin over this period.")
    else:
        lines.append(f"Assessment: {asset_id} is tracking Bitcoin closely.")
    
    return "\n".join(lines)


@tool
def get_market_cap_rankings(limit: int = 50) -> str:
    """
    Get current market cap rankings for tracked assets.
    
    Args:
        limit: Number of top assets to return (default 50, max 200)
    
    Returns a list of assets ranked by market cap with their current prices.
    Use this to understand the market landscape and identify major assets.
    """
    conn = _get_conn()
    limit = min(limit, 200)
    
    result = conn.execute("""
        SELECT 
            am.asset_id,
            am.symbol,
            am.name,
            am.market_cap_rank,
            md.price_usd,
            md.market_cap_usd
        FROM asset_metadata am
        LEFT JOIN (
            SELECT asset_id, price_usd, market_cap_usd,
                   ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY timestamp_unix DESC) as rn
            FROM market_data
        ) md ON am.asset_id = md.asset_id AND md.rn = 1
        WHERE am.market_cap_rank IS NOT NULL
        ORDER BY am.market_cap_rank ASC
        LIMIT ?
    """, [limit]).fetchall()
    
    if not result:
        return "No market cap data available."
    
    lines = [f"Top {limit} Assets by Market Cap:", "=" * 70]
    
    for row in result:
        rank = row[3] or "?"
        symbol = (row[1] or "").upper()
        name = row[2] or row[0]
        price = row[4]
        mcap = row[5]
        
        price_str = f"${price:,.2f}" if price else "N/A"
        mcap_str = f"${mcap/1e9:.2f}B" if mcap else "N/A"
        
        lines.append(f"#{rank:3} {symbol:8} {name[:20]:20} Price: {price_str:>12} MCap: {mcap_str:>10}")
    
    return "\n".join(lines)


@tool
def get_price_change(asset_id: str, days: int = 7) -> str:
    """
    Get price change statistics for an asset over a specified period.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "bitcoin", "ripple") 
                  OR trading symbol (e.g., "BTC", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
        days: Number of days to analyze (default 7)
    
    Returns price change percentage, high, low, and volatility metrics.
    
    Note: Recent data (Jan 2026+) is at 5-minute intervals, so high/low values
    capture intraday extremes. Historical data (pre-2026) is daily, so high/low
    only reflects daily closing prices.
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    conn = _get_conn()
    cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    
    # First get the count and sample timestamps to understand data granularity
    sample_result = conn.execute("""
        SELECT timestamp_unix
        FROM market_data
        WHERE asset_id = ? AND timestamp_unix >= ?
        ORDER BY timestamp_unix ASC
        LIMIT 100
    """, [asset_id, cutoff_ts]).fetchall()
    
    result = conn.execute("""
        SELECT 
            MIN(price_usd) as low,
            MAX(price_usd) as high,
            (SELECT price_usd FROM market_data 
             WHERE asset_id = ? ORDER BY timestamp_unix DESC LIMIT 1) as current,
            (SELECT price_usd FROM market_data 
             WHERE asset_id = ? AND timestamp_unix >= ? 
             ORDER BY timestamp_unix ASC LIMIT 1) as period_start,
            COUNT(*) as data_points
        FROM market_data
        WHERE asset_id = ? AND timestamp_unix >= ?
    """, [asset_id, asset_id, cutoff_ts, asset_id, cutoff_ts]).fetchone()
    
    if not result or not result[2]:
        return f"No price data found for {asset_id}."
    
    low, high, current, start, data_points = result
    pct_change = ((current - start) / start * 100) if start else 0
    volatility = ((high - low) / start * 100) if start else 0
    
    direction = "UP" if pct_change > 0 else "DOWN" if pct_change < 0 else "FLAT"
    
    # Detect data granularity
    timestamps = [r[0] for r in sample_result]
    granularity, interval_secs = _detect_time_granularity(timestamps)
    
    # Build granularity description
    if granularity == "5-minute":
        interval_note = f"Data Granularity: 5-minute intervals ({data_points} data points)"
    elif granularity == "hourly":
        interval_note = f"Data Granularity: Hourly intervals ({data_points} data points)"
    elif granularity == "daily":
        interval_note = f"Data Granularity: Daily intervals ({data_points} data points)"
    else:
        interval_note = f"Data Granularity: {granularity} ({data_points} data points)"
    
    return (
        f"Price Change for {asset_id} ({days} days):\n"
        f"=" * 50 + "\n"
        f"{interval_note}\n"
        f"\n"
        f"Current Price: ${current:,.2f}\n"
        f"Period Start: ${start:,.2f}\n"
        f"Change: {pct_change:+.2f}% ({direction})\n"
        f"Period High: ${high:,.2f}\n"
        f"Period Low: ${low:,.2f}\n"
        f"Volatility (range): {volatility:.2f}%"
    )


@tool
def lookup_asset_id(query: str) -> str:
    """
    Look up the CoinGecko asset_id for a given symbol or name.
    
    Use this tool when you need to find the correct asset_id for a token.
    For example, to find the asset_id for XRP, you would call this with "XRP".
    
    Args:
        query: Symbol (like "XRP", "ETH"), name (like "Ripple", "Ethereum"),
               or partial match to search for
    
    Returns the matching assets with their asset_ids, symbols, and names.
    Use the returned asset_id for other tools like get_price_history, get_sma, etc.
    
    Example mappings:
    - "XRP" -> asset_id: "ripple"
    - "ETH" -> asset_id: "ethereum"  
    - "BTC" -> asset_id: "bitcoin"
    - "SOL" -> asset_id: "solana"
    """
    conn = _get_conn()
    query_lower = query.lower().strip()
    
    # Try exact matches first
    exact_matches = conn.execute("""
        SELECT asset_id, symbol, name, market_cap_rank
        FROM asset_metadata
        WHERE LOWER(symbol) = ? OR LOWER(asset_id) = ? OR LOWER(name) = ?
        ORDER BY market_cap_rank ASC NULLS LAST
    """, [query_lower, query_lower, query_lower]).fetchall()
    
    if exact_matches:
        lines = [f"Exact matches for '{query}':", "=" * 60]
        for row in exact_matches:
            rank_str = f"#{row[3]}" if row[3] else "unranked"
            lines.append(f"  asset_id: \"{row[0]}\" | symbol: {row[1].upper()} | name: {row[2]} | {rank_str}")
        lines.append("")
        lines.append("Use the 'asset_id' value for other tools (e.g., get_price_history, get_sma)")
        return "\n".join(lines)
    
    # Try partial matches
    partial_matches = conn.execute("""
        SELECT asset_id, symbol, name, market_cap_rank
        FROM asset_metadata
        WHERE LOWER(symbol) LIKE ? OR LOWER(asset_id) LIKE ? OR LOWER(name) LIKE ?
        ORDER BY market_cap_rank ASC NULLS LAST
        LIMIT 10
    """, [f"%{query_lower}%", f"%{query_lower}%", f"%{query_lower}%"]).fetchall()
    
    if partial_matches:
        lines = [f"Partial matches for '{query}':", "=" * 60]
        for row in partial_matches:
            rank_str = f"#{row[3]}" if row[3] else "unranked"
            lines.append(f"  asset_id: \"{row[0]}\" | symbol: {row[1].upper()} | name: {row[2]} | {rank_str}")
        lines.append("")
        lines.append("Use the 'asset_id' value for other tools (e.g., get_price_history, get_sma)")
        return "\n".join(lines)
    
    return f"No assets found matching '{query}'. Use get_market_cap_rankings() to see available assets."
