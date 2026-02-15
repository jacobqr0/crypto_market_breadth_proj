"""
Technical analysis tools for CrewAI agents.

Provides technical indicators (SMA, RSI, correlations) with DuckDB caching
for efficient repeated queries. Uses pandas-ta for indicator calculations.

DATA GRANULARITY AND RESAMPLING
===============================
The raw market_data table contains:
- Historical data (Jan 2024 - Jan 2026): DAILY intervals
- Recent data (Jan 2026 onward): 5-MINUTE intervals

To provide consistent technical indicators, ALL functions in this module
RESAMPLE the raw data to DAILY intervals before calculating indicators.
This ensures:
1. SMA/RSI periods are meaningful (e.g., "50-day" means 50 actual calendar days)
2. Calculations are consistent regardless of whether source data is daily or 5-minute
3. Results are comparable across historical and recent time periods

The resampling uses the LAST price of each day (closing price equivalent).
"""

from typing import Optional, Tuple
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import numpy as np
from crewai.tools import tool

from agents.utils.db_connection import get_db_connection

try:
    import pandas_ta as ta
except ImportError:
    ta = None


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Get the shared DuckDB connection."""
    return get_db_connection()


def _resolve_asset_id(identifier: str) -> Tuple[str, Optional[str]]:
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
        If successful, error_message is None
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
        return result[0], None
    
    # Try exact match on symbol
    result = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(symbol) = ?
        LIMIT 1
    """, [identifier_lower]).fetchone()
    
    if result:
        return result[0], None
    
    # Try partial match on name (for cases like "Bitcoin" -> "bitcoin")
    result = conn.execute("""
        SELECT asset_id, symbol, name FROM asset_metadata
        WHERE LOWER(name) = ?
        LIMIT 1
    """, [identifier_lower]).fetchone()
    
    if result:
        return result[0], None
    
    # Return original identifier with error message listing available assets
    # Get a few similar assets to help the agent
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


def _detect_raw_granularity(timestamps: pd.Series) -> Tuple[str, int]:
    """
    Detect the predominant time interval in raw data timestamps.
    
    Args:
        timestamps: Series of timestamps
    
    Returns:
        Tuple of (granularity_description, count_of_raw_points)
    """
    if len(timestamps) < 2:
        return ("unknown", len(timestamps))
    
    # Calculate intervals
    sorted_ts = timestamps.sort_values()
    intervals = sorted_ts.diff().dropna().dt.total_seconds()
    
    if len(intervals) == 0:
        return ("unknown", len(timestamps))
    
    median_interval = intervals.median()
    
    if median_interval < 600:  # < 10 minutes
        return ("5-minute", len(timestamps))
    elif median_interval < 7200:  # < 2 hours
        return ("hourly", len(timestamps))
    elif median_interval < 172800:  # < 2 days
        return ("daily", len(timestamps))
    else:
        return ("sparse", len(timestamps))


def _get_price_series(asset_id: str, days: int = 365) -> pd.DataFrame:
    """
    Fetch price data as a pandas DataFrame.
    
    Returns RAW data which may be at daily intervals (historical, pre-2026) or
    5-minute intervals (recent, 2026+). Callers should resample to daily using
    .resample('D').last() for consistent indicator calculations.
    """
    conn = _get_conn()
    cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    
    result = conn.execute("""
        SELECT timestamp_unix, price_usd
        FROM market_data
        WHERE asset_id = ? AND timestamp_unix >= ? AND price_usd IS NOT NULL
        ORDER BY timestamp_unix ASC
    """, [asset_id, cutoff_ts]).fetchdf()
    
    if result.empty:
        return pd.DataFrame()
    
    result['timestamp'] = pd.to_datetime(result['timestamp_unix'], unit='s')
    result.set_index('timestamp', inplace=True)
    return result


def _cache_indicator(asset_id: str, indicator_name: str, timestamp: int, value: float):
    """Cache an indicator value in DuckDB."""
    conn = _get_conn()
    conn.execute("""
        INSERT INTO technical_indicators_cache 
        (asset_id, indicator_name, timestamp_unix, value, computed_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (asset_id, indicator_name, timestamp_unix) DO UPDATE SET
            value = EXCLUDED.value,
            computed_at = EXCLUDED.computed_at
    """, [asset_id, indicator_name, timestamp, value, datetime.now()])


def _get_cached_indicator(asset_id: str, indicator_name: str, timestamp: int) -> Optional[float]:
    """Get a cached indicator value if available."""
    conn = _get_conn()
    result = conn.execute("""
        SELECT value FROM technical_indicators_cache
        WHERE asset_id = ? AND indicator_name = ? AND timestamp_unix = ?
    """, [asset_id, indicator_name, timestamp]).fetchone()
    
    return result[0] if result else None


@tool
def get_sma(asset_id: str, period: int = 50) -> str:
    """
    Calculate Simple Moving Average (SMA) for an asset.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "bitcoin", "ripple") 
                  OR trading symbol (e.g., "BTC", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
        period: SMA period in days (default 50, common values: 20, 50, 200)
    
    Returns current SMA value and price position relative to SMA.
    SMA is a key trend indicator - price above SMA suggests bullish momentum.
    
    Raw data is resampled to DAILY closing prices before calculation, so the
    "period" parameter always represents actual calendar days regardless of
    whether source data is at 5-minute or daily intervals.
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    df = _get_price_series(asset_id, days=max(period * 2, 365))
    
    if df.empty or len(df) < period:
        return f"Insufficient data for {period}-day SMA calculation on {asset_id}."
    
    # Detect raw data granularity for reporting
    raw_granularity, raw_count = _detect_raw_granularity(df.index.to_series())
    
    # Resample to daily and calculate SMA
    daily = df['price_usd'].resample('D').last().dropna()
    
    if len(daily) < period:
        return f"Insufficient daily data for {period}-day SMA on {asset_id}. Have {len(daily)} days, need {period}."
    
    sma = daily.rolling(window=period).mean()
    current_price = daily.iloc[-1]
    current_sma = sma.iloc[-1]
    
    if pd.isna(current_sma):
        return f"Unable to calculate SMA for {asset_id}."
    
    # Cache the result
    latest_ts = int(daily.index[-1].timestamp())
    _cache_indicator(asset_id, f"sma_{period}", latest_ts, current_sma)
    
    pct_diff = ((current_price - current_sma) / current_sma) * 100
    position = "ABOVE" if current_price > current_sma else "BELOW"
    
    # Determine trend
    if pct_diff > 10:
        trend = "Strongly bullish - price well above SMA"
    elif pct_diff > 2:
        trend = "Moderately bullish"
    elif pct_diff < -10:
        trend = "Strongly bearish - price well below SMA"
    elif pct_diff < -2:
        trend = "Moderately bearish"
    else:
        trend = "Neutral - price near SMA"
    
    return (
        f"SMA Analysis for {asset_id} ({period}-day):\n"
        f"=" * 50 + "\n"
        f"Data: {raw_count} raw points ({raw_granularity}) resampled to {len(daily)} daily values\n"
        f"\n"
        f"Current Price (daily close): ${current_price:,.2f}\n"
        f"{period}-day SMA: ${current_sma:,.2f}\n"
        f"Position: {pct_diff:+.2f}% {position} SMA\n"
        f"Trend: {trend}"
    )


@tool
def get_rsi(asset_id: str, period: int = 14) -> str:
    """
    Calculate Relative Strength Index (RSI) for an asset.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "bitcoin", "ripple") 
                  OR trading symbol (e.g., "BTC", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
        period: RSI period (default 14 days)
    
    Returns RSI value (0-100) with interpretation:
    - RSI > 70: Overbought (potential reversal down)
    - RSI < 30: Oversold (potential reversal up)
    - RSI 30-70: Neutral momentum
    
    Raw data is resampled to DAILY closing prices before calculation, so the
    "period" parameter always represents actual calendar days regardless of
    whether source data is at 5-minute or daily intervals.
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    df = _get_price_series(asset_id, days=max(period * 5, 100))
    
    if df.empty or len(df) < period * 2:
        return f"Insufficient data for RSI calculation on {asset_id}."
    
    # Detect raw data granularity for reporting
    raw_granularity, raw_count = _detect_raw_granularity(df.index.to_series())
    
    # Resample to daily
    daily = df['price_usd'].resample('D').last().dropna()
    
    if len(daily) < period * 2:
        return f"Insufficient daily data for RSI on {asset_id}. Have {len(daily)} days, need {period * 2}."
    
    # Calculate RSI manually (in case pandas_ta not available)
    delta = daily.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    current_rsi = rsi.iloc[-1]
    
    if pd.isna(current_rsi):
        return f"Unable to calculate RSI for {asset_id}."
    
    # Cache the result
    latest_ts = int(daily.index[-1].timestamp())
    _cache_indicator(asset_id, f"rsi_{period}", latest_ts, current_rsi)
    
    # Interpret RSI
    if current_rsi >= 70:
        condition = "OVERBOUGHT - Consider caution, potential pullback"
    elif current_rsi >= 60:
        condition = "Bullish momentum"
    elif current_rsi <= 30:
        condition = "OVERSOLD - Potential buying opportunity"
    elif current_rsi <= 40:
        condition = "Bearish momentum"
    else:
        condition = "Neutral"
    
    return (
        f"RSI Analysis for {asset_id} ({period}-day):\n"
        f"=" * 50 + "\n"
        f"Data: {raw_count} raw points ({raw_granularity}) resampled to {len(daily)} daily values\n"
        f"\n"
        f"Current RSI: {current_rsi:.1f}\n"
        f"Condition: {condition}\n"
        f"\nInterpretation:\n"
        f"- RSI > 70: Overbought\n"
        f"- RSI < 30: Oversold\n"
        f"- RSI 30-70: Neutral zone"
    )


@tool
def get_price_correlation(asset_id_1: str, asset_id_2: str, days: int = 90) -> str:
    """
    Calculate price correlation between two assets.
    
    Args:
        asset_id_1: First asset - can be CoinGecko ID (e.g., "ethereum", "ripple") 
                    OR trading symbol (e.g., "ETH", "XRP")
        asset_id_2: Second asset - can be CoinGecko ID (e.g., "bitcoin") 
                    OR trading symbol (e.g., "BTC")
        days: Period for correlation calculation (default 90 days)
    
    Returns Pearson correlation coefficient (-1 to 1):
    - Near 1: Strong positive correlation (move together)
    - Near 0: No correlation (independent)
    - Near -1: Strong negative correlation (move opposite)
    
    Use this to assess portfolio diversification and risk concentration.
    
    Raw data is resampled to DAILY closing prices for both assets before
    calculating correlation. This ensures consistent comparison whether
    the source data is at 5-minute intervals (recent) or daily (historical).
    """
    # Resolve symbols to asset_ids if needed
    resolved_id_1, error1 = _resolve_asset_id(asset_id_1)
    if error1:
        return error1
    asset_id_1 = resolved_id_1
    
    resolved_id_2, error2 = _resolve_asset_id(asset_id_2)
    if error2:
        return error2
    asset_id_2 = resolved_id_2
    
    df1 = _get_price_series(asset_id_1, days=days)
    df2 = _get_price_series(asset_id_2, days=days)
    
    if df1.empty or df2.empty:
        return f"Insufficient data for correlation between {asset_id_1} and {asset_id_2}."
    
    # Detect raw data granularity for reporting
    raw_gran_1, raw_count_1 = _detect_raw_granularity(df1.index.to_series())
    raw_gran_2, raw_count_2 = _detect_raw_granularity(df2.index.to_series())
    
    # Resample to daily returns
    daily1 = df1['price_usd'].resample('D').last().dropna()
    daily2 = df2['price_usd'].resample('D').last().dropna()
    
    # Calculate returns
    returns1 = daily1.pct_change().dropna()
    returns2 = daily2.pct_change().dropna()
    
    # Align the series
    common_idx = returns1.index.intersection(returns2.index)
    
    if len(common_idx) < 20:
        return f"Insufficient overlapping data for correlation calculation. Need at least 20 days, have {len(common_idx)}."
    
    r1 = returns1.loc[common_idx]
    r2 = returns2.loc[common_idx]
    
    # Calculate correlation
    correlation = r1.corr(r2)
    
    if pd.isna(correlation):
        return f"Unable to calculate correlation."
    
    # Cache the result
    latest_ts = int(common_idx[-1].timestamp())
    _cache_indicator(f"{asset_id_1}_{asset_id_2}", f"correlation_{days}d", latest_ts, correlation)
    
    # Interpret correlation
    if correlation >= 0.8:
        interpretation = "VERY HIGH positive correlation - these assets move together closely"
        risk_note = "Adding both to portfolio provides minimal diversification benefit"
    elif correlation >= 0.5:
        interpretation = "Moderate positive correlation"
        risk_note = "Some diversification benefit when combined"
    elif correlation >= 0.2:
        interpretation = "Low positive correlation"
        risk_note = "Good diversification potential"
    elif correlation >= -0.2:
        interpretation = "Near zero correlation - assets move independently"
        risk_note = "Excellent diversification benefit"
    elif correlation >= -0.5:
        interpretation = "Moderate negative correlation"
        risk_note = "Strong diversification - may hedge each other"
    else:
        interpretation = "High negative correlation - assets move opposite"
        risk_note = "Natural hedge potential"
    
    return (
        f"Price Correlation: {asset_id_1} vs {asset_id_2} ({days} days):\n"
        f"=" * 60 + "\n"
        f"Data: {asset_id_1}: {raw_count_1} raw pts ({raw_gran_1}), {asset_id_2}: {raw_count_2} raw pts ({raw_gran_2})\n"
        f"Analysis based on {len(common_idx)} overlapping daily returns\n"
        f"\n"
        f"Correlation Coefficient: {correlation:.3f}\n"
        f"Interpretation: {interpretation}\n"
        f"Portfolio Risk: {risk_note}"
    )


@tool
def get_momentum_summary(asset_id: str) -> str:
    """
    Get a comprehensive momentum summary for an asset.
    
    Args:
        asset_id: Asset identifier - can be CoinGecko ID (e.g., "bitcoin", "ripple") 
                  OR trading symbol (e.g., "BTC", "XRP"). The tool will resolve symbols
                  to the correct asset_id automatically.
    
    Returns a summary combining SMA positions, RSI, and recent price action
    to assess overall momentum and trend direction.
    
    All indicators are calculated on DAILY closing prices (raw data is
    resampled). This ensures consistent analysis whether source data is
    at 5-minute intervals (recent) or daily (historical).
    """
    # Resolve symbol to asset_id if needed
    resolved_id, error = _resolve_asset_id(asset_id)
    if error:
        return error
    asset_id = resolved_id
    
    df = _get_price_series(asset_id, days=365)
    
    if df.empty or len(df) < 200:
        return f"Insufficient data for momentum analysis on {asset_id}. Need at least 200 data points."
    
    # Detect raw data granularity for reporting
    raw_granularity, raw_count = _detect_raw_granularity(df.index.to_series())
    
    daily = df['price_usd'].resample('D').last().dropna()
    
    if len(daily) < 50:
        return f"Insufficient daily data for momentum analysis on {asset_id}. Have {len(daily)} days, need at least 50."
    
    current_price = daily.iloc[-1]
    
    # Calculate SMAs
    sma_50 = daily.rolling(window=50).mean().iloc[-1]
    sma_200 = daily.rolling(window=200).mean().iloc[-1] if len(daily) >= 200 else None
    
    # Calculate RSI
    delta = daily.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = (100 - (100 / (1 + rs))).iloc[-1]
    
    # Price changes
    change_7d = ((current_price - daily.iloc[-8]) / daily.iloc[-8] * 100) if len(daily) >= 8 else None
    change_30d = ((current_price - daily.iloc[-31]) / daily.iloc[-31] * 100) if len(daily) >= 31 else None
    
    lines = [
        f"Momentum Summary for {asset_id}:",
        "=" * 60,
        f"Data: {raw_count} raw points ({raw_granularity}) resampled to {len(daily)} daily values",
        "",
        f"Current Price (daily close): ${current_price:,.2f}",
        "",
        "Moving Averages (calculated on daily data):",
        f"  50-day SMA: ${sma_50:,.2f} (Price {'+' if current_price > sma_50 else ''}{((current_price/sma_50)-1)*100:.1f}%)",
    ]
    
    if sma_200:
        lines.append(f"  200-day SMA: ${sma_200:,.2f} (Price {'+' if current_price > sma_200 else ''}{((current_price/sma_200)-1)*100:.1f}%)")
        
        # Golden/Death cross
        if sma_50 > sma_200:
            lines.append("  Trend: BULLISH (50-day above 200-day)")
        else:
            lines.append("  Trend: BEARISH (50-day below 200-day)")
    
    lines.extend([
        "",
        f"RSI (14-day, calculated on daily data): {rsi:.1f}",
    ])
    
    if rsi >= 70:
        lines.append("  Status: OVERBOUGHT")
    elif rsi <= 30:
        lines.append("  Status: OVERSOLD")
    else:
        lines.append("  Status: Neutral")
    
    lines.append("")
    lines.append("Price Performance:")
    if change_7d is not None:
        lines.append(f"  7-day change: {change_7d:+.2f}%")
    if change_30d is not None:
        lines.append(f"  30-day change: {change_30d:+.2f}%")
    
    # Overall assessment
    lines.append("")
    bullish_signals = 0
    if current_price > sma_50:
        bullish_signals += 1
    if sma_200 and current_price > sma_200:
        bullish_signals += 1
    if sma_200 and sma_50 > sma_200:
        bullish_signals += 1
    if 40 <= rsi <= 60:
        pass  # Neutral
    elif rsi > 50:
        bullish_signals += 1
    
    if bullish_signals >= 3:
        lines.append("Overall Assessment: BULLISH")
    elif bullish_signals <= 1:
        lines.append("Overall Assessment: BEARISH")
    else:
        lines.append("Overall Assessment: NEUTRAL")
    
    return "\n".join(lines)
