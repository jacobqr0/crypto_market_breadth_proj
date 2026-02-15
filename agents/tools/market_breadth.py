"""
Market breadth and crypto macro internals module.

Provides a suite of market breadth metrics for cryptocurrency analysis including:
- Market returns (equal-weighted, cap-weighted)
- Advancers/decliners and McClellan oscillator
- Percent above moving average
- New highs/lows breadth
- Cross-sectional dispersion and leadership
- Volume internals
- Concentration/dominance metrics
- Size bucket rotation analysis

All functions operate on pandas DataFrames and are designed to:
1. Avoid look-ahead bias (lagged weights)
2. Handle universe eligibility correctly
3. Use vectorized computations for performance
4. Support optional resampling (5-minute or daily data)

DATA SCHEMA
===========
Input market_df columns:
- asset_id (string): CoinGecko asset identifier
- timestamp_unix (int): Unix timestamp in seconds
- price_usd (float): USD price
- market_cap_usd (float): Market capitalization in USD
- volume_usd (float): 24h trading volume in USD

Assumes some missing values and assets that start/end at different times.
"""

from typing import Optional, List, Literal, NamedTuple, Callable, Union
import pandas as pd
import numpy as np


# =============================================================================
# DATA STRUCTURES
# =============================================================================

class MarketPanel(NamedTuple):
    """
    Wide-format market data panel for vectorized breadth computations.
    
    All DataFrames are indexed by timestamp (DatetimeIndex) with asset_ids as columns.
    """
    prices: pd.DataFrame  # Price matrix (time x assets)
    caps: pd.DataFrame    # Market cap matrix (time x assets)
    vols: pd.DataFrame    # Volume matrix (time x assets)


# =============================================================================
# CORE HELPER FUNCTIONS
# =============================================================================

def prepare_market_panel(
    market_df: pd.DataFrame,
    resample_freq: str = "1D",
    price_agg: str = "last",
    cap_agg: str = "last",
    vol_agg: str = "sum",
    exclude_asset_ids: Optional[List[str]] = None,
    include_asset_ids: Optional[List[str]] = None,
) -> MarketPanel:
    """
    Prepare market data for breadth calculations by resampling and pivoting to wide format.
    
    Transforms long-format market data into wide-format matrices (time x assets) for
    efficient vectorized cross-sectional computations.
    
    Args:
        market_df: DataFrame with columns [asset_id, timestamp_unix, price_usd, 
                   market_cap_usd, volume_usd]
        resample_freq: Pandas frequency string for resampling (default "1D" for daily)
        price_agg: Aggregation method for prices during resample ("last", "mean", "first")
        cap_agg: Aggregation method for market caps during resample
        vol_agg: Aggregation method for volumes during resample (default "sum")
        exclude_asset_ids: List of asset_ids to exclude (e.g., stablecoins)
        include_asset_ids: If provided, restrict universe to only these assets
    
    Returns:
        MarketPanel namedtuple containing:
        - prices: DataFrame indexed by timestamp, columns are asset_ids
        - caps: DataFrame indexed by timestamp, columns are asset_ids
        - vols: DataFrame indexed by timestamp, columns are asset_ids
    
    Notes:
        - Timestamps are converted from unix seconds to UTC datetime
        - No forward-fill is applied; gaps remain as NaN
        - Aggregation semantics: price uses last (closing), cap uses last, volume sums
    
    Example:
        >>> panel = prepare_market_panel(
        ...     market_df,
        ...     resample_freq="1D",
        ...     exclude_asset_ids=["tether", "usd-coin"]
        ... )
        >>> panel.prices.shape  # (num_days, num_assets)
    """
    # Validate required columns
    required_cols = ['asset_id', 'timestamp_unix', 'price_usd', 'market_cap_usd', 'volume_usd']
    missing_cols = set(required_cols) - set(market_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Make a copy to avoid modifying the original
    df = market_df.copy()
    
    # Apply asset filtering
    if include_asset_ids is not None:
        df = df[df['asset_id'].isin(include_asset_ids)]
    if exclude_asset_ids is not None:
        df = df[~df['asset_id'].isin(exclude_asset_ids)]
    
    if df.empty:
        raise ValueError("No data remaining after filtering")
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp_unix'], unit='s', utc=True)
    
    # Set timestamp as index for resampling
    df = df.set_index('timestamp')
    
    # Define aggregation functions
    agg_map = {
        'last': 'last',
        'first': 'first',
        'mean': 'mean',
        'sum': 'sum',
    }
    
    # Group by asset and resample
    def resample_and_pivot(df: pd.DataFrame, value_col: str, agg_method: str) -> pd.DataFrame:
        """Resample each asset's time series and pivot to wide format."""
        # Group by asset_id and resample
        resampled = (
            df.groupby('asset_id')[value_col]
            .resample(resample_freq)
            .agg(agg_map[agg_method])
            .unstack(level=0)  # Pivot asset_id to columns
        )
        return resampled
    
    prices_wide = resample_and_pivot(df, 'price_usd', price_agg)
    caps_wide = resample_and_pivot(df, 'market_cap_usd', cap_agg)
    vols_wide = resample_and_pivot(df, 'volume_usd', vol_agg)
    
    # Ensure all matrices have the same index and columns
    all_timestamps = prices_wide.index.union(caps_wide.index).union(vols_wide.index)
    all_assets = prices_wide.columns.union(caps_wide.columns).union(vols_wide.columns)
    
    prices_wide = prices_wide.reindex(index=all_timestamps, columns=all_assets)
    caps_wide = caps_wide.reindex(index=all_timestamps, columns=all_assets)
    vols_wide = vols_wide.reindex(index=all_timestamps, columns=all_assets)
    
    return MarketPanel(prices=prices_wide, caps=caps_wide, vols=vols_wide)


def compute_returns(
    prices_wide: pd.DataFrame,
    window_days: int = 1,
) -> pd.DataFrame:
    """
    Compute returns over a specified window for all assets.
    
    Calculates percentage returns as: (price_t / price_{t-window}) - 1
    
    Args:
        prices_wide: DataFrame with DatetimeIndex and asset_ids as columns
        window_days: Lookback period in number of rows (default 1 for daily returns)
    
    Returns:
        DataFrame of returns with same shape as input.
        First `window_days` rows will be NaN (insufficient history).
    
    Notes:
        - NaN prices propagate to NaN returns
        - No forward-fill is applied
        - For assets with gaps, returns across gaps will be computed
          (caller should handle eligibility if needed)
    
    Example:
        >>> returns = compute_returns(panel.prices, window_days=1)
        >>> returns['bitcoin'].iloc[-1]  # Latest 1-day return for bitcoin
    """
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    
    returns = prices_wide / prices_wide.shift(window_days) - 1
    return returns


def compute_ma(
    prices_wide: pd.DataFrame,
    window_days: int,
    kind: Literal["sma", "ema"] = "sma",
) -> pd.DataFrame:
    """
    Compute moving average for all assets.
    
    Args:
        prices_wide: DataFrame with DatetimeIndex and asset_ids as columns
        window_days: Moving average window in number of rows
        kind: Type of moving average - "sma" (simple) or "ema" (exponential)
    
    Returns:
        DataFrame of moving averages with same shape as input.
        First window_days-1 rows will be NaN for SMA.
    
    Notes:
        - SMA uses equal weights over the window
        - EMA uses exponential weighting with span=window_days
        - NaN values are excluded from calculations (min_periods=1 for EMA)
    
    Example:
        >>> sma_50 = compute_ma(panel.prices, window_days=50, kind="sma")
        >>> ema_20 = compute_ma(panel.prices, window_days=20, kind="ema")
    """
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    
    if kind == "sma":
        ma = prices_wide.rolling(window=window_days, min_periods=window_days).mean()
    elif kind == "ema":
        ma = prices_wide.ewm(span=window_days, min_periods=1, adjust=False).mean()
    else:
        raise ValueError(f"Unknown MA kind: {kind}. Use 'sma' or 'ema'.")
    
    return ma


def lag_weights(
    weights_wide: pd.DataFrame,
    lag: int = 1,
    normalize: bool = True,
) -> pd.DataFrame:
    """
    Lag and optionally normalize weights to avoid look-ahead bias.
    
    Critical for cap-weighted and volume-weighted metrics: weights at time t
    should come from values at time t-1 (or earlier) to prevent look-ahead bias.
    
    Args:
        weights_wide: DataFrame of raw weights (e.g., market caps or volumes)
        lag: Number of periods to lag (default 1)
        normalize: If True, normalize weights to sum to 1 at each timestamp
    
    Returns:
        DataFrame of lagged (and optionally normalized) weights.
        First `lag` rows will be NaN.
    
    Notes:
        - Normalization is row-wise: weights_t / sum(weights_t)
        - NaN values are excluded from normalization denominator
        - If all values in a row are NaN, normalized weights remain NaN
    
    Example:
        >>> lagged_caps = lag_weights(panel.caps, lag=1)
        >>> # lagged_caps.loc['2024-01-02'] uses caps from '2024-01-01'
    """
    if lag < 0:
        raise ValueError("lag must be >= 0")
    
    lagged = weights_wide.shift(lag) if lag > 0 else weights_wide.copy()
    
    if normalize:
        row_sums = lagged.sum(axis=1)
        # Avoid division by zero
        row_sums = row_sums.replace(0, np.nan)
        lagged = lagged.div(row_sums, axis=0)
    
    return lagged


def get_eligible_mask(
    prices_wide: pd.DataFrame,
    window_days: int = 1,
) -> pd.DataFrame:
    """
    Create a boolean mask indicating asset eligibility at each timestamp.
    
    An asset is eligible if it has:
    1. A valid (non-NaN) price at time t
    2. Sufficient history (window_days of valid prices before t)
    
    Args:
        prices_wide: DataFrame with DatetimeIndex and asset_ids as columns
        window_days: Required lookback history in rows
    
    Returns:
        Boolean DataFrame where True indicates eligibility.
    
    Example:
        >>> mask = get_eligible_mask(panel.prices, window_days=20)
        >>> eligible_count = mask.sum(axis=1)
    """
    # Has valid price at t
    has_price = prices_wide.notna()
    
    # Has enough history (count non-NaN in rolling window)
    history_count = prices_wide.notna().rolling(window=window_days, min_periods=1).sum()
    has_history = history_count >= window_days
    
    return has_price & has_history


# =============================================================================
# MARKET RETURN FUNCTIONS
# =============================================================================

def market_return_equal_weight(
    prices_wide: pd.DataFrame,
    window_days: int = 1,
    agg: Literal["mean", "median"] = "mean",
) -> pd.DataFrame:
    """
    Compute equal-weighted market return.
    
    Each eligible asset contributes equally to the market return,
    regardless of market cap or volume.
    
    Args:
        prices_wide: DataFrame with DatetimeIndex and asset_ids as columns
        window_days: Return period in rows (default 1 for daily)
        agg: Aggregation method - "mean" or "median"
    
    Returns:
        DataFrame with columns:
        - market_return_eq: Equal-weighted market return
        - eligible_count: Number of assets included in calculation
    
    Notes:
        - Only assets with valid returns are included
        - Median is more robust to outliers
    
    Example:
        >>> eq_returns = market_return_equal_weight(panel.prices, window_days=1)
        >>> eq_returns['market_return_eq'].plot()
    """
    returns = compute_returns(prices_wide, window_days)
    
    if agg == "mean":
        market_return = returns.mean(axis=1)
    elif agg == "median":
        market_return = returns.median(axis=1)
    else:
        raise ValueError(f"Unknown aggregation: {agg}. Use 'mean' or 'median'.")
    
    eligible_count = returns.notna().sum(axis=1)
    
    result = pd.DataFrame({
        'market_return_eq': market_return,
        'eligible_count': eligible_count,
    }, index=prices_wide.index)
    
    return result


def market_return_cap_weighted(
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    window_days: int = 1,
) -> pd.DataFrame:
    """
    Compute cap-weighted market return using lagged weights.
    
    Market return is the weighted sum of individual asset returns,
    where weights are determined by market cap at t-1 (lagged to avoid look-ahead).
    
    Args:
        prices_wide: DataFrame with DatetimeIndex and asset_ids as columns
        caps_wide: DataFrame of market caps with same structure
        window_days: Return period in rows (default 1 for daily)
    
    Returns:
        DataFrame with columns:
        - market_return_cap: Cap-weighted market return
        - cap_denom_total: Total lagged market cap used (denominator)
        - eligible_count: Number of assets with valid returns AND caps
    
    Notes:
        - CRITICAL: Weights use t-1 market caps to avoid look-ahead bias
        - Assets with missing caps are excluded from weighting
        - Formula: sum(weight_i * return_i) where weight_i = cap_{t-1,i} / sum(cap_{t-1})
    
    Example:
        >>> cap_returns = market_return_cap_weighted(panel.prices, panel.caps)
        >>> cap_returns['market_return_cap'].plot()
    """
    returns = compute_returns(prices_wide, window_days)
    
    # Lag caps by 1 period to avoid look-ahead bias
    lagged_caps = caps_wide.shift(1)
    
    # Create mask for eligible assets: have both return and lagged cap
    eligible = returns.notna() & lagged_caps.notna()
    
    # Zero out ineligible assets for computation
    returns_masked = returns.where(eligible, 0)
    caps_masked = lagged_caps.where(eligible, 0)
    
    # Compute weights (normalized)
    cap_totals = caps_masked.sum(axis=1)
    # Avoid division by zero
    cap_totals_safe = cap_totals.replace(0, np.nan)
    
    # Weighted return: sum(cap_i * return_i) / sum(cap_i)
    weighted_returns = (caps_masked * returns_masked).sum(axis=1)
    market_return = weighted_returns / cap_totals_safe
    
    eligible_count = eligible.sum(axis=1)
    
    result = pd.DataFrame({
        'market_return_cap': market_return,
        'cap_denom_total': cap_totals,
        'eligible_count': eligible_count,
    }, index=prices_wide.index)
    
    return result


def total_return_index(
    returns_series: pd.Series,
    start_value: float = 100.0,
) -> pd.Series:
    """
    Build cumulative total return index from a return series.
    
    Compounds returns over time starting from an initial value.
    
    Args:
        returns_series: Series of periodic returns
        start_value: Starting index value (default 100)
    
    Returns:
        Series of cumulative index values.
    
    Notes:
        - Formula: start_value * cumprod(1 + returns)
        - NaN returns are treated as 0 (no change)
        - First non-NaN return starts the compounding
    
    Example:
        >>> eq_tri = total_return_index(eq_returns['market_return_eq'])
        >>> cap_tri = total_return_index(cap_returns['market_return_cap'])
    """
    # Fill NaN with 0 for compounding (no return = no change)
    returns_filled = returns_series.fillna(0)
    
    # Compound returns
    cumulative = start_value * (1 + returns_filled).cumprod()
    
    return cumulative


def aggregate_returns(
    returns_series: pd.Series,
    period: Literal["M", "Y"] = "M",
) -> pd.Series:
    """
    Aggregate daily returns to monthly or yearly compounded returns.
    
    Args:
        returns_series: Series of daily returns with DatetimeIndex
        period: Aggregation period - "M" (monthly) or "Y" (yearly)
    
    Returns:
        Series of compounded returns for each period.
    
    Notes:
        - Compounding formula: prod(1 + r) - 1
        - Handles NaN by excluding from product
        - Period end dates are used as index
    
    Example:
        >>> monthly = aggregate_returns(daily_returns, period="M")
        >>> yearly = aggregate_returns(daily_returns, period="Y")
    """
    # Fill NaN with 0 for compounding
    returns_filled = returns_series.fillna(0)
    
    # Compound within each period
    compounded = returns_filled.resample(period).apply(
        lambda x: (1 + x).prod() - 1
    )
    
    return compounded


# =============================================================================
# BREADTH METRICS
# =============================================================================

def advancers_decliners(
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    vols_wide: pd.DataFrame,
    window_days: int = 1,
    threshold_pct: float = 0.0,
) -> pd.DataFrame:
    """
    Compute advancers/decliners breadth metrics.
    
    Classifies assets as advancing (positive return) or declining (negative return)
    and computes token-count, cap-weighted, and volume-weighted shares.
    
    Args:
        prices_wide: DataFrame of prices
        caps_wide: DataFrame of market caps
        vols_wide: DataFrame of volumes
        window_days: Return period for classification
        threshold_pct: Minimum absolute return to count (e.g., 0.01 for 1%)
                       0.0 means any positive/negative counts
    
    Returns:
        DataFrame with columns:
        - adv_count: Number of advancing assets
        - dec_count: Number of declining assets
        - unchanged_count: Number with return within threshold
        - net_adv: adv_count - dec_count
        - adv_dec_ratio: Ratio of advancers to decliners (adv_count / dec_count)
        - pct_advancing: adv_count / total eligible
        - pct_declining: dec_count / total eligible
        - cap_adv_share: Lagged-cap share of advancers
        - cap_dec_share: Lagged-cap share of decliners
        - vol_adv_share: Lagged-volume share of advancers
        - vol_dec_share: Lagged-volume share of decliners
        - eligible_count: Total eligible assets
        - total_lagged_cap: Sum of lagged caps for eligible assets
        - total_lagged_vol: Sum of lagged volumes for eligible assets
    
    Notes:
        - CRITICAL: Cap and volume weights use t-1 values (lagged)
        - Threshold allows filtering out noise (small moves)
        - Assets with missing returns excluded from all counts
    
    Example:
        >>> breadth = advancers_decliners(panel.prices, panel.caps, panel.vols)
        >>> breadth['pct_advancing'].plot()  # % of tokens up
    """
    returns = compute_returns(prices_wide, window_days)
    
    # Lag weights to avoid look-ahead
    lagged_caps = caps_wide.shift(1)
    lagged_vols = vols_wide.shift(1)
    
    # Create boolean masks
    is_advancing = returns > threshold_pct
    is_declining = returns < -threshold_pct
    is_unchanged = (returns >= -threshold_pct) & (returns <= threshold_pct)
    has_return = returns.notna()
    
    # Token counts
    adv_count = is_advancing.sum(axis=1)
    dec_count = is_declining.sum(axis=1)
    unchanged_count = (is_unchanged & has_return).sum(axis=1)
    eligible_count = has_return.sum(axis=1)
    
    net_adv = adv_count - dec_count
    
    # Advancers/Decliners ratio (avoid division by zero)
    dec_count_safe = dec_count.replace(0, np.nan)
    adv_dec_ratio = adv_count / dec_count_safe
    
    # Percentages (avoid division by zero)
    eligible_safe = eligible_count.replace(0, np.nan)
    pct_advancing = adv_count / eligible_safe
    pct_declining = dec_count / eligible_safe
    
    # Cap-weighted shares
    # Only include assets with valid return AND lagged cap
    cap_eligible = has_return & lagged_caps.notna()
    
    adv_caps = lagged_caps.where(is_advancing & cap_eligible, 0).sum(axis=1)
    dec_caps = lagged_caps.where(is_declining & cap_eligible, 0).sum(axis=1)
    total_lagged_cap = lagged_caps.where(cap_eligible, 0).sum(axis=1)
    
    total_cap_safe = total_lagged_cap.replace(0, np.nan)
    cap_adv_share = adv_caps / total_cap_safe
    cap_dec_share = dec_caps / total_cap_safe
    
    # Volume-weighted shares
    vol_eligible = has_return & lagged_vols.notna()
    
    adv_vols = lagged_vols.where(is_advancing & vol_eligible, 0).sum(axis=1)
    dec_vols = lagged_vols.where(is_declining & vol_eligible, 0).sum(axis=1)
    total_lagged_vol = lagged_vols.where(vol_eligible, 0).sum(axis=1)
    
    total_vol_safe = total_lagged_vol.replace(0, np.nan)
    vol_adv_share = adv_vols / total_vol_safe
    vol_dec_share = dec_vols / total_vol_safe
    
    result = pd.DataFrame({
        'adv_count': adv_count,
        'dec_count': dec_count,
        'unchanged_count': unchanged_count,
        'net_adv': net_adv,
        'adv_dec_ratio': adv_dec_ratio,
        'pct_advancing': pct_advancing,
        'pct_declining': pct_declining,
        'cap_adv_share': cap_adv_share,
        'cap_dec_share': cap_dec_share,
        'vol_adv_share': vol_adv_share,
        'vol_dec_share': vol_dec_share,
        'eligible_count': eligible_count,
        'total_lagged_cap': total_lagged_cap,
        'total_lagged_vol': total_lagged_vol,
    }, index=prices_wide.index)
    
    return result


def mcclellan_oscillator(
    adv_dec_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute McClellan Oscillator and Summation Index from A/D data.
    
    The McClellan Oscillator is a breadth momentum indicator based on
    the difference between fast and slow EMAs of the ratio-adjusted net advances.
    
    Args:
        adv_dec_df: DataFrame from advancers_decliners() containing
                    adv_count and dec_count columns
    
    Returns:
        DataFrame with columns:
        - rana: Ratio-Adjusted Net Advances = (adv - dec) / (adv + dec)
        - ema_19: 19-period EMA of RANA
        - ema_39: 39-period EMA of RANA
        - mcclellan_osc: ema_19 - ema_39 (the oscillator)
        - mcclellan_sum: Cumulative sum of oscillator (summation index)
    
    Notes:
        - RANA normalizes net advances by total issues
        - Oscillator crosses above 0 = bullish, below 0 = bearish
        - Summation tracks cumulative market breadth momentum
    
    Example:
        >>> breadth = advancers_decliners(panel.prices, panel.caps, panel.vols)
        >>> osc = mcclellan_oscillator(breadth)
        >>> osc['mcclellan_osc'].plot()
    """
    adv = adv_dec_df['adv_count']
    dec = adv_dec_df['dec_count']
    
    # Ratio-Adjusted Net Advances
    total = adv + dec
    total_safe = total.replace(0, np.nan)
    rana = (adv - dec) / total_safe
    
    # EMAs (19 and 39 are traditional McClellan parameters)
    ema_19 = rana.ewm(span=19, min_periods=1, adjust=False).mean()
    ema_39 = rana.ewm(span=39, min_periods=1, adjust=False).mean()
    
    # Oscillator
    mcclellan_osc = ema_19 - ema_39
    
    # Summation Index (cumulative oscillator)
    mcclellan_sum = mcclellan_osc.cumsum()
    
    result = pd.DataFrame({
        'rana': rana,
        'ema_19': ema_19,
        'ema_39': ema_39,
        'mcclellan_osc': mcclellan_osc,
        'mcclellan_sum': mcclellan_sum,
    }, index=adv_dec_df.index)
    
    return result


def pct_above_ma(
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    vols_wide: pd.DataFrame,
    ma_window: int = 50,
    short_ma_window: Optional[int] = None,
    long_ma_window: Optional[int] = None,
    ma_kind: Literal["sma", "ema"] = "sma",
) -> pd.DataFrame:
    """
    Compute percent of assets above moving average.
    
    A key breadth indicator showing market participation in an uptrend.
    
    Args:
        prices_wide: DataFrame of prices
        caps_wide: DataFrame of market caps
        vols_wide: DataFrame of volumes
        ma_window: Primary moving average window (e.g., 50 or 200)
        short_ma_window: Optional short MA for cross analysis (e.g., 50)
        long_ma_window: Optional long MA for cross analysis (e.g., 200)
        ma_kind: Type of moving average ("sma" or "ema")
    
    Returns:
        DataFrame with columns:
        - pct_above_ma: Token-count % above MA
        - cap_above_ma_share: Lagged-cap share above MA
        - vol_above_ma_share: Lagged-volume share above MA
        - eligible_count: Assets with valid price and MA
        If short/long MAs provided:
        - pct_short_gt_long: % of assets with short MA > long MA
        - cap_short_gt_long_share: Cap-weighted share with short > long
    
    Notes:
        - "Above MA" = price >= MA at each timestamp
        - Weights are lagged by 1 period
        - MA cross breadth shows trend strength across universe
    
    Example:
        >>> above_50 = pct_above_ma(panel.prices, panel.caps, panel.vols, ma_window=50)
        >>> above_200 = pct_above_ma(panel.prices, panel.caps, panel.vols, ma_window=200)
    """
    # Compute primary MA
    ma = compute_ma(prices_wide, ma_window, ma_kind)
    
    # Boolean mask: price >= MA
    above_ma = prices_wide >= ma
    has_data = prices_wide.notna() & ma.notna()
    
    # Lag weights
    lagged_caps = caps_wide.shift(1)
    lagged_vols = vols_wide.shift(1)
    
    # Token-count percentage
    eligible_count = has_data.sum(axis=1)
    eligible_safe = eligible_count.replace(0, np.nan)
    pct_above = (above_ma & has_data).sum(axis=1) / eligible_safe
    
    # Cap-weighted share
    cap_eligible = has_data & lagged_caps.notna()
    above_caps = lagged_caps.where(above_ma & cap_eligible, 0).sum(axis=1)
    total_cap = lagged_caps.where(cap_eligible, 0).sum(axis=1)
    total_cap_safe = total_cap.replace(0, np.nan)
    cap_above_share = above_caps / total_cap_safe
    
    # Volume-weighted share
    vol_eligible = has_data & lagged_vols.notna()
    above_vols = lagged_vols.where(above_ma & vol_eligible, 0).sum(axis=1)
    total_vol = lagged_vols.where(vol_eligible, 0).sum(axis=1)
    total_vol_safe = total_vol.replace(0, np.nan)
    vol_above_share = above_vols / total_vol_safe
    
    result = pd.DataFrame({
        'pct_above_ma': pct_above,
        'cap_above_ma_share': cap_above_share,
        'vol_above_ma_share': vol_above_share,
        'eligible_count': eligible_count,
    }, index=prices_wide.index)
    
    # Optional: MA cross analysis
    if short_ma_window is not None and long_ma_window is not None:
        short_ma = compute_ma(prices_wide, short_ma_window, ma_kind)
        long_ma = compute_ma(prices_wide, long_ma_window, ma_kind)
        
        short_gt_long = short_ma > long_ma
        cross_eligible = short_ma.notna() & long_ma.notna()
        
        cross_count = cross_eligible.sum(axis=1)
        cross_safe = cross_count.replace(0, np.nan)
        pct_short_gt_long = (short_gt_long & cross_eligible).sum(axis=1) / cross_safe
        
        # Cap-weighted cross
        cross_cap_eligible = cross_eligible & lagged_caps.notna()
        cross_caps = lagged_caps.where(short_gt_long & cross_cap_eligible, 0).sum(axis=1)
        cross_total_cap = lagged_caps.where(cross_cap_eligible, 0).sum(axis=1)
        cross_total_cap_safe = cross_total_cap.replace(0, np.nan)
        cap_short_gt_long_share = cross_caps / cross_total_cap_safe
        
        result['pct_short_gt_long'] = pct_short_gt_long
        result['cap_short_gt_long_share'] = cap_short_gt_long_share
    
    return result


def new_highs_lows(
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    vols_wide: pd.DataFrame,
    highlow_window: int = 52,
) -> pd.DataFrame:
    """
    Compute new highs and new lows breadth metrics.
    
    Tracks the number and share of assets making new N-period highs/lows.
    A strong breadth indicator of market momentum and breakout activity.
    
    Args:
        prices_wide: DataFrame of prices
        caps_wide: DataFrame of market caps
        vols_wide: DataFrame of volumes
        highlow_window: Lookback window for highs/lows (e.g., 52 for ~1 year, 20 for ~1 month)
    
    Returns:
        DataFrame with columns:
        - new_high_count: Number of assets at N-period high
        - new_low_count: Number of assets at N-period low
        - pct_new_highs: % of eligible assets at new highs
        - pct_new_lows: % of eligible assets at new lows
        - nh_nl_spread: new_high_count - new_low_count
        - nh_nl_ratio: new_high_count / new_low_count (capped)
        - cap_new_highs_share: Lagged-cap share at new highs
        - cap_new_lows_share: Lagged-cap share at new lows
        - vol_new_highs_share: Lagged-volume share at new highs
        - vol_new_lows_share: Lagged-volume share at new lows
        - eligible_count: Assets with sufficient history
    
    Notes:
        - New high: price >= rolling_max over window
        - New low: price <= rolling_min over window
        - Common windows: 20 (monthly), 52 (yearly), 252 (52 weeks trading days)
    
    Example:
        >>> nh_nl = new_highs_lows(panel.prices, panel.caps, panel.vols, highlow_window=52)
        >>> nh_nl['nh_nl_spread'].plot()  # Net new highs
    """
    # Compute rolling highs and lows
    rolling_max = prices_wide.rolling(window=highlow_window, min_periods=highlow_window).max()
    rolling_min = prices_wide.rolling(window=highlow_window, min_periods=highlow_window).min()
    
    # Boolean masks
    is_new_high = prices_wide >= rolling_max
    is_new_low = prices_wide <= rolling_min
    has_data = prices_wide.notna() & rolling_max.notna()
    
    # Lag weights
    lagged_caps = caps_wide.shift(1)
    lagged_vols = vols_wide.shift(1)
    
    # Token counts
    new_high_count = (is_new_high & has_data).sum(axis=1)
    new_low_count = (is_new_low & has_data).sum(axis=1)
    eligible_count = has_data.sum(axis=1)
    
    eligible_safe = eligible_count.replace(0, np.nan)
    pct_new_highs = new_high_count / eligible_safe
    pct_new_lows = new_low_count / eligible_safe
    
    # Spread and ratio
    nh_nl_spread = new_high_count - new_low_count
    # Ratio with floor to avoid division by zero (use 0.5 as minimum)
    new_low_safe = new_low_count.replace(0, 0.5)
    nh_nl_ratio = new_high_count / new_low_safe
    
    # Cap-weighted shares
    cap_eligible = has_data & lagged_caps.notna()
    high_caps = lagged_caps.where(is_new_high & cap_eligible, 0).sum(axis=1)
    low_caps = lagged_caps.where(is_new_low & cap_eligible, 0).sum(axis=1)
    total_cap = lagged_caps.where(cap_eligible, 0).sum(axis=1)
    total_cap_safe = total_cap.replace(0, np.nan)
    
    cap_new_highs_share = high_caps / total_cap_safe
    cap_new_lows_share = low_caps / total_cap_safe
    
    # Volume-weighted shares
    vol_eligible = has_data & lagged_vols.notna()
    high_vols = lagged_vols.where(is_new_high & vol_eligible, 0).sum(axis=1)
    low_vols = lagged_vols.where(is_new_low & vol_eligible, 0).sum(axis=1)
    total_vol = lagged_vols.where(vol_eligible, 0).sum(axis=1)
    total_vol_safe = total_vol.replace(0, np.nan)
    
    vol_new_highs_share = high_vols / total_vol_safe
    vol_new_lows_share = low_vols / total_vol_safe
    
    result = pd.DataFrame({
        'new_high_count': new_high_count,
        'new_low_count': new_low_count,
        'pct_new_highs': pct_new_highs,
        'pct_new_lows': pct_new_lows,
        'nh_nl_spread': nh_nl_spread,
        'nh_nl_ratio': nh_nl_ratio,
        'cap_new_highs_share': cap_new_highs_share,
        'cap_new_lows_share': cap_new_lows_share,
        'vol_new_highs_share': vol_new_highs_share,
        'vol_new_lows_share': vol_new_lows_share,
        'eligible_count': eligible_count,
    }, index=prices_wide.index)
    
    return result


# =============================================================================
# ADVANCED METRICS
# =============================================================================

def cross_sectional_dispersion(
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    window_days: int = 1,
    btc_asset_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute cross-sectional return dispersion and leadership metrics.
    
    Measures the spread of returns across assets, useful for regime detection.
    High dispersion = differentiated market, low dispersion = correlated moves.
    
    Args:
        prices_wide: DataFrame of prices
        caps_wide: DataFrame of market caps
        window_days: Return period for analysis
        btc_asset_id: Optional asset_id for BTC to compute outperformance
    
    Returns:
        DataFrame with columns:
        - mean_return: Cross-sectional mean of returns
        - median_return: Cross-sectional median of returns
        - return_dispersion: Standard deviation of returns (spread)
        - return_iqr: Interquartile range of returns
        - eligible_count: Number of assets in calculation
        If btc_asset_id provided:
        - pct_outperforming_btc: % of assets beating BTC
        - cap_outperforming_btc_share: Cap-weighted share beating BTC
    
    Notes:
        - High dispersion often indicates risk-on environment
        - Low dispersion may indicate macro-driven market
        - BTC outperformance useful for altcoin rotation analysis
    
    Example:
        >>> disp = cross_sectional_dispersion(
        ...     panel.prices, panel.caps, 
        ...     window_days=7, btc_asset_id='bitcoin'
        ... )
    """
    returns = compute_returns(prices_wide, window_days)
    
    # Cross-sectional statistics
    mean_return = returns.mean(axis=1)
    median_return = returns.median(axis=1)
    return_dispersion = returns.std(axis=1)
    
    # IQR (more robust to outliers)
    q75 = returns.quantile(0.75, axis=1)
    q25 = returns.quantile(0.25, axis=1)
    return_iqr = q75 - q25
    
    eligible_count = returns.notna().sum(axis=1)
    
    result = pd.DataFrame({
        'mean_return': mean_return,
        'median_return': median_return,
        'return_dispersion': return_dispersion,
        'return_iqr': return_iqr,
        'eligible_count': eligible_count,
    }, index=prices_wide.index)
    
    # BTC outperformance analysis
    if btc_asset_id is not None and btc_asset_id in returns.columns:
        btc_return = returns[btc_asset_id]
        
        # Exclude BTC from the comparison (comparing OTHER assets to BTC)
        other_assets = [col for col in returns.columns if col != btc_asset_id]
        returns_ex_btc = returns[other_assets]
        
        # Assets outperforming BTC
        outperforms = returns_ex_btc.gt(btc_return, axis=0)
        has_both = returns_ex_btc.notna() & btc_return.notna().values.reshape(-1, 1)
        
        outperf_count = (outperforms & has_both).sum(axis=1)
        total_count = has_both.sum(axis=1)
        total_safe = total_count.replace(0, np.nan)
        
        result['pct_outperforming_btc'] = outperf_count / total_safe
        
        # Cap-weighted outperformance (excluding BTC)
        lagged_caps = caps_wide.shift(1)
        lagged_caps_ex_btc = lagged_caps[other_assets] if btc_asset_id in lagged_caps.columns else lagged_caps
        
        cap_eligible = has_both & lagged_caps_ex_btc.notna()
        
        outperf_caps = lagged_caps_ex_btc.where(outperforms & cap_eligible, 0).sum(axis=1)
        total_cap = lagged_caps_ex_btc.where(cap_eligible, 0).sum(axis=1)
        total_cap_safe = total_cap.replace(0, np.nan)
        
        result['cap_outperforming_btc_share'] = outperf_caps / total_cap_safe
    
    return result


def volume_internals(
    prices_wide: pd.DataFrame,
    vols_wide: pd.DataFrame,
    window_days: int = 1,
    vol_ma_window: int = 20,
) -> pd.DataFrame:
    """
    Compute volume internals (up/down volume analysis).
    
    Analyzes volume participation in advancing vs declining assets.
    Strong up-volume with few advancers = concentrated buying.
    
    Args:
        prices_wide: DataFrame of prices
        vols_wide: DataFrame of volumes
        window_days: Return period for up/down classification
        vol_ma_window: Window for volume moving average
    
    Returns:
        DataFrame with columns:
        - up_volume: Total volume in advancing assets
        - down_volume: Total volume in declining assets
        - unchanged_volume: Total volume in unchanged assets
        - up_down_volume_ratio: up_volume / down_volume
        - pct_volume_in_advancers: up_volume / total_volume
        - pct_above_volume_ma: % of assets with volume > volume MA
        - cap_above_volume_ma_share: Cap-weighted (requires caps)
    
    Notes:
        - Volume uses current period (not lagged) as it's an activity measure
        - Up/down classification based on return sign
        - Volume above MA indicates elevated activity
    
    Example:
        >>> vol_int = volume_internals(panel.prices, panel.vols)
        >>> vol_int['up_down_volume_ratio'].plot()
    """
    returns = compute_returns(prices_wide, window_days)
    
    # Classify assets by return
    is_up = returns > 0
    is_down = returns < 0
    is_unchanged = returns == 0
    has_data = returns.notna() & vols_wide.notna()
    
    # Volume by direction
    up_volume = vols_wide.where(is_up & has_data, 0).sum(axis=1)
    down_volume = vols_wide.where(is_down & has_data, 0).sum(axis=1)
    unchanged_volume = vols_wide.where(is_unchanged & has_data, 0).sum(axis=1)
    total_volume = vols_wide.where(has_data, 0).sum(axis=1)
    
    # Ratios
    eps = 1e-10  # Small value to avoid division by zero
    down_volume_safe = down_volume.replace(0, eps)
    up_down_ratio = up_volume / down_volume_safe
    
    total_safe = total_volume.replace(0, np.nan)
    pct_volume_in_advancers = up_volume / total_safe
    
    # Volume above MA analysis
    vol_ma = vols_wide.rolling(window=vol_ma_window, min_periods=vol_ma_window).mean()
    above_vol_ma = vols_wide > vol_ma
    vol_ma_eligible = vols_wide.notna() & vol_ma.notna()
    
    above_ma_count = (above_vol_ma & vol_ma_eligible).sum(axis=1)
    vol_ma_count = vol_ma_eligible.sum(axis=1)
    vol_ma_count_safe = vol_ma_count.replace(0, np.nan)
    pct_above_vol_ma = above_ma_count / vol_ma_count_safe
    
    result = pd.DataFrame({
        'up_volume': up_volume,
        'down_volume': down_volume,
        'unchanged_volume': unchanged_volume,
        'up_down_volume_ratio': up_down_ratio,
        'pct_volume_in_advancers': pct_volume_in_advancers,
        'pct_above_volume_ma': pct_above_vol_ma,
        'total_volume': total_volume,
    }, index=prices_wide.index)
    
    return result


def concentration_dominance(
    caps_wide: pd.DataFrame,
    btc_asset_id: Optional[str] = None,
    eth_asset_id: Optional[str] = None,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Compute market concentration and dominance metrics.
    
    Measures how market cap is distributed across assets.
    Useful for detecting rotation between large caps and smaller tokens.
    
    Args:
        caps_wide: DataFrame of market caps
        btc_asset_id: Asset ID for Bitcoin dominance calculation
        eth_asset_id: Asset ID for Ethereum dominance calculation
        top_n: Number of top assets for top-N concentration
    
    Returns:
        DataFrame with columns:
        - total_market_cap: Sum of all market caps
        - btc_dominance: BTC market cap / total (if btc_asset_id provided)
        - eth_dominance: ETH market cap / total (if eth_asset_id provided)
        - top_n_cap_share: Share of market cap in top N assets
        - hhi_concentration: Herfindahl-Hirschman Index (sum of squared shares)
        - eligible_count: Number of assets with valid caps
    
    Notes:
        - HHI ranges from 1/N (equal distribution) to 1 (monopoly)
        - High concentration = market driven by few assets
        - Declining BTC dominance often indicates "alt season"
    
    Example:
        >>> conc = concentration_dominance(
        ...     panel.caps, 
        ...     btc_asset_id='bitcoin',
        ...     eth_asset_id='ethereum',
        ...     top_n=10
        ... )
    """
    # Total market cap
    total_cap = caps_wide.sum(axis=1)
    eligible_count = caps_wide.notna().sum(axis=1)
    
    total_cap_safe = total_cap.replace(0, np.nan)
    
    result = pd.DataFrame({
        'total_market_cap': total_cap,
        'eligible_count': eligible_count,
    }, index=caps_wide.index)
    
    # BTC dominance
    if btc_asset_id is not None and btc_asset_id in caps_wide.columns:
        btc_cap = caps_wide[btc_asset_id]
        result['btc_dominance'] = btc_cap / total_cap_safe
    
    # ETH dominance
    if eth_asset_id is not None and eth_asset_id in caps_wide.columns:
        eth_cap = caps_wide[eth_asset_id]
        result['eth_dominance'] = eth_cap / total_cap_safe
    
    # Top N concentration
    def top_n_share(row, n):
        """Compute share of top N assets in a row."""
        valid = row.dropna()
        if len(valid) == 0:
            return np.nan
        if len(valid) < n:
            n = len(valid)
        top_n_sum = valid.nlargest(n).sum()
        return top_n_sum / valid.sum()
    
    result['top_n_cap_share'] = caps_wide.apply(lambda row: top_n_share(row, top_n), axis=1)
    
    # HHI concentration
    def hhi(row):
        """Compute Herfindahl-Hirschman Index."""
        valid = row.dropna()
        if len(valid) == 0 or valid.sum() == 0:
            return np.nan
        shares = valid / valid.sum()
        return (shares ** 2).sum()
    
    result['hhi_concentration'] = caps_wide.apply(hhi, axis=1)
    
    return result


# =============================================================================
# SIZE BUCKET ANALYSIS
# =============================================================================

def assign_size_buckets(
    caps_wide: pd.DataFrame,
    buckets: Optional[List[tuple]] = None,
) -> pd.DataFrame:
    """
    Assign assets to size buckets based on market cap rank.
    
    Uses LAGGED cap ranks to avoid look-ahead bias when used with
    forward-looking metrics.
    
    Args:
        caps_wide: DataFrame of market caps
        buckets: List of (bucket_name, start_rank, end_rank) tuples.
                 Default: [("top_10", 1, 10), ("rank_11_50", 11, 50), 
                          ("rank_51_200", 51, 200), ("tail", 201, None)]
    
    Returns:
        DataFrame with same shape as input, containing bucket labels.
        Values are bucket names (strings) or NaN if asset has no cap.
    
    Notes:
        - Ranks are computed fresh at each timestamp using lagged caps
        - Rank 1 = largest market cap
        - Assets with missing caps get NaN bucket assignment
    
    Example:
        >>> buckets = assign_size_buckets(panel.caps)
        >>> top_10_mask = buckets == "top_10"
    """
    if buckets is None:
        buckets = [
            ("top_10", 1, 10),
            ("rank_11_50", 11, 50),
            ("rank_51_200", 51, 200),
            ("tail", 201, None),
        ]
    
    # Use lagged caps for ranking to avoid look-ahead
    lagged_caps = caps_wide.shift(1)
    
    # Compute ranks at each timestamp (rank 1 = largest)
    # Use method='first' to break ties deterministically
    ranks = lagged_caps.rank(axis=1, method='first', ascending=False)
    
    # Initialize result with NaN
    result = pd.DataFrame(
        index=caps_wide.index,
        columns=caps_wide.columns,
        dtype=object
    )
    
    # Assign buckets based on rank
    for bucket_name, start_rank, end_rank in buckets:
        if end_rank is None:
            mask = ranks >= start_rank
        else:
            mask = (ranks >= start_rank) & (ranks <= end_rank)
        result = result.where(~mask, bucket_name)
    
    # NaN for missing caps
    result = result.where(lagged_caps.notna(), np.nan)
    
    return result


def breadth_by_size_bucket(
    metric_fn: Callable,
    prices_wide: pd.DataFrame,
    caps_wide: pd.DataFrame,
    vols_wide: pd.DataFrame,
    buckets: Optional[List[tuple]] = None,
    **metric_kwargs,
) -> pd.DataFrame:
    """
    Compute breadth metrics segmented by market cap size bucket.
    
    Useful for rotation analysis: are small caps leading or lagging?
    
    Args:
        metric_fn: Breadth function to apply (e.g., advancers_decliners)
        prices_wide: DataFrame of prices
        caps_wide: DataFrame of market caps
        vols_wide: DataFrame of volumes
        buckets: Size bucket definitions (see assign_size_buckets)
        **metric_kwargs: Additional arguments passed to metric_fn
    
    Returns:
        DataFrame in tidy format with columns:
        - timestamp: Date/time
        - bucket: Size bucket name
        - [metric columns from metric_fn]
    
    Notes:
        - Each bucket's metrics are computed independently
        - Use lagged cap ranks for bucket assignment
        - Allows comparing breadth momentum across size segments
    
    Example:
        >>> bucket_breadth = breadth_by_size_bucket(
        ...     advancers_decliners,
        ...     panel.prices, panel.caps, panel.vols,
        ...     window_days=1
        ... )
        >>> bucket_breadth.groupby('bucket')['pct_advancing'].mean()
    """
    # Assign buckets
    bucket_assignments = assign_size_buckets(caps_wide, buckets)
    
    # Get unique bucket names
    if buckets is None:
        buckets = [
            ("top_10", 1, 10),
            ("rank_11_50", 11, 50),
            ("rank_51_200", 51, 200),
            ("tail", 201, None),
        ]
    bucket_names = [b[0] for b in buckets]
    
    results = []
    
    for bucket_name in bucket_names:
        # Create mask for this bucket
        mask = bucket_assignments == bucket_name
        
        # Filter data for this bucket (keep only bucket assets)
        prices_bucket = prices_wide.where(mask)
        caps_bucket = caps_wide.where(mask)
        vols_bucket = vols_wide.where(mask)
        
        # Skip if no data
        if prices_bucket.notna().sum().sum() == 0:
            continue
        
        # Compute metric for this bucket
        metric_result = metric_fn(
            prices_bucket, caps_bucket, vols_bucket, **metric_kwargs
        )
        
        # Add bucket label
        metric_result = metric_result.reset_index()
        metric_result['bucket'] = bucket_name
        
        results.append(metric_result)
    
    if not results:
        return pd.DataFrame()
    
    # Combine all buckets
    combined = pd.concat(results, ignore_index=True)
    
    # Reorder columns
    timestamp_col = combined.columns[0]  # First column is timestamp/index
    cols = [timestamp_col, 'bucket'] + [c for c in combined.columns if c not in [timestamp_col, 'bucket']]
    combined = combined[cols]
    
    return combined
