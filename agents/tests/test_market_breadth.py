"""
Unit tests for market breadth module.

Tests cover:
1. Core helpers (prepare_market_panel, compute_returns, compute_ma, lag_weights)
2. Market returns (equal-weighted, cap-weighted, total return index)
3. Breadth metrics (advancers/decliners, McClellan, % above MA, new highs/lows)
4. Advanced metrics (dispersion, volume internals, concentration)
5. Size bucket analysis

Key verification goals:
- No look-ahead bias (weights use lagged values)
- Correct eligibility counting
- Edge cases (new listings, missing data, zero values)
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from agents.tools.market_breadth import (
    MarketPanel,
    prepare_market_panel,
    compute_returns,
    compute_ma,
    lag_weights,
    get_eligible_mask,
    market_return_equal_weight,
    market_return_cap_weighted,
    total_return_index,
    aggregate_returns,
    advancers_decliners,
    mcclellan_oscillator,
    pct_above_ma,
    new_highs_lows,
    cross_sectional_dispersion,
    volume_internals,
    concentration_dominance,
    assign_size_buckets,
    breadth_by_size_bucket,
)


# =============================================================================
# FIXTURES: Synthetic Test Data
# =============================================================================

@pytest.fixture
def simple_market_df():
    """
    Create a simple 3-asset, 5-day market DataFrame with known values.
    
    Asset A: Price increases 10% daily (100 -> 110 -> 121 -> 133.1 -> 146.41)
    Asset B: Price decreases 5% daily (100 -> 95 -> 90.25 -> 85.74 -> 81.45)
    Asset C: Price stays flat (100 -> 100 -> 100 -> 100 -> 100)
    
    Caps and volumes follow similar patterns.
    """
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    # Create long-format data
    data = []
    
    # Asset A: +10% daily
    prices_a = [100, 110, 121, 133.1, 146.41]
    caps_a = [1000, 1100, 1210, 1331, 1464.1]
    vols_a = [100, 110, 120, 130, 140]
    
    # Asset B: -5% daily
    prices_b = [100, 95, 90.25, 85.7375, 81.450625]
    caps_b = [2000, 1900, 1805, 1714.75, 1629.01]
    vols_b = [200, 190, 180, 170, 160]
    
    # Asset C: flat
    prices_c = [100, 100, 100, 100, 100]
    caps_c = [3000, 3000, 3000, 3000, 3000]
    vols_c = [300, 300, 300, 300, 300]
    
    for i, date in enumerate(dates):
        ts = int(date.timestamp())
        data.append({'asset_id': 'asset_a', 'timestamp_unix': ts, 
                     'price_usd': prices_a[i], 'market_cap_usd': caps_a[i], 'volume_usd': vols_a[i]})
        data.append({'asset_id': 'asset_b', 'timestamp_unix': ts, 
                     'price_usd': prices_b[i], 'market_cap_usd': caps_b[i], 'volume_usd': vols_b[i]})
        data.append({'asset_id': 'asset_c', 'timestamp_unix': ts, 
                     'price_usd': prices_c[i], 'market_cap_usd': caps_c[i], 'volume_usd': vols_c[i]})
    
    return pd.DataFrame(data)


@pytest.fixture
def market_df_with_gaps():
    """
    Create a market DataFrame with missing data (listing gaps, delistings).
    
    Asset A: Full 5-day history
    Asset B: Joins on day 3 (new listing)
    Asset C: Delists after day 3 (has gap)
    """
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    data = []
    
    # Asset A: full history
    for i, date in enumerate(dates):
        ts = int(date.timestamp())
        data.append({
            'asset_id': 'asset_a', 'timestamp_unix': ts,
            'price_usd': 100 + i * 5, 'market_cap_usd': 1000 + i * 50,
            'volume_usd': 100 + i * 10
        })
    
    # Asset B: joins on day 3
    for i, date in enumerate(dates[2:]):
        ts = int(date.timestamp())
        data.append({
            'asset_id': 'asset_b', 'timestamp_unix': ts,
            'price_usd': 50 + i * 2, 'market_cap_usd': 500 + i * 20,
            'volume_usd': 50 + i * 5
        })
    
    # Asset C: delists after day 3
    for i, date in enumerate(dates[:3]):
        ts = int(date.timestamp())
        data.append({
            'asset_id': 'asset_c', 'timestamp_unix': ts,
            'price_usd': 200 - i * 10, 'market_cap_usd': 2000 - i * 100,
            'volume_usd': 200 - i * 20
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def simple_panel(simple_market_df):
    """Pre-prepared market panel from simple_market_df."""
    return prepare_market_panel(simple_market_df)


# =============================================================================
# CORE HELPER TESTS
# =============================================================================

class TestPrepareMarketPanel:
    """Tests for prepare_market_panel()."""
    
    def test_basic_pivot(self, simple_market_df):
        """Test that data is correctly pivoted to wide format."""
        panel = prepare_market_panel(simple_market_df)
        
        # Check structure
        assert isinstance(panel, MarketPanel)
        assert len(panel.prices.columns) == 3  # 3 assets
        assert len(panel.prices) == 5  # 5 days
        
        # Check values
        assert panel.prices.loc[panel.prices.index[0], 'asset_a'] == 100
        assert panel.prices.loc[panel.prices.index[-1], 'asset_a'] == pytest.approx(146.41, rel=0.01)
    
    def test_exclude_assets(self, simple_market_df):
        """Test asset exclusion filter."""
        panel = prepare_market_panel(
            simple_market_df,
            exclude_asset_ids=['asset_c']
        )
        
        assert 'asset_c' not in panel.prices.columns
        assert 'asset_a' in panel.prices.columns
        assert 'asset_b' in panel.prices.columns
    
    def test_include_assets(self, simple_market_df):
        """Test asset inclusion filter."""
        panel = prepare_market_panel(
            simple_market_df,
            include_asset_ids=['asset_a']
        )
        
        assert list(panel.prices.columns) == ['asset_a']
    
    def test_missing_columns_raises(self):
        """Test that missing columns raise ValueError."""
        bad_df = pd.DataFrame({'asset_id': ['a'], 'price': [100]})
        
        with pytest.raises(ValueError, match="Missing required columns"):
            prepare_market_panel(bad_df)
    
    def test_gaps_preserved_as_nan(self, market_df_with_gaps):
        """Test that listing gaps become NaN (no forward fill)."""
        panel = prepare_market_panel(market_df_with_gaps)
        
        # Asset B should have NaN for first 2 days
        assert pd.isna(panel.prices.loc[panel.prices.index[0], 'asset_b'])
        assert pd.isna(panel.prices.loc[panel.prices.index[1], 'asset_b'])
        assert not pd.isna(panel.prices.loc[panel.prices.index[2], 'asset_b'])
        
        # Asset C should have NaN for last 2 days
        assert not pd.isna(panel.prices.loc[panel.prices.index[0], 'asset_c'])
        assert pd.isna(panel.prices.loc[panel.prices.index[3], 'asset_c'])


class TestComputeReturns:
    """Tests for compute_returns()."""
    
    def test_daily_returns(self, simple_panel):
        """Test 1-day return calculation."""
        returns = compute_returns(simple_panel.prices, window_days=1)
        
        # First row should be NaN
        assert returns.iloc[0].isna().all()
        
        # Asset A: (110/100) - 1 = 0.10
        assert returns.loc[returns.index[1], 'asset_a'] == pytest.approx(0.10, rel=0.01)
        
        # Asset B: (95/100) - 1 = -0.05
        assert returns.loc[returns.index[1], 'asset_b'] == pytest.approx(-0.05, rel=0.01)
        
        # Asset C: (100/100) - 1 = 0
        assert returns.loc[returns.index[1], 'asset_c'] == pytest.approx(0.0, abs=0.01)
    
    def test_multi_day_returns(self, simple_panel):
        """Test multi-day return calculation."""
        returns = compute_returns(simple_panel.prices, window_days=2)
        
        # First 2 rows should be NaN
        assert returns.iloc[0].isna().all()
        assert returns.iloc[1].isna().all()
        
        # Asset A: (121/100) - 1 = 0.21
        assert returns.loc[returns.index[2], 'asset_a'] == pytest.approx(0.21, rel=0.01)


class TestComputeMA:
    """Tests for compute_ma()."""
    
    def test_sma(self, simple_panel):
        """Test simple moving average."""
        sma = compute_ma(simple_panel.prices, window_days=3, kind='sma')
        
        # First 2 rows should be NaN
        assert sma.iloc[0].isna().all()
        assert sma.iloc[1].isna().all()
        
        # Asset A: (100 + 110 + 121) / 3 = 110.33
        assert sma.loc[sma.index[2], 'asset_a'] == pytest.approx(110.33, rel=0.01)
    
    def test_ema(self, simple_panel):
        """Test exponential moving average."""
        ema = compute_ma(simple_panel.prices, window_days=3, kind='ema')
        
        # EMA starts from first value (min_periods=1)
        assert not pd.isna(ema.iloc[0]['asset_a'])


class TestLagWeights:
    """Tests for lag_weights() - CRITICAL for no look-ahead bias."""
    
    def test_lag_one_period(self, simple_panel):
        """Test that weights are correctly lagged by 1 period."""
        lagged = lag_weights(simple_panel.caps, lag=1, normalize=False)
        
        # First row should be NaN
        assert lagged.iloc[0].isna().all()
        
        # Second row should have first row's values
        assert lagged.loc[lagged.index[1], 'asset_a'] == simple_panel.caps.loc[simple_panel.caps.index[0], 'asset_a']
    
    def test_normalization(self, simple_panel):
        """Test that normalized weights sum to 1."""
        lagged = lag_weights(simple_panel.caps, lag=1, normalize=True)
        
        # Check row sums (excluding NaN rows)
        row_sums = lagged.iloc[1:].sum(axis=1)
        for s in row_sums:
            assert s == pytest.approx(1.0, rel=0.01)
    
    def test_no_look_ahead_bias(self, simple_panel):
        """
        CRITICAL TEST: Verify weights at time t come from t-1.
        
        Day 2 weights should use Day 1 caps, NOT Day 2 caps.
        """
        lagged = lag_weights(simple_panel.caps, lag=1, normalize=True)
        
        # Day 1 caps: A=1000, B=2000, C=3000, total=6000
        # Day 1 normalized: A=1/6, B=2/6, C=3/6
        
        day2_idx = lagged.index[1]
        day1_total = 1000 + 2000 + 3000
        
        expected_a = 1000 / day1_total
        expected_b = 2000 / day1_total
        expected_c = 3000 / day1_total
        
        assert lagged.loc[day2_idx, 'asset_a'] == pytest.approx(expected_a, rel=0.01)
        assert lagged.loc[day2_idx, 'asset_b'] == pytest.approx(expected_b, rel=0.01)
        assert lagged.loc[day2_idx, 'asset_c'] == pytest.approx(expected_c, rel=0.01)


# =============================================================================
# MARKET RETURN TESTS
# =============================================================================

class TestMarketReturns:
    """Tests for market return functions."""
    
    def test_equal_weight_mean(self, simple_panel):
        """Test equal-weighted mean return."""
        result = market_return_equal_weight(simple_panel.prices, window_days=1, agg='mean')
        
        # Day 2: returns are +10%, -5%, 0% -> mean = 1.67%
        day2_return = result.loc[result.index[1], 'market_return_eq']
        expected = (0.10 + (-0.05) + 0.0) / 3
        assert day2_return == pytest.approx(expected, rel=0.01)
        
        # Eligible count
        assert result.loc[result.index[1], 'eligible_count'] == 3
    
    def test_equal_weight_median(self, simple_panel):
        """Test equal-weighted median return."""
        result = market_return_equal_weight(simple_panel.prices, window_days=1, agg='median')
        
        # Day 2: returns are +10%, -5%, 0% -> median = 0%
        day2_return = result.loc[result.index[1], 'market_return_eq']
        assert day2_return == pytest.approx(0.0, abs=0.01)
    
    def test_cap_weighted_return(self, simple_panel):
        """Test cap-weighted return uses lagged weights."""
        result = market_return_cap_weighted(
            simple_panel.prices, 
            simple_panel.caps, 
            window_days=1
        )
        
        # Day 2: returns +10%, -5%, 0%
        # Day 1 caps (used as weights): A=1000, B=2000, C=3000
        # Weighted return = (1000*0.10 + 2000*(-0.05) + 3000*0) / 6000
        #                 = (100 - 100 + 0) / 6000 = 0
        day2_idx = result.index[1]
        expected = (1000 * 0.10 + 2000 * (-0.05) + 3000 * 0) / 6000
        
        assert result.loc[day2_idx, 'market_return_cap'] == pytest.approx(expected, rel=0.01)
        assert result.loc[day2_idx, 'cap_denom_total'] == pytest.approx(6000, rel=0.01)
    
    def test_total_return_index(self, simple_panel):
        """Test cumulative total return index."""
        eq_returns = market_return_equal_weight(simple_panel.prices)
        tri = total_return_index(eq_returns['market_return_eq'], start_value=100)
        
        # Starts at 100 (after first non-NaN, compounds from there)
        assert tri.iloc[0] == 100  # NaN filled with 0 -> 100 * (1+0) = 100
        
        # Should compound over time
        assert len(tri) == 5


# =============================================================================
# BREADTH METRIC TESTS
# =============================================================================

class TestAdvancersDecliners:
    """Tests for advancers_decliners()."""
    
    def test_basic_counts(self, simple_panel):
        """Test advancer/decliner counting."""
        result = advancers_decliners(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            window_days=1, threshold_pct=0.0
        )
        
        # Day 2: A advances (+10%), B declines (-5%), C unchanged (0%)
        day2_idx = result.index[1]
        
        assert result.loc[day2_idx, 'adv_count'] == 1
        assert result.loc[day2_idx, 'dec_count'] == 1
        assert result.loc[day2_idx, 'unchanged_count'] == 1
        assert result.loc[day2_idx, 'net_adv'] == 0
        assert result.loc[day2_idx, 'pct_advancing'] == pytest.approx(1/3, rel=0.01)
    
    def test_threshold_filtering(self, simple_panel):
        """Test threshold filters out small moves."""
        result = advancers_decliners(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            window_days=1, threshold_pct=0.06  # Only >6% counts
        )
        
        # Day 2: only A (+10%) counts as advancing
        # B (-5%) doesn't meet 6% threshold for declining
        day2_idx = result.index[1]
        
        assert result.loc[day2_idx, 'adv_count'] == 1
        assert result.loc[day2_idx, 'dec_count'] == 0  # -5% < 6%
    
    def test_cap_weighted_share_uses_lagged_caps(self, simple_panel):
        """CRITICAL: Verify cap weights use t-1 values."""
        result = advancers_decliners(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            window_days=1
        )
        
        # Day 2: A advances with Day 1 cap = 1000
        # Total Day 1 caps = 6000
        # cap_adv_share = 1000 / 6000 = 0.1667
        day2_idx = result.index[1]
        
        expected_cap_share = 1000 / 6000
        assert result.loc[day2_idx, 'cap_adv_share'] == pytest.approx(expected_cap_share, rel=0.01)


class TestMcClellanOscillator:
    """Tests for mcclellan_oscillator()."""
    
    def test_rana_calculation(self, simple_panel):
        """Test Ratio-Adjusted Net Advances."""
        breadth = advancers_decliners(
            simple_panel.prices, simple_panel.caps, simple_panel.vols
        )
        result = mcclellan_oscillator(breadth)
        
        # Day 2: adv=1, dec=1 -> RANA = (1-1)/(1+1) = 0
        day2_idx = result.index[1]
        assert result.loc[day2_idx, 'rana'] == pytest.approx(0.0, abs=0.01)
    
    def test_has_required_columns(self, simple_panel):
        """Test output contains all expected columns."""
        breadth = advancers_decliners(
            simple_panel.prices, simple_panel.caps, simple_panel.vols
        )
        result = mcclellan_oscillator(breadth)
        
        required_cols = ['rana', 'ema_19', 'ema_39', 'mcclellan_osc', 'mcclellan_sum']
        for col in required_cols:
            assert col in result.columns


class TestPctAboveMA:
    """Tests for pct_above_ma()."""
    
    def test_basic_above_ma(self, simple_panel):
        """Test percent above MA calculation."""
        result = pct_above_ma(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            ma_window=2
        )
        
        # After 2 days, can compute 2-day MA
        # Day 3: Asset A price=121, MA=(100+110)/2=105 -> above
        #        Asset B price=90.25, MA=(100+95)/2=97.5 -> below
        #        Asset C price=100, MA=(100+100)/2=100 -> equal (above)
        day3_idx = result.index[2]
        
        # 2 of 3 assets above MA
        assert result.loc[day3_idx, 'pct_above_ma'] == pytest.approx(2/3, rel=0.01)
    
    def test_ma_cross_analysis(self, simple_panel):
        """Test short MA > long MA cross detection."""
        result = pct_above_ma(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            ma_window=2,
            short_ma_window=2,
            long_ma_window=3
        )
        
        assert 'pct_short_gt_long' in result.columns


class TestNewHighsLows:
    """Tests for new_highs_lows()."""
    
    def test_new_highs_detection(self, simple_panel):
        """Test new high detection."""
        result = new_highs_lows(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            highlow_window=2
        )
        
        # Asset A makes new highs every day (trending up)
        # After day 2 (first valid window), A should be at new high
        day3_idx = result.index[2]
        
        # A is at new high, B is at new low, C is at both (flat)
        assert result.loc[day3_idx, 'new_high_count'] >= 1  # At least A
    
    def test_spread_and_ratio(self, simple_panel):
        """Test NH-NL spread and ratio."""
        result = new_highs_lows(
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            highlow_window=2
        )
        
        assert 'nh_nl_spread' in result.columns
        assert 'nh_nl_ratio' in result.columns


# =============================================================================
# ADVANCED METRIC TESTS
# =============================================================================

class TestCrossSectionalDispersion:
    """Tests for cross_sectional_dispersion()."""
    
    def test_basic_dispersion(self, simple_panel):
        """Test dispersion calculation."""
        result = cross_sectional_dispersion(
            simple_panel.prices, simple_panel.caps, window_days=1
        )
        
        # Day 2: returns +10%, -5%, 0%
        day2_idx = result.index[1]
        
        # Mean = 1.67%
        assert result.loc[day2_idx, 'mean_return'] == pytest.approx(0.0167, rel=0.1)
        
        # Std dev > 0 (there is dispersion)
        assert result.loc[day2_idx, 'return_dispersion'] > 0
    
    def test_btc_outperformance(self):
        """Test BTC outperformance calculation."""
        # Create panel with 'bitcoin' as an asset
        dates = pd.date_range('2024-01-01', periods=3, freq='D')
        data = []
        
        for i, date in enumerate(dates):
            ts = int(date.timestamp())
            # Bitcoin: +5%
            data.append({'asset_id': 'bitcoin', 'timestamp_unix': ts,
                        'price_usd': 100 * (1.05 ** i), 'market_cap_usd': 1000,
                        'volume_usd': 100})
            # Alt 1: +10% (beats BTC)
            data.append({'asset_id': 'alt1', 'timestamp_unix': ts,
                        'price_usd': 50 * (1.10 ** i), 'market_cap_usd': 500,
                        'volume_usd': 50})
            # Alt 2: +2% (loses to BTC)
            data.append({'asset_id': 'alt2', 'timestamp_unix': ts,
                        'price_usd': 25 * (1.02 ** i), 'market_cap_usd': 250,
                        'volume_usd': 25})
        
        df = pd.DataFrame(data)
        panel = prepare_market_panel(df)
        
        result = cross_sectional_dispersion(
            panel.prices, panel.caps, window_days=1, btc_asset_id='bitcoin'
        )
        
        assert 'pct_outperforming_btc' in result.columns
        
        # Day 2: alt1 (+10%) beats BTC (+5%), alt2 (+2%) loses
        # 1 out of 2 alts outperforms (BTC itself excluded from comparison)
        day2_idx = result.index[1]
        assert result.loc[day2_idx, 'pct_outperforming_btc'] == pytest.approx(0.5, rel=0.1)


class TestVolumeInternals:
    """Tests for volume_internals()."""
    
    def test_up_down_volume(self, simple_panel):
        """Test up/down volume calculation."""
        result = volume_internals(
            simple_panel.prices, simple_panel.vols, window_days=1
        )
        
        # Day 2: A is up (vol=110), B is down (vol=190), C unchanged (vol=300)
        day2_idx = result.index[1]
        
        assert result.loc[day2_idx, 'up_volume'] == pytest.approx(110, rel=0.01)
        assert result.loc[day2_idx, 'down_volume'] == pytest.approx(190, rel=0.01)
    
    def test_up_down_ratio(self, simple_panel):
        """Test up/down volume ratio."""
        result = volume_internals(
            simple_panel.prices, simple_panel.vols, window_days=1
        )
        
        day2_idx = result.index[1]
        expected_ratio = 110 / 190
        assert result.loc[day2_idx, 'up_down_volume_ratio'] == pytest.approx(expected_ratio, rel=0.01)


class TestConcentrationDominance:
    """Tests for concentration_dominance()."""
    
    def test_total_market_cap(self, simple_panel):
        """Test total market cap calculation."""
        result = concentration_dominance(simple_panel.caps)
        
        # Day 1: 1000 + 2000 + 3000 = 6000
        day1_idx = result.index[0]
        assert result.loc[day1_idx, 'total_market_cap'] == pytest.approx(6000, rel=0.01)
    
    def test_dominance_calculation(self):
        """Test BTC/ETH dominance calculation."""
        dates = pd.date_range('2024-01-01', periods=2, freq='D')
        data = []
        
        for i, date in enumerate(dates):
            ts = int(date.timestamp())
            data.append({'asset_id': 'bitcoin', 'timestamp_unix': ts,
                        'price_usd': 40000, 'market_cap_usd': 800000,
                        'volume_usd': 1000})
            data.append({'asset_id': 'ethereum', 'timestamp_unix': ts,
                        'price_usd': 2000, 'market_cap_usd': 200000,
                        'volume_usd': 500})
        
        df = pd.DataFrame(data)
        panel = prepare_market_panel(df)
        
        result = concentration_dominance(
            panel.caps,
            btc_asset_id='bitcoin',
            eth_asset_id='ethereum'
        )
        
        # Total = 1M, BTC = 800k, ETH = 200k
        # BTC dominance = 80%, ETH dominance = 20%
        assert result['btc_dominance'].iloc[0] == pytest.approx(0.80, rel=0.01)
        assert result['eth_dominance'].iloc[0] == pytest.approx(0.20, rel=0.01)
    
    def test_hhi_concentration(self, simple_panel):
        """Test HHI concentration index."""
        result = concentration_dominance(simple_panel.caps)
        
        # Day 1: caps 1000, 2000, 3000 (total 6000)
        # Shares: 1/6, 2/6, 3/6
        # HHI = (1/6)^2 + (2/6)^2 + (3/6)^2 = 1/36 + 4/36 + 9/36 = 14/36 = 0.389
        day1_idx = result.index[0]
        expected_hhi = (1/6)**2 + (2/6)**2 + (3/6)**2
        assert result.loc[day1_idx, 'hhi_concentration'] == pytest.approx(expected_hhi, rel=0.01)


# =============================================================================
# SIZE BUCKET TESTS
# =============================================================================

class TestSizeBuckets:
    """Tests for size bucket functions."""
    
    def test_bucket_assignment(self, simple_panel):
        """Test assets are assigned to correct buckets based on lagged cap rank."""
        buckets = assign_size_buckets(
            simple_panel.caps,
            buckets=[("large", 1, 1), ("mid", 2, 2), ("small", 3, 3)]
        )
        
        # Day 2 uses Day 1 caps: C=3000 (rank 1), B=2000 (rank 2), A=1000 (rank 3)
        day2_idx = buckets.index[1]
        
        assert buckets.loc[day2_idx, 'asset_c'] == 'large'
        assert buckets.loc[day2_idx, 'asset_b'] == 'mid'
        assert buckets.loc[day2_idx, 'asset_a'] == 'small'
    
    def test_lagged_bucket_assignment(self, simple_panel):
        """CRITICAL: Verify bucket assignment uses lagged caps."""
        buckets = assign_size_buckets(simple_panel.caps)
        
        # First row should be all NaN (no lagged data)
        assert buckets.iloc[0].isna().all()
    
    def test_breadth_by_bucket(self, simple_panel):
        """Test breadth computation by size bucket."""
        result = breadth_by_size_bucket(
            advancers_decliners,
            simple_panel.prices, simple_panel.caps, simple_panel.vols,
            buckets=[("top_2", 1, 2), ("bottom", 3, 3)],
            window_days=1
        )
        
        assert 'bucket' in result.columns
        assert set(result['bucket'].unique()).issubset({'top_2', 'bottom'})


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_single_asset(self):
        """Test handling of single-asset universe."""
        dates = pd.date_range('2024-01-01', periods=5, freq='D')
        data = [
            {'asset_id': 'only_one', 'timestamp_unix': int(d.timestamp()),
             'price_usd': 100 + i, 'market_cap_usd': 1000, 'volume_usd': 100}
            for i, d in enumerate(dates)
        ]
        df = pd.DataFrame(data)
        panel = prepare_market_panel(df)
        
        # Should not crash
        result = advancers_decliners(panel.prices, panel.caps, panel.vols)
        assert len(result) == 5
    
    def test_all_nan_row(self):
        """Test handling of all-NaN rows."""
        dates = pd.date_range('2024-01-01', periods=3, freq='D')
        # Asset only has data on middle day
        data = [
            {'asset_id': 'sparse', 'timestamp_unix': int(dates[1].timestamp()),
             'price_usd': 100, 'market_cap_usd': 1000, 'volume_usd': 100}
        ]
        df = pd.DataFrame(data)
        panel = prepare_market_panel(df)
        
        # Should handle gracefully
        result = market_return_equal_weight(panel.prices)
        assert len(result) == 1
    
    def test_zero_volume_handling(self):
        """Test handling of zero volume."""
        dates = pd.date_range('2024-01-01', periods=3, freq='D')
        data = []
        for i, d in enumerate(dates):
            ts = int(d.timestamp())
            data.append({'asset_id': 'zero_vol', 'timestamp_unix': ts,
                        'price_usd': 100 + i, 'market_cap_usd': 1000,
                        'volume_usd': 0})  # Zero volume
        
        df = pd.DataFrame(data)
        panel = prepare_market_panel(df)
        
        # Should not crash on division by zero
        result = volume_internals(panel.prices, panel.vols)
        assert len(result) == 3


# =============================================================================
# ROLLING WINDOW EDGE TESTS
# =============================================================================

class TestRollingWindowEdges:
    """Tests for rolling window behavior at edges."""
    
    def test_returns_nan_at_start(self, simple_panel):
        """Verify first N-1 rows are NaN for N-day window."""
        returns_1d = compute_returns(simple_panel.prices, window_days=1)
        returns_3d = compute_returns(simple_panel.prices, window_days=3)
        
        # 1-day: first row NaN
        assert returns_1d.iloc[0].isna().all()
        assert not returns_1d.iloc[1].isna().all()
        
        # 3-day: first 3 rows NaN
        assert returns_3d.iloc[0].isna().all()
        assert returns_3d.iloc[1].isna().all()
        assert returns_3d.iloc[2].isna().all()
        assert not returns_3d.iloc[3].isna().all()
    
    def test_ma_nan_at_start(self, simple_panel):
        """Verify MA has correct NaN pattern at start."""
        sma_3 = compute_ma(simple_panel.prices, window_days=3, kind='sma')
        
        # SMA: first 2 rows NaN (need 3 points)
        assert sma_3.iloc[0].isna().all()
        assert sma_3.iloc[1].isna().all()
        assert not sma_3.iloc[2].isna().all()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
