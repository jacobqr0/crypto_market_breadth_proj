"""
Technical Analysis schema definition.

This schema defines the output format for the Technical Analyst Agent,
which provides momentum indicators, BTC-relative performance, and market breadth.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from schemas.base import SchemaMeta, Trend, Signal, BTCRelativeTrend


class DailyTimeframe(BaseModel):
    """Daily (D1) technical indicators."""
    sma_50: Optional[float] = Field(None, description="50-day Simple Moving Average")
    sma_200: Optional[float] = Field(None, description="200-day Simple Moving Average")
    rsi_14: Optional[float] = Field(None, description="14-day RSI")
    pct_change_7d: Optional[float] = Field(None, description="7-day price change (%)")
    pct_change_30d: Optional[float] = Field(None, description="30-day price change (%)")


class WeeklyTimeframe(BaseModel):
    """Weekly (W1) technical indicators."""
    sma_20: Optional[float] = Field(None, description="20-week Simple Moving Average")
    rsi_14: Optional[float] = Field(None, description="14-week RSI")
    pct_change_12w: Optional[float] = Field(None, description="12-week price change (%)")


class Timeframes(BaseModel):
    """Technical indicators across timeframes."""
    d1: DailyTimeframe = Field(..., description="Daily timeframe indicators")
    w1: WeeklyTimeframe = Field(..., description="Weekly timeframe indicators")


class BTCRelative(BaseModel):
    """BTC-relative performance analysis."""
    model_config = {"use_enum_values": True}
    
    pair: str = Field(..., description="Trading pair (e.g., ETH/BTC)")
    trend: BTCRelativeTrend = Field(
        ..., 
        description="Relative trend: outperforming, neutral, underperforming, unknown"
    )
    pct_change_30d_vs_btc: Optional[float] = Field(
        None, 
        description="30-day performance vs BTC (%)"
    )


class KeyLevels(BaseModel):
    """Key support and resistance levels."""
    support: List[float] = Field(default_factory=list, description="Support levels")
    resistance: List[float] = Field(default_factory=list, description="Resistance levels")


class AssetTechnical(BaseModel):
    """Technical analysis for a single asset."""
    model_config = {"use_enum_values": True}
    
    symbol: str = Field(..., description="Asset symbol")
    timeframes: Timeframes = Field(..., description="Indicators by timeframe")
    trend: Trend = Field(..., description="Overall trend: bullish, neutral, bearish")
    signal: Signal = Field(..., description="Technical signal")
    btc_relative: BTCRelative = Field(..., description="BTC-relative performance")
    key_levels: KeyLevels = Field(..., description="Support and resistance levels")


class CorrelationPair(BaseModel):
    """Correlation between two assets."""
    pair: str = Field(..., description="Asset pair (e.g., BTC-ETH)")
    corr_90d: Optional[float] = Field(
        None, 
        ge=-1, 
        le=1, 
        description="90-day correlation coefficient"
    )


class Breadth(BaseModel):
    """Market breadth indicators."""
    universe: str = Field(
        ..., 
        description="Universe analyzed: top_50, top_100, custom"
    )
    pct_above_200d: Optional[float] = Field(
        None, 
        description="% of assets above 200-day SMA"
    )
    pct_golden_cross: Optional[float] = Field(
        None, 
        description="% of assets with golden cross (50 > 200 SMA)"
    )
    median_rsi_14: Optional[float] = Field(
        None, 
        description="Median 14-day RSI across universe"
    )
    correlation: List[CorrelationPair] = Field(
        default_factory=list,
        description="Key correlations between assets"
    )


class TechnicalAnalysisSchema(BaseModel):
    """
    Complete technical analysis schema.
    
    This is the output format for the Technical Analyst Agent.
    Contains per-asset analysis and market breadth indicators.
    """
    model_config = {"use_enum_values": True}
    
    meta: SchemaMeta = Field(..., description="Schema metadata")
    assets: List[AssetTechnical] = Field(..., description="Per-asset technical analysis")
    breadth: Breadth = Field(..., description="Market breadth indicators")
