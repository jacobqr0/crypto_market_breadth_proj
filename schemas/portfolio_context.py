"""
Portfolio Context schema definition.

This schema defines the output format for the Portfolio Context Agent,
which provides deterministic portfolio state from the get_portfolio_snapshot tool.
"""

from typing import List, Optional, Literal

from pydantic import BaseModel, Field

from schemas.base import SchemaMeta


class PositionOverLimit(BaseModel):
    """Position that exceeds the single asset limit."""
    symbol: str = Field(..., description="Asset symbol")
    allocation_pct: float = Field(..., description="Current allocation percentage")


class PortfolioTotals(BaseModel):
    """Aggregate portfolio totals."""
    total_cost_basis_usd: float = Field(..., description="Total cost basis in USD")
    total_current_value_usd: Optional[float] = Field(
        None, 
        description="Total current market value in USD (null if pricing incomplete)"
    )
    total_realized_pnl_usd: Optional[float] = Field(
        None, 
        description="Total realized P&L in USD"
    )
    drawdown_from_peak_pct: Optional[float] = Field(
        None, 
        description="Current drawdown from portfolio peak (%)"
    )


class Position(BaseModel):
    """Individual position in the portfolio."""
    symbol: str = Field(..., description="Asset symbol (e.g., BTC, ETH)")
    quantity: float = Field(..., description="Number of units held")
    avg_cost_usd: Optional[float] = Field(
        None, 
        description="Average cost basis per unit in USD"
    )
    total_cost_basis_usd: Optional[float] = Field(
        None, 
        description="Total cost basis for this position"
    )
    current_price_usd: Optional[float] = Field(
        None, 
        description="Current price per unit in USD"
    )
    current_value_usd: Optional[float] = Field(
        None, 
        description="Current market value in USD"
    )
    allocation_pct_by_value: Optional[float] = Field(
        None, 
        description="Position as % of total portfolio value"
    )
    unrealized_pnl_usd: Optional[float] = Field(
        None, 
        description="Unrealized P&L in USD"
    )
    unrealized_pnl_pct: Optional[float] = Field(
        None, 
        description="Unrealized P&L as percentage"
    )
    tier: Optional[Literal[0, 1, 2, 3]] = Field(
        None, 
        description="Tier classification: 0=BTC, 1=large-cap, 2=emerging, 3=tactical"
    )


class DerivedMetrics(BaseModel):
    """Computed metrics derived from positions."""
    btc_quantity: float = Field(..., description="Total BTC quantity held")
    btc_allocation_pct_by_value: float = Field(
        ..., 
        description="BTC as % of portfolio value"
    )
    tier2_3_allocation_pct_by_value: float = Field(
        ..., 
        description="Tier 2+3 combined as % of portfolio value"
    )
    max_single_asset_symbol: Optional[str] = Field(
        None, 
        description="Symbol of largest position"
    )
    max_single_asset_allocation_pct_by_value: float = Field(
        ..., 
        description="Largest position as % of portfolio"
    )


class FrameworkConfig(BaseModel):
    """Investment framework configuration values."""
    btc_target_min_pct: float = Field(40, description="Minimum BTC allocation target")
    btc_target_max_pct: float = Field(60, description="Maximum BTC allocation target")
    single_asset_limit_pct: float = Field(20, description="Max allocation per non-BTC asset")
    tier2_3_max_pct: float = Field(35, description="Max combined Tier 2+3 allocation")
    allow_100pct_btc_if_no_alts: bool = Field(
        False, 
        description="Allow 100% BTC when no altcoins held"
    )


class FrameworkChecks(BaseModel):
    """Results of framework compliance checks."""
    btc_within_target: Optional[bool] = Field(
        None, 
        description="Is BTC allocation within target range?"
    )
    any_position_over_limit: Optional[bool] = Field(
        None, 
        description="Does any non-BTC position exceed limit?"
    )
    positions_over_limit: List[PositionOverLimit] = Field(
        default_factory=list,
        description="Positions exceeding the single asset limit"
    )
    tier2_3_within_limit: Optional[bool] = Field(
        None, 
        description="Is Tier 2+3 allocation within limit?"
    )
    total_allocations_sum_to_100: Optional[bool] = Field(
        None, 
        description="Do allocations sum to 100%?"
    )
    pricing_complete: bool = Field(
        ..., 
        description="Are all positions priced?"
    )
    contradictions_detected: bool = Field(
        False, 
        description="Were data contradictions found?"
    )
    contradictions: List[str] = Field(
        default_factory=list,
        description="List of detected contradictions"
    )


class Framework(BaseModel):
    """Framework configuration and compliance checks."""
    config: FrameworkConfig = Field(..., description="Framework configuration values")
    checks: FrameworkChecks = Field(..., description="Compliance check results")


class PortfolioContextSchema(BaseModel):
    """
    Complete portfolio context schema.
    
    This is the output format for the Portfolio Context Agent.
    All values are computed deterministically by the get_portfolio_snapshot tool.
    """
    model_config = {"use_enum_values": True}
    
    meta: SchemaMeta = Field(..., description="Schema metadata")
    portfolio_totals: PortfolioTotals = Field(..., description="Aggregate totals")
    positions: List[Position] = Field(..., description="All open positions")
    derived: DerivedMetrics = Field(..., description="Computed metrics")
    framework: Framework = Field(..., description="Framework config and compliance")
