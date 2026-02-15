"""
Recommendations schema definition.

This schema defines the output format for the Orchestrator Agent,
which synthesizes all inputs and produces actionable recommendations.

Includes trading plan enforcement: BUY actions require complete trading plans.

Enforcement rules:
- BUY recommendations MUST have a complete trading plan (entry, 2+ TP targets, SL, invalidation)
- Non-WATCH recommendations MUST have at least one allocation field
- take_profit_targets sell_pct must sum to <= 100
- If these rules are violated, BUY is downgraded to WATCH automatically
"""

from typing import List, Optional, Literal, Tuple

from pydantic import BaseModel, Field, model_validator, field_validator

from schemas.base import (
    SchemaMeta,
    Regime,
    Trend,
    Action,
    Confidence,
    TimeHorizon,
    DefaultAction,
)


class MarketContext(BaseModel):
    """Synthesized market context."""
    model_config = {"use_enum_values": True}
    
    macro_regime: Regime = Field(..., description="Macro regime: risk_on, risk_off, neutral")
    technical_env: Trend = Field(
        ..., 
        description="Technical environment: bullish, neutral, bearish"
    )
    key_considerations: List[str] = Field(
        default_factory=list,
        description="Key market considerations"
    )


class TakeProfitTarget(BaseModel):
    """Take profit target specification."""
    target: str = Field(..., description="Price target or condition")
    sell_pct: float = Field(..., ge=0, le=100, description="Percentage to sell at this target")


class TradingPlan(BaseModel):
    """Complete trading plan for a recommendation."""
    entry_strategy: Optional[str] = Field(
        None, 
        description="Specific entry strategy/price level"
    )
    position_size: Optional[str] = Field(
        None, 
        description="Position size guidance"
    )
    take_profit_targets: List[TakeProfitTarget] = Field(
        default_factory=list,
        description="Take profit targets (need >= 2 for BUY)"
    )
    stop_loss: Optional[str] = Field(
        None, 
        description="Stop loss level or condition"
    )
    invalidation_trigger: Optional[str] = Field(
        None, 
        description="Non-price invalidation trigger"
    )

    def is_complete(self) -> bool:
        """Check if trading plan has all required fields for a BUY action."""
        return (
            self.entry_strategy is not None
            and self.position_size is not None
            and len(self.take_profit_targets) >= 2
            and self.stop_loss is not None
            and self.invalidation_trigger is not None
        )
    
    def get_missing_fields(self) -> List[str]:
        """Return list of missing required fields for a complete trading plan."""
        missing = []
        if self.entry_strategy is None:
            missing.append("entry_strategy")
        if self.position_size is None:
            missing.append("position_size")
        if len(self.take_profit_targets) < 2:
            missing.append(f"take_profit_targets (need >=2, have {len(self.take_profit_targets)})")
        if self.stop_loss is None:
            missing.append("stop_loss")
        if self.invalidation_trigger is None:
            missing.append("invalidation_trigger")
        return missing
    
    def get_take_profit_sum(self) -> float:
        """Return the sum of all take_profit_targets sell_pct values."""
        return sum(tp.sell_pct for tp in self.take_profit_targets)
    
    def normalize_take_profit_targets(self) -> Tuple[List[TakeProfitTarget], bool]:
        """
        Normalize take_profit_targets to sum to exactly 100 if > 100.
        
        Returns:
            Tuple of (normalized_targets, was_normalized)
        """
        total = self.get_take_profit_sum()
        if total <= 100:
            return self.take_profit_targets, False
        
        # Normalize proportionally
        normalized = []
        for tp in self.take_profit_targets:
            normalized_pct = (tp.sell_pct / total) * 100
            normalized.append(TakeProfitTarget(target=tp.target, sell_pct=round(normalized_pct, 1)))
        
        return normalized, True


class Rubric(BaseModel):
    """8-question investment rubric."""
    problem_solved: str = Field(..., description="What problem does this asset solve?")
    network_effects: str = Field(
        ..., 
        description="What network effects exist or are emerging?"
    )
    why_now: str = Field(..., description="Why now? (timing, catalysts)")
    invalidation: str = Field(..., description="What would invalidate this thesis?")
    vs_doing_nothing: str = Field(
        ..., 
        description="Why is this better than doing nothing?"
    )
    downside_risks: str = Field(..., description="What are the downside risks?")
    portfolio_fit: str = Field(
        ..., 
        description="Where does this fit in the portfolio?"
    )
    exit_criteria: str = Field(..., description="How will you sell? (exit criteria)")


class Dependencies(BaseModel):
    """Data dependencies for the recommendation."""
    requires: List[str] = Field(
        default_factory=list,
        description="Required conditions"
    )
    data_used: List[str] = Field(
        default_factory=list,
        description="Data sources used"
    )


class Recommendation(BaseModel):
    """Individual investment recommendation."""
    model_config = {"use_enum_values": True}
    
    symbol: str = Field(..., description="Asset symbol")
    action: Action = Field(
        ..., 
        description="Action: buy, hold, reduce, sell, watch"
    )
    conviction: Confidence = Field(..., description="Conviction level")
    tier: Literal[0, 1, 2, 3] = Field(
        ..., 
        description="Tier: 0=BTC, 1=large-cap, 2=emerging, 3=tactical"
    )
    suggested_allocation_pct_portfolio: Optional[float] = Field(
        None, 
        ge=0,
        le=100,
        description="Suggested % of portfolio"
    )
    suggested_allocation_pct_monthly_budget: Optional[float] = Field(
        None, 
        ge=0,
        le=100,
        description="Suggested % of monthly DCA budget"
    )
    time_horizon: TimeHorizon = Field(..., description="Investment time horizon")
    rationale_one_liner: str = Field(
        ..., 
        description="Concise 1-sentence rationale for this recommendation"
    )
    rubric: Rubric = Field(..., description="8-question investment rubric")
    trading_plan: Optional[TradingPlan] = Field(
        None, 
        description="Trading plan (required for BUY)"
    )
    evidence_refs: List[str] = Field(
        default_factory=list,
        description="IDs pointing to evidence appendix sections (e.g., 'macro.regime', 'tech.ETH')"
    )
    prerequisites: List[str] = Field(
        default_factory=list,
        description="Required conditions for this recommendation (e.g., 'macro regime remains risk_on')"
    )
    dependencies: Dependencies = Field(
        default_factory=Dependencies,
        description="Data dependencies"
    )
    _downgraded_from_buy: bool = False  # Internal flag for tracking downgrades
    _downgrade_reason: Optional[str] = None  # Reason for downgrade if applicable


class DefaultRecommendation(BaseModel):
    """Default recommendation when no high-conviction opportunities exist."""
    model_config = {"use_enum_values": True}
    
    action: DefaultAction = Field(
        ..., 
        description="Default action: hold_btc, dca_btc, do_nothing"
    )
    reason: str = Field(..., description="Reason for default recommendation")


class RecommendationsSchema(BaseModel):
    """
    Complete recommendations schema.
    
    This is the output format for the Orchestrator Agent.
    Contains market context, recommendations with rubrics, and trading plans.
    
    Trading plan enforcement rules:
    - If action == 'buy' but trading_plan is incomplete, the action will be
      downgraded to 'watch' and a warning added.
    - Non-WATCH actions must have at least one allocation field.
    - take_profit_targets sell_pct must sum to <= 100.
    """
    meta: SchemaMeta = Field(..., description="Schema metadata")
    executive_summary: str = Field(
        ..., 
        description="Executive summary (2-3 sentences)"
    )
    market_context: MarketContext = Field(..., description="Market context synthesis")
    recommendations: List[Recommendation] = Field(
        ..., 
        description="Individual recommendations"
    )
    default_recommendation: DefaultRecommendation = Field(
        ..., 
        description="Default recommendation"
    )

    model_config = {"use_enum_values": True}

    @model_validator(mode='after')
    def enforce_trading_plan_rule(self) -> 'RecommendationsSchema':
        """
        Enforce trading plan completeness for BUY actions.
        
        If action == 'buy' but trading_plan is incomplete:
        - Downgrade action to 'watch'
        - Add warning to meta.warnings
        """
        for rec in self.recommendations:
            if rec.action == Action.BUY or rec.action == "buy":
                if rec.trading_plan is None or not rec.trading_plan.is_complete():
                    # Downgrade to watch
                    rec.action = Action.WATCH
                    rec._downgraded_from_buy = True
                    
                    # Build detailed reason
                    if rec.trading_plan is None:
                        reason = "no trading plan provided"
                    else:
                        missing = rec.trading_plan.get_missing_fields()
                        reason = f"missing: {', '.join(missing)}"
                    
                    rec._downgrade_reason = reason
                    self.meta.warnings.append(
                        f"{rec.symbol}: Downgraded from BUY to WATCH - {reason}"
                    )
        return self

    @model_validator(mode='after')
    def enforce_allocation_for_actionable(self) -> 'RecommendationsSchema':
        """
        Ensure non-WATCH actions have at least one allocation field.
        
        If action is buy/reduce/sell and no allocation specified:
        - Add warning but don't downgrade (allocation can be added by enforcement layer)
        """
        for rec in self.recommendations:
            action_str = rec.action if isinstance(rec.action, str) else rec.action.value
            
            # Skip WATCH and HOLD - they don't need allocations
            if action_str in ("watch", "hold"):
                continue
            
            has_allocation = (
                rec.suggested_allocation_pct_portfolio is not None 
                or rec.suggested_allocation_pct_monthly_budget is not None
            )
            
            if not has_allocation:
                self.meta.warnings.append(
                    f"{rec.symbol}: {action_str.upper()} recommendation missing allocation "
                    "(needs suggested_allocation_pct_portfolio or suggested_allocation_pct_monthly_budget)"
                )
        return self

    @model_validator(mode='after')
    def enforce_take_profit_sum(self) -> 'RecommendationsSchema':
        """
        Ensure take_profit_targets sell_pct sums to <= 100.
        
        If > 100, normalize proportionally and add warning.
        """
        for rec in self.recommendations:
            if rec.trading_plan is not None and rec.trading_plan.take_profit_targets:
                total_pct = rec.trading_plan.get_take_profit_sum()
                
                if total_pct > 100:
                    # Normalize
                    normalized, _ = rec.trading_plan.normalize_take_profit_targets()
                    rec.trading_plan.take_profit_targets = normalized
                    self.meta.warnings.append(
                        f"{rec.symbol}: take_profit_targets sell_pct summed to {total_pct:.1f}%, "
                        "normalized to 100%"
                    )
        return self
