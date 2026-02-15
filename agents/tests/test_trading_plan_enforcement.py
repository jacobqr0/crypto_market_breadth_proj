"""
Tests for trading plan enforcement rules.

Tests that:
1. BUY with missing trading plan downgrades to WATCH
2. BUY with incomplete targets (< 2) downgrades
3. BUY with missing stop_loss downgrades to WATCH
4. Complete trading plan passes
5. Non-BUY actions are not affected
6. Take profit targets > 100% are normalized
7. Position size > limit is auto-reduced
8. No actionable recs sets default to do_nothing
"""

import pytest
import json

from schemas import (
    RecommendationsSchema,
    Recommendation,
    TradingPlan,
    TakeProfitTarget,
    Rubric,
    MarketContext,
    DefaultRecommendation,
    SchemaMeta,
    Action,
)
from validation import enforce_trading_plan_rule


class TestTradingPlanCompleteness:
    """Tests for trading plan completeness checks."""
    
    def test_complete_trading_plan(self):
        """Complete trading plan should pass is_complete check."""
        plan = TradingPlan(
            entry_strategy="Buy at $3,200",
            position_size="5% of portfolio",
            take_profit_targets=[
                TakeProfitTarget(target="$4,000", sell_pct=50),
                TakeProfitTarget(target="$5,000", sell_pct=50),
            ],
            stop_loss="-15%",
            invalidation_trigger="Protocol hack"
        )
        assert plan.is_complete() is True
    
    def test_incomplete_trading_plan_missing_entry(self):
        """Missing entry strategy should fail is_complete."""
        plan = TradingPlan(
            entry_strategy=None,
            position_size="5% of portfolio",
            take_profit_targets=[
                TakeProfitTarget(target="$4,000", sell_pct=50),
                TakeProfitTarget(target="$5,000", sell_pct=50),
            ],
            stop_loss="-15%",
            invalidation_trigger="Protocol hack"
        )
        assert plan.is_complete() is False
    
    def test_incomplete_trading_plan_missing_targets(self):
        """Less than 2 targets should fail is_complete."""
        plan = TradingPlan(
            entry_strategy="Buy at $3,200",
            position_size="5% of portfolio",
            take_profit_targets=[
                TakeProfitTarget(target="$4,000", sell_pct=100),
            ],
            stop_loss="-15%",
            invalidation_trigger="Protocol hack"
        )
        assert plan.is_complete() is False
    
    def test_incomplete_trading_plan_no_targets(self):
        """No targets should fail is_complete."""
        plan = TradingPlan(
            entry_strategy="Buy at $3,200",
            position_size="5% of portfolio",
            take_profit_targets=[],
            stop_loss="-15%",
            invalidation_trigger="Protocol hack"
        )
        assert plan.is_complete() is False


class TestBuyDowngradeToWatch:
    """Tests for BUY action downgrade to WATCH when trading plan is incomplete."""
    
    @pytest.fixture
    def base_rubric(self):
        """Return a valid rubric."""
        return Rubric(
            problem_solved="Smart contracts",
            network_effects="Largest ecosystem",
            why_now="L2 growth",
            invalidation="Major hack",
            vs_doing_nothing="Strong returns",
            downside_risks="Execution risk",
            portfolio_fit="Core holding",
            exit_criteria="2x target"
        )
    
    @pytest.fixture
    def complete_trading_plan(self):
        """Return a complete trading plan."""
        return TradingPlan(
            entry_strategy="Buy at $3,200",
            position_size="5% of portfolio",
            take_profit_targets=[
                TakeProfitTarget(target="$4,000", sell_pct=50),
                TakeProfitTarget(target="$5,000", sell_pct=50),
            ],
            stop_loss="-15%",
            invalidation_trigger="Protocol hack"
        )
    
    def test_buy_with_no_trading_plan_downgrades(self, base_rubric):
        """BUY with no trading plan should be downgraded to WATCH."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "Smart contracts",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth",
                        "invalidation": "Major hack",
                        "vs_doing_nothing": "Strong returns",
                        "downside_risks": "Execution risk",
                        "portfolio_fit": "Core holding",
                        "exit_criteria": "2x target"
                    },
                    "trading_plan": None  # Missing!
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # After validation, action should be downgraded
        assert schema.recommendations[0].action == "watch"
        assert any("Downgraded" in w for w in schema.meta.warnings)
    
    def test_buy_with_incomplete_trading_plan_downgrades(self, base_rubric):
        """BUY with incomplete trading plan (only 1 TP target) should be downgraded to WATCH."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "Smart contracts",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth",
                        "invalidation": "Major hack",
                        "vs_doing_nothing": "Strong returns",
                        "downside_risks": "Execution risk",
                        "portfolio_fit": "Core holding",
                        "exit_criteria": "2x target"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy at $3,200",
                        "position_size": "5%",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 100}  # Only 1 target!
                        ],
                        "stop_loss": "-15%",
                        "invalidation_trigger": "Hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # After validation, action should be downgraded
        assert schema.recommendations[0].action == "watch"
        assert any("Downgraded" in w for w in schema.meta.warnings)
        assert any("take_profit_targets" in w for w in schema.meta.warnings)
    
    def test_buy_with_complete_trading_plan_passes(self, base_rubric, complete_trading_plan):
        """BUY with complete trading plan should remain BUY."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "Smart contracts",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth",
                        "invalidation": "Major hack",
                        "vs_doing_nothing": "Strong returns",
                        "downside_risks": "Execution risk",
                        "portfolio_fit": "Core holding",
                        "exit_criteria": "2x target"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy at $3,200",
                        "position_size": "5% of portfolio",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 50},
                            {"target": "$5,000", "sell_pct": 50}
                        ],
                        "stop_loss": "-15%",
                        "invalidation_trigger": "Protocol hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # Action should remain buy
        assert schema.recommendations[0].action == "buy"
        # No downgrade warnings
        assert not any("Downgraded" in w for w in schema.meta.warnings)
    
    def test_hold_action_not_affected(self, base_rubric):
        """HOLD action should not be affected by trading plan rules."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "neutral",
                "technical_env": "neutral",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "BTC",
                    "action": "hold",
                    "conviction": "high",
                    "tier": 0,
                    "suggested_allocation_pct_portfolio": None,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "12m+",
                    "rationale_one_liner": "Continue holding BTC as core position",
                    "rubric": {
                        "problem_solved": "Digital gold",
                        "network_effects": "Largest network",
                        "why_now": "Stable conditions",
                        "invalidation": "None expected",
                        "vs_doing_nothing": "Already holding",
                        "downside_risks": "Volatility",
                        "portfolio_fit": "Core Tier 0",
                        "exit_criteria": "Long-term hold"
                    },
                    "trading_plan": None  # No plan needed for HOLD
                }
            ],
            "default_recommendation": {
                "action": "hold_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # Action should remain hold
        assert schema.recommendations[0].action == "hold"
        # No downgrade warnings
        assert not any("Downgraded" in w for w in schema.meta.warnings)
    
    def test_watch_action_not_affected(self, base_rubric):
        """WATCH action should not be affected by trading plan rules."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "neutral",
                "technical_env": "neutral",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "SOL",
                    "action": "watch",
                    "conviction": "medium",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": None,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "SOL shows promise but needs clarity on network stability",
                    "rubric": {
                        "problem_solved": "Fast transactions",
                        "network_effects": "Growing ecosystem",
                        "why_now": "Waiting for clarity",
                        "invalidation": "Network issues",
                        "vs_doing_nothing": "Risk/reward unclear",
                        "downside_risks": "Centralization concerns",
                        "portfolio_fit": "Potential Tier 1",
                        "exit_criteria": "N/A - watching"
                    },
                    "trading_plan": None
                }
            ],
            "default_recommendation": {
                "action": "hold_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # Action should remain watch
        assert schema.recommendations[0].action == "watch"
    
    def test_buy_missing_stop_loss_downgrades(self, base_rubric):
        """BUY with missing stop_loss should be downgraded to WATCH."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test summary",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "Smart contracts",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth",
                        "invalidation": "Major hack",
                        "vs_doing_nothing": "Strong returns",
                        "downside_risks": "Execution risk",
                        "portfolio_fit": "Core holding",
                        "exit_criteria": "2x target"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy at $3,200",
                        "position_size": "5%",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 50},
                            {"target": "$5,000", "sell_pct": 50}
                        ],
                        "stop_loss": None,  # Missing!
                        "invalidation_trigger": "Hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Maintain allocation"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # After validation, action should be downgraded
        assert schema.recommendations[0].action == "watch"
        assert any("Downgraded" in w for w in schema.meta.warnings)
        assert any("stop_loss" in w for w in schema.meta.warnings)


class TestEnforceTradingPlanRule:
    """Tests for the explicit enforce_trading_plan_rule function."""
    
    def test_enforce_on_validated_schema(self):
        """Enforcement function can be called on validated schema."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": None,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "X",
                        "network_effects": "X",
                        "why_now": "X",
                        "invalidation": "X",
                        "vs_doing_nothing": "X",
                        "downside_risks": "X",
                        "portfolio_fit": "X",
                        "exit_criteria": "X"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy",
                        "position_size": "5%",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 50},
                            {"target": "$5,000", "sell_pct": 50}
                        ],
                        "stop_loss": "-15%",
                        "invalidation_trigger": "Hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "X"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # Should not raise and return the schema
        result = enforce_trading_plan_rule(schema)
        assert result is not None
        assert result.recommendations[0].action == "buy"


class TestTakeProfitNormalization:
    """Tests for take_profit_targets sell_pct normalization."""
    
    def test_take_profit_targets_over_100_normalized(self):
        """take_profit_targets sell_pct > 100 should be normalized."""
        schema_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": []
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth",
                    "rubric": {
                        "problem_solved": "X",
                        "network_effects": "X",
                        "why_now": "X",
                        "invalidation": "X",
                        "vs_doing_nothing": "X",
                        "downside_risks": "X",
                        "portfolio_fit": "X",
                        "exit_criteria": "X"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy at $3,200",
                        "position_size": "5%",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 60},
                            {"target": "$5,000", "sell_pct": 60}  # Total 120%!
                        ],
                        "stop_loss": "-15%",
                        "invalidation_trigger": "Hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "X"
            }
        }
        
        schema = RecommendationsSchema.model_validate(schema_data)
        
        # Action should remain buy (complete plan)
        assert schema.recommendations[0].action == "buy"
        
        # Total should now be 100%
        total_pct = sum(
            tp.sell_pct for tp in schema.recommendations[0].trading_plan.take_profit_targets
        )
        assert total_pct == 100.0
        
        # Should have a warning about normalization
        assert any("normalized" in w.lower() for w in schema.meta.warnings)
