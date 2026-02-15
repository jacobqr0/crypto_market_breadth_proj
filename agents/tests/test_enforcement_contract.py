"""
Tests for enforce_recommendations_contract function.

Tests that:
1. Position size > single_asset_limit is auto-reduced
2. No actionable recommendations sets default to do_nothing
3. QA rejection blocks report generation
4. Tier 2+3 allocation limits are warned
5. BTC floor warnings are generated
"""

import pytest
import json

from schemas import (
    RecommendationsSchema,
    PortfolioContextSchema,
    QAReviewSchema,
)
from validation import (
    enforce_recommendations_contract,
    should_block_report_generation,
    get_enforcement_summary,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def valid_portfolio_context():
    """Return a valid portfolio context with typical constraints."""
    return {
        "meta": {
            "agent_name": "portfolio_context",
            "schema_version": "4.0",
            "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
            "data_quality": "ok",
            "warnings": []
        },
        "portfolio_totals": {
            "total_cost_basis_usd": 10000.0,
            "total_current_value_usd": 12000.0,
            "total_realized_pnl_usd": 500.0,
            "drawdown_from_peak_pct": 10.0
        },
        "positions": [
            {
                "symbol": "BTC",
                "quantity": 0.1,
                "avg_cost_usd": 50000.0,
                "total_cost_basis_usd": 5000.0,
                "current_price_usd": 60000.0,
                "current_value_usd": 6000.0,
                "allocation_pct_by_value": 50.0,
                "unrealized_pnl_usd": 1000.0,
                "unrealized_pnl_pct": 20.0,
                "tier": 0
            },
            {
                "symbol": "ETH",
                "quantity": 1.5,
                "avg_cost_usd": 2000.0,
                "total_cost_basis_usd": 3000.0,
                "current_price_usd": 3000.0,
                "current_value_usd": 4500.0,
                "allocation_pct_by_value": 37.5,
                "unrealized_pnl_usd": 1500.0,
                "unrealized_pnl_pct": 50.0,
                "tier": 1
            }
        ],
        "derived": {
            "btc_quantity": 0.1,
            "btc_allocation_pct_by_value": 50.0,
            "tier2_3_allocation_pct_by_value": 0.0,
            "max_single_asset_symbol": "BTC",
            "max_single_asset_allocation_pct_by_value": 50.0
        },
        "framework": {
            "config": {
                "btc_target_min_pct": 40.0,
                "btc_target_max_pct": 60.0,
                "single_asset_limit_pct": 20.0,
                "tier2_3_max_pct": 35.0,
                "allow_100pct_btc_if_no_alts": False
            },
            "checks": {
                "btc_within_target": True,
                "any_position_over_limit": False,
                "positions_over_limit": [],
                "tier2_3_within_limit": True,
                "total_allocations_sum_to_100": True,
                "pricing_complete": True,
                "contradictions_detected": False,
                "contradictions": []
            }
        }
    }


@pytest.fixture
def valid_recommendations():
    """Return valid recommendations with complete trading plan."""
    return {
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
            "key_considerations": ["L2 growth", "Positive ETF flows"]
        },
        "recommendations": [
            {
                "symbol": "SOL",
                "action": "buy",
                "conviction": "high",
                "tier": 1,
                "suggested_allocation_pct_portfolio": 10.0,
                "time_horizon": "6-12m",
                "rationale_one_liner": "SOL shows strong ecosystem growth",
                "rubric": {
                    "problem_solved": "Fast transactions",
                    "network_effects": "Growing DeFi ecosystem",
                    "why_now": "Ecosystem recovery",
                    "invalidation": "Network issues",
                    "vs_doing_nothing": "Higher growth potential",
                    "downside_risks": "Centralization concerns",
                    "portfolio_fit": "Tier 1 large cap",
                    "exit_criteria": "2x target"
                },
                "trading_plan": {
                    "entry_strategy": "Buy at current price",
                    "position_size": "10% of portfolio",
                    "take_profit_targets": [
                        {"target": "$200", "sell_pct": 50},
                        {"target": "$300", "sell_pct": 50}
                    ],
                    "stop_loss": "-20%",
                    "invalidation_trigger": "Network outage > 24h"
                }
            }
        ],
        "default_recommendation": {
            "action": "dca_btc",
            "reason": "Continue regular accumulation"
        }
    }


# =============================================================================
# Test Position Size Auto-Reduction
# =============================================================================

class TestPositionSizeEnforcement:
    """Tests for position size limit enforcement."""
    
    def test_position_size_over_limit_auto_reduced(self, valid_portfolio_context):
        """Position size > single_asset_limit_pct should be auto-reduced."""
        recommendations_data = {
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
                    "symbol": "SOL",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 30.0,  # Over 20% limit!
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "SOL shows strong growth",
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
                        "entry_strategy": "Buy now",
                        "position_size": "30% of portfolio",
                        "take_profit_targets": [
                            {"target": "$200", "sell_pct": 50},
                            {"target": "$300", "sell_pct": 50}
                        ],
                        "stop_loss": "-20%",
                        "invalidation_trigger": "Network outage"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "X"
            }
        }
        
        recommendations = RecommendationsSchema.model_validate(recommendations_data)
        portfolio = PortfolioContextSchema.model_validate(valid_portfolio_context)
        
        enforced, warnings = enforce_recommendations_contract(recommendations, portfolio)
        
        # Allocation should be reduced to 20%
        assert enforced.recommendations[0].suggested_allocation_pct_portfolio == 20.0
        
        # Should have warning about reduction (format: "30.0%" or "30%")
        assert any("reduced" in w.lower() for w in warnings)
        assert any(("30.0%" in w or "30%" in w) and ("20.0%" in w or "20%" in w) for w in warnings)


# =============================================================================
# Test Default Recommendation Logic
# =============================================================================

class TestDefaultRecommendationEnforcement:
    """Tests for default recommendation when no actionable recs."""
    
    def test_no_actionable_sets_default_do_nothing(self, valid_portfolio_context):
        """All WATCH/HOLD recommendations should set default to do_nothing."""
        recommendations_data = {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Test",
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
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "Watching for better entry",
                    "rubric": {
                        "problem_solved": "X",
                        "network_effects": "X",
                        "why_now": "X",
                        "invalidation": "X",
                        "vs_doing_nothing": "X",
                        "downside_risks": "X",
                        "portfolio_fit": "X",
                        "exit_criteria": "X"
                    }
                },
                {
                    "symbol": "BTC",
                    "action": "hold",
                    "conviction": "high",
                    "tier": 0,
                    "time_horizon": "12m+",
                    "rationale_one_liner": "Continue holding core position",
                    "rubric": {
                        "problem_solved": "X",
                        "network_effects": "X",
                        "why_now": "X",
                        "invalidation": "X",
                        "vs_doing_nothing": "X",
                        "downside_risks": "X",
                        "portfolio_fit": "X",
                        "exit_criteria": "X"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Original reason"
            }
        }
        
        recommendations = RecommendationsSchema.model_validate(recommendations_data)
        portfolio = PortfolioContextSchema.model_validate(valid_portfolio_context)
        
        enforced, warnings = enforce_recommendations_contract(recommendations, portfolio)
        
        # Default should be updated to do_nothing
        assert enforced.default_recommendation.action == "do_nothing"
        assert "no" in enforced.default_recommendation.reason.lower() or \
               "actionable" in enforced.default_recommendation.reason.lower()
        
        # Should have warning
        assert any("actionable" in w.lower() for w in warnings)


# =============================================================================
# Test QA Rejection Gating
# =============================================================================

class TestQAGating:
    """Tests for QA rejection gating."""
    
    def test_qa_reject_blocks_report(self):
        """QA overall_status=reject should block report generation."""
        qa_review_data = {
            "meta": {
                "agent_name": "qa_risk",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "overall_status": "reject",
            "recommendations_reviewed": 1,
            "issues_found": 2,
            "compliance_checklist": [
                {
                    "check": "trading_plan_complete",
                    "status": "fail",
                    "notes": "BUY recommendation missing stop_loss"
                },
                {
                    "check": "assets_top_200",
                    "status": "pass",
                    "notes": "All assets in top 200"
                }
            ],
            "per_recommendation": [
                {
                    "symbol": "SOL",
                    "original_action": "buy",
                    "qa_status": "reject",
                    "issues": ["Incomplete trading plan", "Missing stop loss"],
                    "risk": {
                        "correlation_with_portfolio": "medium",
                        "sector_concentration": "Adding to L1 exposure",
                        "conviction": "weak"
                    },
                    "verdict": "reject"
                }
            ],
            "final_verdict": "Rejected due to incomplete trading plan for BUY recommendation"
        }
        
        qa_review = QAReviewSchema.model_validate(qa_review_data)
        
        should_block, reason = should_block_report_generation(qa_review)
        
        assert should_block is True
        assert len(reason) > 0
        assert "trading_plan" in reason.lower() or "stop_loss" in reason.lower()
    
    def test_qa_pass_allows_report(self):
        """QA overall_status=pass should allow report generation."""
        qa_review_data = {
            "meta": {
                "agent_name": "qa_risk",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "overall_status": "pass",
            "recommendations_reviewed": 1,
            "issues_found": 0,
            "compliance_checklist": [
                {
                    "check": "trading_plan_complete",
                    "status": "pass",
                    "notes": "All BUY recommendations have complete trading plans"
                },
                {
                    "check": "assets_top_200",
                    "status": "pass",
                    "notes": "All assets in top 200"
                }
            ],
            "per_recommendation": [
                {
                    "symbol": "SOL",
                    "original_action": "buy",
                    "qa_status": "pass",
                    "issues": [],
                    "risk": {
                        "correlation_with_portfolio": "medium",
                        "sector_concentration": "Diversifies L1 exposure",
                        "conviction": "strong"
                    },
                    "verdict": "proceed"
                }
            ],
            "final_verdict": "All recommendations pass compliance checks"
        }
        
        qa_review = QAReviewSchema.model_validate(qa_review_data)
        
        should_block, reason = should_block_report_generation(qa_review)
        
        assert should_block is False
        assert reason == ""
    
    def test_qa_flag_allows_report(self):
        """QA overall_status=flag should allow report generation (with warnings)."""
        qa_review_data = {
            "meta": {
                "agent_name": "qa_risk",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "overall_status": "flag",
            "recommendations_reviewed": 1,
            "issues_found": 1,
            "compliance_checklist": [
                {
                    "check": "trading_plan_complete",
                    "status": "pass",
                    "notes": "All plans complete"
                },
                {
                    "check": "btc_allocation_40_60",
                    "status": "pass",
                    "notes": "BTC at 41% - near floor but within range"
                }
            ],
            "per_recommendation": [
                {
                    "symbol": "SOL",
                    "original_action": "buy",
                    "qa_status": "flag",
                    "issues": ["BTC allocation near floor after this purchase"],
                    "risk": {
                        "correlation_with_portfolio": "medium",
                        "sector_concentration": "Adding to L1 exposure",
                        "conviction": "adequate"
                    },
                    "verdict": "modify"
                }
            ],
            "final_verdict": "Flagged for human review - BTC allocation near floor"
        }
        
        qa_review = QAReviewSchema.model_validate(qa_review_data)
        
        should_block, reason = should_block_report_generation(qa_review)
        
        assert should_block is False


# =============================================================================
# Test Enforcement Summary
# =============================================================================

class TestEnforcementSummary:
    """Tests for enforcement summary generation."""
    
    def test_summary_counts_downgrades(self, valid_portfolio_context, valid_recommendations):
        """Summary should correctly count downgrades."""
        recommendations = RecommendationsSchema.model_validate(valid_recommendations)
        portfolio = PortfolioContextSchema.model_validate(valid_portfolio_context)
        
        enforced, warnings = enforce_recommendations_contract(recommendations, portfolio)
        
        summary = get_enforcement_summary(recommendations, enforced, warnings)
        
        assert "total_recommendations" in summary
        assert "downgrades_to_watch" in summary
        assert "allocation_reductions" in summary
        assert "actionable_count" in summary
        assert summary["total_recommendations"] == 1
        assert summary["actionable_count"] == 1  # Still a valid BUY


# =============================================================================
# Test BTC Floor Warning
# =============================================================================

class TestBTCFloorWarning:
    """Tests for BTC floor allocation warnings."""
    
    def test_buy_would_push_btc_below_floor_warns(self):
        """BUY that would push BTC below floor should generate warning."""
        # Portfolio with BTC at 42% (near floor)
        portfolio_data = {
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "4.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 10000.0,
                "total_current_value_usd": 10000.0,
                "total_realized_pnl_usd": 0.0
            },
            "positions": [
                {
                    "symbol": "BTC",
                    "quantity": 0.1,
                    "current_price_usd": 42000.0,
                    "current_value_usd": 4200.0,
                    "allocation_pct_by_value": 42.0,  # Near 40% floor
                    "tier": 0
                }
            ],
            "derived": {
                "btc_quantity": 0.1,
                "btc_allocation_pct_by_value": 42.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": "BTC",
                "max_single_asset_allocation_pct_by_value": 42.0
            },
            "framework": {
                "config": {
                    "btc_target_min_pct": 40.0,
                    "btc_target_max_pct": 60.0,
                    "single_asset_limit_pct": 20.0,
                    "tier2_3_max_pct": 35.0,
                    "allow_100pct_btc_if_no_alts": False
                },
                "checks": {
                    "btc_within_target": True,
                    "any_position_over_limit": False,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": True,
                    "pricing_complete": True,
                    "contradictions_detected": False,
                    "contradictions": []
                }
            }
        }
        
        recommendations_data = {
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
                    "suggested_allocation_pct_portfolio": 10.0,  # Would push BTC to 32%!
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH shows strength",
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
                        "entry_strategy": "Buy now",
                        "position_size": "10%",
                        "take_profit_targets": [
                            {"target": "$4k", "sell_pct": 50},
                            {"target": "$5k", "sell_pct": 50}
                        ],
                        "stop_loss": "-15%",
                        "invalidation_trigger": "Protocol hack"
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "X"
            }
        }
        
        recommendations = RecommendationsSchema.model_validate(recommendations_data)
        portfolio = PortfolioContextSchema.model_validate(portfolio_data)
        
        enforced, warnings = enforce_recommendations_contract(recommendations, portfolio)
        
        # Should have warning about BTC floor
        assert any("btc" in w.lower() and "floor" in w.lower() for w in warnings) or \
               any("btc" in w.lower() and "40%" in w.lower() for w in warnings)
