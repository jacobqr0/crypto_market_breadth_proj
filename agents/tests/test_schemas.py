"""
Tests for Pydantic schema definitions.

Tests that:
1. Valid JSON passes for each schema
2. Missing required keys fails validation
3. Non-JSON text fails parse
4. Type validation works (numbers are numbers, etc.)
5. Enum validation works
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from schemas import (
    # Enums
    DataQuality,
    Confidence,
    Trend,
    Signal,
    Regime,
    Action,
    QAStatus,
    # Base
    SchemaMeta,
    Source,
    # Portfolio Context
    PortfolioContextSchema,
    PortfolioTotals,
    Position,
    DerivedMetrics,
    FrameworkConfig,
    FrameworkChecks,
    Framework,
    # Token Research
    TokenResearchSchema,
    UniverseConstraints,
    Universe,
    Thesis,
    AdoptionMetrics,
    Candidate,
    ScoreBreakdown,
    RankedCandidate,
    # Technical Analysis
    TechnicalAnalysisSchema,
    DailyTimeframe,
    WeeklyTimeframe,
    Timeframes,
    BTCRelative,
    KeyLevels,
    AssetTechnical,
    CorrelationPair,
    Breadth,
    # Macro Cycle
    MacroCycleSchema,
    RegimeAssessment,
    MacroFactor,
    MacroFactors,
    CycleAssessment,
    Narrative,
    Implications,
    # Recommendations
    RecommendationsSchema,
    MarketContext,
    TakeProfitTarget,
    TradingPlan,
    Rubric,
    Dependencies,
    Recommendation,
    DefaultRecommendation,
    # QA Review
    QAReviewSchema,
    ComplianceCheck,
    RiskAssessment,
    PerRecommendationReview,
)


class TestSchemaMeta:
    """Tests for the base SchemaMeta model."""
    
    def test_valid_meta(self):
        """Valid meta object should pass."""
        meta = SchemaMeta(
            agent_name="test_agent",
            schema_version="1.0",
            as_of_timestamp_utc="2026-01-18T12:00:00Z",
            data_quality=DataQuality.OK,
            warnings=[]
        )
        assert meta.agent_name == "test_agent"
        assert meta.data_quality == "ok"
    
    def test_meta_with_warnings(self):
        """Meta with warnings should work."""
        meta = SchemaMeta(
            agent_name="test_agent",
            schema_version="1.0",
            as_of_timestamp_utc="2026-01-18T12:00:00Z",
            data_quality=DataQuality.PARTIAL,
            warnings=["Missing price for ETH", "Data may be stale"]
        )
        assert len(meta.warnings) == 2
        assert meta.data_quality == "partial"
    
    def test_meta_missing_required(self):
        """Missing required field should fail."""
        with pytest.raises(ValidationError) as exc_info:
            SchemaMeta(
                schema_version="1.0",
                as_of_timestamp_utc="2026-01-18T12:00:00Z",
                data_quality=DataQuality.OK,
            )
        assert "agent_name" in str(exc_info.value)
    
    def test_meta_invalid_data_quality(self):
        """Invalid data_quality enum should fail."""
        with pytest.raises(ValidationError):
            SchemaMeta(
                agent_name="test",
                schema_version="1.0",
                as_of_timestamp_utc="2026-01-18T12:00:00Z",
                data_quality="invalid_value",
            )


class TestPortfolioContextSchema:
    """Tests for the PortfolioContextSchema."""
    
    @pytest.fixture
    def valid_portfolio_context(self):
        """Return a valid portfolio context dict."""
        return {
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 10000.00,
                "total_current_value_usd": 12000.00,
                "total_realized_pnl_usd": 500.00,
                "drawdown_from_peak_pct": None
            },
            "positions": [
                {
                    "symbol": "BTC",
                    "quantity": 0.5,
                    "avg_cost_usd": 20000.00,
                    "total_cost_basis_usd": 10000.00,
                    "current_price_usd": 24000.00,
                    "current_value_usd": 12000.00,
                    "allocation_pct_by_value": 100.0,
                    "unrealized_pnl_usd": 2000.00,
                    "unrealized_pnl_pct": 20.0,
                    "tier": 0
                }
            ],
            "derived": {
                "btc_quantity": 0.5,
                "btc_allocation_pct_by_value": 100.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": "BTC",
                "max_single_asset_allocation_pct_by_value": 100.0
            },
            "framework": {
                "config": {
                    "btc_target_min_pct": 40,
                    "btc_target_max_pct": 60,
                    "single_asset_limit_pct": 20,
                    "tier2_3_max_pct": 35,
                    "allow_100pct_btc_if_no_alts": False
                },
                "checks": {
                    "btc_within_target": False,
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
    
    def test_valid_portfolio_context(self, valid_portfolio_context):
        """Valid portfolio context should parse correctly."""
        schema = PortfolioContextSchema.model_validate(valid_portfolio_context)
        assert schema.meta.agent_name == "portfolio_context"
        assert schema.portfolio_totals.total_cost_basis_usd == 10000.00
        assert len(schema.positions) == 1
        assert schema.positions[0].symbol == "BTC"
        assert schema.positions[0].tier == 0
    
    def test_portfolio_missing_meta(self, valid_portfolio_context):
        """Missing meta should fail."""
        del valid_portfolio_context["meta"]
        with pytest.raises(ValidationError) as exc_info:
            PortfolioContextSchema.model_validate(valid_portfolio_context)
        assert "meta" in str(exc_info.value)
    
    def test_portfolio_invalid_tier(self, valid_portfolio_context):
        """Invalid tier value should fail."""
        valid_portfolio_context["positions"][0]["tier"] = 5  # Invalid
        with pytest.raises(ValidationError):
            PortfolioContextSchema.model_validate(valid_portfolio_context)
    
    def test_portfolio_nullable_fields(self, valid_portfolio_context):
        """Nullable fields should accept null."""
        valid_portfolio_context["portfolio_totals"]["total_current_value_usd"] = None
        valid_portfolio_context["portfolio_totals"]["drawdown_from_peak_pct"] = None
        schema = PortfolioContextSchema.model_validate(valid_portfolio_context)
        assert schema.portfolio_totals.total_current_value_usd is None


class TestTokenResearchSchema:
    """Tests for the TokenResearchSchema."""
    
    @pytest.fixture
    def valid_token_research(self):
        """Return valid token research dict."""
        return {
            "meta": {
                "agent_name": "token_research",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "universe": {
                "constraints": {
                    "max_mcap_rank": 200,
                    "exclude_memecoins": True
                }
            },
            "candidates": [
                {
                    "symbol": "ETH",
                    "name": "Ethereum",
                    "mcap_rank": 2,
                    "category": "L1",
                    "thesis": {
                        "problem": "Smart contract platform",
                        "why_it_wins": "Network effects and ecosystem",
                        "network_effects": "Largest developer community"
                    },
                    "adoption_metrics": {
                        "tvl_usd": 50000000000,
                        "tvl_change_90d_pct": 15.5,
                        "fees_30d_usd": 100000000,
                        "revenue_30d_usd": 50000000,
                        "dau": 500000,
                        "tx_count_30d": 30000000
                    },
                    "catalysts": ["EIP-4844", "L2 growth"],
                    "risks": ["Scalability", "Regulation"],
                    "sources": [
                        {"name": "DefiLlama", "type": "dashboard", "ref": "https://defillama.com", "as_of": "2026-01-18"}
                    ],
                    "tier_suggestion": 1,
                    "confidence": "high"
                }
            ],
            "ranked_shortlist": [
                {
                    "symbol": "ETH",
                    "score": 8.5,
                    "score_breakdown": {
                        "adoption": 9.0,
                        "moat": 8.5,
                        "catalyst": 7.5,
                        "risk": 8.5
                    }
                }
            ]
        }
    
    def test_valid_token_research(self, valid_token_research):
        """Valid token research should parse correctly."""
        schema = TokenResearchSchema.model_validate(valid_token_research)
        assert schema.meta.agent_name == "token_research"
        assert len(schema.candidates) == 1
        assert schema.candidates[0].symbol == "ETH"
        assert schema.candidates[0].confidence == "high"
    
    def test_score_validation(self, valid_token_research):
        """Scores must be between 0 and 10."""
        valid_token_research["ranked_shortlist"][0]["score"] = 15  # Invalid
        with pytest.raises(ValidationError):
            TokenResearchSchema.model_validate(valid_token_research)
    
    def test_confidence_enum(self, valid_token_research):
        """Confidence must be a valid enum value."""
        valid_token_research["candidates"][0]["confidence"] = "very_high"  # Invalid
        with pytest.raises(ValidationError):
            TokenResearchSchema.model_validate(valid_token_research)


class TestTechnicalAnalysisSchema:
    """Tests for the TechnicalAnalysisSchema."""
    
    @pytest.fixture
    def valid_technical_analysis(self):
        """Return valid technical analysis dict."""
        return {
            "meta": {
                "agent_name": "technical_analysis",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "assets": [
                {
                    "symbol": "ETH",
                    "timeframes": {
                        "d1": {
                            "sma_50": 3200.50,
                            "sma_200": 2800.25,
                            "rsi_14": 55.5,
                            "pct_change_7d": 5.2,
                            "pct_change_30d": 12.8
                        },
                        "w1": {
                            "sma_20": 3100.00,
                            "rsi_14": 52.0,
                            "pct_change_12w": 25.5
                        }
                    },
                    "trend": "bullish",
                    "signal": "bullish",
                    "btc_relative": {
                        "pair": "ETH/BTC",
                        "trend": "outperforming",
                        "pct_change_30d_vs_btc": 3.5
                    },
                    "key_levels": {
                        "support": [3000, 2800],
                        "resistance": [3500, 4000]
                    }
                }
            ],
            "breadth": {
                "universe": "top_100",
                "pct_above_200d": 65.0,
                "pct_golden_cross": 45.0,
                "median_rsi_14": 52.5,
                "correlation": [
                    {"pair": "BTC-ETH", "corr_90d": 0.85}
                ]
            }
        }
    
    def test_valid_technical_analysis(self, valid_technical_analysis):
        """Valid technical analysis should parse correctly."""
        schema = TechnicalAnalysisSchema.model_validate(valid_technical_analysis)
        assert schema.meta.agent_name == "technical_analysis"
        assert len(schema.assets) == 1
        assert schema.assets[0].trend == "bullish"
        assert schema.assets[0].btc_relative.trend == "outperforming"
    
    def test_correlation_bounds(self, valid_technical_analysis):
        """Correlation must be between -1 and 1."""
        valid_technical_analysis["breadth"]["correlation"][0]["corr_90d"] = 1.5  # Invalid
        with pytest.raises(ValidationError):
            TechnicalAnalysisSchema.model_validate(valid_technical_analysis)


class TestMacroCycleSchema:
    """Tests for the MacroCycleSchema."""
    
    @pytest.fixture
    def valid_macro_cycle(self):
        """Return valid macro cycle dict."""
        return {
            "meta": {
                "agent_name": "macro_cycle",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "regime": {
                "stance": "risk_on",
                "confidence": "high"
            },
            "macro": {
                "liquidity": {
                    "summary": "Global liquidity expanding",
                    "signals": ["M2 growth", "Fed balance sheet"]
                },
                "fed_policy": {
                    "summary": "Dovish stance",
                    "signals": ["Rate cuts expected"]
                },
                "inflation": {
                    "summary": "Trending down",
                    "signals": ["CPI falling"]
                },
                "risk_appetite": {
                    "summary": "Elevated",
                    "signals": ["VIX low", "Credit spreads tight"]
                }
            },
            "cycle": {
                "stage": "mid",
                "evidence": ["18 months post-halving"],
                "halving_context": "Mid-cycle typically bullish"
            },
            "narratives": [
                {
                    "name": "AI",
                    "momentum": "rising",
                    "substance": "high",
                    "notes": "Real usage growth"
                }
            ],
            "implications": {
                "favor": ["Large-cap L1s", "AI tokens"],
                "avoid": ["High-leverage positions"]
            },
            "sources": []
        }
    
    def test_valid_macro_cycle(self, valid_macro_cycle):
        """Valid macro cycle should parse correctly."""
        schema = MacroCycleSchema.model_validate(valid_macro_cycle)
        assert schema.meta.agent_name == "macro_cycle"
        assert schema.regime.stance == "risk_on"
        assert schema.cycle.stage == "mid"
    
    def test_regime_enum(self, valid_macro_cycle):
        """Regime stance must be valid enum."""
        valid_macro_cycle["regime"]["stance"] = "bullish"  # Invalid
        with pytest.raises(ValidationError):
            MacroCycleSchema.model_validate(valid_macro_cycle)


class TestRecommendationsSchema:
    """Tests for the RecommendationsSchema."""
    
    @pytest.fixture
    def valid_recommendations(self):
        """Return valid recommendations dict."""
        return {
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "executive_summary": "Market conditions favor selective buying.",
            "market_context": {
                "macro_regime": "risk_on",
                "technical_env": "bullish",
                "key_considerations": ["Fed dovish", "BTC above 200d"]
            },
            "recommendations": [
                {
                    "symbol": "ETH",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "suggested_allocation_pct_monthly_budget": 25.0,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "ETH benefits from L2 growth with strongest DeFi ecosystem",
                    "rubric": {
                        "problem_solved": "Smart contract platform",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth accelerating",
                        "invalidation": "Major hack or regulatory action",
                        "vs_doing_nothing": "Strong risk-adjusted returns",
                        "downside_risks": "Execution risk on upgrades",
                        "portfolio_fit": "Core Tier 1 holding",
                        "exit_criteria": "Take profit at 2x"
                    },
                    "trading_plan": {
                        "entry_strategy": "Buy at current price ($3,200)",
                        "position_size": "5% of portfolio ($500)",
                        "take_profit_targets": [
                            {"target": "$4,000", "sell_pct": 50},
                            {"target": "$5,000", "sell_pct": 50}
                        ],
                        "stop_loss": "-15% from entry",
                        "invalidation_trigger": "Major protocol hack"
                    },
                    "dependencies": {
                        "requires": [],
                        "data_used": ["technical_analysis", "token_research"]
                    }
                }
            ],
            "default_recommendation": {
                "action": "dca_btc",
                "reason": "Maintain BTC allocation during risk-on"
            }
        }
    
    def test_valid_recommendations(self, valid_recommendations):
        """Valid recommendations should parse correctly."""
        schema = RecommendationsSchema.model_validate(valid_recommendations)
        assert schema.meta.agent_name == "orchestrator"
        assert len(schema.recommendations) == 1
        assert schema.recommendations[0].action == "buy"
    
    def test_action_enum(self, valid_recommendations):
        """Action must be valid enum."""
        valid_recommendations["recommendations"][0]["action"] = "acquire"  # Invalid
        with pytest.raises(ValidationError):
            RecommendationsSchema.model_validate(valid_recommendations)
    
    def test_tier_literal(self, valid_recommendations):
        """Tier must be 0, 1, 2, or 3."""
        valid_recommendations["recommendations"][0]["tier"] = 4  # Invalid
        with pytest.raises(ValidationError):
            RecommendationsSchema.model_validate(valid_recommendations)


class TestQAReviewSchema:
    """Tests for the QAReviewSchema."""
    
    @pytest.fixture
    def valid_qa_review(self):
        """Return valid QA review dict."""
        return {
            "meta": {
                "agent_name": "qa_risk",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "overall_status": "pass",
            "recommendations_reviewed": 1,
            "issues_found": 0,
            "compliance_checklist": [
                {
                    "check": "assets_top_200",
                    "status": "pass",
                    "notes": "All assets within top 200"
                },
                {
                    "check": "btc_allocation_40_60",
                    "status": "pass",
                    "notes": "BTC at 55%"
                }
            ],
            "per_recommendation": [
                {
                    "symbol": "ETH",
                    "original_action": "buy",
                    "qa_status": "pass",
                    "issues": [],
                    "risk": {
                        "correlation_with_portfolio": "medium",
                        "sector_concentration": "L1 exposure acceptable",
                        "conviction": "strong"
                    },
                    "verdict": "proceed"
                }
            ],
            "final_verdict": "Recommendations approved for execution."
        }
    
    def test_valid_qa_review(self, valid_qa_review):
        """Valid QA review should parse correctly."""
        schema = QAReviewSchema.model_validate(valid_qa_review)
        assert schema.meta.agent_name == "qa_risk"
        assert schema.overall_status == "pass"
        assert len(schema.compliance_checklist) == 2
    
    def test_overall_status_enum(self, valid_qa_review):
        """Overall status must be valid enum."""
        valid_qa_review["overall_status"] = "approved"  # Invalid
        with pytest.raises(ValidationError):
            QAReviewSchema.model_validate(valid_qa_review)
    
    def test_counts_non_negative(self, valid_qa_review):
        """Counts must be non-negative."""
        valid_qa_review["issues_found"] = -1  # Invalid
        with pytest.raises(ValidationError):
            QAReviewSchema.model_validate(valid_qa_review)
