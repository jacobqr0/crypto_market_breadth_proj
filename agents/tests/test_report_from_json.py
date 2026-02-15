"""
Tests for JSON-based report generation.

Tests that:
1. Report renders correctly from valid JSON
2. Missing JSON produces fallback content
3. Invalid JSON produces error report
4. All section renderers work correctly
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

from agents.utils.report_generator import (
    generate_investment_report,
    generate_error_report,
    _render_macro_section,
    _render_technical_section,
    _render_token_research_section,
    _render_portfolio_section,
    _render_recommendations_section,
    _render_qa_section,
    _try_parse_json,
)


class TestJsonParsing:
    """Tests for JSON parsing utility."""
    
    def test_parse_valid_json(self):
        """Valid JSON should parse correctly."""
        result = _try_parse_json('{"key": "value"}')
        assert result == {"key": "value"}
    
    def test_parse_json_with_fence(self):
        """JSON with markdown fence should parse."""
        result = _try_parse_json('```json\n{"key": "value"}\n```')
        assert result == {"key": "value"}
    
    def test_parse_invalid_json(self):
        """Invalid JSON should return None."""
        result = _try_parse_json("not valid json")
        assert result is None
    
    def test_parse_empty(self):
        """Empty string should return None."""
        result = _try_parse_json("")
        assert result is None


class TestMacroSectionRenderer:
    """Tests for macro section rendering."""
    
    @pytest.fixture
    def macro_data(self):
        return {
            "meta": {
                "agent_name": "macro_cycle",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z"
            },
            "regime": {
                "stance": "risk_on",
                "confidence": "high"
            },
            "macro": {
                "liquidity": {
                    "summary": "Expanding liquidity",
                    "signals": ["M2 growth", "Fed balance sheet"]
                },
                "fed_policy": {
                    "summary": "Dovish",
                    "signals": ["Rate cuts expected"]
                },
                "inflation": {
                    "summary": "Trending down",
                    "signals": ["CPI falling"]
                },
                "risk_appetite": {
                    "summary": "Elevated",
                    "signals": ["VIX low"]
                }
            },
            "cycle": {
                "stage": "mid",
                "evidence": ["18 months post-halving"],
                "halving_context": "Mid-cycle"
            },
            "narratives": [
                {
                    "name": "AI",
                    "momentum": "rising",
                    "substance": "high",
                    "notes": "Real growth"
                }
            ],
            "implications": {
                "favor": ["L1s", "AI tokens"],
                "avoid": ["Leverage"]
            }
        }
    
    def test_render_macro_section(self, macro_data):
        """Macro section should render correctly."""
        result = _render_macro_section(macro_data)
        
        assert "RISK-ON" in result
        assert "Expanding liquidity" in result
        assert "Dovish" in result
        assert "mid" in result.lower()
        assert "AI" in result


class TestTechnicalSectionRenderer:
    """Tests for technical section rendering."""
    
    @pytest.fixture
    def technical_data(self):
        return {
            "meta": {
                "agent_name": "technical_analysis",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z"
            },
            "assets": [
                {
                    "symbol": "ETH",
                    "timeframes": {
                        "d1": {
                            "sma_50": 3200,
                            "sma_200": 2800,
                            "rsi_14": 55,
                            "pct_change_7d": 5.0,
                            "pct_change_30d": 12.0
                        },
                        "w1": {
                            "sma_20": 3100,
                            "rsi_14": 52,
                            "pct_change_12w": 25
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
                "pct_above_200d": 65,
                "pct_golden_cross": 45,
                "median_rsi_14": 52,
                "correlation": [
                    {"pair": "BTC-ETH", "corr_90d": 0.85}
                ]
            }
        }
    
    def test_render_technical_section(self, technical_data):
        """Technical section should render correctly."""
        result = _render_technical_section(technical_data)
        
        assert "ETH" in result
        assert "bullish" in result
        assert "outperforming" in result
        assert "65" in result  # pct_above_200d


class TestPortfolioSectionRenderer:
    """Tests for portfolio section rendering."""
    
    @pytest.fixture
    def portfolio_data(self):
        return {
            "meta": {
                "agent_name": "portfolio_context",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 10000,
                "total_current_value_usd": 12000,
                "total_realized_pnl_usd": 500,
                "drawdown_from_peak_pct": None
            },
            "positions": [
                {
                    "symbol": "BTC",
                    "quantity": 0.5,
                    "avg_cost_usd": 20000,
                    "total_cost_basis_usd": 10000,
                    "current_price_usd": 24000,
                    "current_value_usd": 12000,
                    "allocation_pct_by_value": 100,
                    "unrealized_pnl_usd": 2000,
                    "unrealized_pnl_pct": 20,
                    "tier": 0
                }
            ],
            "derived": {
                "btc_quantity": 0.5,
                "btc_allocation_pct_by_value": 100,
                "tier2_3_allocation_pct_by_value": 0,
                "max_single_asset_symbol": "BTC",
                "max_single_asset_allocation_pct_by_value": 100
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
    
    def test_render_portfolio_section(self, portfolio_data):
        """Portfolio section should render correctly."""
        result = _render_portfolio_section(portfolio_data)
        
        assert "BTC" in result
        assert "10,000" in result or "10000" in result
        assert "12,000" in result or "12000" in result
        assert "Tier 0" in result
        assert "100.0%" in result


class TestRecommendationsSectionRenderer:
    """Tests for recommendations section rendering."""
    
    @pytest.fixture
    def recommendations_data(self):
        return {
            "meta": {
                "agent_name": "orchestrator",
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
                    "rubric": {
                        "problem_solved": "Smart contract platform",
                        "network_effects": "Largest ecosystem",
                        "why_now": "L2 growth accelerating",
                        "invalidation": "Major hack",
                        "vs_doing_nothing": "Strong returns",
                        "downside_risks": "Execution risk",
                        "portfolio_fit": "Core Tier 1 holding",
                        "exit_criteria": "Take profit at 2x"
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
                "reason": "Maintain BTC allocation"
            }
        }
    
    def test_render_recommendations_section(self, recommendations_data):
        """Recommendations section should render correctly."""
        result = _render_recommendations_section(recommendations_data)
        
        assert "ETH" in result
        assert "BUY" in result
        assert "high" in result.lower()
        assert "Trading Plan" in result
        assert "$4,000" in result
        assert "Smart contract platform" in result


class TestQASectionRenderer:
    """Tests for QA section rendering."""
    
    @pytest.fixture
    def qa_data(self):
        return {
            "meta": {
                "agent_name": "qa_risk",
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
                        "sector_concentration": "Acceptable",
                        "conviction": "strong"
                    },
                    "verdict": "proceed"
                }
            ],
            "final_verdict": "Recommendations approved."
        }
    
    def test_render_qa_section(self, qa_data):
        """QA section should render correctly."""
        result = _render_qa_section(qa_data)
        
        assert "PASS" in result
        assert "ETH" in result
        assert "proceed" in result.lower()
        assert "Recommendations approved" in result


class TestErrorReportGeneration:
    """Tests for error report generation."""
    
    def test_generate_error_report(self):
        """Error report should be generated correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validation_errors = [
                {
                    "task": "portfolio_context",
                    "errors": ["Missing field: meta", "Invalid type"]
                },
                {
                    "task": "orchestration",
                    "errors": ["JSON parse error"]
                }
            ]
            
            task_outputs = {
                "portfolio_context": "invalid output here",
                "orchestration": "also invalid"
            }
            
            # Mock the db_path since we don't need audit
            report_path = generate_error_report(
                validation_errors=validation_errors,
                task_outputs=task_outputs,
                reports_dir=tmpdir,
            )
            
            assert Path(report_path).exists()
            
            content = Path(report_path).read_text()
            assert "RUN FAILED" in content
            assert "portfolio_context" in content
            assert "Missing field: meta" in content
            assert "JSON parse error" in content


class TestFullReportGeneration:
    """Tests for full report generation with validated outputs (v5.0 format)."""
    
    @pytest.fixture
    def validated_outputs_complete(self):
        """Return a complete set of validated output dicts for v5.0 report."""
        return {
            "portfolio_context": {
                "meta": {
                    "agent_name": "portfolio_context",
                    "schema_version": "4.0",
                    "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                    "data_quality": "ok",
                    "warnings": []
                },
                "portfolio_totals": {
                    "total_cost_basis_usd": 10000,
                    "total_current_value_usd": 12000,
                    "total_realized_pnl_usd": 0,
                    "drawdown_from_peak_pct": None
                },
                "positions": [{"symbol": "BTC", "tier": 0, "quantity": 0.1, "allocation_pct_by_value": 50}],
                "derived": {
                    "btc_quantity": 0.1,
                    "btc_allocation_pct_by_value": 50.0,
                    "tier2_3_allocation_pct_by_value": 0,
                    "max_single_asset_symbol": "BTC",
                    "max_single_asset_allocation_pct_by_value": 50.0
                },
                "framework": {
                    "config": {"btc_target_min_pct": 40, "btc_target_max_pct": 60, "single_asset_limit_pct": 20, "tier2_3_max_pct": 35},
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
            },
            "macro_analysis": {
                "meta": {"agent_name": "macro_cycle", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
                "regime": {"stance": "risk_on", "confidence": "high"},
                "macro": {
                    "liquidity": {"summary": "Expanding", "signals": []},
                    "fed_policy": {"summary": "Dovish", "signals": []},
                    "inflation": {"summary": "Low", "signals": []},
                    "risk_appetite": {"summary": "High", "signals": []}
                },
                "cycle": {"stage": "mid", "evidence": [], "halving_context": None},
                "narratives": [],
                "implications": {"favor": [], "avoid": []},
                "sources": []
            },
            "orchestration": {
                "meta": {"agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
                "executive_summary": "Risk-on environment",
                "market_context": {"macro_regime": "risk_on", "technical_env": "bullish", "key_considerations": []},
                "recommendations": [{"symbol": "BTC", "action": "hold", "conviction": "high", "tier": 0, "time_horizon": "12m+", "rationale_one_liner": "Hold position", "rubric": {"problem_solved": "Store of value", "network_effects": "Largest", "why_now": "Macro", "invalidation": "None", "vs_doing_nothing": "Growth", "downside_risks": "Volatility", "portfolio_fit": "Core", "exit_criteria": "None"}}],
                "default_recommendation": {"action": "hold_btc", "reason": "Maintain allocation"}
            },
            "qa_risk": {
                "meta": {"agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
                "overall_status": "pass",
                "recommendations_reviewed": 1,
                "issues_found": 0,
                "compliance_checklist": [],
                "per_recommendation": [],
                "final_verdict": "Approved"
            }
        }
    
    def test_report_generation_with_validated_outputs(self, validated_outputs_complete):
        """Report should use validated outputs and show new v5.0 format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Mock the db connection to avoid actual DB operations
            with patch('agents.utils.report_generator.get_db_connection'):
                with patch('agents.utils.report_generator._record_audit'):
                    report_path = generate_investment_report(
                        validated_outputs=validated_outputs_complete,
                        prompt_versions={"test": "1.0"},
                        reports_dir=tmpdir,
                    )
                    
                    assert Path(report_path).exists()
                    
                    content = Path(report_path).read_text()
                    # v5.0 format checks
                    assert "Investment Review" in content
                    assert "ONE-PAGE ACTION PLAN" in content
                    assert "DECISION PACKET" in content
                    assert "EVIDENCE APPENDIX" in content
                    assert "RISK-ON" in content  # From macro section
                    assert "Actionability" in content
    
    def test_report_missing_critical_inputs_not_actionable(self):
        """Report should be NOT ACTIONABLE when critical inputs missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('agents.utils.report_generator.get_db_connection'):
                with patch('agents.utils.report_generator._record_audit'):
                    # Missing orchestration and qa_risk - critical inputs
                    validated_outputs = {
                        "macro_analysis": {
                            "meta": {"agent_name": "macro", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
                            "regime": {"stance": "risk_on", "confidence": "high"}
                        }
                    }
                    
                    report_path = generate_investment_report(
                        validated_outputs=validated_outputs,
                        prompt_versions={},
                        reports_dir=tmpdir,
                    )
                    
                    content = Path(report_path).read_text()
                    # v5.0: Report should be NOT ACTIONABLE
                    assert "NOT ACTIONABLE" in content
                    # Missing data should be noted
                    assert "MISSING" in content or "missing" in content
