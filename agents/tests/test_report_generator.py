"""
Tests for the report generator utilities.

Tests both the legacy report generator functions and the new v5.0 
professional report structure with:
- One-page Action Plan (always present)
- Decision Packet (recommendations table, execution plans)
- Evidence Appendix (with stable anchors)
- Fail-fast behavior for invalid inputs
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

from agents.utils.report_generator import (
    generate_investment_report,
    _generate_markdown,
    _generate_json,
    _build_report_content,
    _check_input_validity,
    _render_action_plan,
    _render_recommendations_table,
    _render_execution_plans,
    _render_do_nothing_justification,
    _render_metadata_header,
    _render_macro_appendix,
    _render_technical_appendix,
    _render_research_appendix,
    _render_portfolio_appendix,
    _generate_markdown_v2,
)
from agents.utils.meta_report_generator import (
    generate_meta_learning_report,
    _parse_analysis_output,
)


# Sample crew output for testing
SAMPLE_CREW_OUTPUT = """
## Executive Summary
This is a test executive summary with investment recommendations.

## Market Regime
The current market regime is RISK-ON with favorable conditions.

## Technical Analysis
Bitcoin shows bullish momentum with price above 50-day SMA.

## Token Research
Ethereum demonstrates strong fundamentals with growing TVL.

## Portfolio Context
Current holdings: 50% BTC, 30% ETH, 20% cash.

## Recommendations
- BUY: More BTC at current levels
- HOLD: ETH position

## Risks
- Regulatory uncertainty
- Macro headwinds
"""


class MockCrewOutput:
    """Mock crew output object."""
    def __init__(self, raw_content: str):
        self.raw = raw_content


class TestBuildReportContent:
    """Tests for _build_report_content function."""
    
    def test_build_with_task_outputs(self):
        """Test building report content from task outputs."""
        task_outputs = {
            "token_research": "Token research output",
            "technical_analysis": "Technical analysis output",
            "macro_analysis": "Macro analysis output",
            "portfolio_context": "Portfolio context output",
            "orchestration": SAMPLE_CREW_OUTPUT,
            "qa_risk": "QA review output",
        }
        
        result = _build_report_content(
            crew_output=MockCrewOutput(SAMPLE_CREW_OUTPUT),
            task_outputs=task_outputs,
            validated_outputs={},
        )
        
        assert "raw_output" in result
        assert "executive_summary" in result
    
    def test_build_with_validated_outputs(self):
        """Test that validated outputs are preferred over raw."""
        task_outputs = {
            "orchestration": SAMPLE_CREW_OUTPUT,
        }
        
        validated_outputs = {
            "orchestration": {
                "meta": {"agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
                "executive_summary": "Validated summary",
                "market_context": {"macro_regime": "risk_on", "technical_env": "bullish", "key_considerations": []},
                "recommendations": [],
                "default_recommendation": {"action": "hold_btc", "reason": "Test"}
            }
        }
        
        result = _build_report_content(
            crew_output=MockCrewOutput(SAMPLE_CREW_OUTPUT),
            task_outputs=task_outputs,
            validated_outputs=validated_outputs,
        )
        
        # Should use validated executive summary
        assert result["executive_summary"] == "Validated summary"


class TestGenerateMarkdown:
    """Tests for _generate_markdown function."""
    
    def test_generate_markdown_structure(self):
        """Test that markdown has correct structure."""
        report_content = {
            "executive_summary": "Test summary",
            "market_regime": "Risk-On",
            "technical_overview": "Bullish",
            "token_research": "ETH looks good",
            "portfolio_context": "50% BTC",
            "recommendations": "Buy more",
            "risks_and_watch_items": "Regulation",
        }
        
        prompt_versions = {
            "token_research": "1.0.0",
            "technical_analyst": "1.0.0",
        }
        
        markdown = _generate_markdown(
            report_content=report_content,
            timestamp="2026-01-11 10:00:00",
            prompt_versions=prompt_versions,
            report_id="test-123",
            qa_blocked=False,
            qa_block_reason="",
        )
        
        # Check structure
        assert "# Investment Review Report" in markdown
        assert "## Executive Summary" in markdown
        assert "## Market Regime Snapshot" in markdown
        assert "## Approved Recommendations" in markdown
        assert "test-123" in markdown
        assert "1.0.0" in markdown
    
    def test_generate_markdown_qa_blocked(self):
        """Test that QA blocked report has correct markers."""
        report_content = {
            "executive_summary": "Test summary",
            "recommendations": "Buy more",
        }
        
        markdown = _generate_markdown(
            report_content=report_content,
            timestamp="2026-01-11 10:00:00",
            prompt_versions={},
            report_id="test-blocked",
            qa_blocked=True,
            qa_block_reason="Incomplete trading plan",
        )
        
        # Check QA blocked markers
        assert "NOT ACTIONABLE" in markdown
        assert "QA REJECTED" in markdown
        assert "Incomplete trading plan" in markdown


class TestGenerateJson:
    """Tests for _generate_json function."""
    
    def test_generate_json_structure(self):
        """Test that JSON has correct structure."""
        report_content = {
            "executive_summary": "Test summary",
            "raw_output": "Full output",
            "validated_json": {},
        }
        
        prompt_versions = {
            "token_research": "1.0.0",
        }
        
        json_content = _generate_json(
            report_content=report_content,
            validated_outputs={},
            timestamp="2026-01-11 10:00:00",
            prompt_versions=prompt_versions,
            report_id="test-456",
            qa_blocked=False,
            qa_block_reason="",
        )
        
        assert json_content["report_id"] == "test-456"
        assert json_content["report_type"] == "investment_review"
        assert "sections" in json_content
        assert "prompt_versions" in json_content
        assert json_content["metadata"]["human_approval_required"] is True
        assert json_content["metadata"]["actionable"] is True
    
    def test_generate_json_qa_blocked(self):
        """Test that QA blocked report has actionable=False."""
        report_content = {
            "executive_summary": "Test summary",
            "validated_json": {},
        }
        
        json_content = _generate_json(
            report_content=report_content,
            validated_outputs={},
            timestamp="2026-01-11 10:00:00",
            prompt_versions={},
            report_id="test-blocked",
            qa_blocked=True,
            qa_block_reason="Missing stop loss",
        )
        
        assert json_content["metadata"]["actionable"] is False
        assert json_content["metadata"]["qa_blocked"] is True
        assert json_content["metadata"]["qa_block_reason"] == "Missing stop loss"


class TestGenerateInvestmentReport:
    """Integration tests for generate_investment_report."""
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories."""
        reports_dir = Path(tempfile.mkdtemp())
        db_dir = Path(tempfile.mkdtemp())
        db_path = db_dir / "test.duckdb"
        
        # Create database with schema
        import duckdb
        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_investment_report (
                report_id VARCHAR PRIMARY KEY,
                report_path VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                token_screener_prompt_version VARCHAR,
                fundamentals_analyst_prompt_version VARCHAR,
                research_synthesizer_prompt_version VARCHAR,
                token_research_prompt_version VARCHAR,
                technical_analyst_prompt_version VARCHAR,
                macro_cycle_prompt_version VARCHAR,
                portfolio_context_prompt_version VARCHAR,
                orchestrator_prompt_version VARCHAR,
                qa_risk_prompt_version VARCHAR
            )
        """)
        conn.close()
        
        yield reports_dir, str(db_path)
        
        shutil.rmtree(reports_dir)
        shutil.rmtree(db_dir)
    
    def test_generate_and_save_report(self, temp_dirs):
        """Test generating and saving a complete report."""
        reports_dir, db_path = temp_dirs
        
        prompt_versions = {
            "token_screener": "1.0.0",
            "fundamentals_analyst": "1.0.0",
            "research_synthesizer": "1.0.0",
            "token_research": "1.0.0",  # Legacy, kept for backward compatibility
            "technical_analyst": "1.0.0",
            "macro_cycle": "1.0.0",
            "portfolio_context": "1.0.0",
            "orchestrator": "1.0.0",
            "qa_risk": "1.0.0",
        }
        
        report_path = generate_investment_report(
            crew_output=SAMPLE_CREW_OUTPUT,
            prompt_versions=prompt_versions,
            db_path=db_path,
            reports_dir=str(reports_dir),
        )
        
        # Check files were created
        assert Path(report_path).exists()
        
        # Check markdown content (v5.0 format: "# Report: YYYY-MM-DD Investment Review")
        md_content = Path(report_path).read_text()
        assert "Investment Review" in md_content
        assert "ONE-PAGE ACTION PLAN" in md_content  # New v5.0 section
        
        # Check JSON file
        json_path = report_path.replace(".md", ".json")
        assert Path(json_path).exists()
        
        with open(json_path) as f:
            json_data = json.load(f)
        assert json_data["report_type"] == "investment_review"


class TestMetaReportGenerator:
    """Tests for meta-learning report generator."""
    
    def test_parse_analysis_output(self):
        """Test parsing post-mortem analysis output."""
        sample_output = """
        ## Executive Summary
        The system performed well but showed bias toward bullish signals.
        
        ## Logic Drift
        Agents were too optimistic about altcoin performance.
        
        ## Evolution Recommendations
        1. Add more bearish indicators
        2. Weight BTC-relative performance higher
        """
        
        result = _parse_analysis_output(sample_output)
        
        assert "executive_summary" in result
        assert "logic_drift" in result
        assert "evolution_recommendations" in result
    
    @pytest.fixture
    def temp_meta_dirs(self):
        """Create temporary directories for meta reports."""
        reports_dir = Path(tempfile.mkdtemp())
        meta_dir = reports_dir / "meta-learning"
        meta_dir.mkdir()
        
        db_dir = Path(tempfile.mkdtemp())
        db_path = db_dir / "test.duckdb"
        
        # Create database with schema
        import duckdb
        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_meta_learning_report (
                report_id VARCHAR PRIMARY KEY,
                report_path VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                post_mortem_prompt_version VARCHAR,
                analysis_period_start TIMESTAMP,
                analysis_period_end TIMESTAMP,
                investment_reports_analyzed INTEGER
            )
        """)
        conn.close()
        
        yield str(meta_dir), str(db_path)
        
        shutil.rmtree(reports_dir)
        shutil.rmtree(db_dir)
    
    def test_generate_meta_learning_report(self, temp_meta_dirs):
        """Test generating a meta-learning report."""
        reports_dir, db_path = temp_meta_dirs
        
        sample_output = "Test analysis output with findings."
        
        report_path = generate_meta_learning_report(
            analysis_output=sample_output,
            prompt_version="1.0.0",
            analysis_period_start=datetime(2026, 1, 1),
            analysis_period_end=datetime(2026, 1, 11),
            reports_analyzed_count=5,
            db_path=db_path,
            reports_dir=reports_dir,
        )
        
        assert Path(report_path).exists()
        
        md_content = Path(report_path).read_text()
        assert "Meta-Learning Report" in md_content


# =============================================================================
# NEW REPORT STRUCTURE TESTS (v5.0)
# =============================================================================

class TestInputValidation:
    """Tests for _check_input_validity fail-fast behavior."""
    
    def test_all_valid_inputs(self):
        """Test with all valid inputs - should be actionable."""
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": False}}
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass"
            },
            "macro_analysis": {
                "meta": {"data_quality": "ok", "agent_name": "macro", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
            "technical_analysis": {
                "meta": {"data_quality": "ok", "agent_name": "technical", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
            "token_research": {
                "meta": {"data_quality": "ok", "agent_name": "research", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
        }
        
        is_actionable, reason, per_input = _check_input_validity(validated_outputs)
        
        assert is_actionable is True
        assert reason == ""
        assert all(q == "ok" for q in per_input.values())
    
    def test_missing_critical_input(self):
        """Test with missing critical input - should NOT be actionable."""
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": False}}
            },
            # orchestration is missing - critical!
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass"
            },
        }
        
        is_actionable, reason, per_input = _check_input_validity(validated_outputs)
        
        assert is_actionable is False
        # Updated for soft validation - now says "completely missing" 
        assert "orchestration: completely missing" in reason
        assert per_input["orchestration"] == "missing"
    
    def test_invalid_data_quality_is_now_warning_not_blocker(self):
        """Test that invalid data_quality is now a WARNING, not a blocker (soft validation).
        
        SOFT VALIDATION CHANGE: data_quality='invalid' is now a warning, not a blocker.
        The data exists and can be used, just with reduced confidence.
        """
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "invalid", "agent_name": "portfolio", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": True, "contradictions": ["test"]}}
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass"
            },
        }
        
        is_actionable, reason, per_input = _check_input_validity(validated_outputs)
        
        # SOFT VALIDATION: data_quality="invalid" is now a WARNING, not a blocker
        # Data exists, so it's still actionable (with warnings)
        assert is_actionable is True  # Changed from False
        # Warnings should mention the issues
        assert "WARNINGS:" in reason
        assert "portfolio_context" in reason
        assert per_input["portfolio_context"] == "invalid"
    
    def test_qa_reject_makes_not_actionable(self):
        """Test that QA reject makes report not actionable."""
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": False}}
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "reject"
            },
        }
        
        is_actionable, reason, per_input = _check_input_validity(validated_outputs)
        
        assert is_actionable is False
        assert "qa_risk" in reason


class TestActionPlanRendering:
    """Tests for _render_action_plan (always present)."""
    
    @pytest.fixture
    def sample_macro(self):
        return {
            "meta": {"as_of_timestamp_utc": "2026-01-18T12:00:00Z"},
            "regime": {"stance": "risk_on", "confidence": "high"},
            "implications": {"favor": ["BTC", "ETH"], "avoid": ["memecoins"]}
        }
    
    @pytest.fixture
    def sample_technical(self):
        return {
            "meta": {"as_of_timestamp_utc": "2026-01-18T12:00:00Z"},
            "breadth": {"pct_above_200d": 65.0, "median_rsi_14": 55.0},
            "assets": [
                {"symbol": "BTC", "trend": "bullish"},
                {"symbol": "ETH", "trend": "bullish"},
            ]
        }
    
    @pytest.fixture
    def sample_portfolio(self):
        return {
            "derived": {
                "btc_allocation_pct_by_value": 55.0,
                "tier2_3_allocation_pct_by_value": 10.0
            },
            "framework": {
                "config": {
                    "btc_target_min_pct": 40,
                    "btc_target_max_pct": 60,
                    "tier2_3_max_pct": 35
                },
                "checks": {
                    "btc_within_target": True,
                    "any_position_over_limit": False,
                    "tier2_3_within_limit": True,
                    "contradictions_detected": False
                }
            }
        }
    
    @pytest.fixture
    def sample_recommendations_with_buy(self):
        return {
            "recommendations": [
                {
                    "symbol": "SOL",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 10.0,
                    "rationale_one_liner": "Strong ecosystem growth"
                }
            ],
            "default_recommendation": {"action": "hold_btc", "reason": "test"}
        }
    
    def test_action_plan_always_present(self, sample_macro, sample_technical, sample_portfolio, sample_recommendations_with_buy):
        """Test that action plan is always rendered."""
        result = _render_action_plan(
            macro=sample_macro,
            technical=sample_technical,
            portfolio=sample_portfolio,
            recommendations=sample_recommendations_with_buy,
            is_actionable=True
        )
        
        assert "## 1. ONE-PAGE ACTION PLAN" in result
        assert "### Market Stance" in result
        assert "### Portfolio Status" in result
        assert "### Do This Now" in result
        assert "### Do Not Do" in result
        assert "### Next Review Triggers" in result
    
    def test_action_plan_shows_buy_recommendation(self, sample_macro, sample_technical, sample_portfolio, sample_recommendations_with_buy):
        """Test that BUY recommendation appears in Do This Now."""
        result = _render_action_plan(
            macro=sample_macro,
            technical=sample_technical,
            portfolio=sample_portfolio,
            recommendations=sample_recommendations_with_buy,
            is_actionable=True
        )
        
        assert "BUY SOL" in result
        assert "Strong ecosystem growth" in result
    
    def test_action_plan_not_actionable(self, sample_macro, sample_technical, sample_portfolio, sample_recommendations_with_buy):
        """Test that not actionable shows STOP message."""
        result = _render_action_plan(
            macro=sample_macro,
            technical=sample_technical,
            portfolio=sample_portfolio,
            recommendations=sample_recommendations_with_buy,
            is_actionable=False
        )
        
        assert "STOP" in result
        assert "NOT ACTIONABLE" in result


class TestRecommendationsTable:
    """Tests for _render_recommendations_table."""
    
    def test_recommendations_table_with_recs(self):
        """Test rendering recommendations table."""
        recommendations = {
            "recommendations": [
                {
                    "symbol": "SOL",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 10.0,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "Strong ecosystem"
                },
                {
                    "symbol": "ETH",
                    "action": "hold",
                    "conviction": "medium",
                    "tier": 1,
                    "time_horizon": "12m+",
                    "rationale_one_liner": "Solid fundamentals"
                }
            ]
        }
        
        result = _render_recommendations_table(recommendations, is_actionable=True)
        
        assert "| Symbol | Action |" in result
        assert "| SOL | BUY |" in result
        assert "| ETH | HOLD |" in result
    
    def test_recommendations_table_empty(self):
        """Test rendering empty recommendations."""
        recommendations = {
            "recommendations": [],
            "default_recommendation": {"action": "hold_btc", "reason": "No opportunities"}
        }
        
        result = _render_recommendations_table(recommendations, is_actionable=True)
        
        assert "HOLD BTC" in result
        assert "No opportunities" in result


class TestExecutionPlans:
    """Tests for _render_execution_plans with complete trading details."""
    
    def test_execution_plan_with_complete_trading_plan(self):
        """Test that complete trading plan renders all details."""
        recommendations = {
            "recommendations": [
                {
                    "symbol": "SOL",
                    "action": "buy",
                    "conviction": "high",
                    "tier": 1,
                    "suggested_allocation_pct_portfolio": 10.0,
                    "time_horizon": "6-12m",
                    "trading_plan": {
                        "entry_strategy": "DCA over 2 weeks",
                        "position_size": "10% of portfolio",
                        "take_profit_targets": [
                            {"target": "$200", "sell_pct": 50},
                            {"target": "$300", "sell_pct": 50}
                        ],
                        "stop_loss": "-20%",
                        "invalidation_trigger": "Network outage > 24h"
                    },
                    "rubric": {
                        "problem_solved": "Fast transactions",
                        "network_effects": "Growing DeFi",
                        "why_now": "Ecosystem recovery",
                        "invalidation": "Network issues",
                        "vs_doing_nothing": "Higher growth",
                        "downside_risks": "Centralization",
                        "portfolio_fit": "Tier 1",
                        "exit_criteria": "2x target"
                    },
                    "evidence_refs": ["tech-SOL", "research-SOL"]
                }
            ]
        }
        
        result = _render_execution_plans(recommendations, is_actionable=True)
        
        # Check all required sections are present
        assert "#### SOL - BUY" in result
        assert "**Time Horizon:**" in result
        assert "**Position Sizing:**" in result
        assert "**Entry Plan:**" in result
        assert "DCA over 2 weeks" in result
        assert "**Exit Plan:**" in result
        assert "**Take Profit Targets:**" in result
        assert "$200" in result
        assert "**Stop Loss:**" in result
        assert "-20%" in result
        assert "**Invalidation Trigger:**" in result
        assert "Network outage" in result
        assert "**Evidence References:**" in result
        assert "**8-Question Rubric (Summary):**" in result
    
    def test_execution_plan_missing_trading_plan(self):
        """Test that missing trading plan shows warnings."""
        recommendations = {
            "recommendations": [
                {
                    "symbol": "AVAX",
                    "action": "buy",
                    "conviction": "medium",
                    "tier": 2,
                    "suggested_allocation_pct_portfolio": 5.0,
                    "time_horizon": "3-6m",
                    # No trading_plan provided
                }
            ]
        }
        
        result = _render_execution_plans(recommendations, is_actionable=True)
        
        assert "#### AVAX - BUY" in result
        assert "not specified" in result.lower() or "N/A" in result


class TestDoNothingJustification:
    """Tests for _render_do_nothing_justification."""
    
    def test_do_nothing_with_evidence(self):
        """Test do nothing section includes evidence bullets."""
        macro = {
            "regime": {"stance": "risk_off", "confidence": "high"},
            "implications": {"avoid": ["alts", "high-beta"]}
        }
        
        technical = {
            "breadth": {"pct_above_200d": 35.0, "median_rsi_14": 42.0},
            "assets": [
                {"symbol": "BTC", "trend": "bearish"},
                {"symbol": "ETH", "trend": "bearish"}
            ]
        }
        
        portfolio = {
            "derived": {
                "btc_allocation_pct_by_value": 35.0,
                "tier2_3_allocation_pct_by_value": 30.0
            },
            "framework": {
                "config": {"btc_target_min_pct": 40, "tier2_3_max_pct": 35},
                "checks": {"btc_within_target": False}
            }
        }
        
        research = {
            "ranked_shortlist": [
                {"symbol": "SOL", "score": 5.5}  # Below 7.0 threshold
            ]
        }
        
        recommendations = {
            "recommendations": [
                {"symbol": "BTC", "action": "hold"}
            ],
            "default_recommendation": {"action": "do_nothing", "reason": "Risk-off regime"}
        }
        
        result = _render_do_nothing_justification(macro, technical, portfolio, research, recommendations)
        
        assert "### 2.3 Why We Are Doing Nothing Now" in result
        assert "RISK-OFF" in result
        assert "Weak breadth" in result
        assert "BTC underweight" in result
        assert "Low conviction" in result
        assert "### What Would Change Our Mind" in result


class TestNewReportIntegration:
    """Integration tests for the new v5.0 report structure."""
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories."""
        reports_dir = Path(tempfile.mkdtemp())
        db_dir = Path(tempfile.mkdtemp())
        db_path = db_dir / "test.duckdb"
        
        # Create database with schema
        import duckdb
        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_investment_report (
                report_id VARCHAR PRIMARY KEY,
                report_path VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                token_screener_prompt_version VARCHAR,
                fundamentals_analyst_prompt_version VARCHAR,
                research_synthesizer_prompt_version VARCHAR,
                token_research_prompt_version VARCHAR,
                technical_analyst_prompt_version VARCHAR,
                macro_cycle_prompt_version VARCHAR,
                portfolio_context_prompt_version VARCHAR,
                orchestrator_prompt_version VARCHAR,
                qa_risk_prompt_version VARCHAR
            )
        """)
        conn.close()
        
        yield reports_dir, str(db_path)
        
        shutil.rmtree(reports_dir)
        shutil.rmtree(db_dir)
    
    @pytest.fixture
    def valid_validated_outputs_with_buy(self):
        """Complete validated outputs with a BUY recommendation."""
        return {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio_context", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "portfolio_totals": {"total_cost_basis_usd": 10000, "total_current_value_usd": 12000},
                "positions": [{"symbol": "BTC", "tier": 0, "quantity": 0.1, "allocation_pct_by_value": 50}],
                "derived": {
                    "btc_quantity": 0.1,
                    "btc_allocation_pct_by_value": 50.0,
                    "tier2_3_allocation_pct_by_value": 0.0,
                    "max_single_asset_allocation_pct_by_value": 50.0
                },
                "framework": {
                    "config": {"btc_target_min_pct": 40, "btc_target_max_pct": 60, "single_asset_limit_pct": 20, "tier2_3_max_pct": 35},
                    "checks": {"btc_within_target": True, "any_position_over_limit": False, "tier2_3_within_limit": True, "pricing_complete": True, "contradictions_detected": False}
                }
            },
            "macro_analysis": {
                "meta": {"data_quality": "ok", "agent_name": "macro_cycle", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "regime": {"stance": "risk_on", "confidence": "high"},
                "macro": {"liquidity": {"summary": "Improving", "signals": []}, "fed_policy": {"summary": "Dovish", "signals": []}, "inflation": {"summary": "Declining", "signals": []}, "risk_appetite": {"summary": "High", "signals": []}},
                "cycle": {"stage": "mid", "evidence": []},
                "narratives": [],
                "implications": {"favor": ["BTC", "ETH"], "avoid": []},
                "sources": []
            },
            "technical_analysis": {
                "meta": {"data_quality": "ok", "agent_name": "technical_analyst", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "assets": [{"symbol": "BTC", "trend": "bullish", "signal": "bullish", "btc_relative": {"trend": "neutral"}, "timeframes": {"d1": {"rsi_14": 55}, "w1": {}}, "key_levels": {}}],
                "breadth": {"universe": "top_50", "pct_above_200d": 65.0, "median_rsi_14": 55.0}
            },
            "token_research": {
                "meta": {"data_quality": "ok", "agent_name": "research_synthesizer", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "universe": {"constraints": {"max_mcap_rank": 200, "exclude_memecoins": True}},
                "candidates": [{"symbol": "SOL", "name": "Solana", "confidence": "high", "thesis": {"problem": "Speed", "why_it_wins": "Fast", "network_effects": "Growing"}, "adoption_metrics": {}}],
                "ranked_shortlist": [{"symbol": "SOL", "score": 7.5, "score_breakdown": {"adoption": 8, "moat": 7, "catalyst": 7, "risk": 8}}]
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "executive_summary": "Risk-on environment favors alt exposure",
                "market_context": {"macro_regime": "risk_on", "technical_env": "bullish", "key_considerations": []},
                "recommendations": [
                    {
                        "symbol": "SOL",
                        "action": "buy",
                        "conviction": "high",
                        "tier": 1,
                        "suggested_allocation_pct_portfolio": 10.0,
                        "time_horizon": "6-12m",
                        "rationale_one_liner": "Strong ecosystem growth and risk-on regime",
                        "rubric": {"problem_solved": "Speed", "network_effects": "DeFi", "why_now": "Recovery", "invalidation": "Outage", "vs_doing_nothing": "Growth", "downside_risks": "Central", "portfolio_fit": "T1", "exit_criteria": "2x"},
                        "trading_plan": {"entry_strategy": "DCA", "position_size": "10%", "take_profit_targets": [{"target": "$200", "sell_pct": 50}, {"target": "$300", "sell_pct": 50}], "stop_loss": "-20%", "invalidation_trigger": "Outage"}
                    }
                ],
                "default_recommendation": {"action": "hold_btc", "reason": "Fallback"}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass",
                "recommendations_reviewed": 1,
                "issues_found": 0,
                "compliance_checklist": [{"check": "Top 200", "status": "pass", "notes": "OK"}],
                "per_recommendation": [],
                "final_verdict": "Proceed with caution"
            }
        }
    
    def test_happy_path_buy_all_sections_present(self, temp_dirs, valid_validated_outputs_with_buy):
        """Test happy path: BUY recommendation with all sections present."""
        reports_dir, db_path = temp_dirs
        
        report_path = generate_investment_report(
            validated_outputs=valid_validated_outputs_with_buy,
            prompt_versions={"orchestrator": "1.0.0"},
            db_path=db_path,
            reports_dir=str(reports_dir),
        )
        
        assert Path(report_path).exists()
        
        md_content = Path(report_path).read_text()
        
        # Check actionability
        assert "**Actionability:** ACTIONABLE" in md_content
        
        # Check all main sections
        assert "## 1. ONE-PAGE ACTION PLAN" in md_content
        assert "## 2. DECISION PACKET" in md_content
        assert "## 3. EVIDENCE APPENDIX" in md_content
        
        # Check BUY recommendation
        assert "BUY SOL" in md_content
        
        # Check execution plan details
        assert "**Time Horizon:**" in md_content
        assert "**Entry Plan:**" in md_content
        assert "**Exit Plan:**" in md_content
        
        # Check evidence appendix sections
        assert "### 4.1 Macro Evidence" in md_content
        assert "### 4.2 Technical Evidence" in md_content
        assert "### 4.3 Fundamentals" in md_content
        assert "### 4.4 Portfolio" in md_content
    
    def test_do_nothing_path_justification_present(self, temp_dirs):
        """Test do nothing path: action plan present with justification."""
        reports_dir, db_path = temp_dirs
        
        # Outputs with no actionable recommendations
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio_context", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "portfolio_totals": {"total_cost_basis_usd": 10000},
                "positions": [],
                "derived": {"btc_allocation_pct_by_value": 0, "tier2_3_allocation_pct_by_value": 0, "max_single_asset_allocation_pct_by_value": 0},
                "framework": {"config": {"btc_target_min_pct": 40, "btc_target_max_pct": 60, "tier2_3_max_pct": 35}, "checks": {"contradictions_detected": False}}
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "executive_summary": "Risk-off conditions",
                "market_context": {"macro_regime": "risk_off", "technical_env": "bearish", "key_considerations": []},
                "recommendations": [{"symbol": "BTC", "action": "watch", "conviction": "medium", "tier": 0, "time_horizon": "3-6m", "rationale_one_liner": "Wait for signal"}],
                "default_recommendation": {"action": "do_nothing", "reason": "Risk-off regime"}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass",
                "recommendations_reviewed": 0,
                "issues_found": 0,
                "compliance_checklist": [],
                "per_recommendation": [],
                "final_verdict": "No actionable recommendations"
            },
            "macro_analysis": {
                "meta": {"data_quality": "ok", "agent_name": "macro", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "regime": {"stance": "risk_off", "confidence": "high"},
                "macro": {},
                "cycle": {},
                "narratives": [],
                "implications": {"favor": [], "avoid": ["alts"]},
                "sources": []
            }
        }
        
        report_path = generate_investment_report(
            validated_outputs=validated_outputs,
            prompt_versions={"orchestrator": "1.0.0"},
            db_path=db_path,
            reports_dir=str(reports_dir),
        )
        
        md_content = Path(report_path).read_text()
        
        # Should have action plan
        assert "## 1. ONE-PAGE ACTION PLAN" in md_content
        
        # Should have do nothing justification
        assert "Why We Are Doing Nothing Now" in md_content
        assert "What Would Change Our Mind" in md_content
    
    def test_invalid_inputs_not_actionable(self, temp_dirs):
        """Test that missing critical inputs produce Not Actionable."""
        reports_dir, db_path = temp_dirs
        
        # Missing orchestration - critical input
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "ok", "agent_name": "portfolio_context", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": False}}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "pass"
            }
        }
        
        report_path = generate_investment_report(
            validated_outputs=validated_outputs,
            prompt_versions={},
            db_path=db_path,
            reports_dir=str(reports_dir),
        )
        
        md_content = Path(report_path).read_text()
        
        assert "**Actionability:** NOT ACTIONABLE" in md_content
        assert "orchestration" in md_content.lower()
    
    def test_contradiction_detected_not_actionable(self, temp_dirs):
        """Test that contradictions make report Not Actionable."""
        reports_dir, db_path = temp_dirs
        
        validated_outputs = {
            "portfolio_context": {
                "meta": {"data_quality": "invalid", "agent_name": "portfolio_context", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "framework": {"checks": {"contradictions_detected": True, "contradictions": ["BTC allocation mismatch"]}}
            },
            "orchestration": {
                "meta": {"data_quality": "ok", "agent_name": "orchestrator", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "recommendations": [],
                "default_recommendation": {"action": "do_nothing", "reason": "Data issues"}
            },
            "qa_risk": {
                "meta": {"data_quality": "ok", "agent_name": "qa_risk", "schema_version": "4.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "warnings": []},
                "overall_status": "reject"
            }
        }
        
        report_path = generate_investment_report(
            validated_outputs=validated_outputs,
            prompt_versions={},
            db_path=db_path,
            reports_dir=str(reports_dir),
        )
        
        md_content = Path(report_path).read_text()
        
        assert "**Actionability:** NOT ACTIONABLE" in md_content
        assert "contradiction" in md_content.lower() or "invalid" in md_content.lower()
