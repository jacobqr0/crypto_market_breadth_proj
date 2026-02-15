"""
Tests for the validation module.

Tests that:
1. validate_task_output() works for each task type
2. JSON cleaning handles markdown fences
3. Schema validation catches errors
4. Validation summary works correctly
5. Orchestration conviction normalization
6. Trading plan numeric-to-string conversion
7. QA risk compliance_checklist status normalization
"""

import pytest
import json

from validation import (
    validate_task_output,
    ValidationResult,
    ValidationError,
    TASK_SCHEMA_MAP,
    STRICT_VALIDATION_TASKS,
    clean_json_output,
    create_retry_prompt,
    validate_all_task_outputs,
    get_validation_summary,
    ResearchPacket,
    build_research_packet_prompt,
    preprocess_agent_output,
    _normalize_conviction_value,
    _normalize_compliance_status,
    _format_price_string,
    coerce_numeric_value,
    NULL_STRINGS,
)


class TestCleanJsonOutput:
    """Tests for the clean_json_output function."""
    
    def test_clean_json_no_fences(self):
        """JSON without fences should pass through."""
        raw = '{"key": "value"}'
        assert clean_json_output(raw) == raw
    
    def test_clean_json_with_json_fence(self):
        """JSON with ```json fence should be cleaned."""
        raw = '```json\n{"key": "value"}\n```'
        assert clean_json_output(raw) == '{"key": "value"}'
    
    def test_clean_json_with_plain_fence(self):
        """JSON with plain ``` fence should be cleaned."""
        raw = '```\n{"key": "value"}\n```'
        assert clean_json_output(raw) == '{"key": "value"}'
    
    def test_clean_json_with_preamble(self):
        """JSON with preamble text should extract JSON."""
        raw = 'Here is the result:\n{"key": "value"}'
        result = clean_json_output(raw)
        assert result == '{"key": "value"}'
    
    def test_clean_json_with_whitespace(self):
        """JSON with whitespace should be trimmed."""
        raw = '  \n{"key": "value"}\n  '
        assert clean_json_output(raw) == '{"key": "value"}'
    
    def test_clean_json_empty(self):
        """Empty string should return empty."""
        assert clean_json_output("") == ""
        assert clean_json_output("   ") == ""


class TestValidateTaskOutput:
    """Tests for the validate_task_output function."""
    
    def test_validate_portfolio_context_valid(self):
        """Valid portfolio context should pass."""
        valid_json = json.dumps({
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
            "positions": [],
            "derived": {
                "btc_quantity": 0.0,
                "btc_allocation_pct_by_value": 0.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": None,
                "max_single_asset_allocation_pct_by_value": 0.0
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
                    "btc_within_target": None,
                    "any_position_over_limit": None,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": None,
                    "total_allocations_sum_to_100": None,
                    "pricing_complete": False,
                    "contradictions_detected": False,
                    "contradictions": []
                }
            }
        })
        
        result = validate_task_output("portfolio_context", valid_json)
        assert result.success is True
        assert result.parsed_data is not None
        assert result.errors == []
    
    def test_validate_portfolio_context_invalid_json(self):
        """Invalid JSON should fail."""
        result = validate_task_output("portfolio_context", "not json at all")
        assert result.success is False
        assert len(result.errors) > 0
        assert "JSON parse error" in result.errors[0]
    
    def test_validate_portfolio_context_missing_field(self):
        """Missing required field should be soft failure (usable but not strict valid)."""
        invalid_json = json.dumps({
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            # Missing portfolio_totals, positions, derived, framework
        })
        
        result = validate_task_output("portfolio_context", invalid_json)
        # SOFT VALIDATION: JSON parses, so success=True, but strict_valid=False
        assert result.success is True  # Data is usable for downstream
        assert result.strict_valid is False  # Schema validation failed
        assert len(result.validation_warnings) > 0  # Has warnings about missing fields
        assert len(result.errors) == 0  # No critical errors
    
    def test_validate_intermediate_task(self):
        """Intermediate tasks without strict schema should pass."""
        result = validate_task_output("fundamentals_analysis", "some prose output")
        assert result.success is True  # Non-strict validation
    
    def test_validate_with_markdown_fence(self):
        """JSON with markdown fence should be cleaned and validated."""
        valid_json = json.dumps({
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 0,
                "total_current_value_usd": 0,
                "total_realized_pnl_usd": 0,
                "drawdown_from_peak_pct": None
            },
            "positions": [],
            "derived": {
                "btc_quantity": 0,
                "btc_allocation_pct_by_value": 0,
                "tier2_3_allocation_pct_by_value": 0,
                "max_single_asset_symbol": None,
                "max_single_asset_allocation_pct_by_value": 0
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
                    "btc_within_target": None,
                    "any_position_over_limit": None,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": None,
                    "total_allocations_sum_to_100": None,
                    "pricing_complete": False,
                    "contradictions_detected": False,
                    "contradictions": []
                }
            }
        })
        
        wrapped = f"```json\n{valid_json}\n```"
        result = validate_task_output("portfolio_context", wrapped)
        assert result.success is True


class TestCreateRetryPrompt:
    """Tests for the create_retry_prompt function."""
    
    def test_retry_prompt_contains_errors(self):
        """Retry prompt should contain the errors."""
        errors = ["Missing field: meta", "Invalid type for quantity"]
        prompt = create_retry_prompt("portfolio_context", errors)
        
        assert "Missing field: meta" in prompt
        assert "Invalid type for quantity" in prompt
        assert "validation" in prompt.lower()
    
    def test_retry_prompt_truncates_errors(self):
        """Retry prompt should truncate long error lists."""
        errors = [f"Error {i}" for i in range(10)]
        prompt = create_retry_prompt("portfolio_context", errors)
        
        # Should only show first 5
        assert "Error 0" in prompt
        assert "Error 4" in prompt
        # Error 5+ should not be shown
        assert "Error 5" not in prompt


class TestValidationSummary:
    """Tests for validation summary functions."""
    
    def test_validation_summary_all_pass(self):
        """Summary with all passing should show all passed."""
        results = {
            "task1": ValidationResult(success=True, task_name="task1"),
            "task2": ValidationResult(success=True, task_name="task2"),
        }
        
        summary = get_validation_summary(results)
        assert summary["total_tasks"] == 2
        assert summary["passed"] == 2
        assert summary["failed"] == 0
        assert summary["all_passed"] is True
    
    def test_validation_summary_with_failures(self):
        """Summary with failures should report them."""
        results = {
            "task1": ValidationResult(success=True, task_name="task1"),
            "task2": ValidationResult(
                success=False, 
                task_name="task2",
                errors=["Error 1"]
            ),
        }
        
        summary = get_validation_summary(results)
        assert summary["total_tasks"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1
        assert summary["all_passed"] is False
        assert len(summary["failed_tasks"]) == 1


class TestResearchPacket:
    """Tests for the ResearchPacket class."""
    
    def test_research_packet_from_outputs(self):
        """Research packet should be created from validated outputs."""
        outputs = {
            "portfolio_context": {"meta": {"agent_name": "portfolio_context"}},
            "macro_analysis": {"meta": {"agent_name": "macro_cycle"}},
        }
        
        packet = ResearchPacket.from_validated_outputs(outputs)
        assert packet.portfolio_context is not None
        assert packet.macro_cycle is not None
        assert packet.technical_analysis is None  # Not provided
    
    def test_research_packet_to_json(self):
        """Research packet should serialize to JSON."""
        packet = ResearchPacket(
            portfolio_context={"test": "data"}
        )
        
        json_str = packet.to_json_string()
        parsed = json.loads(json_str)
        assert "portfolio_context" in parsed
        assert parsed["portfolio_context"]["test"] == "data"
    
    def test_build_research_packet_prompt(self):
        """Build prompt should contain JSON packet."""
        outputs = {
            "portfolio_context": {"meta": {"agent_name": "portfolio_context"}},
        }
        
        prompt = build_research_packet_prompt(outputs)
        assert "RESEARCH_PACKET_JSON" in prompt
        assert "portfolio_context" in prompt


class TestStrictValidationTasks:
    """Tests for strict validation task list."""
    
    def test_strict_tasks_defined(self):
        """Critical tasks should be in strict validation list."""
        assert "portfolio_context" in STRICT_VALIDATION_TASKS
        assert "orchestration" in STRICT_VALIDATION_TASKS
        assert "qa_risk" in STRICT_VALIDATION_TASKS
    
    def test_intermediate_tasks_not_strict(self):
        """Intermediate tasks should not be strict."""
        assert "fundamentals_analysis" not in STRICT_VALIDATION_TASKS
        assert "token_screening" not in STRICT_VALIDATION_TASKS


# =============================================================================
# Tests for Normalization Functions (Fixes validation failures)
# =============================================================================

class TestConvictionNormalization:
    """Tests for orchestration conviction value normalization."""
    
    def test_normalize_conviction_lowercase(self):
        """Already lowercase conviction should pass through."""
        assert _normalize_conviction_value("high") == "high"
        assert _normalize_conviction_value("medium") == "medium"
        assert _normalize_conviction_value("low") == "low"
    
    def test_normalize_conviction_uppercase(self):
        """Uppercase conviction should be normalized to lowercase."""
        assert _normalize_conviction_value("HIGH") == "high"
        assert _normalize_conviction_value("MEDIUM") == "medium"
        assert _normalize_conviction_value("LOW") == "low"
    
    def test_normalize_conviction_mixed_case(self):
        """Mixed case conviction should be normalized to lowercase."""
        assert _normalize_conviction_value("High") == "high"
        assert _normalize_conviction_value("Medium") == "medium"
        assert _normalize_conviction_value("Low") == "low"
    
    def test_normalize_conviction_with_extra_text(self):
        """Conviction with extra text (e.g., 'High (0.7)') should extract first word."""
        assert _normalize_conviction_value("High (0.7)") == "high"
        assert _normalize_conviction_value("Medium confidence") == "medium"
        assert _normalize_conviction_value("Low risk") == "low"
    
    def test_normalize_conviction_with_punctuation(self):
        """Conviction with punctuation should be cleaned."""
        assert _normalize_conviction_value("High,") == "high"
        assert _normalize_conviction_value("(Medium)") == "medium"
        assert _normalize_conviction_value("Low:") == "low"
    
    def test_normalize_conviction_non_string(self):
        """Non-string values should pass through unchanged."""
        assert _normalize_conviction_value(123) == 123
        assert _normalize_conviction_value(None) is None
        assert _normalize_conviction_value({"key": "value"}) == {"key": "value"}
    
    def test_normalize_conviction_invalid_value(self):
        """Unrecognized conviction values should pass through unchanged."""
        assert _normalize_conviction_value("unknown") == "unknown"
        assert _normalize_conviction_value("very high") == "very high"  # Returns original if first word not valid


class TestComplianceStatusNormalization:
    """Tests for qa_risk compliance_checklist status normalization."""
    
    def test_normalize_status_pass_variants(self):
        """Various pass indicators should normalize to 'pass'."""
        assert _normalize_compliance_status("pass") == "pass"
        assert _normalize_compliance_status("PASS") == "pass"
        assert _normalize_compliance_status("Pass") == "pass"
        assert _normalize_compliance_status("passed") == "pass"
        assert _normalize_compliance_status("ok") == "pass"
        assert _normalize_compliance_status("yes") == "pass"
        assert _normalize_compliance_status("✓") == "pass"
        assert _normalize_compliance_status("✔") == "pass"
    
    def test_normalize_status_fail_variants(self):
        """Various fail indicators should normalize to 'fail'."""
        assert _normalize_compliance_status("fail") == "fail"
        assert _normalize_compliance_status("FAIL") == "fail"
        assert _normalize_compliance_status("Fail") == "fail"
        assert _normalize_compliance_status("failed") == "fail"
        assert _normalize_compliance_status("no") == "fail"
        assert _normalize_compliance_status("✗") == "fail"
        assert _normalize_compliance_status("✘") == "fail"
    
    def test_normalize_status_unknown_variants(self):
        """Various unknown indicators should normalize to 'unknown'."""
        assert _normalize_compliance_status("unknown") == "unknown"
        assert _normalize_compliance_status("UNKNOWN") == "unknown"
        assert _normalize_compliance_status("?") == "unknown"
        assert _normalize_compliance_status("unclear") == "unknown"
    
    def test_normalize_status_not_applicable_variants(self):
        """Various not_applicable indicators should normalize correctly."""
        assert _normalize_compliance_status("n/a") == "not_applicable"
        assert _normalize_compliance_status("N/A") == "not_applicable"
        assert _normalize_compliance_status("na") == "not_applicable"
        assert _normalize_compliance_status("not applicable") == "not_applicable"
        assert _normalize_compliance_status("not_applicable") == "not_applicable"
        assert _normalize_compliance_status("-") == "not_applicable"
    
    def test_normalize_status_unrecognized(self):
        """Unrecognized status values should default to 'unknown'."""
        assert _normalize_compliance_status("something else") == "unknown"
        assert _normalize_compliance_status("maybe") == "unknown"
    
    def test_normalize_status_non_string(self):
        """Non-string values should return 'unknown'."""
        assert _normalize_compliance_status(123) == "unknown"
        assert _normalize_compliance_status(None) == "unknown"


class TestPriceStringFormatting:
    """Tests for trading plan numeric-to-string conversion."""
    
    def test_format_integer(self):
        """Integer values should format cleanly without decimals."""
        assert _format_price_string(65000) == "65000"
        assert _format_price_string(100) == "100"
        assert _format_price_string(0) == "0"
    
    def test_format_float_whole_number(self):
        """Float values that are whole numbers should not have decimals."""
        assert _format_price_string(65000.0) == "65000"
        assert _format_price_string(100.0) == "100"
        assert _format_price_string(0.0) == "0"
    
    def test_format_float_with_decimals(self):
        """Float values with decimals should preserve them."""
        assert _format_price_string(65000.50) == "65000.5"
        assert _format_price_string(100.25) == "100.25"
        assert _format_price_string(0.001) == "0.001"


class TestPreprocessOrchestrationOutput:
    """Tests for orchestration-specific preprocessing."""
    
    def test_preprocess_conviction_normalization(self):
        """Orchestration preprocessing should normalize conviction values."""
        raw_json = {
            "recommendations": [
                {"symbol": "ETH", "conviction": "High"},
                {"symbol": "SOL", "conviction": "MEDIUM"},
                {"symbol": "BTC", "conviction": "low"},
            ]
        }
        
        result = preprocess_agent_output("orchestration", raw_json)
        
        assert result["recommendations"][0]["conviction"] == "high"
        assert result["recommendations"][1]["conviction"] == "medium"
        assert result["recommendations"][2]["conviction"] == "low"
    
    def test_preprocess_trading_plan_stop_loss_numeric(self):
        """Orchestration preprocessing should convert numeric stop_loss to string."""
        raw_json = {
            "recommendations": [
                {
                    "symbol": "ETH",
                    "conviction": "high",
                    "trading_plan": {
                        "stop_loss": 3000,
                        "entry_strategy": "DCA"
                    }
                }
            ]
        }
        
        result = preprocess_agent_output("orchestration", raw_json)
        
        assert result["recommendations"][0]["trading_plan"]["stop_loss"] == "3000"
        assert isinstance(result["recommendations"][0]["trading_plan"]["stop_loss"], str)
    
    def test_preprocess_trading_plan_take_profit_targets_numeric(self):
        """Orchestration preprocessing should convert numeric targets to strings."""
        raw_json = {
            "recommendations": [
                {
                    "symbol": "ETH",
                    "conviction": "high",
                    "trading_plan": {
                        "take_profit_targets": [
                            {"target": 4000, "sell_pct": 50},
                            {"target": 5000.50, "sell_pct": 50},
                        ]
                    }
                }
            ]
        }
        
        result = preprocess_agent_output("orchestration", raw_json)
        
        targets = result["recommendations"][0]["trading_plan"]["take_profit_targets"]
        assert targets[0]["target"] == "4000"
        assert targets[1]["target"] == "5000.5"
        assert isinstance(targets[0]["target"], str)
        assert isinstance(targets[1]["target"], str)
    
    def test_preprocess_trading_plan_string_values_unchanged(self):
        """Orchestration preprocessing should leave string values unchanged."""
        raw_json = {
            "recommendations": [
                {
                    "symbol": "ETH",
                    "conviction": "high",
                    "trading_plan": {
                        "stop_loss": "3000",
                        "take_profit_targets": [
                            {"target": "4000", "sell_pct": 50},
                        ]
                    }
                }
            ]
        }
        
        result = preprocess_agent_output("orchestration", raw_json)
        
        assert result["recommendations"][0]["trading_plan"]["stop_loss"] == "3000"
        assert result["recommendations"][0]["trading_plan"]["take_profit_targets"][0]["target"] == "4000"


class TestPreprocessQARiskOutput:
    """Tests for qa_risk-specific preprocessing."""
    
    def test_preprocess_compliance_status_normalization(self):
        """QA risk preprocessing should normalize compliance_checklist status values."""
        raw_json = {
            "compliance_checklist": [
                {"check": "data_quality", "status": "PASS", "notes": "Good"},
                {"check": "assets_top_200", "status": "✓", "notes": "All OK"},
                {"check": "btc_allocation", "status": "fail", "notes": "Below target"},
                {"check": "leverage_check", "status": "n/a", "notes": "Not applicable"},
            ]
        }
        
        result = preprocess_agent_output("qa_risk", raw_json)
        
        assert result["compliance_checklist"][0]["status"] == "pass"
        assert result["compliance_checklist"][1]["status"] == "pass"
        assert result["compliance_checklist"][2]["status"] == "fail"
        assert result["compliance_checklist"][3]["status"] == "not_applicable"
    
    def test_preprocess_per_recommendation_conviction_mapping(self):
        """QA risk preprocessing should map conviction to ConvictionStrength enum."""
        raw_json = {
            "compliance_checklist": [],
            "per_recommendation": [
                {"symbol": "ETH", "risk": {"conviction": "high"}},
                {"symbol": "SOL", "risk": {"conviction": "MEDIUM"}},
                {"symbol": "BTC", "risk": {"conviction": "Low"}},
            ]
        }
        
        result = preprocess_agent_output("qa_risk", raw_json)
        
        # high -> strong, medium -> adequate, low -> weak
        assert result["per_recommendation"][0]["risk"]["conviction"] == "strong"
        assert result["per_recommendation"][1]["risk"]["conviction"] == "adequate"
        assert result["per_recommendation"][2]["risk"]["conviction"] == "weak"

    def test_preprocess_compliance_status_unknown_preserved(self):
        """QA risk preprocessing should preserve 'unknown' status as a valid enum."""
        raw_json = {
            "compliance_checklist": [
                {"check": "drawdown_le_40pct", "status": "unknown", "notes": "Drawdown data missing"}
            ]
        }
        
        result = preprocess_agent_output("qa_risk", raw_json)
        
        assert result["compliance_checklist"][0]["status"] == "unknown"


class TestPreprocessTokenResearchOutput:
    """Tests for token_research-specific preprocessing."""
    
    def test_preprocess_source_type_normalization(self):
        """Token research preprocessing should normalize common source type variants."""
        raw_json = {
            "candidates": [
                {
                    "symbol": "ETH",
                    "sources": [
                        {"name": "AInvest", "type": "analysis", "ref": "https://example.com"},
                        {"name": "Messari", "type": "report", "ref": "https://example.com"},
                        {"name": "Chainlink", "type": "education hub", "ref": "https://example.com"},
                    ]
                }
            ]
        }
        
        result = preprocess_agent_output("token_research", raw_json)
        sources = result["candidates"][0]["sources"]
        
        assert sources[0]["type"] == "article"
        assert sources[1]["type"] == "paper"
        assert sources[2]["type"] == "docs"


class TestValidateOrchestrationWithNormalization:
    """Integration tests for orchestration validation with normalization."""
    
    def test_orchestration_validates_with_uppercase_conviction(self):
        """Orchestration with uppercase conviction should pass after normalization."""
        raw_json = json.dumps({
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "1.0",
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
                    "action": "watch",
                    "conviction": "High",  # Uppercase - should be normalized
                    "tier": 1,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "Test",
                    "rubric": {
                        "problem_solved": "Test",
                        "network_effects": "Test",
                        "why_now": "Test",
                        "invalidation": "Test",
                        "vs_doing_nothing": "Test",
                        "downside_risks": "Test",
                        "portfolio_fit": "Test",
                        "exit_criteria": "Test"
                    },
                    "evidence_refs": [],
                    "prerequisites": [],
                    "dependencies": {"requires": [], "data_used": []}
                }
            ],
            "default_recommendation": {
                "action": "hold_btc",
                "reason": "Test"
            }
        })
        
        result = validate_task_output("orchestration", raw_json)
        
        assert result.success is True
        assert result.errors == []
    
    def test_orchestration_validates_with_numeric_trading_plan(self):
        """Orchestration with numeric stop_loss/targets/position_size should pass after normalization."""
        raw_json = json.dumps({
            "meta": {
                "agent_name": "orchestrator",
                "schema_version": "1.0",
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
                    "action": "watch",
                    "conviction": "high",
                    "tier": 1,
                    "time_horizon": "6-12m",
                    "rationale_one_liner": "Test",
                    "rubric": {
                        "problem_solved": "Test",
                        "network_effects": "Test",
                        "why_now": "Test",
                        "invalidation": "Test",
                        "vs_doing_nothing": "Test",
                        "downside_risks": "Test",
                        "portfolio_fit": "Test",
                        "exit_criteria": "Test"
                    },
                    "trading_plan": {
                        "entry_strategy": "DCA",
                        "position_size": 5,  # Numeric - should be converted to "5%"
                        "take_profit_targets": [
                            {"target": 4000, "sell_pct": 50},  # Numeric - should be converted
                            {"target": 5000, "sell_pct": 50}   # Numeric - should be converted
                        ],
                        "stop_loss": 3000,  # Numeric - should be converted
                        "invalidation_trigger": "Test"
                    },
                    "evidence_refs": [],
                    "prerequisites": [],
                    "dependencies": {"requires": [], "data_used": []}
                }
            ],
            "default_recommendation": {
                "action": "hold_btc",
                "reason": "Test"
            }
        })
        
        result = validate_task_output("orchestration", raw_json)
        
        assert result.success is True
        assert result.errors == []


class TestValidateQARiskWithNormalization:
    """Integration tests for qa_risk validation with normalization."""
    
    def test_qa_risk_validates_with_uppercase_status(self):
        """QA risk with uppercase/symbol status should pass after normalization."""
        raw_json = json.dumps({
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
                {"check": "data_quality", "status": "PASS", "notes": "Good"},  # Uppercase
                {"check": "assets_top_200", "status": "✓", "notes": "All OK"},  # Symbol
                {"check": "leverage", "status": "n/a", "notes": "Not applicable to this portfolio"},  # n/a variant
            ],
            "per_recommendation": [
                {
                    "symbol": "ETH",
                    "original_action": "watch",
                    "qa_status": "pass",
                    "issues": [],
                    "risk": {
                        "correlation_with_portfolio": "low",
                        "sector_concentration": "OK",
                        "conviction": "high"  # Will be mapped to "strong"
                    },
                    "verdict": "proceed"
                }
            ],
            "final_verdict": "All checks passed"
        })
        
        result = validate_task_output("qa_risk", raw_json)
        
        assert result.success is True
        assert result.errors == []


# =============================================================================
# Tests for Soft Validation Behavior
# =============================================================================

class TestSoftValidation:
    """Tests for soft validation - allowing partial data to flow through."""
    
    def test_soft_validation_preserves_partial_data(self):
        """JSON with valid structure but invalid enum should return success=True, strict_valid=False."""
        # JSON that parses but has an invalid enum value
        raw_json = json.dumps({
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "invalid_value_not_in_enum",  # Invalid enum value
                "warnings": []
            },
            "portfolio_totals": {
                "total_cost_basis_usd": 10000.00,
                "total_current_value_usd": 12000.00,
                "total_realized_pnl_usd": 500.00,
                "drawdown_from_peak_pct": None
            },
            "positions": [],
            "derived": {
                "btc_quantity": 0.0,
                "btc_allocation_pct_by_value": 0.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": None,
                "max_single_asset_allocation_pct_by_value": 0.0
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
                    "btc_within_target": None,
                    "any_position_over_limit": None,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": None,
                    "total_allocations_sum_to_100": None,
                    "pricing_complete": False,
                    "contradictions_detected": False,
                    "contradictions": []
                }
            }
        })
        
        result = validate_task_output("portfolio_context", raw_json)
        
        # Soft validation: success=True (JSON parseable), strict_valid=False (schema failed)
        assert result.success is True
        assert result.strict_valid is False
        assert result.parsed_data is not None  # Data still available
        assert len(result.validation_warnings) > 0  # Has warnings about schema failure
        assert len(result.errors) == 0  # No critical errors
    
    def test_soft_validation_fails_on_unparseable_json(self):
        """Malformed JSON should return success=False (critical error)."""
        malformed_json = "{ this is not valid json at all }"
        
        result = validate_task_output("portfolio_context", malformed_json)
        
        assert result.success is False
        assert result.strict_valid is False
        assert len(result.errors) > 0
        assert "JSON parse error" in result.errors[0]
    
    def test_soft_validation_full_success(self):
        """Valid JSON with valid schema should return success=True, strict_valid=True."""
        valid_json = json.dumps({
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
            "positions": [],
            "derived": {
                "btc_quantity": 0.0,
                "btc_allocation_pct_by_value": 0.0,
                "tier2_3_allocation_pct_by_value": 0.0,
                "max_single_asset_symbol": None,
                "max_single_asset_allocation_pct_by_value": 0.0
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
                    "btc_within_target": None,
                    "any_position_over_limit": None,
                    "positions_over_limit": [],
                    "tier2_3_within_limit": None,
                    "total_allocations_sum_to_100": None,
                    "pricing_complete": False,
                    "contradictions_detected": False,
                    "contradictions": []
                }
            }
        })
        
        result = validate_task_output("portfolio_context", valid_json)
        
        assert result.success is True
        assert result.strict_valid is True
        assert len(result.errors) == 0
        assert len(result.validation_warnings) == 0
    
    def test_validation_warnings_attached_for_schema_failures(self):
        """Schema validation errors should appear in validation_warnings, not errors."""
        # JSON with a missing required field
        incomplete_json = json.dumps({
            "meta": {
                "agent_name": "portfolio_context",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            # Missing: portfolio_totals, positions, derived, framework
        })
        
        result = validate_task_output("portfolio_context", incomplete_json)
        
        # Soft validation: success=True (JSON parseable), strict_valid=False (missing fields)
        assert result.success is True
        assert result.strict_valid is False
        assert len(result.validation_warnings) > 0  # Schema errors in warnings
        assert len(result.errors) == 0  # No critical errors
        
        # Check that warnings mention the missing fields
        warnings_text = " ".join(result.validation_warnings)
        assert "portfolio_totals" in warnings_text or "Field required" in warnings_text


class TestEnumPreservation:
    """Tests for preserving 'unknown' and other enum values during coercion."""
    
    def test_unknown_preserved_for_correlation_level(self):
        """'unknown' should be preserved for correlation_with_portfolio field."""
        from validation.task_validation import coerce_numeric_value
        
        # correlation_with_portfolio accepts: high, medium, low, unknown
        result = coerce_numeric_value("unknown", "correlation_with_portfolio")
        assert result == "unknown"
    
    def test_unknown_preserved_for_stage_field(self):
        """'unknown' should be preserved for stage field (CycleStage enum)."""
        from validation.task_validation import coerce_numeric_value
        
        # stage accepts: early, mid, late, unknown
        result = coerce_numeric_value("unknown", "stage")
        assert result == "unknown"
    
    def test_unknown_preserved_for_trend_field(self):
        """'unknown' should be preserved for trend field (BTCRelativeTrend enum)."""
        from validation.task_validation import coerce_numeric_value
        
        # trend accepts: outperforming, neutral, underperforming, unknown
        result = coerce_numeric_value("unknown", "trend")
        assert result == "unknown"
    
    def test_unknown_preserved_for_time_horizon_field(self):
        """'unknown' should be preserved for time_horizon field."""
        from validation.task_validation import coerce_numeric_value
        
        # time_horizon accepts: 3-6m, 6-12m, 12m+, unknown
        result = coerce_numeric_value("unknown", "time_horizon")
        assert result == "unknown"
    
    def test_unknown_preserved_for_status_field(self):
        """'unknown' should be preserved for status field (CheckStatus enum)."""
        from validation.task_validation import coerce_numeric_value
        
        # status accepts: pass, fail, unknown, not_applicable
        result = coerce_numeric_value("unknown", "status")
        assert result == "unknown"
    
    def test_null_strings_coerce_to_none_for_numeric_fields(self):
        """Null-like strings should still become None for non-enum fields."""
        from validation.task_validation import coerce_numeric_value
        
        # For a non-enum field, null-like strings should become None
        assert coerce_numeric_value("n/a", "some_numeric_field") is None
        assert coerce_numeric_value("N/A", "some_numeric_field") is None
        assert coerce_numeric_value("null", "some_numeric_field") is None
        assert coerce_numeric_value("none", "some_numeric_field") is None
        assert coerce_numeric_value("unavailable", "some_numeric_field") is None
        assert coerce_numeric_value("-", "some_numeric_field") is None
    
    def test_unknown_not_in_null_strings(self):
        """'unknown' should NOT be converted to None for any field."""
        from validation.task_validation import NULL_STRINGS
        
        assert "unknown" not in NULL_STRINGS
    
    def test_enum_values_normalized_to_lowercase(self):
        """Enum string values should be normalized to lowercase."""
        from validation.task_validation import coerce_numeric_value
        
        # Uppercase enum values should be normalized
        assert coerce_numeric_value("UNKNOWN", "correlation_with_portfolio") == "unknown"
        assert coerce_numeric_value("High", "confidence") == "high"
        assert coerce_numeric_value("EARLY", "stage") == "early"


class TestValidationSummarySoftValidation:
    """Tests for validation summary with soft validation awareness."""
    
    def test_summary_tracks_strict_valid(self):
        """Summary should track strict_valid count separately from usable."""
        results = {
            "task1": ValidationResult(
                success=True, 
                strict_valid=True, 
                task_name="task1"
            ),
            "task2": ValidationResult(
                success=True, 
                strict_valid=False, 
                task_name="task2",
                validation_warnings=["Some warning"]
            ),
            "task3": ValidationResult(
                success=False, 
                strict_valid=False, 
                task_name="task3",
                errors=["JSON parse error"]
            ),
        }
        
        summary = get_validation_summary(results)
        
        assert summary["total_tasks"] == 3
        assert summary["usable"] == 2  # task1 and task2 are usable
        assert summary["strict_valid"] == 1  # Only task1 is strict valid
        assert summary["failed"] == 1  # task3 failed
        assert summary["with_warnings"] == 1  # task2 has warnings
    
    def test_summary_lists_warned_tasks(self):
        """Summary should list tasks that have validation warnings."""
        results = {
            "task1": ValidationResult(
                success=True, 
                strict_valid=False, 
                task_name="task1",
                validation_warnings=["Warning 1", "Warning 2"]
            ),
        }
        
        summary = get_validation_summary(results)
        
        assert len(summary["warned_tasks"]) == 1
        assert summary["warned_tasks"][0]["task"] == "task1"
        assert len(summary["warned_tasks"][0]["warnings"]) == 2


# =============================================================================
# Integration Tests for Soft Validation Pipeline
# =============================================================================

class TestSoftValidationPipeline:
    """Integration tests verifying that soft validation allows data to flow through the pipeline."""
    
    def test_token_research_with_invalid_source_type_gets_normalized(self):
        """token_research with invalid source type gets normalized and passes."""
        # Simulate token_research output with one invalid source type
        # Note: Invalid source types are normalized to "article" by preprocessing
        raw_json = json.dumps({
            "meta": {
                "agent_name": "token_research",
                "schema_version": "1.0",
                "as_of_timestamp_utc": "2026-01-18T12:00:00Z",
                "data_quality": "ok",
                "warnings": []
            },
            "universe": {
                "constraints": {"max_mcap_rank": 200, "exclude_memecoins": True}
            },
            "candidates": [
                {
                    "symbol": "ETH",
                    "name": "Ethereum",
                    "mcap_rank": 2,
                    "category": "L1",
                    "thesis": {
                        "problem": "Smart contract platform",
                        "why_it_wins": "First mover advantage",
                        "network_effects": "Largest ecosystem"
                    },
                    "adoption_metrics": {
                        "tvl_usd": 50000000000,
                        "tvl_change_90d_pct": 15.5,
                        "fees_30d_usd": 100000000,
                        "revenue_30d_usd": 50000000,
                        "dau": 500000,
                        "tx_count_30d": 30000000
                    },
                    "catalysts": ["ETH ETF"],
                    "risks": ["Regulatory risk"],
                    "sources": [
                        {"name": "DefiLlama", "type": "invalid_type_xyz", "ref": "https://defillama.com", "as_of": "2026-01-18"}
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
        })
        
        result = validate_task_output("token_research", raw_json)
        
        # Data is usable (success=True)
        assert result.success is True
        
        # Source type normalization should have converted "invalid_type_xyz" to "article"
        # so it passes strict validation
        assert result.strict_valid is True
        
        # The data should be accessible for downstream agents
        # Note: When strict_valid=True, parsed_data is a Pydantic model
        assert result.parsed_data is not None
        
        # Access data via model attributes (it's a Pydantic model when strict_valid)
        if hasattr(result.parsed_data, 'candidates'):
            # Pydantic model
            assert len(result.parsed_data.candidates) == 1
            assert result.parsed_data.candidates[0].symbol == "ETH"
        else:
            # Dict (soft validation fallback)
            assert "candidates" in result.parsed_data
            assert len(result.parsed_data["candidates"]) == 1
            assert result.parsed_data["candidates"][0]["symbol"] == "ETH"
    
    def test_qa_risk_with_unknown_correlation_validates(self):
        """qa_risk with 'unknown' correlation should pass full validation."""
        raw_json = json.dumps({
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
                {"check": "data_quality", "status": "pass", "notes": "OK"}
            ],
            "per_recommendation": [
                {
                    "symbol": "ETH",
                    "original_action": "watch",
                    "qa_status": "pass",
                    "issues": [],
                    "risk": {
                        "correlation_with_portfolio": "unknown",  # Valid enum value
                        "sector_concentration": "OK",
                        "conviction": "high"
                    },
                    "verdict": "proceed"
                }
            ],
            "final_verdict": "All checks passed"
        })
        
        result = validate_task_output("qa_risk", raw_json)
        
        # "unknown" is a valid CorrelationLevel enum value - should pass strict validation
        assert result.success is True
        assert result.strict_valid is True
        assert len(result.validation_warnings) == 0
        assert len(result.errors) == 0
    
    def test_multiple_tasks_with_mixed_validation_status(self):
        """Simulate multiple task outputs with different validation statuses."""
        # Simulate outputs from different tasks
        task_outputs = {}
        
        # portfolio_context: valid
        task_outputs["portfolio_context"] = json.dumps({
            "meta": {"agent_name": "portfolio_context", "schema_version": "1.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "ok", "warnings": []},
            "portfolio_totals": {"total_cost_basis_usd": 10000, "total_current_value_usd": 12000, "total_realized_pnl_usd": 0, "drawdown_from_peak_pct": None},
            "positions": [],
            "derived": {"btc_quantity": 0, "btc_allocation_pct_by_value": 0, "tier2_3_allocation_pct_by_value": 0, "max_single_asset_symbol": None, "max_single_asset_allocation_pct_by_value": 0},
            "framework": {
                "config": {"btc_target_min_pct": 40, "btc_target_max_pct": 60, "single_asset_limit_pct": 20, "tier2_3_max_pct": 35, "allow_100pct_btc_if_no_alts": False},
                "checks": {"btc_within_target": None, "any_position_over_limit": None, "positions_over_limit": [], "tier2_3_within_limit": None, "total_allocations_sum_to_100": None, "pricing_complete": False, "contradictions_detected": False, "contradictions": []}
            }
        })
        
        # technical_analysis: partial (missing some optional fields but valid structure)
        task_outputs["technical_analysis"] = json.dumps({
            "meta": {"agent_name": "technical_analysis", "schema_version": "1.0", "as_of_timestamp_utc": "2026-01-18T12:00:00Z", "data_quality": "partial", "warnings": ["some_tool_failed"]},
            "assets": [],
            "breadth": {"universe": "custom", "pct_above_200d": None, "pct_golden_cross": None, "median_rsi_14": None, "correlation": []}
        })
        
        # Validate all outputs
        results = {}
        for task_name, raw_output in task_outputs.items():
            results[task_name] = validate_task_output(task_name, raw_output)
        
        # Both should be usable (success=True)
        assert results["portfolio_context"].success is True
        assert results["technical_analysis"].success is True
        
        # portfolio_context should be strict valid
        assert results["portfolio_context"].strict_valid is True
        
        # Both have data that can be passed downstream
        assert results["portfolio_context"].parsed_data is not None
        assert results["technical_analysis"].parsed_data is not None
        
        # Get summary
        summary = get_validation_summary(results)
        assert summary["usable"] == 2
        assert summary["all_usable"] is True
