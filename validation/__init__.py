"""
Validation module for structured agent outputs.

This module provides validation utilities for the multi-agent crypto investing system.
It validates agent outputs against Pydantic schemas and provides retry logic for
failed validations.

Main exports:
- validate_task_output: Validate raw output against task's schema
- ValidationResult: Result object with success status, parsed data, and errors
- ValidationError: Exception raised when validation fails after retry
- enforce_recommendations_contract: Enforce portfolio constraints on recommendations
- should_block_report_generation: Check if QA rejected the run
- preprocess_agent_output: Apply task-specific normalization to agent output
- _normalize_conviction_value: Normalize conviction enum values
- _normalize_compliance_status: Normalize compliance check status values
- _format_price_string: Format numeric prices as clean strings

Debug Mode:
Set DEBUG_VALIDATION=1 to write debug files to reports/debug/ when validation fails.
"""

from validation.task_validation import (
    validate_task_output,
    ValidationResult,
    ValidationError,
    TASK_SCHEMA_MAP,
    STRICT_VALIDATION_TASKS,
    clean_json_output,
    create_retry_prompt,
    validate_all_task_outputs,
    get_validation_summary,
    enforce_trading_plan_rule,
    enforce_recommendations_contract,
    should_block_report_generation,
    get_enforcement_summary,
    ResearchPacket,
    build_research_packet_prompt,
    preprocess_agent_output,
    _normalize_conviction_value,
    _normalize_compliance_status,
    _format_price_string,
    coerce_numeric_value,
    NULL_STRINGS,
    ENUM_FIELDS_WITH_UNKNOWN,
)

__all__ = [
    "validate_task_output",
    "ValidationResult",
    "ValidationError",
    "TASK_SCHEMA_MAP",
    "STRICT_VALIDATION_TASKS",
    "clean_json_output",
    "create_retry_prompt",
    "validate_all_task_outputs",
    "get_validation_summary",
    "enforce_trading_plan_rule",
    "enforce_recommendations_contract",
    "should_block_report_generation",
    "get_enforcement_summary",
    "ResearchPacket",
    "build_research_packet_prompt",
    "preprocess_agent_output",
    "_normalize_conviction_value",
    "_normalize_compliance_status",
    "_format_price_string",
    "coerce_numeric_value",
    "NULL_STRINGS",
    "ENUM_FIELDS_WITH_UNKNOWN",
]
