"""
Task validation module for structured agent outputs.

This module validates agent outputs against Pydantic schemas and provides
retry logic for failed validations.

The validation flow:
1. Strip markdown fences if present
2. JSON parse
3. Schema validate against task's schema
4. Return ValidationResult(success, parsed_data, errors)

Debug Mode:
Set DEBUG_VALIDATION=1 to write debug files to reports/debug/ when validation fails.
This captures raw output, cleaned JSON, and parsed JSON at each stage.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, ValidationError as PydanticValidationError

from schemas.portfolio_context import PortfolioContextSchema


# =============================================================================
# Debug Instrumentation
# =============================================================================

def _write_debug_output(
    task_name: str, 
    run_id: str, 
    stage: str, 
    content: Any,
    errors: Optional[List[str]] = None
) -> None:
    """
    Write debug output when DEBUG_VALIDATION=1.
    
    Creates debug files in reports/debug/ to help diagnose validation failures.
    Each file contains the content at a specific stage of the validation pipeline.
    
    Note: For technical_analysis task, raw output is ALWAYS written (even on success)
    to help diagnose empty assets issues.
    
    Args:
        task_name: Name of the task being validated
        run_id: Unique identifier for this run (typically timestamp)
        stage: Stage of validation (raw, cleaned, parsed, preprocessed, errors)
        content: Content to write (string, dict, or any JSON-serializable object)
        errors: Optional list of validation errors to include
    
    Files created:
        - {run_id}_{task_name}_raw.txt - Raw LLM output
        - {run_id}_{task_name}_cleaned.json - After clean_json_output()
        - {run_id}_{task_name}_parsed.json - After json.loads()
        - {run_id}_{task_name}_preprocessed.json - After preprocess_agent_output()
        - {run_id}_{task_name}_errors.json - Validation errors with context
    """
    if not os.environ.get("DEBUG_VALIDATION"):
        return
    
    try:
        debug_dir = Path("reports/debug")
        debug_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine file extension and content format
        if stage == "raw":
            filename = f"{run_id}_{task_name}_raw.txt"
            # Truncate to first 20KB to avoid huge files
            write_content = str(content)[:20000]
            if len(str(content)) > 20000:
                write_content += "\n\n... (truncated, original length: {} chars)".format(len(str(content)))
            
            with open(debug_dir / filename, "w", encoding="utf-8") as f:
                f.write(write_content)
        else:
            filename = f"{run_id}_{task_name}_{stage}.json"
            
            # Build output structure
            if stage == "errors":
                output = {
                    "task_name": task_name,
                    "run_id": run_id,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "errors": errors or [],
                    "content_preview": str(content)[:5000] if content else None
                }
            elif isinstance(content, dict):
                output = content
            else:
                output = {"content": str(content)[:20000]}
            
            with open(debug_dir / filename, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, default=str)
        
        logger.debug(f"Debug output written: {debug_dir / filename}")
        
    except Exception as e:
        # Don't let debug output failures break validation
        logger.warning(f"Failed to write debug output for {task_name}/{stage}: {e}")


def _get_debug_run_id() -> str:
    """Generate a unique run ID for debug files."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")
from schemas.token_research import TokenResearchSchema
from schemas.technical_analysis import TechnicalAnalysisSchema
from schemas.macro_cycle import MacroCycleSchema
from schemas.recommendations import RecommendationsSchema
from schemas.qa_review import QAReviewSchema

logger = logging.getLogger(__name__)


# =============================================================================
# Schema Mapping
# =============================================================================

TASK_SCHEMA_MAP: Dict[str, Type[BaseModel]] = {
    "portfolio_context": PortfolioContextSchema,
    "token_research": TokenResearchSchema,
    "token_screening": None,  # Intermediate task, no strict schema
    "fundamentals_analysis": None,  # Intermediate task, no strict schema
    "news_sentiment": None,  # Intermediate task, no strict schema
    "technical_analysis": TechnicalAnalysisSchema,
    "macro_analysis": MacroCycleSchema,
    "orchestration": RecommendationsSchema,
    "qa_risk": QAReviewSchema,
}

# Tasks that require strict JSON validation
STRICT_VALIDATION_TASKS = {
    "portfolio_context",
    "token_research",
    "technical_analysis",
    "macro_analysis",
    "orchestration",
    "qa_risk",
}


# =============================================================================
# Validation Result and Exception
# =============================================================================

@dataclass
class ValidationResult:
    """
    Result of validating a task output.
    
    Soft Validation Semantics:
    - success: True if data is usable for downstream agents (JSON parsed successfully)
    - strict_valid: True if data also passes full schema validation
    - validation_warnings: Schema validation errors that don't block inter-agent flow
    
    This allows partial data to flow through the pipeline while tracking
    which outputs need attention in the final report.
    """
    success: bool  # True if JSON parsed (usable for downstream)
    parsed_data: Optional[Union[BaseModel, Dict[str, Any]]] = None
    raw_json: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)  # Critical errors (JSON parse failures)
    task_name: str = ""
    strict_valid: bool = False  # True only if schema validation passed
    validation_warnings: List[str] = field(default_factory=list)  # Schema errors (non-blocking)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "strict_valid": self.strict_valid,
            "task_name": self.task_name,
            "errors": self.errors,
            "validation_warnings": self.validation_warnings,
            "has_parsed_data": self.parsed_data is not None,
        }


class ValidationError(Exception):
    """
    Exception raised when validation fails after retry.
    
    Contains detailed information about the validation failure
    to help with debugging and error reporting.
    """
    def __init__(
        self, 
        task_name: str, 
        errors: List[str], 
        raw_output: Optional[str] = None
    ):
        self.task_name = task_name
        self.errors = errors
        self.raw_output = raw_output
        
        error_summary = "; ".join(errors[:3])
        if len(errors) > 3:
            error_summary += f" (and {len(errors) - 3} more)"
        
        super().__init__(
            f"Task '{task_name}' failed validation after retry: {error_summary}"
        )


# =============================================================================
# JSON Cleaning Utilities
# =============================================================================

def clean_json_output(raw_output: str) -> str:
    """
    Clean raw output to extract JSON content.
    
    Handles common issues:
    - Markdown code fences (```json ... ```)
    - Leading/trailing whitespace
    - Natural language preamble before JSON
    - Trailing text after JSON (e.g., explanations)
    
    Args:
        raw_output: Raw string output from an agent
    
    Returns:
        Cleaned string that should be valid JSON
    """
    if not raw_output:
        return ""
    
    cleaned = raw_output.strip()
    
    # Remove markdown code fences
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    
    cleaned = cleaned.strip()
    
    # Always try to extract the first complete JSON object
    # This handles both preamble text AND trailing text after JSON
    if "{" in cleaned:
        json_start = cleaned.find("{")
        
        # Find the matching closing brace using brace counting
        brace_count = 0
        json_end = json_start
        in_string = False
        escape_next = False
        
        for i, char in enumerate(cleaned[json_start:], json_start):
            # Handle string escaping for accurate brace matching
            if escape_next:
                escape_next = False
                continue
            
            if char == "\\" and in_string:
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            # Only count braces outside of strings
            if not in_string:
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
        
        if json_end > json_start:
            potential_json = cleaned[json_start:json_end]
            # Verify it's valid JSON before returning
            try:
                json.loads(potential_json)
                return potential_json
            except json.JSONDecodeError:
                pass
    
    return cleaned


# =============================================================================
# Numeric Coercion Utilities
# =============================================================================

# Fields that should be integers (not floats)
INTEGER_FIELDS = {"dau", "tx_count_30d", "mcap_rank"}

# Fields that should always be strings (even if they look like numbers)
STRING_FIELDS = {"schema_version"}

# String values that should be converted to None for NUMERIC fields only
# IMPORTANT: "unknown" is NOT in this list because it's a valid enum value
# for CycleStage, BTCRelativeTrend, TimeHorizon, CorrelationLevel, CheckStatus
NULL_STRINGS = {"n/a", "na", "null", "none", "unavailable", "-", ""}

# =============================================================================
# Enum Field Registry (Type-Aware Coercion)
# =============================================================================

# Fields where "unknown" is a VALID enum value and must be preserved
# Maps field_name -> set of valid enum string values
ENUM_FIELDS_WITH_UNKNOWN = {
    # From CycleStage enum
    "stage": {"early", "mid", "late", "unknown"},
    # From BTCRelativeTrend enum
    "trend": {"outperforming", "neutral", "underperforming", "unknown", "bullish", "bearish"},
    # From TimeHorizon enum
    "time_horizon": {"3-6m", "6-12m", "12m+", "unknown"},
    # From CorrelationLevel enum
    "correlation_with_portfolio": {"high", "medium", "low", "unknown"},
    # From CheckStatus enum
    "status": {"pass", "fail", "unknown", "not_applicable"},
    # From Confidence enum (doesn't have unknown but for reference)
    "confidence": {"high", "medium", "low"},
    # From DataQuality enum
    "data_quality": {"ok", "partial", "invalid"},
    # From Signal/Trend enums
    "signal": {"bullish", "neutral", "bearish"},
}

# All fields that should preserve string enum values (don't coerce to None or numbers)
ENUM_STRING_FIELDS = set(ENUM_FIELDS_WITH_UNKNOWN.keys())

# Conviction value mapping: Confidence enum (high/medium/low) -> ConvictionStrength enum (strong/adequate/weak)
# The orchestrator uses Confidence enum, but qa_risk schema uses ConvictionStrength
CONVICTION_MAPPING = {
    "high": "strong",
    "medium": "adequate",
    "low": "weak",
}

# Valid conviction values for orchestration recommendations (Confidence enum)
VALID_CONVICTION_VALUES = {"high", "medium", "low"}

# Compliance checklist status mapping for qa_risk
# Maps various LLM output formats to valid CheckStatus enum values
COMPLIANCE_STATUS_MAPPING = {
    # Pass variants
    "✓": "pass",
    "pass": "pass",
    "passed": "pass",
    "ok": "pass",
    "yes": "pass",
    "true": "pass",
    "✔": "pass",
    "✔️": "pass",
    # Fail variants
    "✗": "fail",
    "✘": "fail",
    "fail": "fail",
    "failed": "fail",
    "no": "fail",
    "false": "fail",
    "❌": "fail",
    # Unknown variants
    "unknown": "unknown",
    "?": "unknown",
    "unclear": "unknown",
    "pending": "unknown",
    # Not applicable variants
    "n/a": "not_applicable",
    "na": "not_applicable",
    "not applicable": "not_applicable",
    "not_applicable": "not_applicable",
    "not-applicable": "not_applicable",
    "-": "not_applicable",
    "none": "not_applicable",
}

# Source type mapping for token_research
# Maps common LLM source type variants to valid SourceType enum values
SOURCE_TYPE_MAPPING = {
    "analysis": "article",
    "report": "paper",
    "education hub": "docs",
    "education": "docs",
    "research": "paper",
    "newsletter": "blog",
    "press release": "article",
    "press_release": "article",
    "whitepaper": "paper",
    "white paper": "paper",
    "docs": "docs",
    "documentation": "docs",
    "blog": "blog",
    "article": "article",
    "dataset": "dataset",
    "dashboard": "dashboard",
    "api": "api",
    "url": "url",
}


def normalize_positions_over_limit(data: Any) -> List[Dict[str, Any]]:
    """
    Normalize positions_over_limit to proper PositionOverLimit object format.
    
    Handles cases where agent returns:
    - Simple string array: ["BTC", "ETH"] -> [{"symbol": "BTC", "allocation_pct": 0.0}, ...]
    - Mixed array: ["BTC", {"symbol": "ETH", "allocation_pct": 25.0}]
    - Already correct format: [{"symbol": "BTC", "allocation_pct": 100.0}]
    
    Args:
        data: The positions_over_limit value from agent output
    
    Returns:
        List of properly formatted PositionOverLimit dictionaries
    """
    if not isinstance(data, list):
        return []
    
    normalized = []
    for item in data:
        if isinstance(item, str):
            # Simple string - convert to object with symbol and default allocation
            normalized.append({
                "symbol": item.upper(),
                "allocation_pct": 0.0  # Will be flagged but won't crash validation
            })
        elif isinstance(item, dict):
            # Already a dict - ensure it has required fields
            normalized.append({
                "symbol": item.get("symbol", "UNKNOWN"),
                "allocation_pct": float(item.get("allocation_pct", 0.0))
            })
        # Skip other types
    
    return normalized


def detect_incomplete_output(raw_output: str) -> Tuple[bool, str]:
    """
    Detect if agent output is incomplete chain-of-thought instead of final JSON.
    
    This happens when an agent outputs raw ReAct-style reasoning:
    - {"Thought": "I need to...", "Action": "tool_name", "Action Input": {...}}
    - Natural language reasoning before JSON
    
    Args:
        raw_output: Raw string output from agent
    
    Returns:
        Tuple of (is_incomplete, error_message)
    """
    if not raw_output:
        return True, "Empty output from agent"
    
    # Patterns indicating incomplete chain-of-thought output
    cot_patterns = [
        '"Thought":', '"Action":', '"Action Input":',
        '{"Thought"', '"Thought":',
        'Action Input', 'Observation:',
    ]
    
    # Check if the output starts with chain-of-thought pattern
    for pattern in cot_patterns:
        if pattern in raw_output[:500]:  # Check first 500 chars
            # But make sure there's no valid JSON following
            if '{"meta":' not in raw_output:
                return True, f"Agent returned chain-of-thought (found '{pattern}') instead of final JSON output"
    
    return False, ""


def generate_fallback_json(task_name: str, error_type: str) -> Dict[str, Any]:
    """
    Generate a fallback JSON structure for a task when agent output is invalid.
    
    Args:
        task_name: Name of the task
        error_type: Type of error that occurred
    
    Returns:
        Minimal valid JSON structure for the task
    """
    from datetime import datetime
    
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    # Base meta structure
    meta = {
        "agent_name": task_name,
        "schema_version": "1.0",
        "as_of_timestamp_utc": timestamp,
        "data_quality": "invalid",
        "warnings": [error_type, "fallback_json_generated"]
    }
    
    # Task-specific fallback structures
    fallbacks = {
        "token_research": {
            "meta": meta,
            "universe": {"constraints": {"max_mcap_rank": 200, "exclude_memecoins": True}},
            "candidates": [],
            "ranked_shortlist": []
        },
        "macro_analysis": {
            "meta": {**meta, "agent_name": "macro_cycle"},
            "regime": {"stance": "neutral", "confidence": "low"},
            "macro": {
                "liquidity": {"summary": "Unable to analyze", "signals": []},
                "fed_policy": {"summary": "Unable to analyze", "signals": []},
                "inflation": {"summary": "Unable to analyze", "signals": []},
                "risk_appetite": {"summary": "Unable to analyze", "signals": []}
            },
            "cycle": {"stage": "unknown", "evidence": [], "halving_context": "Unable to determine"},
            "narratives": [],
            "implications": {"favor": [], "avoid": []},
            "sources": []
        },
        "technical_analysis": {
            "meta": {**meta, "agent_name": "technical_analysis"},
            "assets": [],
            "breadth": {
                "universe": "top_200",
                "pct_above_200d": None,
                "pct_golden_cross": None,
                "median_rsi_14": None,
                "correlation": []
            }
        },
        "portfolio_context": {
            "meta": {**meta, "agent_name": "portfolio_context"},
            "portfolio_totals": {
                "total_cost_basis_usd": 0,
                "total_current_value_usd": None,
                "total_realized_pnl_usd": None,
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
                    "contradictions_detected": True,
                    "contradictions": [error_type]
                }
            }
        }
    }
    
    return fallbacks.get(task_name, {"meta": meta})


def coerce_numeric_value(value: Any, field_name: str = "") -> Any:
    """
    Coerce a value to the appropriate numeric type if it's a formatted string.
    
    Handles common LLM formatting issues:
    - Numbers with commas: "1,234,567" -> 1234567
    - Numbers with currency: "$1000" -> 1000
    - NA/null strings: "N/A" -> None (for non-enum fields only)
    - schema_version: 4.0 -> "4.0" (convert to string)
    
    Type-Aware Coercion:
    - Enum fields (like status, stage, trend) preserve their string values
    - "unknown" is preserved for enum fields where it's a valid value
    - Only non-enum numeric fields coerce null-like strings to None
    
    Args:
        value: The value to coerce
        field_name: Optional field name to determine type-aware handling
    
    Returns:
        Coerced value (int, float, None, or original value)
    """
    if value is None:
        return None
    
    # Handle fields that should be strings (like schema_version)
    if field_name in STRING_FIELDS:
        if isinstance(value, (int, float)):
            # Convert numeric to string (e.g., 4.0 -> "4.0", 1 -> "1.0")
            str_val = str(value)
            # Ensure version strings have decimal point (4 -> "4.0")
            if "." not in str_val:
                str_val = str_val + ".0"
            return str_val
        return str(value) if value is not None else value
    
    # TYPE-AWARE COERCION: Check if this is an enum field FIRST
    # Enum fields should preserve their string values and not be coerced
    if field_name in ENUM_STRING_FIELDS:
        if isinstance(value, str):
            # Normalize case for enum matching
            normalized = value.lower().strip()
            valid_values = ENUM_FIELDS_WITH_UNKNOWN.get(field_name, set())
            
            # If the normalized value is valid for this enum, preserve it
            if normalized in valid_values:
                return normalized
            
            # Otherwise return the original string (schema validation will catch invalid values)
            return value
        return value
    
    # NOTE: Conviction mapping (high/medium/low -> strong/adequate/weak) is now handled
    # specifically in preprocess_agent_output() for qa_risk only, not here.
    # This avoids incorrectly mapping orchestration.recommendations[].conviction
    
    if isinstance(value, (int, float)):
        # If it's already a number but should be an integer, convert
        if field_name in INTEGER_FIELDS and isinstance(value, float):
            return int(value)
        return value
    
    if not isinstance(value, str):
        return value

    # For non-enum string fields, check for null-like strings
    # IMPORTANT: "unknown" is not in NULL_STRINGS to preserve enum values
    if value.lower().strip() in NULL_STRINGS:
        return None
    
    # Try to parse as number
    cleaned = value.strip()
    
    # Remove common formatting
    cleaned = cleaned.replace(",", "")  # Remove commas
    cleaned = cleaned.replace("$", "")  # Remove dollar signs
    cleaned = cleaned.replace("%", "")  # Remove percent signs
    cleaned = cleaned.strip()
    
    if not cleaned:
        return None
    
    # Try integer first (for integer fields)
    if field_name in INTEGER_FIELDS:
        try:
            return int(float(cleaned))  # Handle "1.0" -> 1
        except (ValueError, TypeError):
            return value  # Return original if conversion fails
    
    # Try float for other numeric fields
    try:
        result = float(cleaned)
        # If it's a whole number and looks like it should be int, convert
        if result.is_integer() and "." not in value:
            return int(result)
        return result
    except (ValueError, TypeError):
        return value  # Return original if not a number


def coerce_numeric_values_recursive(data: Any, parent_key: str = "") -> Any:
    """
    Recursively coerce numeric values in a data structure.
    
    Args:
        data: Dictionary, list, or value to process
        parent_key: Key of the parent field (for context)
    
    Returns:
        Data structure with coerced numeric values
    """
    if isinstance(data, dict):
        return {
            k: coerce_numeric_values_recursive(v, k)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            coerce_numeric_values_recursive(item, parent_key)
            for item in data
        ]
    else:
        return coerce_numeric_value(data, parent_key)


def _format_price_string(value: Union[int, float]) -> str:
    """
    Format a numeric price value as a clean string.
    
    Handles:
    - Integer values: 65000 -> "65000"
    - Float values with no decimals: 65000.0 -> "65000"
    - Float values with decimals: 65000.50 -> "65000.50"
    
    Args:
        value: Numeric price value
    
    Returns:
        Clean string representation
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _normalize_conviction_value(conviction: Any) -> str:
    """
    Normalize conviction values to valid enum format.
    
    Handles:
    - Case variations: "High", "HIGH", "high" -> "high"
    - Extra text: "High (0.7)", "Medium confidence" -> "medium"
    - Already valid values pass through
    
    Args:
        conviction: Raw conviction value from LLM
    
    Returns:
        Normalized conviction string ("high", "medium", or "low")
        or original value if not recognizable
    """
    if not isinstance(conviction, str):
        return conviction
    
    # Extract the first word and normalize
    # This handles cases like "High (0.7)" or "Medium confidence"
    normalized = conviction.lower().split()[0].strip("().,:")
    
    if normalized in VALID_CONVICTION_VALUES:
        return normalized
    
    # Return original if we can't normalize
    return conviction


def _normalize_compliance_status(status: Any) -> str:
    """
    Normalize compliance checklist status to valid enum format.
    
    Handles:
    - Case variations: "PASS", "Pass", "pass" -> "pass"
    - Unicode checkmarks: "✓", "✔" -> "pass"
    - Common variations: "ok", "yes", "failed" -> appropriate enum
    
    Args:
        status: Raw status value from LLM
    
    Returns:
        Normalized status string ("pass", "fail", "unknown", or "not_applicable")
    """
    if not isinstance(status, str):
        return "unknown"
    
    # Normalize and look up in mapping
    normalized_key = status.lower().strip()
    return COMPLIANCE_STATUS_MAPPING.get(normalized_key, "unknown")


def _normalize_source_type(source_type: Any) -> str:
    """
    Normalize token research source types to valid enum values.
    
    Handles:
    - Common variants: "analysis", "report", "education hub"
    - Case variations and punctuation
    
    Args:
        source_type: Raw source type value from LLM
    
    Returns:
        Normalized source type string (valid SourceType enum)
    """
    if not isinstance(source_type, str):
        return "article"
    
    normalized = source_type.lower().strip()
    normalized = normalized.replace("-", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    
    return SOURCE_TYPE_MAPPING.get(normalized, "article")


def preprocess_agent_output(task_name: str, raw_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply task-specific coercion and normalization to agent output.
    
    This function handles:
    1. Task-specific enum normalization FIRST (before generic coercion can alter values):
       - orchestration: conviction enum normalization
       - qa_risk: compliance_checklist status normalization, conviction mapping
    2. General coercion (schema_version, numeric formatting)
    3. Task-specific numeric-to-string conversion AFTER generic coercion:
       - orchestration: trading plan stop_loss and take_profit_targets
       - portfolio_context: positions_over_limit
    
    Args:
        task_name: Name of the task
        raw_json: Parsed JSON from agent output
    
    Returns:
        Preprocessed JSON ready for schema validation
    """
    # Step 1: Task-specific enum normalization BEFORE generic coercion
    # This ensures values like "n/a" or "High" are normalized before NULL_STRINGS check
    
    if task_name == "orchestration":
        # Normalize conviction values BEFORE generic coercion
        for rec in raw_json.get("recommendations", []):
            # Normalize conviction: "High", "HIGH", "High (0.7)" -> "high"
            if "conviction" in rec:
                rec["conviction"] = _normalize_conviction_value(rec["conviction"])
    
    if task_name == "qa_risk":
        # Normalize compliance_checklist status values BEFORE generic coercion
        # This ensures "n/a" is mapped to "not_applicable" before NULL_STRINGS converts it to None
        for check in raw_json.get("compliance_checklist", []):
            if "status" in check:
                check["status"] = _normalize_compliance_status(check["status"])
        
        # Map per_recommendation conviction values (high/medium/low -> strong/adequate/weak)
        for rec in raw_json.get("per_recommendation", []):
            if "risk" in rec and "conviction" in rec["risk"]:
                conv = rec["risk"]["conviction"]
                if isinstance(conv, str):
                    # First normalize case, then map to ConvictionStrength
                    normalized = conv.lower().strip()
                    rec["risk"]["conviction"] = CONVICTION_MAPPING.get(
                        normalized, normalized
                    )

    if task_name == "token_research":
        # Normalize sources[].type values to valid SourceType enums
        for candidate in raw_json.get("candidates", []):
            for source in candidate.get("sources", []):
                if "type" in source:
                    source["type"] = _normalize_source_type(source["type"])
    
    # Step 2: Apply recursive numeric coercion (handles schema_version, numeric formatting)
    raw_json = coerce_numeric_values_recursive(raw_json)
    
    # Step 3: Task-specific numeric-to-string conversion AFTER generic coercion
    # This ensures numbers stay as strings (generic coercion might convert string numbers to numbers)
    
    if task_name == "orchestration":
        # Convert trading plan numeric fields to strings AFTER generic coercion
        for rec in raw_json.get("recommendations", []):
            if "trading_plan" in rec and rec["trading_plan"]:
                tp = rec["trading_plan"]
                
                # Convert stop_loss to string if numeric
                if "stop_loss" in tp and isinstance(tp["stop_loss"], (int, float)):
                    tp["stop_loss"] = _format_price_string(tp["stop_loss"])
                
                # Convert take_profit_targets[].target to strings if numeric
                for tpt in tp.get("take_profit_targets", []):
                    if "target" in tpt and isinstance(tpt["target"], (int, float)):
                        tpt["target"] = _format_price_string(tpt["target"])
                
                # Also convert position_size to string if numeric
                if "position_size" in tp and isinstance(tp["position_size"], (int, float)):
                    tp["position_size"] = f"{tp['position_size']}%"
    
    if task_name == "portfolio_context":
        # Normalize positions_over_limit if present
        framework = raw_json.get("framework", {})
        checks = framework.get("checks", {})
        if "positions_over_limit" in checks:
            checks["positions_over_limit"] = normalize_positions_over_limit(
                checks["positions_over_limit"]
            )
    
    return raw_json


# =============================================================================
# Core Validation Functions
# =============================================================================

def validate_task_output(
    task_name: str,
    raw_output: str,
    strict: bool = True
) -> ValidationResult:
    """
    Validate raw task output against its schema with SOFT VALIDATION.
    
    Soft Validation Semantics:
    - success=True means JSON parsed successfully (data is usable for downstream agents)
    - strict_valid=True means data also passes full schema validation
    - validation_warnings contains schema errors that don't block inter-agent flow
    
    This allows partial data to flow through the pipeline while tracking
    which outputs need attention in the final report.
    
    Steps:
    1. Clean the output (remove markdown fences, etc.)
    2. Parse as JSON -> if fails, success=False (critical error)
    3. Validate against the task's schema -> if fails, success=True but strict_valid=False
    4. Return ValidationResult with appropriate flags
    
    Debug Mode:
    Set DEBUG_VALIDATION=1 to write debug files to reports/debug/ when validation fails.
    
    Args:
        task_name: Name of the task (maps to schema)
        raw_output: Raw string output from the agent
        strict: If True, attempt schema validation (but don't block on failure).
    
    Returns:
        ValidationResult with success status, parsed data, and any errors/warnings
    """
    errors: List[str] = []
    validation_warnings: List[str] = []
    run_id = _get_debug_run_id()
    
    # Check if task has a schema
    schema_class = TASK_SCHEMA_MAP.get(task_name)
    
    if schema_class is None:
        # No strict schema - just try to parse JSON for intermediate tasks
        if task_name not in STRICT_VALIDATION_TASKS:
            # Try to parse as JSON anyway for logging
            raw_json = None
            try:
                cleaned = clean_json_output(raw_output)
                if cleaned:
                    raw_json = json.loads(cleaned)
            except (json.JSONDecodeError, Exception):
                pass
            return ValidationResult(
                success=True,
                parsed_data=raw_json,
                raw_json=raw_json,
                errors=[],
                task_name=task_name,
                strict_valid=True,  # No schema = trivially valid
                validation_warnings=[],
            )
    
    # Step 0: Check for incomplete chain-of-thought output
    # This is a warning, not a blocker - try to extract JSON anyway
    is_incomplete, incomplete_reason = detect_incomplete_output(raw_output)
    if is_incomplete:
        logger.warning(f"Task {task_name}: {incomplete_reason}")
        _write_debug_output(task_name, run_id, "raw", raw_output)
        validation_warnings.append(incomplete_reason)
        # Continue processing - there might still be valid JSON in the output
    
    # Step 1: Clean the output
    cleaned = clean_json_output(raw_output)
    
    if not cleaned:
        # CRITICAL ERROR: No usable output at all
        errors.append("Empty output after cleaning")
        _write_debug_output(task_name, run_id, "raw", raw_output)
        _write_debug_output(task_name, run_id, "errors", None, errors)
        
        # Generate fallback only when there's truly nothing to parse
        if task_name in STRICT_VALIDATION_TASKS:
            fallback_json = generate_fallback_json(task_name, "Empty output - using fallback")
            return ValidationResult(
                success=False,
                parsed_data=fallback_json,  # Provide fallback as parsed_data
                raw_json=fallback_json,
                errors=errors,
                task_name=task_name,
                strict_valid=False,
                validation_warnings=["fallback_json_generated"],
            )
        
        return ValidationResult(
            success=False,
            parsed_data=None,
            raw_json=None,
            errors=errors,
            task_name=task_name,
            strict_valid=False,
            validation_warnings=[],
        )
    
    # Step 2: Parse as JSON
    raw_json = None
    try:
        raw_json = json.loads(cleaned)
    except json.JSONDecodeError as e:
        # CRITICAL ERROR: Cannot parse JSON - this blocks downstream
        errors.append(f"JSON parse error: {e}")
        errors.append(f"Output starts with: {cleaned[:100]}...")
        _write_debug_output(task_name, run_id, "raw", raw_output)
        _write_debug_output(task_name, run_id, "cleaned", {"cleaned_output": cleaned[:5000]})
        _write_debug_output(task_name, run_id, "errors", None, errors)
        
        # Generate fallback only when JSON parsing fails completely
        if task_name in STRICT_VALIDATION_TASKS:
            fallback_json = generate_fallback_json(task_name, f"JSON parse error: {e}")
            return ValidationResult(
                success=False,
                parsed_data=fallback_json,
                raw_json=fallback_json,
                errors=errors,
                task_name=task_name,
                strict_valid=False,
                validation_warnings=["fallback_json_generated"],
            )
        
        return ValidationResult(
            success=False,
            parsed_data=None,
            raw_json=None,
            errors=errors,
            task_name=task_name,
            strict_valid=False,
            validation_warnings=[],
        )
    
    # JSON PARSED SUCCESSFULLY - from here on, success=True (data is usable)
    
    # Write debug output for parsed JSON (before preprocessing)
    _write_debug_output(task_name, run_id, "parsed", raw_json)
    
    # Step 2.5: Apply comprehensive preprocessing (coercion + task-specific normalization)
    # This handles: schema_version, conviction mapping, positions_over_limit, numeric formatting
    raw_json = preprocess_agent_output(task_name, raw_json)
    
    # Write debug output for preprocessed JSON
    _write_debug_output(task_name, run_id, "preprocessed", raw_json)
    
    # Step 3: Validate against schema (if strict and schema exists)
    # Schema validation failure is a WARNING, not an error - data still flows
    if strict and schema_class is not None:
        try:
            parsed_model = schema_class.model_validate(raw_json)
            
            # Schema validation PASSED - full success
            # Always write raw output for technical_analysis to help debug empty assets issue
            if task_name == "technical_analysis":
                _write_debug_output(task_name, run_id, "raw", raw_output)
                # Check for empty assets and add warning
                assets = raw_json.get("assets", [])
                if not assets:
                    validation_warnings.append(
                        "Technical analysis has empty assets array - no assets were analyzed"
                    )
                    logger.warning(
                        f"Technical analysis passed validation but has empty assets array. "
                        f"Raw output written to reports/debug/{run_id}_technical_analysis_raw.txt"
                    )
            
            return ValidationResult(
                success=True,
                parsed_data=parsed_model,
                raw_json=raw_json,
                errors=[],
                task_name=task_name,
                strict_valid=True,  # Full schema validation passed
                validation_warnings=validation_warnings,
            )
            
        except PydanticValidationError as e:
            # Schema validation FAILED - but data is still usable (soft validation)
            # Extract readable error messages as WARNINGS, not errors
            for error in e.errors():
                loc = ".".join(str(x) for x in error["loc"])
                msg = error["msg"]
                validation_warnings.append(f"Schema warning at '{loc}': {msg}")
            
            # Write debug output for validation warnings
            _write_debug_output(task_name, run_id, "raw", raw_output)
            _write_debug_output(task_name, run_id, "errors", raw_json, validation_warnings)
            
            logger.warning(
                f"Task {task_name}: Schema validation failed with {len(validation_warnings)} warnings. "
                f"Data will still flow to downstream agents (soft validation)."
            )
            
            # SOFT VALIDATION: Return success=True with the raw JSON data
            # Downstream agents can still use this data
            return ValidationResult(
                success=True,  # JSON parsed - usable for downstream
                parsed_data=raw_json,  # Pass the raw JSON (not the model)
                raw_json=raw_json,
                errors=[],  # No critical errors
                task_name=task_name,
                strict_valid=False,  # Schema validation failed
                validation_warnings=validation_warnings,
            )
    else:
        # Non-strict mode or no schema - just use raw JSON
        return ValidationResult(
            success=True,
            parsed_data=raw_json,
            raw_json=raw_json,
            errors=[],
            task_name=task_name,
            strict_valid=True,  # No schema = trivially valid
            validation_warnings=validation_warnings,
        )


def create_retry_prompt(task_name: str, errors: List[str]) -> str:
    """
    Create an augmented prompt for a retry attempt.
    
    Args:
        task_name: Name of the failed task
        errors: List of validation errors from the first attempt
    
    Returns:
        Augmented prompt string to prepend to the retry
    """
    error_list = "\n".join(f"- {e}" for e in errors[:5])
    
    return f"""Your previous output failed schema validation. Please fix the errors and try again.

VALIDATION ERRORS:
{error_list}

REQUIREMENTS:
1. Output ONLY valid JSON - no markdown code fences, no preamble
2. Your entire response must be a single JSON object starting with {{ and ending with }}
3. Ensure all required fields are present and have correct types
4. Numbers must be numbers (not strings), booleans must be true/false (not strings)
5. Use null for missing optional values, not empty strings

Try again with ONLY the corrected JSON output:"""


def validate_all_task_outputs(
    task_outputs: Dict[str, str]
) -> Dict[str, ValidationResult]:
    """
    Validate all task outputs at once.
    
    Args:
        task_outputs: Dictionary mapping task names to raw outputs
    
    Returns:
        Dictionary mapping task names to ValidationResults
    """
    results = {}
    
    for task_name, raw_output in task_outputs.items():
        strict = task_name in STRICT_VALIDATION_TASKS
        results[task_name] = validate_task_output(task_name, raw_output, strict=strict)
    
    return results


def get_validation_summary(results: Dict[str, ValidationResult]) -> Dict[str, Any]:
    """
    Get a summary of validation results with soft validation awareness.
    
    Args:
        results: Dictionary of validation results
    
    Returns:
        Summary dictionary with counts for:
        - usable: JSON parsed successfully (success=True)
        - strict_valid: Also passed schema validation
        - failed: JSON parsing failed (success=False)
        - with_warnings: Had validation warnings
    """
    total = len(results)
    usable = sum(1 for r in results.values() if r.success)
    strict_valid = sum(1 for r in results.values() if r.success and r.strict_valid)
    failed = sum(1 for r in results.values() if not r.success)
    with_warnings = sum(1 for r in results.values() if r.validation_warnings)
    
    # Tasks that failed completely (JSON parse errors)
    failed_tasks = [
        {"task": name, "errors": result.errors}
        for name, result in results.items()
        if not result.success
    ]
    
    # Tasks with validation warnings (soft failures)
    warned_tasks = [
        {"task": name, "warnings": result.validation_warnings}
        for name, result in results.items()
        if result.success and not result.strict_valid
    ]
    
    return {
        "total_tasks": total,
        "usable": usable,  # JSON parsed successfully
        "strict_valid": strict_valid,  # Also passed schema validation
        "failed": failed,  # JSON parsing failed
        "with_warnings": with_warnings,
        "all_usable": failed == 0,  # All data usable for downstream
        "all_strict_valid": strict_valid == total,  # All passed schema validation
        "failed_tasks": failed_tasks,
        "warned_tasks": warned_tasks,
        # Backward compatibility
        "passed": usable,
        "all_passed": failed == 0,
    }


# =============================================================================
# Cross-Task Validation
# =============================================================================

def validate_technical_analysis_coverage(
    tech_output: Optional[Dict[str, Any]], 
    token_research_output: Optional[Dict[str, Any]]
) -> List[str]:
    """
    Validate that technical analysis covers token research candidates.
    
    This function detects when the Technical Analyst agent returns empty
    assets despite receiving candidates from token_research.
    
    Args:
        tech_output: Parsed technical_analysis output (can be None)
        token_research_output: Parsed token_research output (can be None)
    
    Returns:
        List of warning messages if coverage issues are detected
    """
    warnings = []
    
    if tech_output is None or token_research_output is None:
        return warnings
    
    # Extract symbols from token research
    candidates = token_research_output.get("candidates", [])
    shortlist = token_research_output.get("ranked_shortlist", [])
    
    # Get unique symbols from both sources
    expected_symbols = set()
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("symbol"):
            expected_symbols.add(candidate["symbol"].upper())
    for item in shortlist:
        if isinstance(item, dict) and item.get("symbol"):
            expected_symbols.add(item["symbol"].upper())
    
    # Extract analyzed symbols from technical analysis
    assets = tech_output.get("assets", [])
    analyzed_symbols = set()
    for asset in assets:
        if isinstance(asset, dict) and asset.get("symbol"):
            analyzed_symbols.add(asset["symbol"].upper())
    
    # Check for coverage issues
    if expected_symbols and not analyzed_symbols:
        warnings.append(
            f"CRITICAL: Technical analysis has empty assets array but token_research "
            f"provided {len(expected_symbols)} candidates: {sorted(expected_symbols)}. "
            f"The Technical Analyst agent may not have used its tools."
        )
    elif expected_symbols and analyzed_symbols:
        missing_symbols = expected_symbols - analyzed_symbols
        if missing_symbols:
            warnings.append(
                f"Technical analysis missing coverage for {len(missing_symbols)} "
                f"candidates: {sorted(missing_symbols)}"
            )
    
    return warnings


def validate_cross_task_outputs(
    validated_outputs: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Perform cross-task validation on all validated outputs.
    
    This function checks consistency and coverage between related tasks.
    
    Args:
        validated_outputs: Dictionary of validated outputs by task name
    
    Returns:
        Dictionary with validation results:
        - warnings: List of warning messages
        - errors: List of error messages (critical issues)
        - passed: Boolean indicating if all cross-task checks passed
    """
    warnings = []
    errors = []
    
    # Convert BaseModel objects to dicts for easier access
    def to_dict(obj):
        if obj is None:
            return None
        if isinstance(obj, BaseModel):
            return obj.model_dump()
        if isinstance(obj, dict):
            return obj
        return None
    
    tech_output = to_dict(validated_outputs.get("technical_analysis"))
    token_research_output = to_dict(validated_outputs.get("token_research"))
    
    # Validate technical analysis coverage
    coverage_warnings = validate_technical_analysis_coverage(
        tech_output, token_research_output
    )
    
    # Classify as errors if critical
    for warning in coverage_warnings:
        if "CRITICAL" in warning:
            errors.append(warning)
        else:
            warnings.append(warning)
    
    return {
        "warnings": warnings,
        "errors": errors,
        "passed": len(errors) == 0,
    }


# =============================================================================
# Trading Plan Enforcement
# =============================================================================

# =============================================================================
# Research Packet for Orchestrator/QA
# =============================================================================

class ResearchPacket(BaseModel):
    """
    Combined research packet for Orchestrator and QA agents.
    
    Contains validated outputs from upstream agents, enabling
    downstream agents to consume structured data only.
    """
    portfolio_context: Optional[Dict[str, Any]] = None
    macro_cycle: Optional[Dict[str, Any]] = None
    technical_analysis: Optional[Dict[str, Any]] = None
    token_research: Optional[Dict[str, Any]] = None
    
    def to_json_string(self) -> str:
        """Serialize the research packet to JSON string for prompt injection."""
        return json.dumps(self.model_dump(exclude_none=True), indent=2)
    
    @classmethod
    def from_validated_outputs(
        cls, 
        validated_outputs: Dict[str, Any]
    ) -> "ResearchPacket":
        """
        Create a ResearchPacket from validated task outputs.
        
        Args:
            validated_outputs: Dictionary of validated outputs by task name
        
        Returns:
            ResearchPacket instance
        """
        def to_dict(obj):
            if obj is None:
                return None
            if isinstance(obj, BaseModel):
                return obj.model_dump()
            if isinstance(obj, dict):
                return obj
            return None
        
        return cls(
            portfolio_context=to_dict(validated_outputs.get("portfolio_context")),
            macro_cycle=to_dict(validated_outputs.get("macro_analysis")),
            technical_analysis=to_dict(validated_outputs.get("technical_analysis")),
            token_research=to_dict(validated_outputs.get("token_research")),
        )


def build_research_packet_prompt(validated_outputs: Dict[str, Any]) -> str:
    """
    Build a RESEARCH_PACKET_JSON prompt section from validated outputs.
    
    Args:
        validated_outputs: Dictionary of validated outputs by task name
    
    Returns:
        Formatted string to inject into Orchestrator/QA prompts
    """
    packet = ResearchPacket.from_validated_outputs(validated_outputs)
    return f"""
## RESEARCH_PACKET_JSON

The following is the validated research data from upstream agents. Use ONLY this data for your analysis.

```json
{packet.to_json_string()}
```

Parse this JSON and reference the specific fields. Do NOT invent data not present in this packet.
"""


# =============================================================================
# Trading Plan Enforcement
# =============================================================================

def enforce_trading_plan_rule(
    recommendations: RecommendationsSchema
) -> RecommendationsSchema:
    """
    Enforce trading plan completeness for BUY actions.
    
    If action == 'buy' but trading_plan is incomplete:
    - Downgrade action to 'watch'
    - Add warning to meta.warnings
    
    This is called automatically by the schema validator, but can also
    be called manually for additional enforcement.
    
    Args:
        recommendations: Validated RecommendationsSchema
    
    Returns:
        Modified schema with enforced rules
    """
    # The RecommendationsSchema already has a model_validator that does this,
    # but we provide this function for explicit enforcement if needed
    from schemas.base import Action
    
    for rec in recommendations.recommendations:
        if rec.action == Action.BUY or rec.action == "buy":
            if rec.trading_plan is None or not rec.trading_plan.is_complete():
                rec.action = Action.WATCH
                if f"{rec.symbol}: Downgraded" not in str(recommendations.meta.warnings):
                    recommendations.meta.warnings.append(
                        f"{rec.symbol}: Downgraded from BUY to WATCH - trading plan incomplete"
                    )
    
    return recommendations


# =============================================================================
# Recommendations Contract Enforcement
# =============================================================================

def enforce_recommendations_contract(
    recommendations: RecommendationsSchema,
    portfolio_context: PortfolioContextSchema
) -> Tuple[RecommendationsSchema, List[str]]:
    """
    Enforce all recommendations contract rules against portfolio constraints.
    
    This function applies deterministic rules AFTER schema validation:
    1. Trading plan completeness (already done by schema, but re-checked)
    2. Position size limits (auto-reduce if exceeds single_asset_limit_pct)
    3. Tier 2+3 allocation limits
    4. Default recommendation when no actionable recs
    
    Args:
        recommendations: Validated RecommendationsSchema from Orchestrator
        portfolio_context: Validated PortfolioContextSchema with framework config
    
    Returns:
        Tuple of (enforced_recommendations, warnings_list)
    """
    from schemas.base import Action, DefaultAction
    
    warnings = []
    
    # Extract framework config
    config = portfolio_context.framework.config
    single_asset_limit = config.single_asset_limit_pct
    tier2_3_max = config.tier2_3_max_pct
    btc_target_min = config.btc_target_min_pct
    
    # Current allocations
    derived = portfolio_context.derived
    current_tier2_3_pct = derived.tier2_3_allocation_pct_by_value
    current_btc_pct = derived.btc_allocation_pct_by_value
    
    # Track how much we're proposing to add to tier 2+3
    proposed_tier2_3_addition = 0.0
    
    # Process each recommendation
    for rec in recommendations.recommendations:
        action_str = rec.action if isinstance(rec.action, str) else rec.action.value
        
        # 1. Re-enforce trading plan rule for BUY actions
        if action_str == "buy":
            if rec.trading_plan is None or not rec.trading_plan.is_complete():
                rec.action = Action.WATCH
                rec._downgraded_from_buy = True
                
                if rec.trading_plan is None:
                    reason = "no trading plan provided"
                else:
                    missing = rec.trading_plan.get_missing_fields()
                    reason = f"missing: {', '.join(missing)}"
                
                rec._downgrade_reason = reason
                warning = f"{rec.symbol}: Downgraded from BUY to WATCH - {reason}"
                if warning not in recommendations.meta.warnings:
                    recommendations.meta.warnings.append(warning)
                    warnings.append(warning)
                continue  # Skip further checks for downgraded recs
        
        # 2. Check position size limits for BUY actions
        if action_str == "buy" and rec.suggested_allocation_pct_portfolio is not None:
            if rec.suggested_allocation_pct_portfolio > single_asset_limit:
                original_alloc = rec.suggested_allocation_pct_portfolio
                rec.suggested_allocation_pct_portfolio = single_asset_limit
                
                # Also update trading_plan.position_size if present
                if rec.trading_plan and rec.trading_plan.position_size:
                    rec.trading_plan.position_size = f"{single_asset_limit}% of portfolio (reduced from {original_alloc}%)"
                
                warning = (
                    f"{rec.symbol}: Allocation reduced from {original_alloc}% to {single_asset_limit}% "
                    f"(single_asset_limit_pct)"
                )
                recommendations.meta.warnings.append(warning)
                warnings.append(warning)
        
        # 3. Track Tier 2+3 additions
        if action_str == "buy" and rec.tier in (2, 3):
            allocation = rec.suggested_allocation_pct_portfolio or 0
            proposed_tier2_3_addition += allocation
        
        # 4. Check if buy would push BTC below floor
        if action_str == "buy" and rec.suggested_allocation_pct_portfolio:
            # Simplistic check: if current BTC is near floor and we're buying alts, warn
            new_btc_pct_approx = current_btc_pct - rec.suggested_allocation_pct_portfolio
            if new_btc_pct_approx < btc_target_min and rec.tier != 0:
                warning = (
                    f"{rec.symbol}: BUY would reduce BTC allocation to ~{new_btc_pct_approx:.1f}% "
                    f"(below {btc_target_min}% floor) - consider reducing allocation or DCA approach"
                )
                recommendations.meta.warnings.append(warning)
                warnings.append(warning)
    
    # 5. Check total Tier 2+3 limit
    projected_tier2_3 = current_tier2_3_pct + proposed_tier2_3_addition
    if projected_tier2_3 > tier2_3_max:
        warning = (
            f"Tier 2+3 allocation would reach {projected_tier2_3:.1f}% "
            f"(limit: {tier2_3_max}%) - some recommendations may need reduction"
        )
        recommendations.meta.warnings.append(warning)
        warnings.append(warning)
    
    # 6. Set default recommendation if no actionable recs
    actionable_actions = {"buy", "reduce", "sell"}
    has_actionable = any(
        (rec.action if isinstance(rec.action, str) else rec.action.value) in actionable_actions
        for rec in recommendations.recommendations
    )
    
    if not has_actionable:
        # Determine appropriate default - ALWAYS update when no actionable recs
        if portfolio_context.meta.data_quality == "invalid":
            recommendations.default_recommendation.action = DefaultAction.DO_NOTHING
            recommendations.default_recommendation.reason = (
                "Portfolio data quality invalid - no trades until data issues resolved"
            )
        else:
            # Set to do_nothing since there are no actionable recommendations
            recommendations.default_recommendation.action = DefaultAction.DO_NOTHING
            recommendations.default_recommendation.reason = (
                "No high-conviction actionable recommendations - "
                "all candidates downgraded or set to WATCH/HOLD"
            )
        
        warning = "No actionable recommendations (BUY/REDUCE/SELL) after enforcement"
        if warning not in recommendations.meta.warnings:
            recommendations.meta.warnings.append(warning)
            warnings.append(warning)
    
    return recommendations, warnings


def should_block_report_generation(qa_review: QAReviewSchema) -> Tuple[bool, str]:
    """
    Determine if report generation should be blocked based on QA review.
    
    Args:
        qa_review: Validated QAReviewSchema
    
    Returns:
        Tuple of (should_block, reason)
    """
    from schemas.base import OverallStatus
    
    status = qa_review.overall_status
    status_str = status if isinstance(status, str) else status.value
    
    if status_str == "reject":
        # Find the most critical reason
        reject_reasons = []
        
        # Check compliance checklist for failures
        for check in qa_review.compliance_checklist:
            check_status = check.status if isinstance(check.status, str) else check.status.value
            if check_status == "fail":
                reject_reasons.append(f"{check.check}: {check.notes}")
        
        # Check per-recommendation rejections
        for per_rec in qa_review.per_recommendation:
            verdict = per_rec.verdict if isinstance(per_rec.verdict, str) else per_rec.verdict.value
            if verdict == "reject":
                reject_reasons.append(f"{per_rec.symbol} rejected: {', '.join(per_rec.issues)}")
        
        reason = "; ".join(reject_reasons[:3]) if reject_reasons else "QA review rejected"
        if len(reject_reasons) > 3:
            reason += f" (+{len(reject_reasons) - 3} more issues)"
        
        return True, reason
    
    return False, ""


def get_enforcement_summary(
    original_recommendations: RecommendationsSchema,
    enforced_recommendations: RecommendationsSchema,
    warnings: List[str]
) -> Dict[str, Any]:
    """
    Generate a summary of enforcement actions taken.
    
    Args:
        original_recommendations: Recommendations before enforcement
        enforced_recommendations: Recommendations after enforcement
        warnings: List of warnings generated during enforcement
    
    Returns:
        Summary dictionary with counts and details
    """
    from schemas.base import Action
    
    summary = {
        "total_recommendations": len(enforced_recommendations.recommendations),
        "downgrades_to_watch": 0,
        "allocation_reductions": 0,
        "warnings_count": len(warnings),
        "warnings": warnings,
        "actionable_count": 0,
        "details": []
    }
    
    actionable_actions = {Action.BUY, Action.REDUCE, Action.SELL, "buy", "reduce", "sell"}
    
    for i, rec in enumerate(enforced_recommendations.recommendations):
        action = rec.action if isinstance(rec.action, str) else rec.action.value
        
        if action in actionable_actions or rec.action in actionable_actions:
            summary["actionable_count"] += 1
        
        if rec._downgraded_from_buy:
            summary["downgrades_to_watch"] += 1
            summary["details"].append({
                "symbol": rec.symbol,
                "action": "downgraded_to_watch",
                "reason": rec._downgrade_reason or "trading plan incomplete"
            })
    
    # Count allocation reductions from warnings
    summary["allocation_reductions"] = sum(
        1 for w in warnings if "Allocation reduced" in w
    )
    
    return summary
