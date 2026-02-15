"""
Investment report generator for the CrewAI system.

Generates professional, three-section Investment Review reports:
1. ONE-PAGE ACTION PLAN (always present)
2. DECISION PACKET (recommendations table, execution plans)
3. EVIDENCE APPENDIX (detailed, learning-oriented)

This module renders reports ONLY from validated JSON schemas.
If validation fails or critical inputs are missing, it generates
a "Not Actionable" report with reasons.

Report Structure:
- Metadata header with Actionability status
- One-page Action Plan (always present, even for "do nothing")
- Decision Packet with execution plans
- Evidence Appendix with stable anchors for cross-referencing
"""

import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple

from pydantic import BaseModel

from agents.utils.db_connection import get_db_connection

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Critical inputs that must be valid for actionable recommendations
CRITICAL_INPUTS = ["portfolio_context", "orchestration", "qa_risk"]

# All expected inputs for the report
ALL_INPUTS = [
    "portfolio_context",
    "macro_analysis",
    "technical_analysis",
    "token_research",
    "orchestration",
    "qa_risk",
]

# Actionable recommendation actions
ACTIONABLE_ACTIONS = {"buy", "reduce", "sell"}


# =============================================================================
# JSON Cleaning Utility
# =============================================================================

def _try_parse_json(raw_output: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to parse raw output as JSON.
    
    Args:
        raw_output: Raw string output
    
    Returns:
        Parsed JSON dict if valid, None otherwise
    """
    if not raw_output or not raw_output.strip():
        return None
    
    cleaned = raw_output.strip()
    
    # Remove markdown code fences if present
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        return None


def _to_dict(obj: Any) -> Optional[Dict[str, Any]]:
    """Convert Pydantic model or dict to dict."""
    if obj is None:
        return None
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return None


# =============================================================================
# Input Validation (Soft Validation Aware)
# =============================================================================

def _check_input_validity(
    validated_outputs: Dict[str, Any],
    validation_status: Optional[Dict[str, Dict[str, Any]]] = None
) -> Tuple[bool, str, Dict[str, str]]:
    """
    Check if all critical inputs are available for report generation.
    
    SOFT VALIDATION BEHAVIOR:
    - Data is usable if it exists (JSON parsed successfully)
    - Report is "Not Actionable" only if critical inputs are COMPLETELY MISSING
    - Schema validation warnings don't block report generation
    - data_quality="invalid" is a WARNING, not a blocker (data still usable)
    
    Args:
        validated_outputs: Dictionary of validated outputs by task name
        validation_status: Per-task validation status from soft validation
    
    Returns:
        Tuple of (is_actionable, reason_if_not, per_input_quality)
    """
    per_input_quality: Dict[str, str] = {}
    issues: List[str] = []
    warnings: List[str] = []
    
    for input_name in ALL_INPUTS:
        data = _to_dict(validated_outputs.get(input_name))
        
        if data is None:
            # CRITICAL: Data completely missing (not even JSON parseable)
            per_input_quality[input_name] = "missing"
            if input_name in CRITICAL_INPUTS:
                issues.append(f"{input_name}: completely missing")
        else:
            # Data exists - check quality level
            meta = data.get("meta", {})
            quality = meta.get("data_quality", "ok")
            
            # Check validation_status if provided
            if validation_status and input_name in validation_status:
                status = validation_status[input_name]
                if status.get("strict_valid", False):
                    per_input_quality[input_name] = quality  # Use meta quality
                elif status.get("usable", False):
                    # Usable but not strict valid - add warning indicator
                    per_input_quality[input_name] = f"{quality} (with warnings)"
                    if status.get("warnings"):
                        warnings.append(f"{input_name}: {len(status['warnings'])} validation warnings")
                else:
                    # Not usable (shouldn't happen if data exists, but be safe)
                    per_input_quality[input_name] = "unusable"
                    if input_name in CRITICAL_INPUTS:
                        issues.append(f"{input_name}: validation failed")
            else:
                # No validation_status - use meta quality
                per_input_quality[input_name] = quality
            
            # SOFT VALIDATION: data_quality="invalid" is now a WARNING, not a blocker
            # The data exists and can be used, just with reduced confidence
            if quality == "invalid":
                warnings.append(f"{input_name}: data_quality=invalid (data still usable)")
    
    # Check for contradictions in portfolio context (warning, not blocker)
    portfolio = _to_dict(validated_outputs.get("portfolio_context"))
    if portfolio:
        checks = portfolio.get("framework", {}).get("checks", {})
        if checks.get("contradictions_detected", False):
            warnings.append("portfolio_context: contradictions detected (review manually)")
    
    # Check QA overall status - reject IS still a blocker
    qa = _to_dict(validated_outputs.get("qa_risk"))
    if qa:
        overall_status = qa.get("overall_status", "")
        if isinstance(overall_status, str) and overall_status.lower() == "reject":
            issues.append("qa_risk: overall_status=reject")
    
    is_actionable = len(issues) == 0
    
    # Build reason string including both issues and warnings
    reason_parts = []
    if issues:
        reason_parts.append("BLOCKING: " + "; ".join(issues))
    if warnings:
        reason_parts.append("WARNINGS: " + "; ".join(warnings))
    reason = " | ".join(reason_parts) if reason_parts else ""
    
    return is_actionable, reason, per_input_quality


# =============================================================================
# NEW REPORT STRUCTURE: Metadata Header
# =============================================================================

def _render_metadata_header(
    report_id: str,
    timestamp: str,
    is_actionable: bool,
    actionability_reason: str,
    per_input_quality: Dict[str, str],
) -> str:
    """
    Render the metadata header with Actionability status.
    
    This appears at the very top of the report and immediately
    tells the reader whether recommendations can be executed.
    
    SOFT VALIDATION:
    - Inputs with "(with warnings)" are usable but had schema validation issues
    - Report is still ACTIONABLE if data exists (soft validation passed)
    """
    lines = [
        f"# Report: {datetime.now().strftime('%Y-%m-%d')} Investment Review",
        "",
        f"**Generated:** {timestamp} UTC",
        f"**Report ID:** {report_id}",
        "",
    ]
    
    # Actionability banner
    if is_actionable:
        lines.append("**Actionability:** ACTIONABLE")
    else:
        lines.append("**Actionability:** NOT ACTIONABLE")
        lines.append("")
        # Parse actionability reason to show blocking issues vs warnings separately
        if "BLOCKING:" in actionability_reason and "WARNINGS:" in actionability_reason:
            parts = actionability_reason.split(" | ")
            for part in parts:
                if part.startswith("BLOCKING:"):
                    lines.append(f"> **Reason:** {part.replace('BLOCKING: ', '')}")
                elif part.startswith("WARNINGS:"):
                    lines.append(f"> **Note:** {part.replace('WARNINGS: ', '')}")
        else:
            lines.append(f"> **Reason:** {actionability_reason}")
    
    lines.append("")
    
    # Check for any warnings in the input quality
    has_warnings = any("(with warnings)" in str(q) for q in per_input_quality.values())
    if has_warnings and is_actionable:
        lines.append("> **Note:** Some inputs had schema validation warnings but data is still usable.")
        lines.append("")
    
    # Input quality table
    lines.append("**Input Quality Summary:**")
    lines.append("")
    lines.append("| Input | Quality | Status |")
    lines.append("|-------|---------|--------|")
    
    quality_display = {
        "ok": ("OK", "STRICT VALID"),
        "partial": ("PARTIAL", "USABLE"),
        "invalid": ("INVALID", "USABLE"),
        "missing": ("MISSING", "UNUSABLE"),
    }
    
    for input_name in ALL_INPUTS:
        quality = per_input_quality.get(input_name, "missing")
        
        # Handle "(with warnings)" suffix
        has_warning = "(with warnings)" in str(quality)
        base_quality = quality.replace(" (with warnings)", "") if has_warning else quality
        
        if base_quality in quality_display:
            display, status = quality_display[base_quality]
        else:
            display = base_quality.upper()
            status = "USABLE" if base_quality != "missing" else "UNUSABLE"
        
        # Adjust status for warnings
        if has_warning:
            status = "SOFT VALID"
            display = f"{display} (warnings)"
        
        # Add bold marker for critical issues
        if quality == "missing" and input_name in CRITICAL_INPUTS:
            display = f"**{display}**"
            status = f"**{status}**"
        
        lines.append(f"| {input_name} | {display} | {status} |")
    
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: One-Page Action Plan (Always Present)
# =============================================================================

def _render_action_plan(
    macro: Optional[Dict[str, Any]],
    technical: Optional[Dict[str, Any]],
    portfolio: Optional[Dict[str, Any]],
    recommendations: Optional[Dict[str, Any]],
    is_actionable: bool,
) -> str:
    """
    Render the One-Page Action Plan section.
    
    This section is ALWAYS present, even when the recommendation is "do nothing".
    It provides a quick executive summary of what to do (or not do).
    """
    lines = [
        "---",
        "",
        "## 1. ONE-PAGE ACTION PLAN",
        "",
    ]
    
    # Market Stance (1-line summary)
    lines.append("### Market Stance")
    lines.append("")
    
    macro_regime = "unknown"
    macro_confidence = "unknown"
    if macro:
        regime = macro.get("regime", {})
        macro_regime = regime.get("stance", "unknown")
        if isinstance(macro_regime, str):
            macro_regime = macro_regime.upper().replace("_", "-")
        macro_confidence = regime.get("confidence", "unknown")
    
    tech_env = "unknown"
    if technical:
        # Try to derive from assets or breadth
        assets = technical.get("assets", [])
        bullish_count = sum(1 for a in assets if a.get("trend", "").lower() == "bullish")
        bearish_count = sum(1 for a in assets if a.get("trend", "").lower() == "bearish")
        if assets:
            if bullish_count > bearish_count:
                tech_env = "bullish"
            elif bearish_count > bullish_count:
                tech_env = "bearish"
            else:
                tech_env = "neutral"
    
    breadth_stats = ""
    if technical:
        breadth = technical.get("breadth", {})
        pct_above_200d = breadth.get("pct_above_200d")
        median_rsi = breadth.get("median_rsi_14")
        
        stats_parts = []
        if pct_above_200d is not None:
            stats_parts.append(f"pct_above_200d={pct_above_200d:.1f}%")
        if median_rsi is not None:
            stats_parts.append(f"median_RSI={median_rsi:.1f}")
        if stats_parts:
            breadth_stats = " | ".join(stats_parts)
    
    lines.append(f"- **Macro Regime:** {macro_regime} (confidence: {macro_confidence})")
    lines.append(f"- **Technical Environment:** {tech_env.title()}")
    if breadth_stats:
        lines.append(f"- **Breadth:** {breadth_stats}")
    else:
        lines.append("- **Breadth:** _data not available_")
    lines.append("")
    
    # Portfolio Status (compliance)
    lines.append("### Portfolio Status")
    lines.append("")
    
    if portfolio:
        derived = portfolio.get("derived", {})
        framework = portfolio.get("framework", {})
        checks = framework.get("checks", {})
        config = framework.get("config", {})
        
        btc_pct = derived.get("btc_allocation_pct_by_value", 0)
        tier23_pct = derived.get("tier2_3_allocation_pct_by_value", 0)
        any_over_limit = checks.get("any_position_over_limit", False)
        
        # Determine compliance status
        btc_ok = checks.get("btc_within_target", True)
        tier23_ok = checks.get("tier2_3_within_limit", True)
        contradictions = checks.get("contradictions_detected", False)
        
        if contradictions:
            compliance = "REJECT (contradictions detected)"
        elif any_over_limit:
            compliance = "FLAG (position over limit)"
        elif not btc_ok or not tier23_ok:
            compliance = "FLAG"
        else:
            compliance = "PASS"
        
        lines.append(f"- **BTC:** {btc_pct:.1f}% | **Tier2+3:** {tier23_pct:.1f}%")
        lines.append(f"- **Any position > limit:** {'Yes' if any_over_limit else 'No'}")
        lines.append(f"- **Compliance:** {compliance}")
    else:
        lines.append("- _Portfolio data not available_")
    lines.append("")
    
    # "Do this now" (max 3 bullets)
    lines.append("### Do This Now")
    lines.append("")
    
    if not is_actionable:
        lines.append("- **STOP:** Do not execute any trades - report is NOT ACTIONABLE")
        lines.append("- Review the issues in the Actionability section above")
        lines.append("- Re-run analysis after addressing the problems")
    elif recommendations:
        recs = recommendations.get("recommendations", [])
        default_rec = recommendations.get("default_recommendation", {})
        
        # Filter to actionable recommendations
        actionable_recs = [
            r for r in recs 
            if r.get("action", "").lower() in ACTIONABLE_ACTIONS
        ]
        
        if actionable_recs:
            for rec in actionable_recs[:3]:
                symbol = rec.get("symbol", "?")
                action = rec.get("action", "?").upper()
                alloc = rec.get("suggested_allocation_pct_portfolio")
                alloc_str = f" ({alloc}% of portfolio)" if alloc else ""
                rationale = rec.get("rationale_one_liner", "")
                lines.append(f"- **{action} {symbol}**{alloc_str}: {rationale}")
        else:
            # No actionable recs - show default
            default_action = default_rec.get("action", "do_nothing")
            if isinstance(default_action, str):
                default_action = default_action.upper().replace("_", " ")
            default_reason = default_rec.get("reason", "No high-conviction opportunities")
            lines.append(f"- **{default_action}:** {default_reason}")
            
            # Add WATCH items
            watch_recs = [r for r in recs if r.get("action", "").lower() == "watch"]
            for rec in watch_recs[:2]:
                symbol = rec.get("symbol", "?")
                rationale = rec.get("rationale_one_liner", "")
                lines.append(f"- **WATCH {symbol}:** {rationale}")
    else:
        lines.append("- _No recommendations available_")
    lines.append("")
    
    # "Do not do" (1-2 bullets)
    lines.append("### Do Not Do")
    lines.append("")
    
    do_not_do_items = []
    
    # Derive from macro implications
    if macro:
        implications = macro.get("implications", {})
        avoid = implications.get("avoid", [])
        for item in avoid[:2]:
            do_not_do_items.append(f"Avoid {item} (macro regime)")
    
    # Derive from portfolio constraints
    if portfolio:
        checks = portfolio.get("framework", {}).get("checks", {})
        if checks.get("any_position_over_limit"):
            do_not_do_items.append("Do not add to positions already over limit")
        if not checks.get("btc_within_target", True):
            btc_pct = portfolio.get("derived", {}).get("btc_allocation_pct_by_value", 0)
            config = portfolio.get("framework", {}).get("config", {})
            btc_min = config.get("btc_target_min_pct", 40)
            if btc_pct < btc_min:
                do_not_do_items.append(f"Do not buy alts until BTC >= {btc_min}%")
    
    if do_not_do_items:
        for item in do_not_do_items[:2]:
            lines.append(f"- {item}")
    else:
        lines.append("- _No specific restrictions_")
    lines.append("")
    
    # Next review triggers (3-6 bullets)
    lines.append("### Next Review Triggers")
    lines.append("")
    lines.append("Re-evaluate when any of these conditions occur:")
    lines.append("")
    
    triggers = []
    
    # Breadth-based triggers
    if technical:
        breadth = technical.get("breadth", {})
        pct_above_200d = breadth.get("pct_above_200d")
        if pct_above_200d is not None:
            if pct_above_200d < 50:
                triggers.append(f"pct_above_200d rises above 60% (currently {pct_above_200d:.1f}%) -> consider increasing alt exposure")
            else:
                triggers.append(f"pct_above_200d falls below 40% (currently {pct_above_200d:.1f}%) -> reduce alt exposure")
    
    # Macro-based triggers
    if macro:
        regime = macro.get("regime", {})
        stance = regime.get("stance", "").lower()
        if stance == "risk_off":
            triggers.append("Macro regime flips to RISK-ON -> revisit alt allocations")
        elif stance == "risk_on":
            triggers.append("Macro regime flips to RISK-OFF -> reduce risk exposure")
        else:
            triggers.append("Macro regime changes from NEUTRAL -> adjust accordingly")
    
    # Portfolio-based triggers
    if portfolio:
        derived = portfolio.get("derived", {})
        btc_pct = derived.get("btc_allocation_pct_by_value", 0)
        config = portfolio.get("framework", {}).get("config", {})
        btc_max = config.get("btc_target_max_pct", 60)
        btc_min = config.get("btc_target_min_pct", 40)
        
        if btc_pct > btc_max:
            triggers.append(f"BTC falls to {btc_max}% target -> rebalance")
        elif btc_pct < btc_min:
            triggers.append(f"BTC rises to {btc_min}% target -> rebalance")
    
    # Time-based trigger
    triggers.append("Weekly scheduled review (regardless of conditions)")
    
    for trigger in triggers[:6]:
        lines.append(f"- {trigger}")
    
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Recommendations Table
# =============================================================================

def _render_recommendations_table(
    recommendations: Optional[Dict[str, Any]],
    is_actionable: bool,
) -> str:
    """
    Render the recommendations summary table.
    
    Columns: Symbol | Action | Conviction | Tier | Allocation | Time Horizon | Rationale
    """
    lines = [
        "### 2.1 Recommendations Summary",
        "",
    ]
    
    if not is_actionable:
        lines.append("> **NOT ACTIONABLE:** The recommendations below cannot be executed due to data quality issues.")
        lines.append("")
    
    if not recommendations:
        lines.append("_No recommendations available._")
        return "\n".join(lines)
    
    recs = recommendations.get("recommendations", [])
    
    if not recs:
        # Show default recommendation
        default_rec = recommendations.get("default_recommendation", {})
        default_action = default_rec.get("action", "do_nothing")
        if isinstance(default_action, str):
            default_action = default_action.upper().replace("_", " ")
        default_reason = default_rec.get("reason", "N/A")
        
        lines.append(f"**Default Action:** {default_action}")
        lines.append(f"**Reason:** {default_reason}")
        return "\n".join(lines)
    
    lines.append("| Symbol | Action | Conviction | Tier | Allocation | Time Horizon | Rationale |")
    lines.append("|--------|--------|------------|------|------------|--------------|-----------|")
    
    for rec in recs:
        symbol = rec.get("symbol", "?")
        action = rec.get("action", "?")
        if isinstance(action, str):
            action = action.upper()
        conviction = rec.get("conviction", "?")
        if isinstance(conviction, str):
            conviction = conviction.title()
        tier = rec.get("tier", "?")
        
        alloc_portfolio = rec.get("suggested_allocation_pct_portfolio")
        alloc_budget = rec.get("suggested_allocation_pct_monthly_budget")
        alloc_parts = []
        if alloc_portfolio:
            alloc_parts.append(f"{alloc_portfolio}% port")
        if alloc_budget:
            alloc_parts.append(f"{alloc_budget}% budget")
        alloc_str = " / ".join(alloc_parts) if alloc_parts else "-"
        
        time_horizon = rec.get("time_horizon", "?")
        rationale = rec.get("rationale_one_liner", "-")
        # Truncate long rationales for table
        if len(rationale) > 50:
            rationale = rationale[:47] + "..."
        
        lines.append(f"| {symbol} | {action} | {conviction} | {tier} | {alloc_str} | {time_horizon} | {rationale} |")
    
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Execution Plans
# =============================================================================

def _render_execution_plans(
    recommendations: Optional[Dict[str, Any]],
    is_actionable: bool,
) -> str:
    """
    Render detailed execution plans for each recommendation.
    
    Each plan includes: time horizon, position sizing, entry plan, 
    exit plan, evidence links, and condensed rubric.
    """
    lines = [
        "### 2.2 Execution Plans",
        "",
    ]
    
    if not is_actionable:
        lines.append("> **NOT ACTIONABLE:** Do not execute these plans.")
        lines.append("")
    
    if not recommendations:
        lines.append("_No execution plans available._")
        return "\n".join(lines)
    
    recs = recommendations.get("recommendations", [])
    
    # Filter to recommendations that need execution plans (not just HOLD)
    actionable_recs = [
        r for r in recs
        if r.get("action", "").lower() in {"buy", "reduce", "sell", "watch"}
    ]
    
    if not actionable_recs:
        lines.append("_No actionable recommendations requiring execution plans._")
        return "\n".join(lines)
    
    for rec in actionable_recs:
        symbol = rec.get("symbol", "?")
        action = rec.get("action", "?")
        if isinstance(action, str):
            action = action.upper()
        conviction = rec.get("conviction", "?")
        if isinstance(conviction, str):
            conviction = conviction.title()
        tier = rec.get("tier", "?")
        
        # Anchor for this execution plan
        anchor = f"exec-{symbol}"
        lines.append(f"<a id=\"{anchor}\"></a>")
        lines.append("")
        lines.append(f"#### {symbol} - {action}")
        lines.append("")
        lines.append(f"**Conviction:** {conviction} | **Tier:** {tier}")
        lines.append("")
        
        # Time Horizon (required)
        time_horizon = rec.get("time_horizon", "_not specified_")
        lines.append(f"**Time Horizon:** {time_horizon}")
        lines.append("")
        
        # Position Sizing (required)
        lines.append("**Position Sizing:**")
        alloc_portfolio = rec.get("suggested_allocation_pct_portfolio")
        alloc_budget = rec.get("suggested_allocation_pct_monthly_budget")
        
        if alloc_portfolio or alloc_budget:
            if alloc_portfolio:
                lines.append(f"- {alloc_portfolio}% of portfolio")
            if alloc_budget:
                lines.append(f"- {alloc_budget}% of monthly DCA budget")
        else:
            lines.append("- _Not specified - determine based on conviction and tier_")
        lines.append("")
        
        # Trading Plan details
        trading_plan = rec.get("trading_plan", {})
        
        # Entry Plan
        lines.append("**Entry Plan:**")
        entry_strategy = trading_plan.get("entry_strategy") if trading_plan else None
        if entry_strategy:
            lines.append(f"- {entry_strategy}")
        else:
            if action == "BUY":
                lines.append("- _Entry strategy not specified - required for execution_")
            else:
                lines.append("- _N/A for this action type_")
        lines.append("")
        
        # Exit Plan
        lines.append("**Exit Plan:**")
        
        # Take profit targets
        targets = trading_plan.get("take_profit_targets", []) if trading_plan else []
        if targets:
            lines.append("- **Take Profit Targets:**")
            for t in targets:
                target = t.get("target", "?")
                sell_pct = t.get("sell_pct", 0)
                lines.append(f"  - {target}: Sell {sell_pct}%")
        else:
            if action in ("BUY", "REDUCE", "SELL"):
                lines.append("- **Take Profit:** _Not specified - required for execution_")
        
        # Stop loss
        stop_loss = trading_plan.get("stop_loss") if trading_plan else None
        if stop_loss:
            lines.append(f"- **Stop Loss:** {stop_loss}")
        else:
            if action == "BUY":
                lines.append("- **Stop Loss:** _Not specified - required for execution_")
        
        # Invalidation trigger
        invalidation = trading_plan.get("invalidation_trigger") if trading_plan else None
        if invalidation:
            lines.append(f"- **Invalidation Trigger:** {invalidation}")
        else:
            if action == "BUY":
                lines.append("- **Invalidation Trigger:** _Not specified_")
        
        lines.append("")
        
        # Evidence Links (anchor references)
        lines.append("**Evidence References:**")
        evidence_refs = rec.get("evidence_refs", [])
        if evidence_refs:
            for ref in evidence_refs[:8]:
                lines.append(f"- [{ref}](#{ref})")
        else:
            # Generate default evidence links
            lines.append(f"- [Technical: {symbol}](#tech-{symbol})")
            lines.append(f"- [Research: {symbol}](#research-{symbol})")
            lines.append("- [Macro Regime](#macro-regime)")
            lines.append("- [Portfolio Compliance](#portfolio-compliance)")
        lines.append("")
        
        # Condensed Rubric
        rubric = rec.get("rubric", {})
        if rubric:
            lines.append("**8-Question Rubric (Summary):**")
            lines.append("")
            lines.append(f"1. Problem: {rubric.get('problem_solved', 'N/A')[:80]}")
            lines.append(f"2. Network Effects: {rubric.get('network_effects', 'N/A')[:80]}")
            lines.append(f"3. Why Now: {rubric.get('why_now', 'N/A')[:80]}")
            lines.append(f"4. Invalidation: {rubric.get('invalidation', 'N/A')[:80]}")
            lines.append(f"5. vs Nothing: {rubric.get('vs_doing_nothing', 'N/A')[:80]}")
            lines.append(f"6. Downside: {rubric.get('downside_risks', 'N/A')[:80]}")
            lines.append(f"7. Portfolio Fit: {rubric.get('portfolio_fit', 'N/A')[:80]}")
            lines.append(f"8. Exit: {rubric.get('exit_criteria', 'N/A')[:80]}")
            lines.append("")
        
        lines.append("---")
        lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Do Nothing Justification
# =============================================================================

def _render_do_nothing_justification(
    macro: Optional[Dict[str, Any]],
    technical: Optional[Dict[str, Any]],
    portfolio: Optional[Dict[str, Any]],
    research: Optional[Dict[str, Any]],
    recommendations: Optional[Dict[str, Any]],
) -> str:
    """
    Render justification for "do nothing" recommendation.
    
    This section is only rendered when there are no BUY/REDUCE/SELL actions.
    It must include evidence-backed reasons and measurable triggers for re-entry.
    """
    lines = [
        "### 2.3 Why We Are Doing Nothing Now",
        "",
        "No BUY, REDUCE, or SELL actions are recommended at this time. Here's why:",
        "",
    ]
    
    evidence_bullets = []
    
    # Macro reasons
    if macro:
        regime = macro.get("regime", {})
        stance = regime.get("stance", "").lower()
        confidence = regime.get("confidence", "")
        
        if stance == "risk_off":
            evidence_bullets.append(f"**Macro Regime is RISK-OFF** (confidence: {confidence}) - unfavorable for new positions")
        elif stance == "neutral":
            evidence_bullets.append(f"**Macro Regime is NEUTRAL** (confidence: {confidence}) - waiting for clearer signal")
        
        # Check implications
        implications = macro.get("implications", {})
        avoid = implications.get("avoid", [])
        if avoid:
            evidence_bullets.append(f"Macro analysis suggests avoiding: {', '.join(avoid[:3])}")
    
    # Technical/breadth reasons
    if technical:
        breadth = technical.get("breadth", {})
        pct_above_200d = breadth.get("pct_above_200d")
        median_rsi = breadth.get("median_rsi_14")
        
        if pct_above_200d is not None and pct_above_200d < 50:
            evidence_bullets.append(f"**Weak breadth:** Only {pct_above_200d:.1f}% of assets above 200-day SMA")
        
        if median_rsi is not None:
            if median_rsi > 70:
                evidence_bullets.append(f"**Overbought conditions:** Median RSI at {median_rsi:.1f}")
            elif median_rsi < 30:
                evidence_bullets.append(f"**Oversold but not confirmed:** Median RSI at {median_rsi:.1f} - waiting for reversal")
        
        # Check individual assets
        assets = technical.get("assets", [])
        bearish_count = sum(1 for a in assets if a.get("trend", "").lower() == "bearish")
        if assets and bearish_count > len(assets) / 2:
            evidence_bullets.append(f"**Majority bearish:** {bearish_count}/{len(assets)} assets in bearish trend")
    
    # Portfolio constraint reasons
    if portfolio:
        derived = portfolio.get("derived", {})
        framework = portfolio.get("framework", {})
        checks = framework.get("checks", {})
        config = framework.get("config", {})
        
        btc_pct = derived.get("btc_allocation_pct_by_value", 0)
        btc_min = config.get("btc_target_min_pct", 40)
        
        if btc_pct < btc_min:
            evidence_bullets.append(f"**BTC underweight:** Currently {btc_pct:.1f}% vs {btc_min}% minimum - must increase BTC before alts")
        
        tier23_pct = derived.get("tier2_3_allocation_pct_by_value", 0)
        tier23_max = config.get("tier2_3_max_pct", 35)
        
        if tier23_pct >= tier23_max:
            evidence_bullets.append(f"**Tier 2+3 at limit:** Currently {tier23_pct:.1f}% vs {tier23_max}% max - no capacity for emerging assets")
        
        if checks.get("any_position_over_limit"):
            evidence_bullets.append("**Position over limit:** Must reduce existing position before adding new ones")
    
    # Research weakness reasons
    if research:
        ranked = research.get("ranked_shortlist", [])
        if not ranked:
            evidence_bullets.append("**No strong candidates:** Research did not identify high-conviction opportunities")
        else:
            # Check scores
            high_conviction = [r for r in ranked if r.get("score", 0) >= 7.0]
            if not high_conviction:
                top_score = max((r.get("score", 0) for r in ranked), default=0)
                evidence_bullets.append(f"**Low conviction:** Top candidate scored only {top_score:.1f}/10 - below 7.0 threshold")
    
    # Default recommendation reason
    if recommendations:
        default_rec = recommendations.get("default_recommendation", {})
        reason = default_rec.get("reason", "")
        if reason:
            evidence_bullets.append(f"**Orchestrator assessment:** {reason}")
        
        # Check for downgraded recommendations
        meta = recommendations.get("meta", {})
        warnings = meta.get("warnings", [])
        downgrade_warnings = [w for w in warnings if "Downgraded" in w]
        if downgrade_warnings:
            for w in downgrade_warnings[:2]:
                evidence_bullets.append(f"**Downgrade:** {w}")
    
    # Render bullets
    if evidence_bullets:
        for bullet in evidence_bullets[:10]:
            lines.append(f"- {bullet}")
    else:
        lines.append("- _Specific reasons not available - defaulting to conservative approach_")
    
    lines.append("")
    
    # What would change our mind
    lines.append("### What Would Change Our Mind")
    lines.append("")
    lines.append("We would reconsider and potentially enter positions when:")
    lines.append("")
    
    change_triggers = []
    
    if macro:
        regime = macro.get("regime", {})
        stance = regime.get("stance", "").lower()
        if stance == "risk_off":
            change_triggers.append("Macro regime shifts to RISK-ON or NEUTRAL with improving liquidity")
        elif stance == "neutral":
            change_triggers.append("Macro regime confirms RISK-ON with high confidence")
    
    if technical:
        breadth = technical.get("breadth", {})
        pct_above_200d = breadth.get("pct_above_200d")
        if pct_above_200d is not None and pct_above_200d < 50:
            change_triggers.append(f"Breadth improves: pct_above_200d rises above 60% (currently {pct_above_200d:.1f}%)")
    
    if portfolio:
        derived = portfolio.get("derived", {})
        btc_pct = derived.get("btc_allocation_pct_by_value", 0)
        config = portfolio.get("framework", {}).get("config", {})
        btc_min = config.get("btc_target_min_pct", 40)
        if btc_pct < btc_min:
            change_triggers.append(f"BTC allocation reaches {btc_min}% target")
    
    if research:
        change_triggers.append("Research identifies candidate with score >= 7.0/10 and strong catalysts")
    
    change_triggers.append("High-conviction opportunity emerges with complete trading plan (entry, TP, SL, invalidation)")
    
    for trigger in change_triggers[:6]:
        lines.append(f"- {trigger}")
    
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Evidence Appendix - Macro
# =============================================================================

def _render_macro_appendix(data: Optional[Dict[str, Any]]) -> str:
    """
    Render macro evidence appendix with stable anchors.
    
    Includes: regime, confidence, key drivers, evidence by factor,
    cycle stage, narratives table, sources.
    """
    lines = [
        "<a id=\"macro-evidence\"></a>",
        "",
        "### 4.1 Macro Evidence",
        "",
    ]
    
    if not data:
        lines.append("_Macro analysis data not available._")
        return "\n".join(lines)
    
    meta = data.get("meta", {})
    regime = data.get("regime", {})
    macro = data.get("macro", {})
    cycle = data.get("cycle", {})
    narratives = data.get("narratives", [])
    sources = data.get("sources", [])
    
    # Regime anchor and details
    lines.append("<a id=\"macro-regime\"></a>")
    lines.append("")
    lines.append("#### Regime Assessment")
    lines.append("")
    
    stance = regime.get("stance", "unknown")
    if isinstance(stance, str):
        stance = stance.upper().replace("_", "-")
    confidence = regime.get("confidence", "unknown")
    
    lines.append(f"**Current Regime:** {stance}")
    lines.append(f"**Confidence:** {confidence}")
    lines.append("")
    
    # Key drivers by factor
    lines.append("#### Key Drivers")
    lines.append("")
    
    for factor_name in ["liquidity", "fed_policy", "inflation", "risk_appetite"]:
        anchor = f"macro-{factor_name}"
        factor = macro.get(factor_name, {})
        
        lines.append(f"<a id=\"{anchor}\"></a>")
        lines.append("")
        lines.append(f"**{factor_name.replace('_', ' ').title()}**")
        
        if factor:
            summary = factor.get("summary", "N/A")
            lines.append(f"- Summary: {summary}")
            
            signals = factor.get("signals", [])
            if signals:
                lines.append("- Signals:")
                for signal in signals[:5]:
                    lines.append(f"  - {signal}")
        else:
            lines.append("- _Data not available_")
        lines.append("")
    
    # Cycle assessment
    lines.append("<a id=\"macro-cycle\"></a>")
    lines.append("")
    lines.append("#### Cycle Position")
    lines.append("")
    
    stage = cycle.get("stage", "unknown")
    if isinstance(stage, str):
        stage = stage.title()
    lines.append(f"**Stage:** {stage}")
    
    halving = cycle.get("halving_context")
    if halving:
        lines.append(f"**Halving Context:** {halving}")
    
    evidence = cycle.get("evidence", [])
    if evidence:
        lines.append("**Evidence:**")
        for e in evidence[:6]:
            lines.append(f"- {e}")
    lines.append("")
    
    # Narratives table
    if narratives:
        lines.append("#### Active Narratives")
        lines.append("")
        lines.append("| Narrative | Momentum | Substance | Notes |")
        lines.append("|-----------|----------|-----------|-------|")
        
        for narrative in narratives[:8]:
            name = narrative.get("name", "?")
            momentum = narrative.get("momentum", "?")
            substance = narrative.get("substance", "?")
            notes = narrative.get("notes", "")
            if len(notes) > 50:
                notes = notes[:47] + "..."
            lines.append(f"| {name} | {momentum} | {substance} | {notes} |")
        lines.append("")
    
    # Sources
    if sources:
        lines.append("#### Sources")
        lines.append("")
        for source in sources[:10]:
            name = source.get("name", "?")
            ref = source.get("ref", "")
            source_type = source.get("type", "url")
            as_of = source.get("as_of", "")
            
            if ref:
                lines.append(f"- [{name}]({ref}) ({source_type}){' - ' + as_of if as_of else ''}")
            else:
                lines.append(f"- {name} ({source_type})")
        lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Evidence Appendix - Technical
# =============================================================================

def _render_technical_appendix(data: Optional[Dict[str, Any]]) -> str:
    """
    Render technical evidence appendix with stable anchors.
    
    Includes: breadth stats, educational bullets, per-asset snapshots,
    key levels, correlations.
    """
    lines = [
        "<a id=\"technical-evidence\"></a>",
        "",
        "### 4.2 Technical Evidence",
        "",
    ]
    
    if not data:
        lines.append("_Technical analysis data not available._")
        return "\n".join(lines)
    
    meta = data.get("meta", {})
    assets = data.get("assets", [])
    breadth = data.get("breadth", {})
    
    # Breadth section with anchor
    lines.append("<a id=\"tech-breadth\"></a>")
    lines.append("")
    lines.append("#### Market Breadth")
    lines.append("")
    
    universe = breadth.get("universe", "unknown")
    pct_above_200d = breadth.get("pct_above_200d")
    pct_golden = breadth.get("pct_golden_cross")
    median_rsi = breadth.get("median_rsi_14")
    
    lines.append(f"**Universe:** {universe}")
    lines.append("")
    
    lines.append("| Metric | Value | Interpretation |")
    lines.append("|--------|-------|----------------|")
    
    if pct_above_200d is not None:
        interpretation = "Strong" if pct_above_200d > 60 else ("Weak" if pct_above_200d < 40 else "Neutral")
        lines.append(f"| % Above 200-day SMA | {pct_above_200d:.1f}% | {interpretation} breadth |")
    
    if pct_golden is not None:
        interpretation = "Bullish" if pct_golden > 50 else "Bearish"
        lines.append(f"| % Golden Cross | {pct_golden:.1f}% | {interpretation} trend |")
    
    if median_rsi is not None:
        if median_rsi > 70:
            interpretation = "Overbought"
        elif median_rsi < 30:
            interpretation = "Oversold"
        else:
            interpretation = "Neutral"
        lines.append(f"| Median RSI (14) | {median_rsi:.1f} | {interpretation} |")
    
    lines.append("")
    
    # Educational bullets
    lines.append("**What Breadth Implies:**")
    lines.append("")
    if pct_above_200d is not None:
        if pct_above_200d > 60:
            lines.append("- High breadth typically supports risk-on allocations and alt exposure")
        elif pct_above_200d < 40:
            lines.append("- Low breadth suggests BTC preference over alts")
        else:
            lines.append("- Neutral breadth - be selective with alt exposure")
    lines.append("- Breadth divergence from price can signal trend weakness")
    lines.append("- Use breadth to confirm or question conviction on individual assets")
    lines.append("")
    
    # Per-asset snapshots
    if assets:
        lines.append("#### Per-Asset Technical Snapshots")
        lines.append("")
        
        for asset in assets:
            symbol = asset.get("symbol", "?")
            anchor = f"tech-{symbol}"
            
            lines.append(f"<a id=\"{anchor}\"></a>")
            lines.append("")
            lines.append(f"**{symbol}**")
            lines.append("")
            
            trend = asset.get("trend", "?")
            signal = asset.get("signal", "?")
            btc_rel = asset.get("btc_relative", {})
            btc_trend = btc_rel.get("trend", "?")
            btc_pct_30d = btc_rel.get("pct_change_30d_vs_btc")
            
            lines.append(f"- **Trend:** {trend} | **Signal:** {signal}")
            btc_pct_str = f"{btc_pct_30d:+.1f}%" if btc_pct_30d is not None else "N/A"
            lines.append(f"- **BTC-Relative:** {btc_trend} ({btc_pct_str} vs BTC 30d)")
            
            timeframes = asset.get("timeframes", {})
            d1 = timeframes.get("d1", {})
            
            sma_50 = d1.get("sma_50")
            sma_200 = d1.get("sma_200")
            rsi = d1.get("rsi_14")
            pct_7d = d1.get("pct_change_7d")
            pct_30d = d1.get("pct_change_30d")
            
            if sma_50 or sma_200:
                sma_str = f"SMA50: ${sma_50:,.0f}" if sma_50 else ""
                if sma_200:
                    sma_str += f" | SMA200: ${sma_200:,.0f}" if sma_str else f"SMA200: ${sma_200:,.0f}"
                lines.append(f"- **Moving Averages:** {sma_str}")
            
            if rsi:
                rsi_cond = "Overbought" if rsi > 70 else ("Oversold" if rsi < 30 else "Neutral")
                lines.append(f"- **RSI (14):** {rsi:.1f} ({rsi_cond})")
            
            if pct_7d is not None or pct_30d is not None:
                pct_str = ""
                if pct_7d is not None:
                    pct_str = f"7d: {pct_7d:+.1f}%"
                if pct_30d is not None:
                    pct_str += f" | 30d: {pct_30d:+.1f}%" if pct_str else f"30d: {pct_30d:+.1f}%"
                lines.append(f"- **Price Change:** {pct_str}")
            
            # Key levels
            key_levels = asset.get("key_levels", {})
            support = key_levels.get("support", [])
            resistance = key_levels.get("resistance", [])
            
            if support or resistance:
                lines.append("- **Key Levels:**")
                if support:
                    support_str = ", ".join(f"${s:,.0f}" for s in support[:3])
                    lines.append(f"  - Support: {support_str}")
                if resistance:
                    resistance_str = ", ".join(f"${r:,.0f}" for r in resistance[:3])
                    lines.append(f"  - Resistance: {resistance_str}")
            
            lines.append("")
    
    # Correlations
    correlations = breadth.get("correlation", [])
    if correlations:
        lines.append("#### Correlations")
        lines.append("")
        lines.append("| Pair | 90-day Correlation | Interpretation |")
        lines.append("|------|-------------------|----------------|")
        
        for corr in correlations[:8]:
            pair = corr.get("pair", "?")
            corr_val = corr.get("corr_90d")
            if corr_val is not None:
                if corr_val > 0.8:
                    interp = "High - limited diversification"
                elif corr_val > 0.5:
                    interp = "Moderate - some diversification"
                elif corr_val > 0:
                    interp = "Low - good diversification"
                else:
                    interp = "Negative - hedging potential"
                lines.append(f"| {pair} | {corr_val:.2f} | {interp} |")
        lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Evidence Appendix - Research/Fundamentals
# =============================================================================

def _render_research_appendix(data: Optional[Dict[str, Any]]) -> str:
    """
    Render fundamentals/token research evidence appendix with stable anchors.
    
    Includes: ranked shortlist table, per-candidate details,
    adoption metrics, catalysts, risks, sources.
    """
    lines = [
        "<a id=\"research-evidence\"></a>",
        "",
        "### 4.3 Fundamentals / Token Research Evidence",
        "",
    ]
    
    if not data:
        lines.append("_Token research data not available._")
        return "\n".join(lines)
    
    meta = data.get("meta", {})
    candidates = data.get("candidates", [])
    ranked = data.get("ranked_shortlist", [])
    
    # Ranked shortlist table
    if ranked:
        lines.append("#### Ranked Shortlist")
        lines.append("")
        lines.append("| Rank | Symbol | Score | Adoption | Moat | Catalyst | Risk | Confidence |")
        lines.append("|------|--------|-------|----------|------|----------|------|------------|")
        
        for i, item in enumerate(ranked, 1):
            symbol = item.get("symbol", "?")
            score = item.get("score", 0)
            breakdown = item.get("score_breakdown", {})
            adoption = breakdown.get("adoption", 0)
            moat = breakdown.get("moat", 0)
            catalyst = breakdown.get("catalyst", 0)
            risk = breakdown.get("risk", 0)
            
            # Find confidence from candidates
            candidate = next((c for c in candidates if c.get("symbol") == symbol), {})
            confidence = candidate.get("confidence", "?")
            
            lines.append(f"| {i} | {symbol} | {score:.1f} | {adoption:.1f} | {moat:.1f} | {catalyst:.1f} | {risk:.1f} | {confidence} |")
        
        lines.append("")
    
    # Per-candidate details
    if candidates:
        lines.append("#### Candidate Analysis")
        lines.append("")
        
        for candidate in candidates[:10]:
            symbol = candidate.get("symbol", "?")
            anchor = f"research-{symbol}"
            
            lines.append(f"<a id=\"{anchor}\"></a>")
            lines.append("")
            
            name = candidate.get("name", "?")
            mcap_rank = candidate.get("mcap_rank")
            category = candidate.get("category", "Unknown")
            confidence = candidate.get("confidence", "unknown")
            tier = candidate.get("tier_suggestion")
            
            lines.append(f"**{symbol} - {name}**")
            lines.append("")
            
            rank_str = f"#{mcap_rank}" if mcap_rank else "N/A"
            tier_str = f"Tier {tier}" if tier else "N/A"
            lines.append(f"Rank: {rank_str} | Category: {category} | Tier: {tier_str} | Confidence: {confidence}")
            lines.append("")
            
            # Thesis
            thesis = candidate.get("thesis", {})
            if thesis:
                lines.append("**Thesis:**")
                lines.append(f"- Problem: {thesis.get('problem', 'N/A')}")
                lines.append(f"- Why It Wins: {thesis.get('why_it_wins', 'N/A')}")
                lines.append(f"- Network Effects: {thesis.get('network_effects', 'N/A')}")
                lines.append("")
            
            # Adoption metrics
            metrics = candidate.get("adoption_metrics", {})
            if metrics:
                lines.append("**Adoption Metrics:**")
                
                tvl = metrics.get("tvl_usd")
                tvl_change = metrics.get("tvl_change_90d_pct")
                fees = metrics.get("fees_30d_usd")
                revenue = metrics.get("revenue_30d_usd")
                dau = metrics.get("dau")
                tx_count = metrics.get("tx_count_30d")
                
                if tvl is not None:
                    tvl_str = f"${tvl/1e9:.2f}B" if tvl >= 1e9 else f"${tvl/1e6:.1f}M"
                    change_str = f" ({tvl_change:+.1f}% 90d)" if tvl_change is not None else ""
                    lines.append(f"- TVL: {tvl_str}{change_str}")
                
                if fees is not None:
                    lines.append(f"- Fees (30d): ${fees/1e6:.2f}M")
                
                if revenue is not None:
                    lines.append(f"- Revenue (30d): ${revenue/1e6:.2f}M")
                
                if dau is not None:
                    lines.append(f"- DAU: {dau:,}")
                
                if tx_count is not None:
                    lines.append(f"- Transactions (30d): {tx_count:,}")
                
                lines.append("")
            
            # Catalysts
            catalysts = candidate.get("catalysts", [])
            if catalysts:
                lines.append("**Catalysts:**")
                for c in catalysts[:5]:
                    lines.append(f"- {c}")
                lines.append("")
            
            # Risks
            risks = candidate.get("risks", [])
            if risks:
                lines.append("**Risks:**")
                for r in risks[:5]:
                    lines.append(f"- {r}")
                lines.append("")
            
            # Sources
            sources = candidate.get("sources", [])
            if sources:
                lines.append("**Sources:**")
                for source in sources[:5]:
                    name = source.get("name", "?")
                    ref = source.get("ref", "")
                    if ref:
                        lines.append(f"- [{name}]({ref})")
                    else:
                        lines.append(f"- {name}")
                lines.append("")
            
            lines.append("---")
            lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# NEW REPORT STRUCTURE: Evidence Appendix - Portfolio
# =============================================================================

def _render_portfolio_appendix(data: Optional[Dict[str, Any]]) -> str:
    """
    Render portfolio evidence appendix with stable anchors.
    
    Includes: totals table, positions table, compliance checks,
    constraint impact summary.
    """
    lines = [
        "<a id=\"portfolio-evidence\"></a>",
        "",
        "### 4.4 Portfolio & Framework Evidence",
        "",
    ]
    
    if not data:
        lines.append("_Portfolio data not available._")
        return "\n".join(lines)
    
    meta = data.get("meta", {})
    totals = data.get("portfolio_totals", {})
    positions = data.get("positions", [])
    derived = data.get("derived", {})
    framework = data.get("framework", {})
    config = framework.get("config", {})
    checks = framework.get("checks", {})
    
    # Data quality warning
    data_quality = meta.get("data_quality", "unknown")
    if data_quality == "invalid":
        lines.append("> **DATA QUALITY: INVALID** - Portfolio contains contradictions or errors.")
        lines.append("")
        contradictions = checks.get("contradictions", [])
        if contradictions:
            lines.append("**Contradictions detected:**")
            for c in contradictions:
                lines.append(f"- {c}")
            lines.append("")
    elif data_quality == "partial":
        lines.append("> **DATA QUALITY: PARTIAL** - Some pricing data is missing.")
        lines.append("")
    
    # Portfolio totals with anchor
    lines.append("<a id=\"portfolio-totals\"></a>")
    lines.append("")
    lines.append("#### Portfolio Totals")
    lines.append("")
    
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    
    cost_basis = totals.get("total_cost_basis_usd", 0)
    lines.append(f"| Total Cost Basis | ${cost_basis:,.2f} |")
    
    current_value = totals.get("total_current_value_usd")
    if current_value is not None:
        lines.append(f"| Total Current Value | ${current_value:,.2f} |")
        unrealized = current_value - cost_basis
        unrealized_pct = (unrealized / cost_basis * 100) if cost_basis > 0 else 0
        lines.append(f"| Unrealized P&L | ${unrealized:,.2f} ({unrealized_pct:+.1f}%) |")
    else:
        lines.append("| Total Current Value | _Pricing incomplete_ |")
    
    realized = totals.get("total_realized_pnl_usd")
    if realized is not None:
        lines.append(f"| Total Realized P&L | ${realized:,.2f} |")
    
    drawdown = totals.get("drawdown_from_peak_pct")
    if drawdown is not None:
        lines.append(f"| Drawdown from Peak | {drawdown:.1f}% |")
    
    lines.append("")
    
    # Positions table with anchor
    lines.append("<a id=\"portfolio-positions\"></a>")
    lines.append("")
    lines.append("#### Positions")
    lines.append("")
    
    if positions:
        lines.append("| Symbol | Tier | Quantity | Price | Value | Allocation | P&L |")
        lines.append("|--------|------|----------|-------|-------|------------|-----|")
        
        for pos in positions:
            symbol = pos.get("symbol", "?").upper()
            tier = pos.get("tier", "?")
            quantity = pos.get("quantity", 0)
            price = pos.get("current_price_usd")
            value = pos.get("current_value_usd")
            alloc = pos.get("allocation_pct_by_value")
            unrealized = pos.get("unrealized_pnl_usd")
            unrealized_pct = pos.get("unrealized_pnl_pct")
            
            price_str = f"${price:,.2f}" if price else "N/A"
            value_str = f"${value:,.2f}" if value else "N/A"
            alloc_str = f"{alloc:.1f}%" if alloc is not None else "?"
            
            pnl_str = ""
            if unrealized is not None and unrealized_pct is not None:
                pnl_str = f"${unrealized:,.0f} ({unrealized_pct:+.1f}%)"
            
            lines.append(f"| {symbol} | {tier} | {quantity:.6f} | {price_str} | {value_str} | {alloc_str} | {pnl_str} |")
        
        lines.append("")
    else:
        lines.append("_No open positions_")
        lines.append("")
    
    # Compliance checks with anchor
    lines.append("<a id=\"portfolio-compliance\"></a>")
    lines.append("")
    lines.append("#### Framework Compliance")
    lines.append("")
    
    lines.append("| Check | Status | Notes |")
    lines.append("|-------|--------|-------|")
    
    # BTC within target
    btc_ok = checks.get("btc_within_target")
    btc_pct = derived.get("btc_allocation_pct_by_value", 0)
    btc_min = config.get("btc_target_min_pct", 40)
    btc_max = config.get("btc_target_max_pct", 60)
    btc_status = "PASS" if btc_ok else ("FAIL" if btc_ok is False else "?")
    lines.append(f"| BTC within {btc_min}-{btc_max}% | {btc_status} | Currently {btc_pct:.1f}% |")
    
    # Any position over limit
    over_limit = checks.get("any_position_over_limit")
    single_limit = config.get("single_asset_limit_pct", 20)
    over_status = "FAIL" if over_limit else ("PASS" if over_limit is False else "?")
    over_positions = checks.get("positions_over_limit", [])
    over_notes = ", ".join(f"{p.get('symbol', '?')}" for p in over_positions) if over_positions else "-"
    lines.append(f"| No position > {single_limit}% | {over_status} | {over_notes} |")
    
    # Tier 2+3 within limit
    tier23_ok = checks.get("tier2_3_within_limit")
    tier23_pct = derived.get("tier2_3_allocation_pct_by_value", 0)
    tier23_max = config.get("tier2_3_max_pct", 35)
    tier23_status = "PASS" if tier23_ok else ("FAIL" if tier23_ok is False else "?")
    lines.append(f"| Tier 2+3 <= {tier23_max}% | {tier23_status} | Currently {tier23_pct:.1f}% |")
    
    # Pricing complete
    pricing_ok = checks.get("pricing_complete")
    pricing_status = "PASS" if pricing_ok else ("FAIL" if pricing_ok is False else "?")
    lines.append(f"| Pricing complete | {pricing_status} | - |")
    
    # Contradictions
    contradictions_detected = checks.get("contradictions_detected", False)
    contra_status = "FAIL" if contradictions_detected else "PASS"
    lines.append(f"| No contradictions | {contra_status} | - |")
    
    lines.append("")
    
    # Constraint impact summary
    lines.append("#### Constraints Impacting Decisions")
    lines.append("")
    
    constraints = []
    
    if btc_ok is False:
        if btc_pct < btc_min:
            constraints.append(f"BTC underweight at {btc_pct:.1f}% - must increase before adding alts")
        elif btc_pct > btc_max:
            constraints.append(f"BTC overweight at {btc_pct:.1f}% - consider rebalancing to alts")
    
    if over_limit:
        for p in over_positions:
            constraints.append(f"{p.get('symbol', '?')} at {p.get('allocation_pct', 0):.1f}% exceeds {single_limit}% limit")
    
    remaining_tier23 = tier23_max - tier23_pct
    if remaining_tier23 > 0:
        constraints.append(f"Tier 2+3 capacity remaining: {remaining_tier23:.1f}%")
    else:
        constraints.append(f"Tier 2+3 at limit ({tier23_pct:.1f}%) - no capacity for emerging assets")
    
    if constraints:
        for c in constraints:
            lines.append(f"- {c}")
    else:
        lines.append("- _No binding constraints currently_")
    
    lines.append("")
    lines.append(f"_Snapshot as of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# LEGACY Section Renderers (kept for backward compatibility)
# =============================================================================

def _render_macro_section(data: Dict[str, Any]) -> str:
    """Render macro/cycle section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    regime = data.get("regime", {})
    macro = data.get("macro", {})
    cycle = data.get("cycle", {})
    narratives = data.get("narratives", [])
    implications = data.get("implications", {})
    
    # Regime assessment
    stance = regime.get("stance", "unknown").upper().replace("_", "-")
    confidence = regime.get("confidence", "unknown")
    lines.append(f"**Market Regime:** {stance} (Confidence: {confidence})")
    lines.append("")
    
    # Macro factors
    lines.append("### Macro Factors")
    lines.append("")
    
    for factor_name in ["liquidity", "fed_policy", "inflation", "risk_appetite"]:
        factor = macro.get(factor_name, {})
        if factor:
            summary = factor.get("summary", "N/A")
            lines.append(f"**{factor_name.replace('_', ' ').title()}:** {summary}")
            signals = factor.get("signals", [])
            if signals:
                for signal in signals[:3]:
                    lines.append(f"  - {signal}")
            lines.append("")
    
    # Cycle assessment
    lines.append("### Cycle Position")
    lines.append("")
    stage = cycle.get("stage", "unknown")
    lines.append(f"**Stage:** {stage.title()}")
    
    halving = cycle.get("halving_context")
    if halving:
        lines.append(f"**Halving Context:** {halving}")
    
    evidence = cycle.get("evidence", [])
    if evidence:
        lines.append("**Evidence:**")
        for e in evidence[:5]:
            lines.append(f"  - {e}")
    lines.append("")
    
    # Narratives
    if narratives:
        lines.append("### Active Narratives")
        lines.append("")
        for narrative in narratives[:5]:
            name = narrative.get("name", "Unknown")
            momentum = narrative.get("momentum", "unknown")
            substance = narrative.get("substance", "unknown")
            notes = narrative.get("notes", "")
            lines.append(f"- **{name}**: Momentum: {momentum}, Substance: {substance}")
            if notes:
                lines.append(f"  _{notes}_")
        lines.append("")
    
    # Implications
    favor = implications.get("favor", [])
    avoid = implications.get("avoid", [])
    
    if favor:
        lines.append("**Favor:** " + ", ".join(favor))
    if avoid:
        lines.append("**Avoid:** " + ", ".join(avoid))
    
    lines.append("")
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


def _render_technical_section(data: Dict[str, Any]) -> str:
    """Render technical analysis section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    assets = data.get("assets", [])
    breadth = data.get("breadth", {})
    
    # Market breadth summary
    lines.append("### Market Breadth")
    lines.append("")
    
    universe = breadth.get("universe", "unknown")
    pct_above_200d = breadth.get("pct_above_200d")
    pct_golden = breadth.get("pct_golden_cross")
    median_rsi = breadth.get("median_rsi_14")
    
    lines.append(f"**Universe:** {universe}")
    if pct_above_200d is not None:
        lines.append(f"**% Above 200-day SMA:** {pct_above_200d:.1f}%")
    if pct_golden is not None:
        lines.append(f"**% Golden Cross:** {pct_golden:.1f}%")
    if median_rsi is not None:
        lines.append(f"**Median RSI (14):** {median_rsi:.1f}")
    lines.append("")
    
    # Per-asset analysis
    if assets:
        lines.append("### Asset Technical Analysis")
        lines.append("")
        lines.append("| Symbol | Trend | Signal | BTC Relative | RSI | 30d Change |")
        lines.append("|--------|-------|--------|--------------|-----|------------|")
        
        for asset in assets:
            symbol = asset.get("symbol", "?")
            trend = asset.get("trend", "?")
            signal = asset.get("signal", "?")
            btc_rel = asset.get("btc_relative", {})
            btc_trend = btc_rel.get("trend", "?")
            
            timeframes = asset.get("timeframes", {})
            d1 = timeframes.get("d1", {})
            rsi = d1.get("rsi_14")
            rsi_str = f"{rsi:.1f}" if rsi else "N/A"
            pct_30d = d1.get("pct_change_30d")
            pct_str = f"{pct_30d:+.1f}%" if pct_30d else "N/A"
            
            lines.append(f"| {symbol} | {trend} | {signal} | {btc_trend} | {rsi_str} | {pct_str} |")
        
        lines.append("")
    
    # Correlations
    correlations = breadth.get("correlation", [])
    if correlations:
        lines.append("### Key Correlations")
        lines.append("")
        for corr in correlations[:5]:
            pair = corr.get("pair", "?")
            corr_val = corr.get("corr_90d")
            if corr_val is not None:
                lines.append(f"- {pair}: {corr_val:.2f}")
        lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


def _render_token_research_section(data: Dict[str, Any]) -> str:
    """Render token research section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    candidates = data.get("candidates", [])
    ranked = data.get("ranked_shortlist", [])
    
    # Ranked shortlist
    if ranked:
        lines.append("### Ranked Candidates")
        lines.append("")
        lines.append("| Rank | Symbol | Score | Adoption | Moat | Catalyst | Risk |")
        lines.append("|------|--------|-------|----------|------|----------|------|")
        
        for i, item in enumerate(ranked, 1):
            symbol = item.get("symbol", "?")
            score = item.get("score", 0)
            breakdown = item.get("score_breakdown", {})
            adoption = breakdown.get("adoption", 0)
            moat = breakdown.get("moat", 0)
            catalyst = breakdown.get("catalyst", 0)
            risk = breakdown.get("risk", 0)
            
            lines.append(f"| {i} | {symbol} | {score:.1f} | {adoption:.1f} | {moat:.1f} | {catalyst:.1f} | {risk:.1f} |")
        
        lines.append("")
    
    # Candidate details
    if candidates:
        lines.append("### Candidate Analysis")
        lines.append("")
        
        for candidate in candidates[:8]:
            symbol = candidate.get("symbol", "?")
            name = candidate.get("name", "?")
            mcap_rank = candidate.get("mcap_rank")
            category = candidate.get("category", "Unknown")
            confidence = candidate.get("confidence", "unknown")
            tier = candidate.get("tier_suggestion")
            
            lines.append(f"#### {symbol} - {name}")
            lines.append("")
            
            rank_str = f"#{mcap_rank}" if mcap_rank else "N/A"
            tier_str = f"Tier {tier}" if tier else "N/A"
            lines.append(f"**Rank:** {rank_str} | **Category:** {category} | **Tier:** {tier_str} | **Confidence:** {confidence}")
            lines.append("")
            
            # Thesis
            thesis = candidate.get("thesis", {})
            if thesis:
                lines.append(f"**Problem:** {thesis.get('problem', 'N/A')}")
                lines.append(f"**Why It Wins:** {thesis.get('why_it_wins', 'N/A')}")
                lines.append(f"**Network Effects:** {thesis.get('network_effects', 'N/A')}")
                lines.append("")
            
            # Metrics
            metrics = candidate.get("adoption_metrics", {})
            if metrics:
                tvl = metrics.get("tvl_usd")
                dau = metrics.get("dau")
                revenue = metrics.get("revenue_30d_usd")
                
                metric_parts = []
                if tvl:
                    metric_parts.append(f"TVL: ${tvl/1e9:.2f}B" if tvl >= 1e9 else f"TVL: ${tvl/1e6:.1f}M")
                if dau:
                    metric_parts.append(f"DAU: {dau:,}")
                if revenue:
                    metric_parts.append(f"Revenue (30d): ${revenue/1e6:.1f}M")
                
                if metric_parts:
                    lines.append("**Metrics:** " + " | ".join(metric_parts))
                    lines.append("")
            
            # Catalysts and risks
            catalysts = candidate.get("catalysts", [])
            if catalysts:
                lines.append("**Catalysts:** " + ", ".join(catalysts[:3]))
            
            risks = candidate.get("risks", [])
            if risks:
                lines.append("**Risks:** " + ", ".join(risks[:3]))
            
            lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


def _render_portfolio_section(data: Dict[str, Any]) -> str:
    """Render portfolio context section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    totals = data.get("portfolio_totals", {})
    positions = data.get("positions", [])
    derived = data.get("derived", {})
    framework = data.get("framework", {})
    config = framework.get("config", {})
    checks = framework.get("checks", {})
    
    # Data quality warning
    data_quality = meta.get("data_quality", "unknown")
    if data_quality == "invalid":
        lines.append("### DATA QUALITY: INVALID")
        lines.append("")
        lines.append("**This portfolio snapshot contains contradictions or errors.**")
        lines.append("**Recommendations from this run may not be actionable.**")
        lines.append("")
        if checks.get("contradictions"):
            lines.append("**Contradictions detected:**")
            for c in checks["contradictions"]:
                lines.append(f"- {c}")
            lines.append("")
    elif data_quality == "partial":
        lines.append("### DATA QUALITY: PARTIAL")
        lines.append("")
        lines.append("**Some pricing data is missing. Compliance checks may be incomplete.**")
        lines.append("")
    
    # Warnings
    if meta.get("warnings"):
        for warning in meta["warnings"]:
            lines.append(f"_Warning: {warning}_")
        lines.append("")
    
    # Portfolio Totals
    lines.append("### Portfolio Totals")
    lines.append("")
    lines.append(f"- **Total Cost Basis:** ${totals.get('total_cost_basis_usd', 0):,.2f}")
    
    total_value = totals.get("total_current_value_usd")
    if total_value is not None:
        lines.append(f"- **Total Current Value:** ${total_value:,.2f}")
        cost_basis = totals.get("total_cost_basis_usd", 0)
        if cost_basis > 0:
            unrealized_pnl = total_value - cost_basis
            unrealized_pct = (unrealized_pnl / cost_basis) * 100
            lines.append(f"- **Unrealized P&L:** ${unrealized_pnl:,.2f} ({unrealized_pct:+.1f}%)")
    else:
        lines.append("- **Total Current Value:** _Pricing incomplete_")
    
    lines.append(f"- **Total Realized P&L:** ${totals.get('total_realized_pnl_usd', 0):,.2f}")
    lines.append("")
    
    # Positions
    lines.append("### Positions")
    lines.append("")
    
    if not positions:
        lines.append("_No open positions_")
    else:
        for pos in positions:
            symbol = pos.get("symbol", "?").upper()
            quantity = pos.get("quantity", 0)
            tier = pos.get("tier")
            tier_str = f"Tier {tier}" if tier is not None else "Tier ?"
            
            alloc = pos.get("allocation_pct_by_value")
            alloc_str = f"{alloc:.1f}%" if alloc is not None else "?%"
            
            current_price = pos.get("current_price_usd")
            price_str = f"${current_price:,.2f}" if current_price else "N/A"
            
            current_value = pos.get("current_value_usd")
            value_str = f"${current_value:,.2f}" if current_value else "N/A"
            
            unrealized = pos.get("unrealized_pnl_usd")
            unrealized_pct = pos.get("unrealized_pnl_pct")
            pnl_str = ""
            if unrealized is not None and unrealized_pct is not None:
                pnl_str = f" | P&L: ${unrealized:,.2f} ({unrealized_pct:+.1f}%)"
            
            lines.append(f"- **{symbol}** ({tier_str}): {quantity:.6f} @ {price_str} = {value_str} ({alloc_str}){pnl_str}")
    
    lines.append("")
    
    # Allocation Summary
    lines.append("### Allocation Summary")
    lines.append("")
    
    btc_alloc = derived.get("btc_allocation_pct_by_value", 0)
    lines.append(f"- **BTC Allocation:** {btc_alloc:.1f}% (target: {config.get('btc_target_min_pct', 40)}-{config.get('btc_target_max_pct', 60)}%)")
    
    tier23_alloc = derived.get("tier2_3_allocation_pct_by_value", 0)
    lines.append(f"- **Tier 2+3 Allocation:** {tier23_alloc:.1f}% (max: {config.get('tier2_3_max_pct', 35)}%)")
    
    max_alloc = derived.get("max_single_asset_allocation_pct_by_value", 0)
    max_symbol = derived.get("max_single_asset_symbol") or "?"
    if isinstance(max_symbol, str):
        lines.append(f"- **Max Single Asset:** {max_symbol.upper()} at {max_alloc:.1f}%")
    lines.append("")
    
    # Framework Compliance
    lines.append("### Framework Compliance")
    lines.append("")
    
    def _bool_to_status(val, true_text="Yes", false_text="No"):
        if val is None:
            return "Unknown"
        return true_text if val else false_text
    
    btc_ok = checks.get("btc_within_target")
    lines.append(f"- **BTC within target:** {_bool_to_status(btc_ok)}")
    
    over_limit = checks.get("any_position_over_limit")
    lines.append(f"- **Any position over limit:** {_bool_to_status(over_limit, 'Yes (VIOLATION)', 'No')}")
    
    tier23_ok = checks.get("tier2_3_within_limit")
    lines.append(f"- **Tier 2+3 within limit:** {_bool_to_status(tier23_ok)}")
    
    pricing_ok = checks.get("pricing_complete")
    lines.append(f"- **Pricing complete:** {_bool_to_status(pricing_ok)}")
    
    lines.append("")
    lines.append(f"_Snapshot as of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


def _render_recommendations_section(data: Dict[str, Any]) -> str:
    """Render recommendations section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    executive_summary = data.get("executive_summary", "")
    market_context = data.get("market_context", {})
    recommendations = data.get("recommendations", [])
    default_rec = data.get("default_recommendation", {})
    
    # Executive summary
    if executive_summary:
        lines.append(executive_summary)
        lines.append("")
    
    # Market context
    lines.append("### Market Context")
    lines.append("")
    
    macro_regime = market_context.get("macro_regime", "unknown").upper().replace("_", "-")
    tech_env = market_context.get("technical_env", "unknown")
    lines.append(f"**Macro Regime:** {macro_regime}")
    lines.append(f"**Technical Environment:** {tech_env.title()}")
    
    considerations = market_context.get("key_considerations", [])
    if considerations:
        lines.append("**Key Considerations:**")
        for c in considerations[:5]:
            lines.append(f"  - {c}")
    lines.append("")
    
    # Warnings (including downgrade warnings)
    warnings = meta.get("warnings", [])
    if warnings:
        lines.append("### Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")
    
    # Individual recommendations
    if recommendations:
        lines.append("### Recommendations")
        lines.append("")
        
        for rec in recommendations:
            symbol = rec.get("symbol", "?")
            action = rec.get("action", "unknown").upper()
            conviction = rec.get("conviction", "unknown")
            tier = rec.get("tier", "?")
            time_horizon = rec.get("time_horizon", "unknown")
            
            lines.append(f"#### {symbol} - {action}")
            lines.append("")
            lines.append(f"**Conviction:** {conviction.title()} | **Tier:** {tier} | **Horizon:** {time_horizon}")
            
            # Allocations
            alloc_portfolio = rec.get("suggested_allocation_pct_portfolio")
            alloc_budget = rec.get("suggested_allocation_pct_monthly_budget")
            if alloc_portfolio or alloc_budget:
                alloc_parts = []
                if alloc_portfolio:
                    alloc_parts.append(f"{alloc_portfolio}% of portfolio")
                if alloc_budget:
                    alloc_parts.append(f"{alloc_budget}% of monthly budget")
                lines.append(f"**Suggested Allocation:** " + " | ".join(alloc_parts))
            lines.append("")
            
            # 8-Question Rubric
            rubric = rec.get("rubric", {})
            if rubric:
                lines.append("**Investment Rubric:**")
                lines.append("")
                lines.append(f"1. **Problem Solved:** {rubric.get('problem_solved', 'N/A')}")
                lines.append(f"2. **Network Effects:** {rubric.get('network_effects', 'N/A')}")
                lines.append(f"3. **Why Now:** {rubric.get('why_now', 'N/A')}")
                lines.append(f"4. **Invalidation:** {rubric.get('invalidation', 'N/A')}")
                lines.append(f"5. **vs Doing Nothing:** {rubric.get('vs_doing_nothing', 'N/A')}")
                lines.append(f"6. **Downside Risks:** {rubric.get('downside_risks', 'N/A')}")
                lines.append(f"7. **Portfolio Fit:** {rubric.get('portfolio_fit', 'N/A')}")
                lines.append(f"8. **Exit Criteria:** {rubric.get('exit_criteria', 'N/A')}")
                lines.append("")
            
            # Trading Plan (for BUY actions)
            trading_plan = rec.get("trading_plan")
            if trading_plan and action in ["BUY", "buy"]:
                lines.append("**Trading Plan:**")
                lines.append("")
                lines.append(f"- **Entry Strategy:** {trading_plan.get('entry_strategy', 'N/A')}")
                lines.append(f"- **Position Size:** {trading_plan.get('position_size', 'N/A')}")
                
                targets = trading_plan.get("take_profit_targets", [])
                if targets:
                    lines.append("- **Take Profit Targets:**")
                    for t in targets:
                        lines.append(f"  - {t.get('target', 'N/A')} - Sell {t.get('sell_pct', 0)}%")
                
                lines.append(f"- **Stop Loss:** {trading_plan.get('stop_loss', 'N/A')}")
                lines.append(f"- **Invalidation Trigger:** {trading_plan.get('invalidation_trigger', 'N/A')}")
                lines.append("")
            
            lines.append("---")
            lines.append("")
    
    # Default recommendation
    if default_rec:
        lines.append("### Default Recommendation")
        lines.append("")
        default_action = default_rec.get("action", "unknown").upper().replace("_", " ")
        reason = default_rec.get("reason", "N/A")
        lines.append(f"**Action:** {default_action}")
        lines.append(f"**Reason:** {reason}")
        lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


def _render_qa_section(data: Dict[str, Any]) -> str:
    """Render QA review section from validated JSON."""
    lines = []
    
    meta = data.get("meta", {})
    overall_status = data.get("overall_status", "unknown").upper()
    reviewed_count = data.get("recommendations_reviewed", 0)
    issues_count = data.get("issues_found", 0)
    checklist = data.get("compliance_checklist", [])
    per_rec = data.get("per_recommendation", [])
    final_verdict = data.get("final_verdict", "")
    
    # Overall status
    status_emoji = {"PASS": "PASS", "FLAG": "FLAG", "REJECT": "REJECT"}.get(overall_status, overall_status)
    lines.append(f"### QA Review Status: {status_emoji}")
    lines.append("")
    lines.append(f"**Recommendations Reviewed:** {reviewed_count}")
    lines.append(f"**Issues Found:** {issues_count}")
    lines.append("")
    
    # Compliance checklist
    if checklist:
        lines.append("### Compliance Checklist")
        lines.append("")
        lines.append("| Check | Status | Notes |")
        lines.append("|-------|--------|-------|")
        
        for check in checklist:
            check_name = check.get("check", "?")
            status = check.get("status", "unknown")
            notes = check.get("notes", "")
            
            status_icon = {"pass": "PASS", "fail": "FAIL", "unknown": "?"}.get(status, status)
            lines.append(f"| {check_name} | {status_icon} | {notes} |")
        
        lines.append("")
    
    # Per-recommendation review
    if per_rec:
        lines.append("### Per-Recommendation Review")
        lines.append("")
        
        for review in per_rec:
            symbol = review.get("symbol", "?")
            original_action = review.get("original_action", "?")
            qa_status = review.get("qa_status", "unknown").upper()
            verdict = review.get("verdict", "unknown")
            issues = review.get("issues", [])
            risk = review.get("risk", {})
            
            lines.append(f"#### {symbol} ({original_action}) - {qa_status}")
            lines.append("")
            lines.append(f"**Verdict:** {verdict.title()}")
            
            if risk:
                corr = risk.get("correlation_with_portfolio", "unknown")
                sector = risk.get("sector_concentration", "N/A")
                conviction = risk.get("conviction", "unknown")
                lines.append(f"**Risk:** Correlation: {corr} | Sector: {sector} | Conviction: {conviction}")
            
            if issues:
                lines.append("**Issues:**")
                for issue in issues:
                    lines.append(f"  - {issue}")
            
            lines.append("")
    
    # Final verdict
    if final_verdict:
        lines.append("### Final Verdict")
        lines.append("")
        lines.append(final_verdict)
        lines.append("")
    
    lines.append(f"_As of: {meta.get('as_of_timestamp_utc', 'unknown')}_")
    
    return "\n".join(lines)


# =============================================================================
# Main Report Generation
# =============================================================================

def generate_investment_report(
    crew_output: Any = None,
    task_outputs: Optional[Dict[str, str]] = None,
    validated_outputs: Optional[Dict[str, Any]] = None,
    prompt_versions: Optional[Dict[str, str]] = None,
    db_path: str = "market_data.duckdb",
    reports_dir: str = "reports",
    qa_blocked: bool = False,
    qa_block_reason: str = "",
    validation_status: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """
    Generate and save a professional Investment Review report.
    
    NEW REPORT STRUCTURE (v5.0):
    1. Metadata Header (with Actionability status)
    2. One-Page Action Plan (ALWAYS present)
    3. Decision Packet (recommendations table, execution plans)
    4. Evidence Appendix (detailed, with stable anchors)
    5. Validation Warnings (if any soft validation issues)
    
    SOFT VALIDATION BEHAVIOR:
    - Data is usable if JSON parsed successfully (even with schema warnings)
    - Report is marked "Not Actionable" only if critical inputs are MISSING entirely
    - Schema validation warnings are displayed but don't block report generation
    
    Args:
        crew_output: Output from the CrewAI crew execution (optional, for backward compat)
        task_outputs: Dictionary mapping task names to raw string outputs
        validated_outputs: Dictionary mapping task names to validated Pydantic models or dicts
        prompt_versions: Dictionary mapping agent names to prompt versions
        db_path: Path to DuckDB database for audit tracking
        reports_dir: Directory to save reports
        qa_blocked: Whether QA rejected the run (legacy, now auto-detected)
        qa_block_reason: Reason for QA rejection (legacy, now auto-detected)
        validation_status: Per-task validation status with usable/strict_valid/warnings
    
    Returns:
        Path to the generated markdown report
    """
    if task_outputs is None:
        task_outputs = {}
    if validated_outputs is None:
        validated_outputs = {}
    if prompt_versions is None:
        prompt_versions = {}
    
    # Create reports directory
    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)
    
    # Generate report ID and filename
    report_id = str(uuid.uuid4())
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    md_filename = f"{date_str}-investment-review.md"
    json_filename = f"{date_str}-investment-review.json"
    
    md_path = reports_path / md_filename
    json_path = reports_path / json_filename
    
    # PHASE 1: Check input validity (soft validation aware)
    is_actionable, actionability_reason, per_input_quality = _check_input_validity(
        validated_outputs, validation_status
    )
    
    # Legacy qa_blocked takes precedence if set
    if qa_blocked and not actionability_reason:
        is_actionable = False
        actionability_reason = qa_block_reason or "QA blocked"
    
    # Extract validated data as dicts
    macro_data = _to_dict(validated_outputs.get("macro_analysis"))
    technical_data = _to_dict(validated_outputs.get("technical_analysis"))
    portfolio_data = _to_dict(validated_outputs.get("portfolio_context"))
    research_data = _to_dict(validated_outputs.get("token_research"))
    recommendations_data = _to_dict(validated_outputs.get("orchestration"))
    qa_data = _to_dict(validated_outputs.get("qa_risk"))
    
    # PHASE 2: Generate Markdown report with new structure
    markdown_content = _generate_markdown_v2(
        report_id=report_id,
        timestamp=timestamp_str,
        is_actionable=is_actionable,
        actionability_reason=actionability_reason,
        per_input_quality=per_input_quality,
        macro=macro_data,
        technical=technical_data,
        portfolio=portfolio_data,
        research=research_data,
        recommendations=recommendations_data,
        qa=qa_data,
        prompt_versions=prompt_versions,
    )
    
    # PHASE 3: Generate JSON report (updated)
    json_content = _generate_json_v2(
        report_id=report_id,
        timestamp=timestamp_str,
        is_actionable=is_actionable,
        actionability_reason=actionability_reason,
        per_input_quality=per_input_quality,
        validated_outputs=validated_outputs,
        prompt_versions=prompt_versions,
    )
    
    # Save reports
    with open(md_path, 'w') as f:
        f.write(markdown_content)
    
    with open(json_path, 'w') as f:
        json.dump(json_content, f, indent=2, default=str)
    
    # Record in audit table
    _record_audit(
        report_id=report_id,
        report_path=str(md_path),
        prompt_versions=prompt_versions,
        db_path=db_path,
    )
    
    logger.info(f"Investment report generated: {md_path}")
    return str(md_path)


def _generate_markdown_v2(
    report_id: str,
    timestamp: str,
    is_actionable: bool,
    actionability_reason: str,
    per_input_quality: Dict[str, str],
    macro: Optional[Dict[str, Any]],
    technical: Optional[Dict[str, Any]],
    portfolio: Optional[Dict[str, Any]],
    research: Optional[Dict[str, Any]],
    recommendations: Optional[Dict[str, Any]],
    qa: Optional[Dict[str, Any]],
    prompt_versions: Dict[str, str],
) -> str:
    """
    Generate the new professional Markdown report content.
    
    Structure:
    1. Metadata Header
    2. One-Page Action Plan (always present)
    3. Decision Packet
    4. Evidence Appendix
    """
    sections = []
    
    # 1. METADATA HEADER
    sections.append(_render_metadata_header(
        report_id=report_id,
        timestamp=timestamp,
        is_actionable=is_actionable,
        actionability_reason=actionability_reason,
        per_input_quality=per_input_quality,
    ))
    
    # 2. ONE-PAGE ACTION PLAN (always present)
    sections.append(_render_action_plan(
        macro=macro,
        technical=technical,
        portfolio=portfolio,
        recommendations=recommendations,
        is_actionable=is_actionable,
    ))
    
    # 3. DECISION PACKET
    sections.append("---")
    sections.append("")
    sections.append("## 2. DECISION PACKET")
    sections.append("")
    
    # 3.1 Recommendations Table
    sections.append(_render_recommendations_table(
        recommendations=recommendations,
        is_actionable=is_actionable,
    ))
    
    # 3.2 Execution Plans
    sections.append(_render_execution_plans(
        recommendations=recommendations,
        is_actionable=is_actionable,
    ))
    
    # 3.3 Do Nothing Justification (if applicable)
    if recommendations:
        recs = recommendations.get("recommendations", [])
        has_actionable = any(
            r.get("action", "").lower() in ACTIONABLE_ACTIONS
            for r in recs
        )
        
        if not has_actionable:
            sections.append(_render_do_nothing_justification(
                macro=macro,
                technical=technical,
                portfolio=portfolio,
                research=research,
                recommendations=recommendations,
            ))
    else:
        # No recommendations at all - show do nothing
        sections.append(_render_do_nothing_justification(
            macro=macro,
            technical=technical,
            portfolio=portfolio,
            research=research,
            recommendations=recommendations,
        ))
    
    # 3.4 QA Review Summary (condensed)
    if qa:
        sections.append(_render_qa_summary(qa))
    
    # 4. EVIDENCE APPENDIX
    sections.append("---")
    sections.append("")
    sections.append("## 3. EVIDENCE APPENDIX")
    sections.append("")
    sections.append("_Detailed evidence supporting the recommendations above. Use anchors to navigate._")
    sections.append("")
    
    # 4.1 Macro Evidence
    sections.append(_render_macro_appendix(macro))
    
    # 4.2 Technical Evidence
    sections.append(_render_technical_appendix(technical))
    
    # 4.3 Research Evidence
    sections.append(_render_research_appendix(research))
    
    # 4.4 Portfolio Evidence
    sections.append(_render_portfolio_appendix(portfolio))
    
    # FOOTER: Report Metadata
    sections.append("---")
    sections.append("")
    sections.append("## Report Metadata")
    sections.append("")
    sections.append("### Prompt Versions Used")
    sections.append("")
    
    for agent_name, version in prompt_versions.items():
        sections.append(f"- **{agent_name}**: v{version}")
    
    sections.append("")
    sections.append("---")
    sections.append("")
    sections.append("*This report was generated by the CrewAI Investment System v5.0.*")
    sections.append("*Human approval is required before executing any trades.*")
    
    return "\n".join(sections)


def _render_qa_summary(qa: Dict[str, Any]) -> str:
    """Render a condensed QA review summary for the Decision Packet."""
    lines = [
        "### 2.4 QA Review Summary",
        "",
    ]
    
    overall_status = qa.get("overall_status", "unknown")
    if isinstance(overall_status, str):
        overall_status = overall_status.upper()
    
    issues_found = qa.get("issues_found", 0)
    reviewed_count = qa.get("recommendations_reviewed", 0)
    
    lines.append(f"**Overall Status:** {overall_status}")
    lines.append(f"**Recommendations Reviewed:** {reviewed_count}")
    lines.append(f"**Issues Found:** {issues_found}")
    lines.append("")
    
    # Compliance checklist summary
    checklist = qa.get("compliance_checklist", [])
    if checklist:
        failed_checks = [c for c in checklist if c.get("status", "").lower() == "fail"]
        if failed_checks:
            lines.append("**Failed Compliance Checks:**")
            for check in failed_checks[:5]:
                lines.append(f"- {check.get('check', '?')}: {check.get('notes', '')}")
            lines.append("")
    
    # Final verdict
    final_verdict = qa.get("final_verdict", "")
    if final_verdict:
        # Truncate long verdicts
        if len(final_verdict) > 200:
            final_verdict = final_verdict[:197] + "..."
        lines.append(f"**Final Verdict:** {final_verdict}")
        lines.append("")
    
    return "\n".join(lines)


def _generate_json_v2(
    report_id: str,
    timestamp: str,
    is_actionable: bool,
    actionability_reason: str,
    per_input_quality: Dict[str, str],
    validated_outputs: Dict[str, Any],
    prompt_versions: Dict[str, str],
) -> Dict[str, Any]:
    """Generate the JSON report content (v2 with actionability)."""
    
    # Convert validated outputs to dicts
    validated_json = {
        name: _to_dict(validated_outputs.get(name))
        for name in ALL_INPUTS
    }
    
    result = {
        "report_id": report_id,
        "generated_at": timestamp,
        "report_type": "investment_review",
        "report_version": "5.0",
        "prompt_versions": prompt_versions,
        "actionability": {
            "is_actionable": is_actionable,
            "reason": actionability_reason if not is_actionable else None,
            "per_input_quality": per_input_quality,
        },
        "validation_status": {
            task: data is not None
            for task, data in validated_json.items()
        },
        "validated_outputs": validated_json,
        "metadata": {
            "system_version": "5.0.0",
            "human_approval_required": True,
            "json_validated": any(v is not None for v in validated_json.values()),
        }
    }
    
    # Extract compliance summary from portfolio context
    portfolio_json = validated_json.get("portfolio_context")
    if portfolio_json:
        framework = portfolio_json.get("framework", {})
        checks = framework.get("checks", {})
        result["compliance_summary"] = {
            "data_quality": portfolio_json.get("meta", {}).get("data_quality", "unknown"),
            "btc_within_target": checks.get("btc_within_target"),
            "any_position_over_limit": checks.get("any_position_over_limit"),
            "tier2_3_within_limit": checks.get("tier2_3_within_limit"),
            "pricing_complete": checks.get("pricing_complete"),
            "contradictions_detected": checks.get("contradictions_detected", False),
        }
    
    # Extract recommendations summary
    orchestration_json = validated_json.get("orchestration")
    if orchestration_json:
        recs = orchestration_json.get("recommendations", [])
        result["recommendations_summary"] = {
            "total_count": len(recs),
            "actionable_count": sum(
                1 for r in recs 
                if r.get("action", "").lower() in ACTIONABLE_ACTIONS
            ),
            "default_action": orchestration_json.get("default_recommendation", {}).get("action"),
        }
    
    return result


def _build_report_content(
    crew_output: Any,
    task_outputs: Dict[str, str],
    validated_outputs: Dict[str, Any],
) -> Dict[str, Any]:
    """Build report content from validated and raw outputs."""
    
    # Helper to get validated data as dict
    def to_dict(obj):
        if obj is None:
            return None
        if isinstance(obj, BaseModel):
            return obj.model_dump()
        if isinstance(obj, dict):
            return obj
        return None
    
    # Helper to render section
    def render_section(task_name: str, renderer_func, fallback_key: str = None):
        validated = validated_outputs.get(task_name)
        validated_dict = to_dict(validated)
        
        if validated_dict:
            try:
                return renderer_func(validated_dict)
            except Exception as e:
                logger.warning(f"Failed to render {task_name} from JSON: {e}")
        
        # Fallback to raw output
        raw = task_outputs.get(fallback_key or task_name, "")
        if raw:
            return raw
        
        return f"_No {task_name.replace('_', ' ')} data available._"
    
    # Build sections
    sections = {
        "raw_output": str(crew_output.raw if hasattr(crew_output, 'raw') else crew_output),
        "market_regime": render_section("macro_analysis", _render_macro_section),
        "technical_overview": render_section("technical_analysis", _render_technical_section),
        "token_research": render_section("token_research", _render_token_research_section),
        "portfolio_context": render_section("portfolio_context", _render_portfolio_section),
        "recommendations": render_section("orchestration", _render_recommendations_section),
        "risks_and_watch_items": render_section("qa_risk", _render_qa_section),
    }
    
    # Extract executive summary from recommendations
    orchestration = validated_outputs.get("orchestration")
    orchestration_dict = to_dict(orchestration)
    if orchestration_dict:
        sections["executive_summary"] = orchestration_dict.get("executive_summary", "")
    else:
        sections["executive_summary"] = _extract_executive_summary_from_raw(task_outputs)
    
    # Store validated JSON for JSON report
    sections["validated_json"] = {
        "portfolio_context": to_dict(validated_outputs.get("portfolio_context")),
        "macro_analysis": to_dict(validated_outputs.get("macro_analysis")),
        "technical_analysis": to_dict(validated_outputs.get("technical_analysis")),
        "token_research": to_dict(validated_outputs.get("token_research")),
        "orchestration": to_dict(validated_outputs.get("orchestration")),
        "qa_risk": to_dict(validated_outputs.get("qa_risk")),
    }
    
    return sections


def _extract_executive_summary_from_raw(task_outputs: Dict[str, str]) -> str:
    """Extract executive summary from raw task outputs."""
    orchestration = task_outputs.get("orchestration", "")
    
    if orchestration:
        lines = orchestration.split('\n')
        summary_lines = []
        in_summary = False
        
        for line in lines:
            line_lower = line.lower().strip()
            
            if 'executive summary' in line_lower:
                in_summary = True
                continue
            
            if in_summary and line.startswith('##'):
                break
            
            if in_summary:
                summary_lines.append(line)
        
        if summary_lines:
            return '\n'.join(summary_lines).strip()
    
    return "_Executive summary will be populated from agent analysis._"


def _generate_markdown(
    report_content: Dict[str, Any],
    timestamp: str,
    prompt_versions: Dict[str, str],
    report_id: str,
    qa_blocked: bool = False,
    qa_block_reason: str = "",
) -> str:
    """Generate the Markdown report content."""
    
    md_lines = [
        "# Investment Review Report",
        "",
        f"**Generated:** {timestamp}",
        f"**Report ID:** {report_id}",
    ]
    
    # Add QA blocked warning at top if applicable
    if qa_blocked:
        md_lines.extend([
            "",
            "---",
            "",
            "## NOT ACTIONABLE - QA REJECTED",
            "",
            f"**This report has been flagged as NOT ACTIONABLE by the QA review.**",
            "",
            f"**Reason:** {qa_block_reason}",
            "",
            "**No trades should be executed based on this report.** Review the issues below and re-run the analysis after addressing the problems.",
        ])
    
    md_lines.extend([
        "",
        "---",
        "",
        "## Executive Summary",
        "",
        report_content.get("executive_summary", "_No executive summary generated._"),
        "",
        "---",
        "",
        "## Market Regime Snapshot",
        "",
        report_content.get("market_regime", "_No market regime analysis available._"),
        "",
        "---",
        "",
        "## Technical Momentum Overview",
        "",
        report_content.get("technical_overview", "_No technical analysis available._"),
        "",
        "---",
        "",
        "## Token Research Highlights",
        "",
        report_content.get("token_research", "_No token research available._"),
        "",
        "---",
        "",
        "## Portfolio Context Summary",
        "",
        report_content.get("portfolio_context", "_No portfolio context available._"),
        "",
        "---",
        "",
    ])
    
    # Conditional recommendations section based on QA status
    if qa_blocked:
        md_lines.extend([
            "## Recommendations (NOT ACTIONABLE)",
            "",
            "**The following recommendations have been rejected by QA and should NOT be executed:**",
            "",
            report_content.get("recommendations", "_No recommendations generated._"),
        ])
    else:
        md_lines.extend([
            "## Approved Recommendations",
            "",
            report_content.get("recommendations", "_No recommendations generated._"),
        ])
    
    md_lines.extend([
        "",
        "---",
        "",
        "## Risks & Watch Items",
        "",
        report_content.get("risks_and_watch_items", "_No risk items flagged._"),
        "",
        "---",
        "",
    ])
    
    # Actions section depends on QA status
    if qa_blocked:
        md_lines.extend([
            "## Actions Taken",
            "",
            "_This report is NOT ACTIONABLE. No trades should be executed._",
            "",
            "_Required: Address the QA issues and re-run the analysis._",
        ])
    else:
        md_lines.extend([
            "## Actions Taken",
            "",
            "_Manual input required: Record any trades executed based on this report._",
        ])
    
    md_lines.extend([
        "",
        "---",
        "",
        "## Report Metadata",
        "",
        "### Prompt Versions Used",
        "",
    ])
    
    for agent_name, version in prompt_versions.items():
        md_lines.append(f"- **{agent_name}**: v{version}")
    
    # Add QA status to metadata
    if qa_blocked:
        md_lines.extend([
            "",
            "### QA Status",
            "",
            f"- **Status:** REJECTED",
            f"- **Reason:** {qa_block_reason}",
            f"- **Actionable:** No",
        ])
    
    md_lines.extend([
        "",
        "---",
        "",
        "*This report was generated by the CrewAI Investment System. Human approval is required before executing any trades.*",
    ])
    
    return '\n'.join(md_lines)


def _generate_json(
    report_content: Dict[str, Any],
    validated_outputs: Dict[str, Any],
    timestamp: str,
    prompt_versions: Dict[str, str],
    report_id: str,
    qa_blocked: bool = False,
    qa_block_reason: str = "",
) -> Dict[str, Any]:
    """Generate the JSON report content."""
    
    validated_json = report_content.get("validated_json", {})
    
    result = {
        "report_id": report_id,
        "generated_at": timestamp,
        "report_type": "investment_review",
        "prompt_versions": prompt_versions,
        "validation_status": {
            task: data is not None
            for task, data in validated_json.items()
        },
        "sections": {
            "executive_summary": report_content.get("executive_summary", ""),
            "market_regime": report_content.get("market_regime", ""),
            "technical_overview": report_content.get("technical_overview", ""),
            "token_research": report_content.get("token_research", ""),
            "portfolio_context": report_content.get("portfolio_context", ""),
            "recommendations": report_content.get("recommendations", ""),
            "risks_and_watch_items": report_content.get("risks_and_watch_items", ""),
        },
        "validated_outputs": validated_json,
        "raw_output": report_content.get("raw_output", ""),
        "actions_taken": [],
        "metadata": {
            "system_version": "4.0.0",
            "human_approval_required": True,
            "json_validated": any(validated_json.values()),
            "qa_blocked": qa_blocked,
            "qa_block_reason": qa_block_reason if qa_blocked else None,
            "actionable": not qa_blocked,
        }
    }
    
    # Extract compliance summary from portfolio context
    portfolio_json = validated_json.get("portfolio_context")
    if portfolio_json:
        framework = portfolio_json.get("framework", {})
        checks = framework.get("checks", {})
        result["compliance_summary"] = {
            "data_quality": portfolio_json.get("meta", {}).get("data_quality", "unknown"),
            "btc_within_target": checks.get("btc_within_target"),
            "any_position_over_limit": checks.get("any_position_over_limit"),
            "tier2_3_within_limit": checks.get("tier2_3_within_limit"),
            "pricing_complete": checks.get("pricing_complete"),
            "contradictions_detected": checks.get("contradictions_detected", False),
        }
    
    return result


# =============================================================================
# Error Report Generation
# =============================================================================

def generate_error_report(
    validation_errors: List[Dict[str, Any]],
    task_outputs: Optional[Dict[str, str]] = None,
    db_path: str = "market_data.duckdb",
    reports_dir: str = "reports",
) -> str:
    """
    Generate an error report when validation fails.
    
    This is called when critical validation failures occur and
    a normal report cannot be generated safely.
    
    Args:
        validation_errors: List of validation error dictionaries
        task_outputs: Raw task outputs for debugging
        db_path: Path to DuckDB database
        reports_dir: Directory to save reports
    
    Returns:
        Path to the error report
    """
    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)
    
    report_id = str(uuid.uuid4())
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    md_filename = f"{date_str}-investment-review-ERROR.md"
    md_path = reports_path / md_filename
    
    # Build error report
    lines = [
        "# INVESTMENT REVIEW - RUN FAILED",
        "",
        f"**Generated:** {timestamp_str}",
        f"**Report ID:** {report_id}",
        "",
        "---",
        "",
        "## Validation Failures",
        "",
        "The following tasks failed JSON schema validation. No recommendations were generated.",
        "",
    ]
    
    for error in validation_errors:
        task = error.get("task", "unknown")
        errors = error.get("errors", [])
        
        lines.append(f"### Task: {task}")
        lines.append("")
        
        if errors:
            lines.append("**Errors:**")
            for e in errors[:10]:
                lines.append(f"- {e}")
        else:
            lines.append("_No specific error details available._")
        
        lines.append("")
    
    lines.extend([
        "---",
        "",
        "## Required Actions",
        "",
        "1. Review the validation errors above",
        "2. Check agent prompts for JSON output compliance",
        "3. Ensure all required fields are present in agent outputs",
        "4. Re-run the crew after fixing the issues",
        "",
        "---",
        "",
        "## Debug Information",
        "",
    ])
    
    # Add truncated raw outputs for debugging
    if task_outputs:
        for task_name, output in task_outputs.items():
            lines.append(f"### {task_name} (first 500 chars)")
            lines.append("")
            lines.append("```")
            lines.append(output[:500] if output else "_Empty output_")
            if output and len(output) > 500:
                lines.append(f"... ({len(output) - 500} more characters)")
            lines.append("```")
            lines.append("")
    
    lines.extend([
        "---",
        "",
        "*This is an error report. No investment recommendations were generated.*",
    ])
    
    # Write report
    with open(md_path, 'w') as f:
        f.write('\n'.join(lines))
    
    logger.warning(f"Error report generated: {md_path}")
    return str(md_path)


# =============================================================================
# Audit Recording
# =============================================================================

def _record_audit(
    report_id: str,
    report_path: str,
    prompt_versions: Dict[str, str],
    db_path: str,
) -> None:
    """Record the report generation in the audit table."""
    try:
        conn = get_db_connection(db_path)
        
        conn.execute("""
            INSERT INTO audit_investment_report (
                report_id,
                report_path,
                created_at,
                token_research_prompt_version,
                token_screener_prompt_version,
                fundamentals_analyst_prompt_version,
                research_synthesizer_prompt_version,
                technical_analyst_prompt_version,
                macro_cycle_prompt_version,
                portfolio_context_prompt_version,
                orchestrator_prompt_version,
                qa_risk_prompt_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            report_id,
            report_path,
            datetime.now(),
            prompt_versions.get("token_research", "unknown"),
            prompt_versions.get("token_screener", "unknown"),
            prompt_versions.get("fundamentals_analyst", "unknown"),
            prompt_versions.get("research_synthesizer", "unknown"),
            prompt_versions.get("technical_analyst", "unknown"),
            prompt_versions.get("macro_cycle", "unknown"),
            prompt_versions.get("portfolio_context", "unknown"),
            prompt_versions.get("orchestrator", "unknown"),
            prompt_versions.get("qa_risk", "unknown"),
        ])
        
        logger.info(f"Audit record created for report {report_id}")
        
    except Exception as e:
        logger.error(f"Failed to record audit: {e}")
