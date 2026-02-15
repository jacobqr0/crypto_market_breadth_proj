"""
Meta-learning report generator for the Post-Mortem Architect.

Generates both Markdown and JSON reports from the Post-Mortem
analysis output, storing them in the reports/meta-learning/ directory.
"""

import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import duckdb

logger = logging.getLogger(__name__)


def generate_meta_learning_report(
    analysis_output: Any,
    prompt_version: str,
    analysis_period_start: datetime,
    analysis_period_end: datetime,
    reports_analyzed_count: int,
    db_path: str = "market_data.duckdb",
    reports_dir: str = "reports/meta-learning",
) -> str:
    """
    Generate and save a meta-learning report from Post-Mortem analysis.
    
    Args:
        analysis_output: Output from the Post-Mortem crew execution
        prompt_version: Version of the post_mortem prompt used
        analysis_period_start: Start of analysis period
        analysis_period_end: End of analysis period
        reports_analyzed_count: Number of investment reports analyzed
        db_path: Path to DuckDB database for audit tracking
        reports_dir: Directory to save meta-learning reports
    
    Returns:
        Path to the generated markdown report
    """
    # Create reports directory if needed
    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)
    
    # Generate report ID and filename
    report_id = str(uuid.uuid4())
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    md_filename = f"{date_str}-meta-learning-report.md"
    json_filename = f"{date_str}-meta-learning-report.json"
    
    md_path = reports_path / md_filename
    json_path = reports_path / json_filename
    
    # Parse analysis output
    report_content = _parse_analysis_output(analysis_output)
    
    # Generate Markdown report
    markdown_content = _generate_markdown(
        report_content=report_content,
        timestamp=timestamp_str,
        prompt_version=prompt_version,
        report_id=report_id,
        analysis_period_start=analysis_period_start,
        analysis_period_end=analysis_period_end,
        reports_analyzed_count=reports_analyzed_count,
    )
    
    # Generate JSON report
    json_content = _generate_json(
        report_content=report_content,
        timestamp=timestamp_str,
        prompt_version=prompt_version,
        report_id=report_id,
        analysis_period_start=analysis_period_start,
        analysis_period_end=analysis_period_end,
        reports_analyzed_count=reports_analyzed_count,
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
        prompt_version=prompt_version,
        analysis_period_start=analysis_period_start,
        analysis_period_end=analysis_period_end,
        reports_analyzed_count=reports_analyzed_count,
        db_path=db_path,
    )
    
    logger.info(f"Meta-learning report generated: {md_path}")
    return str(md_path)


def _parse_analysis_output(analysis_output: Any) -> Dict[str, Any]:
    """Parse the Post-Mortem analysis output into structured sections."""
    # Handle different output formats from CrewAI
    if hasattr(analysis_output, 'raw'):
        raw_output = analysis_output.raw
    elif isinstance(analysis_output, str):
        raw_output = analysis_output
    else:
        raw_output = str(analysis_output)
    
    # Extract sections from the output
    sections = {
        "raw_output": raw_output,
        "executive_summary": "",
        "performance_analysis": "",
        "logic_drift": "",
        "performance_gaps": "",
        "missed_opportunities": "",
        "evolution_recommendations": "",
        "prompt_refinements": "",
    }
    
    # Try to extract sections based on common headers
    current_section = "raw_output"
    
    if isinstance(raw_output, str):
        lines = raw_output.split('\n')
        for line in lines:
            line_lower = line.lower().strip()
            
            if 'executive summary' in line_lower:
                current_section = "executive_summary"
            elif 'performance' in line_lower and 'analysis' in line_lower:
                current_section = "performance_analysis"
            elif 'logic drift' in line_lower or 'bias' in line_lower:
                current_section = "logic_drift"
            elif 'performance gap' in line_lower or 'gap' in line_lower:
                current_section = "performance_gaps"
            elif 'missed' in line_lower or 'opportunity' in line_lower:
                current_section = "missed_opportunities"
            elif 'evolution' in line_lower or 'recommendation' in line_lower:
                current_section = "evolution_recommendations"
            elif 'prompt' in line_lower and 'refin' in line_lower:
                current_section = "prompt_refinements"
            
            if current_section in sections:
                sections[current_section] += line + '\n'
    
    return sections


def _generate_markdown(
    report_content: Dict[str, Any],
    timestamp: str,
    prompt_version: str,
    report_id: str,
    analysis_period_start: datetime,
    analysis_period_end: datetime,
    reports_analyzed_count: int,
) -> str:
    """Generate the Markdown meta-learning report content."""
    
    period_start_str = analysis_period_start.strftime("%Y-%m-%d")
    period_end_str = analysis_period_end.strftime("%Y-%m-%d")
    
    md_lines = [
        "# Meta-Learning Report",
        "",
        f"**Generated:** {timestamp}",
        f"**Report ID:** {report_id}",
        f"**Analysis Period:** {period_start_str} to {period_end_str}",
        f"**Investment Reports Analyzed:** {reports_analyzed_count}",
        "",
        "---",
        "",
        "## Executive Summary",
        "",
        report_content.get("executive_summary", "_No executive summary generated._"),
        "",
        "---",
        "",
        "## Performance Analysis",
        "",
        report_content.get("performance_analysis", "_No performance analysis available._"),
        "",
        "---",
        "",
        "## Logic Drift Analysis",
        "",
        "This section identifies systematic biases in agent behavior.",
        "",
        report_content.get("logic_drift", "_No logic drift patterns identified._"),
        "",
        "---",
        "",
        "## Performance Gaps",
        "",
        "Where recommendations didn't perform as expected.",
        "",
        report_content.get("performance_gaps", "_No performance gaps identified._"),
        "",
        "---",
        "",
        "## Missed Opportunities",
        "",
        "Assets or signals that should have been identified but weren't.",
        "",
        report_content.get("missed_opportunities", "_No missed opportunities identified._"),
        "",
        "---",
        "",
        "## Evolution Recommendations",
        "",
        "Prioritized suggestions for system improvement.",
        "",
        report_content.get("evolution_recommendations", "_No recommendations generated._"),
        "",
        "---",
        "",
        "## Prompt Refinement Suggestions",
        "",
        "Specific changes recommended for agent prompts.",
        "",
        report_content.get("prompt_refinements", "_No prompt refinements suggested._"),
        "",
        "---",
        "",
        "## Report Metadata",
        "",
        f"- **Post-Mortem Prompt Version:** v{prompt_version}",
        f"- **System Version:** 1.0.0",
        "",
        "---",
        "",
        "## Action Items",
        "",
        "_Human review required: Implement approved prompt changes and parameter adjustments._",
        "",
        "| Item | Status | Notes |",
        "|------|--------|-------|",
        "| Review logic drift findings | [ ] | |",
        "| Apply prompt refinements | [ ] | |",
        "| Update risk parameters | [ ] | |",
        "",
        "---",
        "",
        "*This meta-learning report was generated by the Post-Mortem Architect. It does not make trading decisions but informs long-term system improvements.*",
    ]
    
    return '\n'.join(md_lines)


def _generate_json(
    report_content: Dict[str, Any],
    timestamp: str,
    prompt_version: str,
    report_id: str,
    analysis_period_start: datetime,
    analysis_period_end: datetime,
    reports_analyzed_count: int,
) -> Dict[str, Any]:
    """Generate the JSON meta-learning report content."""
    
    return {
        "report_id": report_id,
        "generated_at": timestamp,
        "report_type": "meta_learning",
        "analysis_period": {
            "start": analysis_period_start.isoformat(),
            "end": analysis_period_end.isoformat(),
            "reports_analyzed": reports_analyzed_count,
        },
        "prompt_version": prompt_version,
        "sections": {
            "executive_summary": report_content.get("executive_summary", ""),
            "performance_analysis": report_content.get("performance_analysis", ""),
            "logic_drift": report_content.get("logic_drift", ""),
            "performance_gaps": report_content.get("performance_gaps", ""),
            "missed_opportunities": report_content.get("missed_opportunities", ""),
            "evolution_recommendations": report_content.get("evolution_recommendations", ""),
            "prompt_refinements": report_content.get("prompt_refinements", ""),
        },
        "raw_output": report_content.get("raw_output", ""),
        "action_items": [
            {"item": "Review logic drift findings", "status": "pending"},
            {"item": "Apply prompt refinements", "status": "pending"},
            {"item": "Update risk parameters", "status": "pending"},
        ],
        "metadata": {
            "system_version": "1.0.0",
            "affects_trading_decisions": False,
            "requires_human_review": True,
        }
    }


def _record_audit(
    report_id: str,
    report_path: str,
    prompt_version: str,
    analysis_period_start: datetime,
    analysis_period_end: datetime,
    reports_analyzed_count: int,
    db_path: str,
) -> None:
    """Record the meta-learning report in the audit table."""
    try:
        conn = duckdb.connect(db_path)
        
        conn.execute("""
            INSERT INTO audit_meta_learning_report (
                report_id,
                report_path,
                created_at,
                post_mortem_prompt_version,
                analysis_period_start,
                analysis_period_end,
                investment_reports_analyzed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            report_id,
            report_path,
            datetime.now(),
            prompt_version,
            analysis_period_start,
            analysis_period_end,
            reports_analyzed_count,
        ])
        
        conn.close()
        logger.info(f"Audit record created for meta-learning report {report_id}")
        
    except Exception as e:
        logger.error(f"Failed to record meta-learning audit: {e}")
