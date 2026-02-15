"""
Post-Mortem Architect - Meta-Learning Agent.

This module runs SEPARATELY from the core investment crew. It analyzes
historical performance, identifies where the system succeeded or failed,
and proposes improvements for future iterations.

The Post-Mortem Architect:
- Does NOT influence real-time trading decisions
- Runs on a separate schedule (monthly/quarterly)
- Generates meta-learning reports for system improvement
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

from crewai import Agent, Task, Crew, Process, LLM

from agents.tools.portfolio_tools import (
    get_open_positions,
    get_trade_history,
    get_realized_pnl_summary,
    get_portfolio_summary,
)
from agents.tools.market_data_tools import (
    get_price_history,
    get_btc_relative_price,
    get_price_change,
)
from agents.utils.prompt_loader import load_prompt, get_prompt_version
from agents.utils.meta_report_generator import generate_meta_learning_report

logger = logging.getLogger(__name__)


# =============================================================================
# Report Reader Tool
# =============================================================================

def read_investment_reports(reports_dir: str = "reports", days: int = 30) -> str:
    """
    Read recent investment reports for analysis.
    
    Args:
        reports_dir: Directory containing investment reports
        days: Number of days of reports to read
    
    Returns:
        Concatenated report contents
    """
    reports_path = Path(reports_dir)
    if not reports_path.exists():
        return "No reports directory found."
    
    cutoff_date = datetime.now() - timedelta(days=days)
    
    reports = []
    for report_file in sorted(reports_path.glob("*.json"), reverse=True):
        # Parse date from filename (format: YYYY-MM-DD-investment-review.json)
        try:
            date_str = report_file.stem[:10]  # First 10 chars
            report_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            if report_date >= cutoff_date:
                with open(report_file, 'r') as f:
                    report_data = json.load(f)
                    reports.append({
                        "date": date_str,
                        "file": report_file.name,
                        "content": report_data,
                    })
        except (ValueError, json.JSONDecodeError) as e:
            logger.warning(f"Could not parse report {report_file}: {e}")
            continue
    
    if not reports:
        return f"No investment reports found in the last {days} days."
    
    # Format reports for agent consumption
    output_lines = [f"Investment Reports from last {days} days ({len(reports)} reports found):", "=" * 60]
    
    for report in reports[:10]:  # Limit to 10 most recent
        output_lines.append(f"\n### Report: {report['date']}")
        output_lines.append(f"File: {report['file']}")
        
        content = report['content']
        if isinstance(content, dict):
            # Extract key sections
            if 'executive_summary' in content:
                output_lines.append(f"Executive Summary: {content['executive_summary'][:500]}...")
            if 'recommendations' in content:
                output_lines.append(f"Recommendations: {json.dumps(content['recommendations'], indent=2)[:500]}...")
    
    return "\n".join(output_lines)


# =============================================================================
# LLM Configuration Helper
# =============================================================================

def _create_llm_from_prompty(prompt_data: Dict[str, Any]) -> LLM:
    """
    Create an LLM instance from prompty file model configuration.
    
    Args:
        prompt_data: Parsed prompty file data containing model configuration
    
    Returns:
        Configured LLM instance
    """
    model_config = prompt_data.get("model", {})
    configuration = model_config.get("configuration", {})
    
    # Extract model settings with defaults
    model_name = configuration.get("model", "gpt-4o")
    temperature = configuration.get("temperature", 0.7)
    
    # Create LLM with OpenAI provider
    return LLM(
        model=f"openai/{model_name}",
        temperature=temperature,
    )


# =============================================================================
# Post-Mortem Agent Definition
# =============================================================================

def create_post_mortem_agent() -> Agent:
    """Create the Post-Mortem Architect agent."""
    prompt_data = load_prompt("post_mortem")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Post-Mortem Architect",
        goal="Analyze historical performance and identify systematic improvements for the investment agent system",
        backstory=prompt_data.get("system", "You are the Post-Mortem Architect analyzing system performance."),
        llm=llm,
        tools=[
            get_portfolio_summary,
            get_open_positions,
            get_trade_history,
            get_realized_pnl_summary,
            get_price_history,
            get_btc_relative_price,
            get_price_change,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_post_mortem_task(
    agent: Agent,
    period_months: int = 1,
    reports_summary: str = "",
) -> Task:
    """
    Create the meta-learning analysis task.
    
    Args:
        agent: The Post-Mortem agent
        period_months: Number of months to analyze
        reports_summary: Summary of investment reports to analyze
    
    Returns:
        Task for meta-learning analysis
    """
    return Task(
        description=f"""Conduct a comprehensive post-mortem analysis of the investment system's performance.

Analysis Period: Last {period_months} month(s)

## Your Responsibilities:

### 1. Performance Analysis
- Compare portfolio performance vs BTC benchmark
- Calculate realized P&L and win rate on closed positions
- Identify best and worst performing recommendations

### 2. Logic Drift Detection
Look for systematic biases in agent behavior:
- Excessive bullishness or bearishness patterns
- Consistent overweighting of certain narratives
- Ignoring specific risk factors repeatedly
- Recency bias in analysis

### 3. Missed Opportunity Analysis
Identify blind spots:
- Assets that performed well but weren't recommended
- Macro risks that were underweighted
- Technical signals that were ignored

### 4. Framework Alignment Check
Assess adherence to investment principles:
- Were 8-question rubrics properly applied?
- Was BTC allocation maintained within 40-60%?
- Were position sizing rules followed?

### 5. Generate Improvement Recommendations
Provide specific, actionable suggestions:
- Prompt refinements for specific agents
- New metrics or data sources to incorporate
- Risk parameter adjustments
- Process improvements

## Previous Investment Reports Summary:
{reports_summary if reports_summary else "No previous reports available for analysis."}

## Important:
- Be objective and data-driven
- Focus on systemic issues, not one-off errors
- Prioritize improvements by potential impact
- This analysis does NOT make trading decisions

Output a comprehensive Meta-Learning Report.""",
        expected_output="""Meta-Learning Report with:
1. Executive Summary of key findings
2. Performance vs benchmark analysis
3. Logic drift patterns identified
4. Missed opportunities catalogued
5. Framework compliance assessment
6. Prioritized improvement recommendations with specific prompt changes suggested""",
        agent=agent,
    )


# =============================================================================
# Execution Function
# =============================================================================

def run_meta_learning(
    db_path: str = "market_data.duckdb",
    period_months: int = 1,
    reports_dir: str = "reports",
    save_report: bool = True,
) -> Dict[str, Any]:
    """
    Run the Post-Mortem Architect meta-learning analysis.
    
    This function runs SEPARATELY from the core investment crew.
    It analyzes historical performance and generates improvement suggestions.
    
    Args:
        db_path: Path to DuckDB database
        period_months: Number of months to analyze
        reports_dir: Directory containing investment reports
        save_report: Whether to save the meta-learning report
    
    Returns:
        Dictionary containing analysis output and report metadata
    """
    # Set environment variable for tools
    os.environ["DUCKDB_PATH"] = db_path
    
    logger.info(f"Starting Post-Mortem analysis for last {period_months} month(s)...")
    
    # Read previous investment reports
    reports_summary = read_investment_reports(
        reports_dir=reports_dir,
        days=period_months * 30,
    )
    
    # Create agent and task
    post_mortem_agent = create_post_mortem_agent()
    post_mortem_task = create_post_mortem_task(
        agent=post_mortem_agent,
        period_months=period_months,
        reports_summary=reports_summary,
    )
    
    # Create single-agent crew
    meta_learning_crew = Crew(
        agents=[post_mortem_agent],
        tasks=[post_mortem_task],
        process=Process.sequential,
        verbose=True,
    )
    
    logger.info("Executing Post-Mortem analysis...")
    start_time = datetime.now()
    
    try:
        result = meta_learning_crew.kickoff()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Post-Mortem analysis completed in {execution_time:.1f} seconds")
        
        # Generate and save report
        report_path = None
        if save_report:
            prompt_version = get_prompt_version("post_mortem")
            
            # Calculate analysis period
            analysis_end = datetime.now()
            analysis_start = analysis_end - timedelta(days=period_months * 30)
            
            report_path = generate_meta_learning_report(
                analysis_output=result,
                prompt_version=prompt_version,
                analysis_period_start=analysis_start,
                analysis_period_end=analysis_end,
                reports_analyzed_count=reports_summary.count("### Report:"),
                db_path=db_path,
            )
            logger.info(f"Meta-learning report saved to: {report_path}")
        
        return {
            "success": True,
            "output": result,
            "report_path": report_path,
            "execution_time_seconds": execution_time,
            "period_months": period_months,
        }
        
    except Exception as e:
        logger.error(f"Post-Mortem analysis failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "execution_time_seconds": (datetime.now() - start_time).total_seconds(),
        }


def get_performance_summary(db_path: str = "market_data.duckdb") -> Dict[str, Any]:
    """
    Get a quick performance summary for the portfolio.
    
    Utility function that can be called independently to get
    current performance metrics without running full analysis.
    
    Args:
        db_path: Path to DuckDB database
    
    Returns:
        Dictionary with performance metrics
    """
    os.environ["DUCKDB_PATH"] = db_path
    
    from source.portfolio_store import PortfolioStore
    
    store = PortfolioStore(db_path)
    
    try:
        summary = store.get_portfolio_summary()
        pnl = store.get_realized_pnl_summary()
        positions = store.get_open_positions()
        
        return {
            "total_positions": summary["total_positions"],
            "total_cost_basis_usd": summary["total_cost_basis_usd"],
            "total_realized_pnl_usd": summary["total_realized_pnl_usd"],
            "total_trades": summary["total_trades"],
            "total_buys": pnl["total_buys"],
            "total_sells": pnl["total_sells"],
            "positions": [
                {"asset_id": p["asset_id"], "symbol": p["symbol"], "quantity": p["quantity"]}
                for p in positions
            ],
        }
    finally:
        store.close()
