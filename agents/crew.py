"""
Core CrewAI crew definition for the investment system.

This module defines the 6-agent crew that generates investment recommendations:
1. Token Research Agent - Alpha discovery and fundamentals
2. Technical Analyst Agent - Momentum and breadth analysis
3. Macro/Cycle Agent - Market regime assessment
4. Portfolio Context Agent - Current holdings data
5. Orchestrator Agent - Decision synthesis
6. QA/Risk Agent - Compliance and risk review

The Post-Mortem Architect is NOT part of this crew - it runs separately.
"""

import json
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from crewai import Agent, Task, Crew, Process, LLM

from agents.tools.portfolio_tools import (
    get_open_positions,
    get_position,
    get_trade_history,
    get_realized_pnl_summary,
    get_portfolio_summary,
    get_portfolio_snapshot,
)
from agents.utils.db_connection import close_db_connection
from agents.tools.market_data_tools import (
    get_price_history,
    get_btc_relative_price,
    get_market_cap_rankings,
    get_price_change,
    lookup_asset_id,
)
from agents.tools.technical_tools import (
    get_sma,
    get_rsi,
    get_price_correlation,
    get_momentum_summary,
)
from agents.tools.serper_tools import (
    search_web,
    search_crypto_news,
    search_market_metrics,
    search_macro_conditions,
    search_asset_fundamentals,
)
from agents.utils.prompt_loader import load_prompt, get_all_prompt_versions
from agents.utils.report_generator import generate_investment_report

# Import validation module
from validation.task_validation import (
    validate_task_output,
    ValidationResult,
    ValidationError,
    STRICT_VALIDATION_TASKS,
    get_validation_summary,
    enforce_trading_plan_rule,
    enforce_recommendations_contract,
    should_block_report_generation,
    get_enforcement_summary,
    build_research_packet_prompt,
)

# Import schemas for output_pydantic enforcement
from schemas.recommendations import RecommendationsSchema

logger = logging.getLogger(__name__)


# =============================================================================
# Portfolio JSON Validation
# =============================================================================

def validate_portfolio_json(output: str) -> Dict[str, Any]:
    """
    Parse and validate portfolio context output as JSON.
    
    The Portfolio Context Agent should output strict JSON from the
    get_portfolio_snapshot tool. This function validates the output.
    
    Args:
        output: Raw output string from the Portfolio Context Agent
    
    Returns:
        Parsed portfolio snapshot dictionary
    
    Raises:
        ValueError: If output is not valid JSON or missing required keys
    """
    # Strip any whitespace or markdown code fences the LLM might have added
    cleaned = output.strip()
    
    # Remove markdown code fences if present (fallback for non-compliant output)
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise ValueError(f"Portfolio output is not valid JSON: {e}")
    
    # Validate required top-level keys
    required_keys = ["meta", "portfolio_totals", "positions", "derived", "framework"]
    missing_keys = [key for key in required_keys if key not in data]
    
    if missing_keys:
        raise ValueError(f"Portfolio JSON missing required keys: {missing_keys}")
    
    # Validate framework structure
    if "checks" not in data.get("framework", {}):
        raise ValueError("Portfolio JSON missing framework.checks")
    
    return data


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
    
    Note:
        Temperature is optional. If not specified in the prompty file,
        the LLM will use its default temperature setting. This allows
        flexibility for models that don't support temperature control
        (like GPT-5 models as of Jan 2026) while maintaining backward
        compatibility with models that do.
    """
    model_config = prompt_data.get("model", {})
    configuration = model_config.get("configuration", {})
    
    # Extract model settings with defaults
    model_name = configuration.get("model", "gpt-4o")
    
    # Build LLM kwargs
    llm_kwargs = {
        "model": f"openai/{model_name}",
    }
    
    # Only set temperature if explicitly provided in configuration
    # This allows models that don't support temperature (like GPT-5)
    # to work without errors
    if "temperature" in configuration:
        llm_kwargs["temperature"] = configuration["temperature"]
    
    # Create LLM with OpenAI provider
    return LLM(**llm_kwargs)


# =============================================================================
# Agent Definitions
# =============================================================================

def create_token_research_agent() -> Agent:
    """Create the Token Research Agent for alpha discovery."""
    prompt_data = load_prompt("token_research")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Token Research Agent",
        goal="Discover high-quality crypto assets with strong fundamentals, network effects, and real adoption metrics",
        backstory=prompt_data.get("system", "You are a Token Research Agent specializing in crypto fundamentals."),
        llm=llm,
        tools=[
            search_web,
            search_crypto_news,
            search_market_metrics,
            search_asset_fundamentals,
            get_market_cap_rankings,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_token_screener_agent() -> Agent:
    """Create the Token Screener Agent for multi-lens discovery."""
    prompt_data = load_prompt("token_screener")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Token Screener Agent",
        goal="Comprehensive multi-lens discovery to cast wide net and prevent missed opportunities",
        backstory=prompt_data.get("system", "You are a Token Screener Agent specializing in multi-lens discovery."),
        llm=llm,
        tools=[
            get_market_cap_rankings,
            search_web,
            search_crypto_news,
            get_price_change,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_fundamentals_analyst_agent() -> Agent:
    """Create the Fundamentals Analyst Agent for deep fundamental research."""
    prompt_data = load_prompt("fundamentals_analyst")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Fundamentals Analyst Agent",
        goal="Conduct thorough fundamental analysis on token candidates",
        backstory=prompt_data.get("system", "You are a Fundamentals Analyst specializing in on-chain metrics."),
        llm=llm,
        tools=[
            search_asset_fundamentals,
            search_market_metrics,
            search_web,
            get_market_cap_rankings,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_research_synthesizer_agent() -> Agent:
    """Create the Research Synthesizer Agent for news analysis and final ranking."""
    prompt_data = load_prompt("research_synthesizer")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Research Synthesizer Agent",
        goal="Synthesize research findings into ranked investment recommendations",
        backstory=prompt_data.get("system", "You are a Research Synthesizer specializing in combining research into actionable recommendations."),
        llm=llm,
        tools=[
            search_crypto_news,
            search_web,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_technical_analyst_agent() -> Agent:
    """Create the Technical Analyst Agent for momentum analysis."""
    prompt_data = load_prompt("technical_analyst")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Technical Analyst Agent",
        goal="Evaluate price momentum using technical indicators and assess BTC-relative performance",
        backstory=prompt_data.get("system", "You are a Technical Analyst specializing in crypto momentum."),
        llm=llm,
        tools=[
            get_price_history,
            get_btc_relative_price,
            get_price_change,
            get_sma,
            get_rsi,
            get_price_correlation,
            get_momentum_summary,
            lookup_asset_id,  # Helps resolve symbols to asset_ids
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_macro_cycle_agent() -> Agent:
    """Create the Macro/Cycle Agent for market regime assessment."""
    prompt_data = load_prompt("macro_cycle")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Macro Economic / Market Cycle Agent",
        goal="Assess macroeconomic conditions and determine if the environment favors risk-taking",
        backstory=prompt_data.get("system", "You are a Macro Analyst assessing crypto market cycles."),
        llm=llm,
        tools=[
            search_web,
            search_macro_conditions,
            search_crypto_news,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_portfolio_context_agent() -> Agent:
    """
    Create the Portfolio Context Agent for current holdings data.
    
    This agent is a thin wrapper that calls get_portfolio_snapshot and
    outputs the JSON verbatim. It does NOT interpret or compute compliance -
    all compliance checks are done deterministically in the tool.
    """
    prompt_data = load_prompt("portfolio_context")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Portfolio Context Agent",
        goal="Call get_portfolio_snapshot and output the JSON result verbatim",
        backstory=prompt_data.get("system", "You are a Portfolio Context Agent that outputs deterministic JSON."),
        llm=llm,
        tools=[
            get_portfolio_snapshot,  # Only tool needed - returns complete snapshot with compliance
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_orchestrator_agent() -> Agent:
    """Create the Orchestrator Agent for decision synthesis."""
    prompt_data = load_prompt("orchestrator")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="Orchestrator Agent",
        goal="Synthesize insights and generate investment recommendations as a valid JSON object only",
        backstory=prompt_data.get("system", "You are the Orchestrator synthesizing investment decisions."),
        llm=llm,
        tools=[
            get_portfolio_summary,
            get_open_positions,
        ],
        verbose=True,
        allow_delegation=False,
    )


def create_qa_risk_agent() -> Agent:
    """Create the QA/Risk Agent for compliance review."""
    prompt_data = load_prompt("qa_risk")
    llm = _create_llm_from_prompty(prompt_data)
    
    return Agent(
        role="QA / Risk Assessor Agent",
        goal="Review recommendations for factual accuracy, framework compliance, and risk management",
        backstory=prompt_data.get("system", "You are the QA/Risk gatekeeper ensuring compliance."),
        llm=llm,
        tools=[
            get_portfolio_summary,
            get_open_positions,
            get_price_correlation,
        ],
        verbose=True,
        allow_delegation=False,
    )


# =============================================================================
# Task Definitions
# =============================================================================

def create_tasks(
    token_screener_agent: Agent,
    fundamentals_analyst_agent: Agent,
    research_synthesizer_agent: Agent,
    technical_analyst_agent: Agent,
    macro_cycle_agent: Agent,
    portfolio_context_agent: Agent,
    orchestrator_agent: Agent,
    qa_risk_agent: Agent,
    focus_assets: Optional[list] = None,
) -> list:
    """
    Create the sequential tasks for the investment crew.
    
    Args:
        *_agent: The agent instances
        focus_assets: Optional list of specific assets to analyze
    
    Returns:
        List of Task objects in execution order
    """
    
    focus_description = ""
    if focus_assets:
        focus_description = f"\n\nFocus your analysis on these specific assets: {', '.join(focus_assets)}"
    
    # Task 1: Multi-Lens Discovery & Screening
    token_screening_task = Task(
        description=f"""Conduct comprehensive multi-lens discovery and screening to identify promising cryptocurrency investment opportunities.

Use ALL of these screening approaches in parallel:

1. **Market Cap Screening**:
   - Primary: Top 200 by market cap (framework requirement)
   - Secondary: Also consider rank 201-350 IF they meet exception criteria

2. **Momentum Screening**:
   - Identify assets with strong recent momentum (price/volume)
   - Look for assets climbing rankings (e.g., 250→180 in last 30 days)
   - Include even if currently outside top 200

3. **Narrative/Catalyst Screening**:
   - Search for assets with recent strong news or catalysts
   - Identify emerging narratives (AI, RWA, etc.)
   - Include if they show early adoption signals

4. **Sector Leadership Screening**:
   - Identify category leaders in key sectors (L1s, L2s, DeFi, RWA)
   - Include if they're top 3 in their category, even if rank 201-300

**Exception Criteria** (for assets outside top 200):
- TVL > $50M OR DAU > 10K OR protocol revenue > $1M/month
- Recent positive momentum (20%+ price increase in 30 days)
- Clear network effects or major partnership
- NOT a meme coin, anonymous team, or experimental token

**Output Format**:
- Standard candidates: 10-15 from top 200
- Exception candidates: 3-5 from rank 201-350 (with explicit justification)
- Total: 15-20 candidates for deep analysis
- For each exception, provide: market_cap_rank, exception_reason, justification

Avoid: meme coins, anonymous teams, leverage/derivatives.
{focus_description}

Output a structured candidate list with standard and exception categories.""",
        expected_output="A comprehensive candidate list with 15-20 assets including standard top 200 candidates and justified exceptions from rank 201-350, with basic info and exception justifications.",
        agent=token_screener_agent,
    )
    
    # Task 2: Deep Fundamental Analysis
    fundamentals_analysis_task = Task(
        description=f"""Conduct deep fundamental analysis on the candidates identified by the Token Screener.

Using the candidate list from the previous task, for each candidate:

1. **On-Chain Metrics Analysis**:
   - TVL trends (growing/declining/stable)
   - Daily Active Users (DAU) and growth trajectory
   - Protocol revenue or transaction volume
   - Network transaction counts and fees
   - Token distribution and holder concentration

2. **Qualitative Assessment**:
   - Real-world use case and problem being solved
   - Network effects (existing or emerging)
   - Team credibility and track record
   - Ecosystem integrations and partnerships
   - Competitive positioning

3. **Risk Evaluation**:
   - Known weaknesses or concerns
   - Regulatory considerations
   - Technical risks or vulnerabilities
   - Economic model sustainability

4. **Competitive Position**:
   - How does this compare to similar projects?
   - What are the moats or differentiators?

Output detailed fundamental analysis for each candidate.
{focus_description}""",
        expected_output="Deep fundamental analysis report for each candidate including adoption metrics, network effects, risk factors, and competitive positioning.",
        agent=fundamentals_analyst_agent,
        context=[token_screening_task],
    )
    
    # Task 3: News & Sentiment Check
    news_sentiment_task = Task(
        description=f"""Research recent news and sentiment for each candidate from the fundamentals analysis.

For each candidate:
1. Search for recent news articles and developments
2. Assess market sentiment (positive/negative/neutral)
3. Identify catalysts, risks, and market narratives
4. Note any recent developments that could impact investment thesis

Output news/sentiment summary for each candidate.
{focus_description}""",
        expected_output="News and sentiment analysis report for each candidate including recent developments, catalysts, risks, and market narratives.",
        agent=research_synthesizer_agent,
        context=[fundamentals_analysis_task],
    )
    
    # Task 4: Ranking & Final Selection
    token_research_task = Task(
        description=f"""Synthesize the fundamental analysis and news/sentiment to produce final ranked recommendations.

Using the fundamental analysis and news/sentiment from previous tasks:

1. **Score Each Candidate** (1-10 scale) on:
   - Adoption metrics strength (weight: 30%)
   - Network effects potential (weight: 25%)
   - Team/ecosystem quality (weight: 20%)
   - Risk-adjusted opportunity (weight: 25%)

2. **Rank Candidates**: Order by composite score

3. **Select Top 5-8**: Choose the highest-conviction opportunities

4. **Final Assessment** for top candidates:
   - Tier classification (1/2/3)
   - Investment thesis (2-3 sentences)
   - Primary risks to monitor
   - Why this beats alternatives

**CRITICAL**: Output MUST be valid JSON in the exact format specified in your system prompt. Include all required fields including is_exception, exception_reason, and exception_justification for any exception candidates.
{focus_description}

Output a ranked JSON list with top 5-8 recommendations and complete analysis.""",
        expected_output="Synthesized research report in JSON format with ranked candidates, top 5-8 recommendations, tier classifications, investment theses, composite scores, and risk assessments.",
        agent=research_synthesizer_agent,
        context=[fundamentals_analysis_task, news_sentiment_task],
    )
    
    # Task 5: Technical Analysis
    technical_analysis_task = Task(
        description=f"""Analyze technical momentum for the assets identified by the Token Research Synthesizer.

**CONTEXT FROM TOKEN RESEARCH:**
The previous task (token_research) output contains:
- "candidates": Array of token candidates with symbols (e.g., ETH, BTC, SOL)
- "ranked_shortlist": Ranked list with symbols and scores

**MANDATORY WORKFLOW - YOU MUST FOLLOW THESE STEPS:**

1. **Parse the context JSON** to extract ALL symbols from "candidates" and "ranked_shortlist"

2. **Call technical tools for EACH symbol** - You MUST use your tools:
   - get_momentum_summary(symbol) - Comprehensive analysis for each asset
   - get_sma(symbol, 50) and get_sma(symbol, 200) - Moving averages
   - get_rsi(symbol, 14) - RSI momentum reading
   - get_price_correlation(symbol, "bitcoin", 90) - BTC correlation
   
   Example: For symbol "ETH", call get_momentum_summary("ETH"), get_sma("ETH", 50), etc.

3. **Build JSON output** using REAL DATA from your tool calls (not made up values)

4. **Include market breadth** assessment: how many assets are in uptrends vs downtrends?

**CRITICAL REQUIREMENTS:**
- You MUST call get_momentum_summary() for EACH symbol before generating output
- An empty "assets" array is NOT ACCEPTABLE when candidates exist in the context
- Use actual data from tool responses - do NOT hallucinate indicator values
- Analyze ALL top recommendations regardless of exception status
{focus_description}

Output technical ratings and momentum assessments for each asset in valid JSON format.""",
        expected_output="Technical analysis JSON with SMA positions, RSI readings, BTC-relative performance, and bullish/neutral/bearish classifications for EACH asset from the token research candidates.",
        agent=technical_analyst_agent,
        context=[token_research_task],
    )
    
    # Task 6: Macro Analysis
    macro_analysis_task = Task(
        description="""Assess the current macroeconomic environment and crypto market cycle.

Your responsibilities:
1. Research current Fed policy, liquidity conditions, and risk appetite
2. Identify where we are in the crypto market cycle (early/mid/late)
3. Assess dominant market narratives and their substance
4. Determine market regime: RISK-ON / RISK-OFF / NEUTRAL

Answer the key question: "Is this a good time to take risk in crypto markets?"

Output a market regime assessment with supporting evidence.""",
        expected_output="Macro analysis report with market regime classification (Risk-On/Risk-Off/Neutral), cycle position assessment, and key risks to monitor.",
        agent=macro_cycle_agent,
    )
    
    # Task 7: Portfolio Context (Deterministic JSON Output)
    portfolio_context_task = Task(
        description="""Call get_portfolio_snapshot and output the JSON result verbatim.

CRITICAL INSTRUCTIONS:
1. Call ONLY the get_portfolio_snapshot tool
2. Output the tool response AS-IS - do not modify, summarize, or interpret it
3. Do NOT add any natural language commentary
4. Do NOT compute or restate compliance - the tool already did this deterministically
5. Your entire output must be valid JSON and nothing else
6. Do NOT wrap the JSON in markdown code fences

The get_portfolio_snapshot tool returns a complete JSON object with:
- meta: Timestamp, data quality, warnings
- portfolio_totals: Cost basis, current value, realized P&L
- positions: All positions with prices, values, allocations, tiers
- derived: BTC allocation %, tier 2+3 allocation %, max single asset
- framework: Config and compliance check booleans

Output ONLY the raw JSON object. No preamble, no explanation.""",
        expected_output="A valid JSON object containing the complete portfolio snapshot with meta, portfolio_totals, positions, derived, and framework sections.",
        agent=portfolio_context_agent,
    )
    
    # Task 8: Orchestration (Decision Synthesis)
    orchestration_task = Task(
        description="""Synthesize all agent inputs and generate investment recommendations.

You have context from:
- Token Research: Ranked JSON list with top recommendations, composite scores, and investment theses
- Technical Analysis: Momentum signals and BTC-relative performance
- Macro Analysis: Market regime and risk environment
- Portfolio Context: Current holdings and constraints

Your responsibilities:
1. Integrate all agent findings into a coherent view
2. Apply the 8-question rubric to each potential recommendation:
   - What problem does this asset solve?
   - What network effects exist or are emerging?
   - Why now?
   - What would invalidate this thesis?
   - Why is this better than doing nothing?
   - What are the downside risks?
   - Where does this fit (Tier 0-3)?
   - How will you sell? (exit criteria)

3. Generate specific recommendations: BUY / HOLD / REDUCE / SELL / WATCH
4. Default: "Hold Cash/BTC" unless high-conviction opportunity exists

CRITICAL: Output ONLY a valid JSON object. No explanations, no markdown, no narrative text.
Your response must start with { and end with }. Nothing else.""",
        expected_output="A valid JSON object conforming to the RecommendationsSchema with meta, executive_summary, market_context, recommendations array, and default_recommendation. No text before or after the JSON.",
        agent=orchestrator_agent,
        context=[token_research_task, technical_analysis_task, macro_analysis_task, portfolio_context_task],
        output_pydantic=RecommendationsSchema,
    )
    
    # Task 9: QA/Risk Review
    qa_risk_task = Task(
        description="""Review the Orchestrator's recommendations for compliance and risk.

Your responsibilities:
1. Verify framework compliance:
   - All assets in top 200 by market cap? (Exception: rank 201-350 only if explicitly flagged as exception with strong justification)
   - No single position > 20% of portfolio?
   - BTC allocation remains 40-60%?
   - No leverage/derivatives/yield farming?

2. **Validate Exception Candidates** (rank 201-350):
   - Verify exception justifications meet framework criteria
   - Check that exception candidates have strong adoption metrics (TVL > $50M OR DAU > 10K OR revenue > $1M/month)
   - Verify recent momentum (20%+ price increase) or major partnership/network effects
   - Flag any final recommendations outside top 200 for explicit human approval

3. Assess portfolio-level risk:
   - Correlation between proposed and existing holdings
   - Sector concentration
   - Current drawdown status

4. Validate 8-question rubric:
   - Were all questions answered substantively?
   - Is conviction justified by evidence?

5. Final verdict for each recommendation: PASS / FLAG / REJECT
   - Exception candidates require FLAG status for human review even if they pass other checks

Output a QA review with compliance checklist, exception candidate validation, risk assessment, per-recommendation verdicts (PASS/FLAG/REJECT), and any required modifications before execution.""",
        expected_output="QA review report with compliance checklist, exception candidate validation, risk assessment, per-recommendation verdicts (PASS/FLAG/REJECT), and any required modifications before execution.",
        agent=qa_risk_agent,
        context=[orchestration_task, portfolio_context_task, token_research_task],
    )
    
    return [
        token_screening_task,
        fundamentals_analysis_task,
        news_sentiment_task,
        token_research_task,
        technical_analysis_task,
        macro_analysis_task,
        portfolio_context_task,
        orchestration_task,
        qa_risk_task,
    ]


# =============================================================================
# Crew Definition
# =============================================================================

def create_investment_crew(focus_assets: Optional[list] = None) -> Crew:
    """
    Create the investment crew with all agents.
    
    Args:
        focus_assets: Optional list of specific assets to analyze
    
    Returns:
        Configured Crew instance
    """
    # Create token research chain agents
    token_screener_agent = create_token_screener_agent()
    fundamentals_analyst_agent = create_fundamentals_analyst_agent()
    research_synthesizer_agent = create_research_synthesizer_agent()
    
    # Create other agents
    technical_analyst_agent = create_technical_analyst_agent()
    macro_cycle_agent = create_macro_cycle_agent()
    portfolio_context_agent = create_portfolio_context_agent()
    orchestrator_agent = create_orchestrator_agent()
    qa_risk_agent = create_qa_risk_agent()
    
    # Create tasks
    tasks = create_tasks(
        token_screener_agent=token_screener_agent,
        fundamentals_analyst_agent=fundamentals_analyst_agent,
        research_synthesizer_agent=research_synthesizer_agent,
        technical_analyst_agent=technical_analyst_agent,
        macro_cycle_agent=macro_cycle_agent,
        portfolio_context_agent=portfolio_context_agent,
        orchestrator_agent=orchestrator_agent,
        qa_risk_agent=qa_risk_agent,
        focus_assets=focus_assets,
    )
    
    # Create crew with tracing enabled for detailed error logging
    investment_crew = Crew(
        agents=[
            token_screener_agent,
            fundamentals_analyst_agent,
            research_synthesizer_agent,
            technical_analyst_agent,
            macro_cycle_agent,
            portfolio_context_agent,
            orchestrator_agent,
            qa_risk_agent,
        ],
        tasks=tasks,
        process=Process.sequential,
        verbose=True,
        tracing=True,  # Enable tracing for detailed error logs
    )
    
    return investment_crew


# =============================================================================
# Execution Function
# =============================================================================

def run_investment_crew(
    db_path: str = "market_data.duckdb",
    focus_assets: Optional[list] = None,
    save_report: bool = True,
) -> Dict[str, Any]:
    """
    Execute the investment crew and generate a report.
    
    Args:
        db_path: Path to the DuckDB database
        focus_assets: Optional list of specific assets to analyze
        save_report: Whether to save the report to disk
    
    Returns:
        Dictionary containing crew output and report metadata
    """
    # Set environment variable for tools
    os.environ["DUCKDB_PATH"] = db_path
    
    logger.info("Creating investment crew...")
    crew = create_investment_crew(focus_assets=focus_assets)
    
    logger.info("Executing investment crew...")
    start_time = datetime.now()
    
    try:
        result = crew.kickoff()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Crew execution completed in {execution_time:.1f} seconds")
        
        # Extract individual task outputs for report generation
        task_names = [
            "token_screening",
            "fundamentals_analysis",
            "news_sentiment",
            "token_research",
            "technical_analysis", 
            "macro_analysis",
            "portfolio_context",
            "orchestration",
            "qa_risk"
        ]
        
        task_outputs = {}
        validated_outputs = {}
        validation_status = {}  # Track soft validation status per task
        validation_errors = []  # Critical errors (JSON parse failures)
        validation_warnings_all = []  # Non-blocking warnings (schema failures)
        
        for i, task in enumerate(crew.tasks):
            if i < len(task_names):
                task_name = task_names[i]
                if task.output:
                    raw_output = task.output.raw if hasattr(task.output, 'raw') else str(task.output)
                    task_outputs[task_name] = raw_output
                else:
                    task_outputs[task_name] = ""
                    raw_output = ""
                
                logger.debug(f"Captured output for {task_name}: {len(task_outputs[task_name])} chars")
                
                # Validate output for strict validation tasks with SOFT VALIDATION
                if task_name in STRICT_VALIDATION_TASKS and raw_output:
                    validation_result = validate_task_output(task_name, raw_output)
                    
                    # Track validation status for each task
                    validation_status[task_name] = {
                        "usable": validation_result.success,
                        "strict_valid": validation_result.strict_valid,
                        "warnings": validation_result.validation_warnings,
                    }
                    
                    if validation_result.success:
                        # SOFT VALIDATION: Data is usable even if not strictly valid
                        validated_outputs[task_name] = validation_result.parsed_data
                        
                        if validation_result.strict_valid:
                            logger.info(f"Task {task_name} passed strict validation")
                        else:
                            # Data usable but has schema warnings
                            logger.warning(
                                f"Task {task_name} passed soft validation with "
                                f"{len(validation_result.validation_warnings)} warnings"
                            )
                            validation_warnings_all.append({
                                "task": task_name,
                                "warnings": validation_result.validation_warnings,
                            })
                    else:
                        # CRITICAL: JSON parse failure - data NOT usable
                        logger.error(
                            f"Task {task_name} failed validation (JSON parse error): "
                            f"{validation_result.errors}"
                        )
                        validation_errors.append({
                            "task": task_name,
                            "errors": validation_result.errors,
                        })
                        # Still store the raw JSON/fallback if available
                        validated_outputs[task_name] = validation_result.raw_json
        
        # Check for CRITICAL validation failures (JSON parse errors only)
        # Schema validation failures are now soft (warnings, not blockers)
        critical_failures = [
            e for e in validation_errors 
            if e["task"] in {"orchestration", "qa_risk", "portfolio_context"}
        ]
        
        if critical_failures:
            logger.error(f"Critical validation failures (JSON parse errors): {critical_failures}")
            # Generate error report instead of normal report
            if save_report:
                from agents.utils.report_generator import generate_error_report
                report_path = generate_error_report(
                    validation_errors=validation_errors,
                    task_outputs=task_outputs,
                    db_path=db_path,
                )
                logger.info(f"Error report saved to: {report_path}")
            
            return {
                "success": False,
                "error": f"Critical validation failures: {[e['task'] for e in critical_failures]}",
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings_all,
                "validation_status": validation_status,
                "report_path": report_path if save_report else None,
                "execution_time_seconds": execution_time,
                "focus_assets": focus_assets,
            }
        
        # =============================================================
        # PHASE 5: Apply Recommendations Contract Enforcement
        # =============================================================
        enforcement_summary = None
        enforcement_warnings = []
        
        if (validated_outputs.get("orchestration") is not None and 
            validated_outputs.get("portfolio_context") is not None):
            
            logger.info("Applying recommendations contract enforcement...")
            
            try:
                from schemas.recommendations import RecommendationsSchema
                from schemas.portfolio_context import PortfolioContextSchema
                
                # Convert to schema objects if they're dicts
                orchestration_data = validated_outputs["orchestration"]
                portfolio_data = validated_outputs["portfolio_context"]
                
                if isinstance(orchestration_data, dict):
                    orchestration_schema = RecommendationsSchema.model_validate(orchestration_data)
                else:
                    orchestration_schema = orchestration_data
                
                if isinstance(portfolio_data, dict):
                    portfolio_schema = PortfolioContextSchema.model_validate(portfolio_data)
                else:
                    portfolio_schema = portfolio_data
                
                # Apply enforcement
                enforced_recs, enforcement_warnings = enforce_recommendations_contract(
                    recommendations=orchestration_schema,
                    portfolio_context=portfolio_schema
                )
                
                # Update validated outputs with enforced recommendations
                validated_outputs["orchestration"] = enforced_recs
                
                # Get enforcement summary
                enforcement_summary = get_enforcement_summary(
                    original_recommendations=orchestration_schema,
                    enforced_recommendations=enforced_recs,
                    warnings=enforcement_warnings
                )
                
                logger.info(
                    f"Enforcement complete: {enforcement_summary['downgrades_to_watch']} downgrades, "
                    f"{enforcement_summary['allocation_reductions']} allocation reductions, "
                    f"{enforcement_summary['actionable_count']} actionable recommendations"
                )
                
            except Exception as e:
                logger.error(f"Enforcement failed: {e}")
                enforcement_warnings.append(f"Enforcement error: {str(e)}")
        
        # =============================================================
        # PHASE 5: QA Gating Check
        # =============================================================
        qa_blocked = False
        qa_block_reason = ""
        
        if validated_outputs.get("qa_risk") is not None:
            from schemas.qa_review import QAReviewSchema
            
            qa_data = validated_outputs["qa_risk"]
            if isinstance(qa_data, dict):
                qa_schema = QAReviewSchema.model_validate(qa_data)
            else:
                qa_schema = qa_data
            
            qa_blocked, qa_block_reason = should_block_report_generation(qa_schema)
            
            if qa_blocked:
                logger.warning(f"QA rejected run: {qa_block_reason}")
        
        # Build validation summary from tracked status (avoid re-validating)
        validation_summary = {
            "total_tasks": len(validation_status),
            "usable": sum(1 for s in validation_status.values() if s["usable"]),
            "strict_valid": sum(1 for s in validation_status.values() if s["strict_valid"]),
            "with_warnings": sum(1 for s in validation_status.values() if s["warnings"]),
            "failed": len(validation_errors),
        }
        logger.info(
            f"Validation summary: {validation_summary['usable']}/{validation_summary['total_tasks']} usable, "
            f"{validation_summary['strict_valid']} strict valid, "
            f"{validation_summary['with_warnings']} with warnings"
        )
        
        # Generate and save report
        report_path = None
        if save_report:
            prompt_versions = get_all_prompt_versions()
            report_path = generate_investment_report(
                crew_output=result,
                task_outputs=task_outputs,
                validated_outputs=validated_outputs,
                prompt_versions=prompt_versions,
                db_path=db_path,
                qa_blocked=qa_blocked,
                qa_block_reason=qa_block_reason,
                validation_status=validation_status,  # Pass validation status for report
            )
            logger.info(f"Report saved to: {report_path}")
        
        return {
            "success": True,
            "output": result,
            "report_path": report_path,
            "execution_time_seconds": execution_time,
            "focus_assets": focus_assets,
            "validation_summary": validation_summary,
            "validation_status": validation_status,  # Detailed per-task status
            "validation_warnings": validation_warnings_all,  # Non-blocking schema warnings
            "validated_outputs": {k: v is not None for k, v in validated_outputs.items()},
            "enforcement_summary": enforcement_summary,
            "enforcement_warnings": enforcement_warnings,
            "qa_blocked": qa_blocked,
            "qa_block_reason": qa_block_reason if qa_blocked else None,
        }
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return {
            "success": False,
            "error": str(e),
            "validation_errors": [{"task": e.task_name, "errors": e.errors}],
            "execution_time_seconds": (datetime.now() - start_time).total_seconds(),
        }
        
    except Exception as e:
        logger.error(f"Crew execution failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "execution_time_seconds": (datetime.now() - start_time).total_seconds(),
        }
    
    finally:
        # Always close the shared database connection
        logger.info("Closing shared database connection...")
        close_db_connection()


# Global crew instance for import
investment_crew = None
