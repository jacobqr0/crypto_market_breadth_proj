# CrewAI Investment Agent System

A multi-agent AI system for cryptocurrency investment research and recommendations, built on [CrewAI](https://www.crewai.com/). The system uses 8 specialized agents working sequentially to generate comprehensive investment reports aligned with a personal investment framework.

## Overview

The investment agent system consists of two separate processes:

1. **Core Investment Crew** (8 agents in 6 phases) - Runs weekly to generate investment recommendations
2. **Post-Mortem Architect** (1 agent) - Runs monthly for meta-learning and system improvement

### Core Agents

| Agent | Role | Model | Key Tools |
|-------|------|-------|-----------|
| Token Screener | Multi-lens discovery to cast a wide net | gpt-4o-mini | market_cap_rankings, search_web |
| Fundamentals Analyst | Deep on-chain metrics and qualitative analysis | gpt-4o-mini | search_asset_fundamentals, search_market_metrics |
| Research Synthesizer | Combines research into ranked shortlist | gpt-4o-mini | search_crypto_news |
| Technical Analyst | Momentum using SMAs, RSI, breadth, BTC-relative | gpt-4o-mini | get_sma, get_rsi, get_momentum_summary |
| Macro/Cycle | Market regime assessment (risk_on/risk_off/neutral) | gpt-4o-mini | search_macro_conditions |
| Portfolio Context | Current holdings and compliance (deterministic JSON) | gpt-3.5-turbo | get_portfolio_snapshot |
| Orchestrator | Synthesizes all inputs with 8-question rubric | gpt-4o | get_portfolio_summary |
| QA/Risk | Validates recommendations against framework | gpt-4o | get_price_correlation |

**Note:** Temperature settings are optional in prompty files. If omitted, the model uses its default.

### Post-Mortem Architect

| Agent | Role | Model |
|-------|------|-------|
| Post-Mortem | Analyzes historical performance and identifies improvements | gpt-4o |

Runs separately to analyze historical performance, identify logic drift, and suggest prompt refinements.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Core Investment Crew                                 │
│                                                                              │
│  RESEARCH PHASE (3 agents)                                                   │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────────┐            │
│  │Token Screener│─▶│Fundamentals      │─▶│Research Synthesizer │            │
│  │(15-20 cands) │  │Analyst (deep)    │  │(ranked shortlist)   │            │
│  └──────────────┘  └──────────────────┘  └─────────────────────┘            │
│                                                   │                          │
│  ANALYSIS PHASE                                   ▼                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                       │
│  │Technical     │  │Macro/Cycle   │  │Portfolio     │                       │
│  │Analyst       │  │Agent         │  │Context       │                       │
│  └──────────────┘  └──────────────┘  └──────────────┘                       │
│         │                 │                 │                                │
│         └─────────────────┼─────────────────┘                                │
│                           ▼                                                  │
│  DECISION PHASE    ┌──────────────┐  ┌──────────────┐                       │
│                    │Orchestrator  │─▶│QA/Risk       │                       │
│                    │(8Q rubric)   │  │(compliance)  │                       │
│                    └──────────────┘  └──────────────┘                       │
│                                             │                                │
│                                             ▼                                │
│                    ┌────────────────────────────────────────────┐           │
│                    │ Professional Investment Report (v5.0)      │           │
│                    │ - Actionability: ACTIONABLE | NOT ACTIONABLE│           │
│                    │ - One-Page Action Plan (always present)    │           │
│                    │ - Decision Packet with execution plans     │           │
│                    │ - Evidence Appendix with stable anchors    │           │
│                    └────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Separate Meta-Learning Process                      │
│  ┌──────────────────┐                                           │
│  │Post-Mortem       │───▶ Meta-Learning Report                  │
│  │Architect         │     (monthly/quarterly)                   │
│  └──────────────────┘                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Python 3.9+ (tested with Python 3.11+)
- OpenAI API key (for LLM access)
- Serper API key (for web search)
- Existing market data in DuckDB (from the data ingestion pipeline)

## Installation

1. **Install dependencies** (from project root):

```bash
pip install -r requirements.txt
```

2. **Set environment variables**:

```bash
# Required
export OPENAI_API_KEY="your-openai-api-key"
export SERPER_API_KEY="your-serper-api-key"

# Optional (defaults to market_data.duckdb)
export DUCKDB_PATH="path/to/your/database.duckdb"
```

## Quick Start

```bash
export OPENAI_API_KEY='your-key-here'
export SERPER_API_KEY='your-key-here'
```

### Run Investment Analysis (On-Demand)

```bash
# Basic run with default settings
python run_crew.py

# Specify custom database path
python run_crew.py --db-path my_data.duckdb

# Focus analysis on specific assets
python run_crew.py --focus ethereum solana

# Enable verbose logging
python run_crew.py --verbose
```

### Run Post-Mortem Analysis

```bash
# Analyze last month
python run_post_mortem.py

# Analyze last 3 months
python run_post_mortem.py --period-months 3

# Just show performance summary
python run_post_mortem.py --summary-only
```

### Programmatic Usage

```python
from agents.crew import run_investment_crew
from agents.post_mortem import run_meta_learning

# Run investment analysis
result = run_investment_crew(
    db_path="market_data.duckdb",
    focus_assets=["ethereum", "solana"],
    save_report=True
)

if result["success"]:
    print(f"Report saved to: {result['report_path']}")
    print(result["output"])

# Run meta-learning analysis
meta_result = run_meta_learning(
    db_path="market_data.duckdb",
    period_months=1
)
```

## JSON Schemas (Pydantic Validation)

All agent outputs are validated against strict Pydantic schemas. This ensures consistency and enables fail-fast behavior.

### Schema Files (`schemas/`)

| Schema | Purpose |
|--------|---------|
| `base.py` | Common enums (`DataQuality`, `Confidence`, `Action`, `Regime`), `SchemaMeta`, `Source` |
| `portfolio_context.py` | `PortfolioContextSchema` - positions, allocations, compliance checks |
| `macro_cycle.py` | `MacroCycleSchema` - regime assessment, cycle position, narratives |
| `technical_analysis.py` | `TechnicalAnalysisSchema` - breadth, per-asset technicals, correlations |
| `token_research.py` | `TokenResearchSchema` - candidates, adoption metrics, ranked shortlist |
| `recommendations.py` | `RecommendationsSchema` - recommendations with trading plans, 8Q rubric |
| `qa_review.py` | `QAReviewSchema` - compliance checklist, per-recommendation review |

### Validation Tasks

The `validation/task_validation.py` module validates agent outputs:

```python
STRICT_VALIDATION_TASKS = {
    "portfolio_context",
    "token_research", 
    "technical_analysis",
    "macro_analysis",
    "orchestration",
    "qa_risk",
}
```

### Enforcement Rules

Trading plans are automatically enforced:
- **BUY without complete trading plan** → Downgraded to WATCH
- **Missing entry/exit strategy** → Warning added to report
- **Position size violates limits** → Flagged for review

## Report Generation (v5.0)

The report generator produces professional, learning-oriented Investment Reviews.

### Report Structure

```
1. METADATA HEADER
   - Report: YYYY-MM-DD Investment Review
   - Actionability: ACTIONABLE | NOT ACTIONABLE
   - Input Quality Summary table (per-agent data quality)

2. ONE-PAGE ACTION PLAN (always present)
   - Market Stance (macro regime, technical env, breadth)
   - Portfolio Status (BTC %, Tier2+3 %, Compliance: PASS/FLAG/REJECT)
   - "Do This Now" (max 3 executable bullets)
   - "Do Not Do" (evidence-backed restrictions)
   - Next Review Triggers (measurable conditions)

3. DECISION PACKET
   - 2.1 Recommendations Summary Table
   - 2.2 Execution Plans (per recommendation)
         - Time Horizon (required)
         - Position Sizing (portfolio % AND/OR monthly budget %)
         - Entry Plan (required for BUY)
         - Exit Plan (take profit targets, stop loss, invalidation)
         - Evidence Links (anchors to appendix)
         - 8-Question Rubric (condensed)
   - 2.3 "Why We Are Doing Nothing Now" (when no actionable recs)
   - 2.4 QA Review Summary

4. EVIDENCE APPENDIX (with stable anchors)
   - 4.1 Macro Evidence (#macro-regime, #macro-liquidity, etc.)
   - 4.2 Technical Evidence (#tech-breadth, #tech-BTC, etc.)
   - 4.3 Fundamentals Evidence (#research-SOL, etc.)
   - 4.4 Portfolio Evidence (#portfolio-compliance, etc.)
```

### Fail-Fast Behavior

If critical inputs are missing or invalid (`data_quality == "invalid"`), the report:
- Is marked **NOT ACTIONABLE**
- Shows reason in metadata header
- Displays "STOP" in Action Plan
- Does not render executable trade instructions

### Key Functions (`agents/utils/report_generator.py`)

| Function | Purpose |
|----------|---------|
| `generate_investment_report()` | Main entry point (v5.0 structure) |
| `_check_input_validity()` | Fail-fast validation |
| `_render_metadata_header()` | Actionability status + quality table |
| `_render_action_plan()` | Always-present one-page summary |
| `_render_recommendations_table()` | Summary table |
| `_render_execution_plans()` | Detailed per-recommendation plans |
| `_render_do_nothing_justification()` | Evidence-backed "why not now" |
| `_render_macro_appendix()` | Macro evidence with anchors |
| `_render_technical_appendix()` | Technical evidence with anchors |
| `_render_research_appendix()` | Research evidence with anchors |
| `_render_portfolio_appendix()` | Portfolio evidence with anchors |

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes | OpenAI API key for LLM access |
| `SERPER_API_KEY` | Yes | Serper API key for web search |
| `DUCKDB_PATH` | No | Path to DuckDB database (default: `market_data.duckdb`) |
| `DEBUG_VALIDATION` | No | Set to `1` to write debug files when validation fails |

### Prompty Files

Agent behavior is configured via prompty files in `prompts/`:

```
prompts/
├── token_screener.prompty        # Multi-lens discovery
├── fundamentals_analyst.prompty  # Deep on-chain analysis
├── research_synthesizer.prompty  # Ranked shortlist generation
├── token_research.prompty        # Legacy (for backward compat)
├── technical_analyst.prompty     # Momentum and breadth
├── macro_cycle.prompty           # Market regime assessment
├── portfolio_context.prompty     # Deterministic JSON output
├── orchestrator.prompty          # Decision synthesis with 8Q rubric
├── qa_risk.prompty               # Compliance and risk review
└── post_mortem.prompty           # Meta-learning analysis
```

#### Prompty File Format

```yaml
---
name: Token Screener Agent
version: 1.0.0
description: Multi-lens discovery for crypto candidates
model:
  api: openai
  configuration:
    model: gpt-4o-mini
    temperature: 0.3  # Optional
---

system:
You are a Token Screener Agent specializing in...

user:
{{task_description}}
```

### Investment Framework Constraints

| Constraint | Value |
|------------|-------|
| Maximum portfolio drawdown | 40% |
| Maximum single asset allocation | 15-20% |
| BTC allocation target | 40-60% |
| Asset universe | Top 200 by market cap (with exceptions) |
| Tier 2+3 maximum | 35% |
| Monthly budget | $100 |

## Output

### Investment Reports

Generated in `reports/` directory:

```
reports/
├── 2026-01-18-investment-review.md    # Human-readable (v5.0 format)
└── 2026-01-18-investment-review.json  # Machine-readable with actionability
```

### Meta-Learning Reports

Generated in `reports/meta-learning/`:

```
reports/meta-learning/
├── 2026-01-18-meta-learning-report.md
└── 2026-01-18-meta-learning-report.json
```

## Database Schema

### `audit_investment_report`

Tracks prompt versions used for each investment report:

| Column | Type | Description |
|--------|------|-------------|
| `report_id` | VARCHAR | Unique report identifier |
| `report_path` | VARCHAR | Path to saved report |
| `created_at` | TIMESTAMP | When report was generated |
| `token_screener_prompt_version` | VARCHAR | Version used |
| `fundamentals_analyst_prompt_version` | VARCHAR | Version used |
| `research_synthesizer_prompt_version` | VARCHAR | Version used |
| `technical_analyst_prompt_version` | VARCHAR | Version used |
| `macro_cycle_prompt_version` | VARCHAR | Version used |
| `portfolio_context_prompt_version` | VARCHAR | Version used |
| `orchestrator_prompt_version` | VARCHAR | Version used |
| `qa_risk_prompt_version` | VARCHAR | Version used |

### `audit_meta_learning_report`

| Column | Type | Description |
|--------|------|-------------|
| `report_id` | VARCHAR | Unique report identifier |
| `report_path` | VARCHAR | Path to saved report |
| `created_at` | TIMESTAMP | When report was generated |
| `post_mortem_prompt_version` | VARCHAR | Version used |
| `analysis_period_start` | TIMESTAMP | Start of analysis period |
| `analysis_period_end` | TIMESTAMP | End of analysis period |
| `investment_reports_analyzed` | INTEGER | Count of reports reviewed |

## Agent Tools

### Portfolio Tools (`agents/tools/portfolio_tools.py`)
- `get_open_positions()` - Current holdings
- `get_position(asset_id)` - Single position details
- `get_trade_history(asset_id)` - Trade ledger
- `get_realized_pnl_summary()` - P&L statistics
- `get_portfolio_summary()` - High-level overview
- `get_portfolio_snapshot()` - Complete JSON with compliance checks (deterministic)

### Market Data Tools (`agents/tools/market_data_tools.py`)
- `get_price_history(asset_id, days)` - Historical prices
- `get_btc_relative_price(asset_id, days)` - BTC-relative performance
- `get_market_cap_rankings(limit)` - Top assets by market cap
- `get_price_change(asset_id, days)` - Price change statistics

### Technical Tools (`agents/tools/technical_tools.py`)
- `get_sma(asset_id, period)` - Simple Moving Average
- `get_rsi(asset_id, period)` - Relative Strength Index
- `get_price_correlation(asset_id_1, asset_id_2, days)` - Correlation coefficient
- `get_momentum_summary(asset_id)` - Comprehensive momentum analysis

### Web Search Tools (`agents/tools/serper_tools.py`)
- `search_web(query)` - General web search
- `search_crypto_news(asset_name)` - Recent news
- `search_market_metrics(asset_name)` - TVL, DAU, revenue data
- `search_macro_conditions()` - Fed policy, liquidity conditions
- `search_asset_fundamentals(asset_name)` - Project fundamentals

## Testing

Run the agent system tests:

```bash
# From project root
python -m pytest agents/tests/ -v

# Run specific test categories
python -m pytest agents/tests/test_report_generator.py -v  # Report generation (37 tests)
python -m pytest agents/tests/test_schemas.py -v           # Pydantic schemas
python -m pytest agents/tests/test_validation.py -v        # Task validation
python -m pytest agents/tests/test_enforcement_contract.py -v  # Trading plan enforcement
python -m pytest agents/tests/test_trading_plan_enforcement.py -v
python -m pytest agents/tests/test_report_from_json.py -v  # JSON report rendering
python -m pytest agents/tests/test_portfolio_snapshot.py -v
python -m pytest agents/tests/test_prompt_loader.py -v
python -m pytest agents/tests/test_tools.py -v
```

### Test Coverage

| Test File | Coverage |
|-----------|----------|
| `test_report_generator.py` | v5.0 report structure, action plan, execution plans, do-nothing justification |
| `test_report_from_json.py` | JSON rendering, section renderers, error reports |
| `test_schemas.py` | Pydantic schema validation, enforcement rules |
| `test_enforcement_contract.py` | Trading plan downgrade rules |
| `test_trading_plan_enforcement.py` | BUY without complete plan → WATCH |
| `test_validation.py` | Task output validation, retry logic |
| `test_portfolio_snapshot.py` | Deterministic portfolio JSON |
| `test_prompt_loader.py` | Prompty file parsing |
| `test_tools.py` | Tool imports and functionality |

## Troubleshooting

### "OPENAI_API_KEY environment variable not set"

```bash
export OPENAI_API_KEY="your-key-here"
```

### "SERPER_API_KEY environment variable not set"

```bash
export SERPER_API_KEY="your-key-here"
```

### "No price data found for asset"

Ensure market data has been ingested:
```bash
cd source
python ingestion.py
```

### "Report marked NOT ACTIONABLE"

Check the Actionability section in the report for specific reasons:
- Missing critical inputs (portfolio_context, orchestration, qa_risk)
- `data_quality == "invalid"` for critical inputs
- `contradictions_detected == true` in portfolio
- QA `overall_status == "reject"`

### Rate limit errors from OpenAI

The system makes multiple LLM calls. If you hit rate limits:
1. Wait and retry
2. Use lower-tier models for some agents
3. Upgrade your OpenAI plan

### Validation errors for orchestration or qa_risk

The validation system automatically normalizes common LLM output variations:

**Orchestration normalization:**
- Conviction: "High", "HIGH", "High (0.7)" → "high"
- Trading plan numeric fields: `stop_loss: 3000` → `stop_loss: "3000"`
- Take profit targets: `target: 4000` → `target: "4000"`
- Position size: `position_size: 5` → `position_size: "5%"`

**QA Risk normalization:**
- Compliance status: "PASS", "✓", "ok" → "pass"
- Compliance status: "FAIL", "✗" → "fail"
- Compliance status: "n/a", "NA" → "not_applicable"
- Conviction mapping: "high" → "strong", "medium" → "adequate", "low" → "weak"

If validation still fails, enable debug mode for detailed diagnostics:

```bash
DEBUG_VALIDATION=1 python run_crew.py
```

This writes debug files to `reports/debug/`:
- `{timestamp}_{task}_raw.txt` - Raw LLM output
- `{timestamp}_{task}_parsed.json` - After JSON parsing
- `{timestamp}_{task}_preprocessed.json` - After normalization
- `{timestamp}_{task}_errors.json` - Validation errors with context

### Tracing timeout warnings

The tracing timeout warning (`Error sending events to backend`) appears after validation failures and is not causal. It occurs when CrewAI's tracing backend is unreachable. This is cosmetic and does not affect the validation results. To disable tracing entirely, set `tracing=False` in the Crew constructor in `crew.py`.

## Project Structure

```
agents/
├── __init__.py                 # Package exports
├── README.md                   # This file
├── crew.py                     # Core 8-agent crew definition
├── post_mortem.py              # Post-Mortem Architect
├── tools/
│   ├── __init__.py
│   ├── portfolio_tools.py      # Portfolio data access + get_portfolio_snapshot
│   ├── market_data_tools.py    # Price/market data queries
│   ├── technical_tools.py      # SMA, RSI, correlations
│   └── serper_tools.py         # Web search
├── utils/
│   ├── __init__.py
│   ├── db_connection.py        # DuckDB connection management
│   ├── prompt_loader.py        # Prompty file parsing
│   ├── report_generator.py     # Investment report builder (v5.0)
│   └── meta_report_generator.py # Meta-learning report builder
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_enforcement_contract.py
    ├── test_portfolio_snapshot.py
    ├── test_prompt_loader.py
    ├── test_report_from_json.py
    ├── test_report_generator.py
    ├── test_schemas.py
    ├── test_tools.py
    ├── test_trading_plan_enforcement.py
    └── test_validation.py

schemas/
├── __init__.py
├── base.py                     # Common enums and base models
├── macro_cycle.py              # MacroCycleSchema
├── portfolio_context.py        # PortfolioContextSchema
├── qa_review.py                # QAReviewSchema
├── recommendations.py          # RecommendationsSchema
├── technical_analysis.py       # TechnicalAnalysisSchema
└── token_research.py           # TokenResearchSchema

validation/
├── __init__.py
└── task_validation.py          # Output validation and enforcement

prompts/
├── token_screener.prompty
├── fundamentals_analyst.prompty
├── research_synthesizer.prompty
├── token_research.prompty
├── technical_analyst.prompty
├── macro_cycle.prompty
├── portfolio_context.prompty
├── orchestrator.prompty
├── qa_risk.prompty
└── post_mortem.prompty

reports/
└── meta-learning/

run_crew.py                     # CLI for investment analysis
run_post_mortem.py              # CLI for meta-learning
schedule_crew.py                # Scheduled investment analysis
schedule_post_mortem.py         # Scheduled meta-learning
```

## Important Notes

1. **Human Approval Required**: The system generates recommendations but does not execute trades. All recommendations require human review and approval.

2. **Fail-Fast Design**: Reports are marked NOT ACTIONABLE if critical inputs are missing or invalid, preventing execution of unreliable recommendations.

3. **Evidence Traceability**: Every recommendation links to evidence in the appendix via stable anchors, supporting learning and accountability.

4. **Not Financial Advice**: This is a research and learning tool. Always do your own research before making investment decisions.

5. **API Costs**: Running the full crew makes multiple LLM and search API calls. Monitor your usage and costs.

6. **Data Freshness**: Ensure market data is up-to-date before running analysis. Stale data leads to stale recommendations.
