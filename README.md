# Crypto Market Breadth Project

An end-to-end cryptocurrency investment research platform combining a resilient data ingestion pipeline, a multi-agent AI investment analysis system, and portfolio management -- all backed by a local DuckDB database.

## Features

- **Data Ingestion Pipeline**: Restartable, idempotent ingestion of top 350 crypto assets from CoinGecko with ~2 years of historical hourly price, market cap, and volume data
- **Multi-Agent Investment System**: 8-agent CrewAI crew (Token Screener, Fundamentals Analyst, Research Synthesizer, Technical Analyst, Macro/Cycle, Portfolio Context, Orchestrator, QA/Risk) producing structured investment reports
- **Post-Mortem Architect**: Separate meta-learning agent that analyzes historical performance and suggests system improvements
- **Portfolio Management**: Trade recording, position tracking, and realized P&L calculation via `PortfolioStore`
- **Structured Validation**: Pydantic schemas for all agent outputs with soft/strict validation and trading plan enforcement
- **Report Generation (v5.0)**: Professional reports with actionability gating, one-page action plan, decision packet, and evidence appendix
- **Scheduling**: Cron-compatible scripts for automated weekly analysis and monthly meta-learning

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Pipeline                                │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────────┐     │
│  │ CoinGecko API│──▶│ Ingestion    │──▶│ DuckDB (market_data.duckdb)  │     │
│  │ (top 350)    │   │ Orchestrator │   │ - asset_metadata             │     │
│  └──────────────┘   └──────────────┘   │ - market_data (hourly OHLCV) │     │
│                                         │ - positions & trades         │     │
│                                         └──────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         Core Investment Crew                                  │
│                                                                               │
│  RESEARCH PHASE (3 agents)                                                    │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────────┐             │
│  │Token Screener│─▶│Fundamentals      │─▶│Research Synthesizer │             │
│  │(15-20 cands) │  │Analyst (deep)    │  │(ranked shortlist)   │             │
│  └──────────────┘  └──────────────────┘  └─────────────────────┘             │
│                                                   │                           │
│  ANALYSIS PHASE                                   ▼                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                        │
│  │Technical     │  │Macro/Cycle   │  │Portfolio     │                        │
│  │Analyst       │  │Agent         │  │Context       │                        │
│  └──────────────┘  └──────────────┘  └──────────────┘                        │
│         │                 │                 │                                  │
│         └─────────────────┼─────────────────┘                                 │
│                           ▼                                                   │
│  DECISION PHASE    ┌──────────────┐  ┌──────────────┐                        │
│                    │Orchestrator  │─▶│QA/Risk       │                        │
│                    │(8Q rubric)   │  │(compliance)  │                        │
│                    └──────────────┘  └──────────────┘                        │
│                                             │                                 │
│                                             ▼                                 │
│                    ┌────────────────────────────────────────────┐             │
│                    │ Professional Investment Report (v5.0)      │             │
│                    │ - Actionability: ACTIONABLE | NOT ACTIONABLE│            │
│                    │ - One-Page Action Plan (always present)    │             │
│                    │ - Decision Packet with execution plans     │             │
│                    │ - Evidence Appendix with stable anchors    │             │
│                    └────────────────────────────────────────────┘             │
└───────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────┐
│              Separate Meta-Learning Process                    │
│  ┌──────────────────┐                                         │
│  │Post-Mortem       │───▶ Meta-Learning Report                │
│  │Architect         │     (monthly/quarterly)                 │
│  └──────────────────┘                                         │
└───────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Python 3.9+ (tested with Python 3.11+)
- pip package manager

**For the data ingestion pipeline only:**
- CoinGecko API key (optional for free tier; required for pro tier with higher rate limits)

**For the multi-agent investment system:**
- OpenAI API key (required -- powers all LLM agents)
- Serper API key (required -- powers web search tools)

## Installation

1. **Clone the repository**

```bash
git clone <repository-url>
cd market_breadth_proj
```

2. **Create a virtual environment and install dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or use the provided helper script:

```bash
source activate.zsh
```

3. **Install test dependencies** (optional)

```bash
pip install -r requirements-test.txt
```

## Environment Variables

| Variable | Required For | Description |
|----------|-------------|-------------|
| `OPENAI_API_KEY` | Agent system | OpenAI API key for LLM access |
| `SERPER_API_KEY` | Agent system | Serper API key for web search tools |
| `COINGECKO_API_KEY` | Data ingestion (optional) | CoinGecko API key for higher rate limits |
| `DUCKDB_PATH` | Optional | Path to DuckDB database (default: `market_data.duckdb`) |
| `DEBUG_VALIDATION` | Optional | Set to `1` to write debug files when validation fails |

Set them in your shell profile (`~/.zshrc`, `~/.bashrc`) for persistence:

```bash
export OPENAI_API_KEY="your-openai-api-key"
export SERPER_API_KEY="your-serper-api-key"
export COINGECKO_API_KEY="your-coingecko-api-key"  # optional
```

## Quick Start

### Step 1: Ingest Market Data

Populate the DuckDB database with historical crypto market data:

```bash
cd source
python ingestion.py
```

This will:
1. Fetch the top 350 cryptocurrencies by market cap from CoinGecko
2. Backfill ~2 years (729 days) of historical hourly data for each asset
3. Store everything in a local DuckDB database (`market_data.duckdb`)

The pipeline is restartable -- if interrupted by rate limits or errors, simply re-run and it resumes from where it left off.

### Step 2: Record Portfolio Trades (optional)

If you want the agent system to incorporate your actual portfolio holdings, record trades using `PortfolioStore`:

```python
from source.portfolio_store import PortfolioStore
from datetime import datetime

store = PortfolioStore("market_data.duckdb")

store.record_buy_trade(
    asset_id="bitcoin",
    symbol="btc",
    quantity=0.01,
    price_usd=95000.0,
    executed_at=datetime(2026, 1, 15, 10, 30, 0),
    fees_usd=1.50,
)

positions = store.get_open_positions()
for pos in positions:
    print(f"{pos['symbol'].upper()}: {pos['quantity']} @ ${pos['avg_cost_basis_usd']:.2f}")

store.close()
```

A helper script (`source/add_sol_trade.py`) is included as a template for recording trades from the command line.

### Step 3: Run Investment Analysis

```bash
export OPENAI_API_KEY="your-key"
export SERPER_API_KEY="your-key"

# Basic run
python run_crew.py

# Use a custom database path
python run_crew.py --db-path my_data.duckdb

# Focus analysis on specific assets
python run_crew.py --focus ethereum solana

# Enable verbose logging
python run_crew.py --verbose

# Run without saving the report to disk
python run_crew.py --no-save
```

Reports are saved to `reports/` as both Markdown (`.md`) and JSON (`.json`).

### Step 4: Run Post-Mortem Analysis (optional)

```bash
# Analyze last month of performance
python run_post_mortem.py

# Analyze last 3 months
python run_post_mortem.py --period-months 3

# Just show a quick performance summary
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
    save_report=True,
)

if result["success"]:
    print(f"Report saved to: {result['report_path']}")

# Run meta-learning analysis
meta_result = run_meta_learning(
    db_path="market_data.duckdb",
    period_months=1,
)
```

## Scheduling

Both processes support automated execution via cron or any task scheduler.

**Investment Crew** -- run weekly (e.g., every Sunday at 9am):

```cron
0 9 * * 0 /path/to/python /path/to/schedule_crew.py
```

**Post-Mortem Architect** -- run monthly (e.g., 1st of each month at 10am):

```cron
0 10 1 * * /path/to/python /path/to/schedule_post_mortem.py
```

Both scripts log to a `logs/` directory and check for required environment variables before executing.

## Multi-Agent System

### Core Agents

| Agent | Role | Model | Key Tools |
|-------|------|-------|-----------|
| Token Screener | Multi-lens discovery to cast a wide net | gpt-4o-mini | market_cap_rankings, search_web |
| Fundamentals Analyst | Deep on-chain metrics and qualitative analysis | gpt-4o-mini | search_asset_fundamentals, search_market_metrics |
| Research Synthesizer | Combines research into ranked shortlist | gpt-4o-mini | search_crypto_news |
| Technical Analyst | Momentum using SMAs, RSI, breadth, BTC-relative | gpt-4o-mini | get_sma, get_rsi, get_momentum_summary |
| Macro/Cycle | Market regime assessment (risk-on/risk-off/neutral) | gpt-4o-mini | search_macro_conditions |
| Portfolio Context | Current holdings and compliance (deterministic JSON) | gpt-3.5-turbo | get_portfolio_snapshot |
| Orchestrator | Synthesizes all inputs with 8-question rubric | gpt-4o | get_portfolio_summary |
| QA/Risk | Validates recommendations against framework | gpt-4o | get_price_correlation |

### Post-Mortem Architect

| Agent | Role | Model |
|-------|------|-------|
| Post-Mortem | Analyzes historical performance and identifies improvements | gpt-4o |

Runs separately to analyze historical performance, detect logic drift, and suggest prompt refinements.

### Task Execution Order (9 sequential tasks)

1. **Token Screening** -- Multi-lens discovery (market cap, momentum, narrative, sector leadership)
2. **Fundamentals Analysis** -- Deep on-chain metrics, qualitative assessment, risk evaluation
3. **News & Sentiment** -- Recent news, catalysts, market narratives
4. **Token Research Synthesis** -- Score, rank, and select top 5-8 candidates
5. **Technical Analysis** -- SMA, RSI, BTC-relative performance, market breadth
6. **Macro Analysis** -- Fed policy, liquidity conditions, market cycle position
7. **Portfolio Context** -- Deterministic JSON snapshot of current holdings and compliance
8. **Orchestration** -- Synthesize all inputs, apply 8-question rubric, generate recommendations
9. **QA/Risk Review** -- Validate compliance, assess risk, issue PASS/FLAG/REJECT verdicts

### Validation and Enforcement

All agent outputs for critical tasks are validated against Pydantic schemas (see `schemas/`):

- **Soft validation**: JSON is parseable and usable, even if some schema fields are imperfect
- **Strict validation**: Full schema conformance
- **Trading plan enforcement**: BUY recommendations without a complete trading plan are automatically downgraded to WATCH

Validated tasks: `portfolio_context`, `token_research`, `technical_analysis`, `macro_analysis`, `orchestration`, `qa_risk`.

### Report Structure (v5.0)

```
1. METADATA HEADER
   - Actionability: ACTIONABLE | NOT ACTIONABLE
   - Input Quality Summary (per-agent data quality)

2. ONE-PAGE ACTION PLAN (always present)
   - Market Stance, Portfolio Status
   - "Do This Now" (max 3 bullets)
   - "Do Not Do" (evidence-backed)
   - Next Review Triggers

3. DECISION PACKET
   - Recommendations Summary Table
   - Execution Plans (entry/exit, position sizing, time horizon)
   - QA Review Summary

4. EVIDENCE APPENDIX (with stable anchors)
   - Macro, Technical, Fundamentals, Portfolio evidence sections
```

If critical inputs are missing or invalid, the report is marked **NOT ACTIONABLE** and does not render executable trade instructions.

### Agent Tools

**Portfolio Tools** (`agents/tools/portfolio_tools.py`):
`get_open_positions`, `get_position`, `get_trade_history`, `get_realized_pnl_summary`, `get_portfolio_summary`, `get_portfolio_snapshot`

**Market Data Tools** (`agents/tools/market_data_tools.py`):
`get_price_history`, `get_btc_relative_price`, `get_market_cap_rankings`, `get_price_change`, `lookup_asset_id`

**Technical Tools** (`agents/tools/technical_tools.py`):
`get_sma`, `get_rsi`, `get_price_correlation`, `get_momentum_summary`

**Web Search Tools** (`agents/tools/serper_tools.py`):
`search_web`, `search_crypto_news`, `search_market_metrics`, `search_macro_conditions`, `search_asset_fundamentals`

### Prompty Configuration

Agent behavior is configured via prompty files in `prompts/`. Each file specifies the model, optional temperature, system prompt, and user prompt template:

```yaml
---
name: Token Screener Agent
version: 1.0.0
description: Multi-lens discovery for crypto candidates
model:
  api: openai
  configuration:
    model: gpt-4o-mini
    temperature: 0.3  # Optional -- omit to use model default
---

system:
You are a Token Screener Agent specializing in...

user:
{{task_description}}
```

## Data Ingestion Pipeline

### How It Works

The `IngestionOrchestrator` (in `source/ingestion.py`) manages the full lifecycle:

1. **Initial run**: Fetches top 350 assets by market cap (2 pages of 250 from CoinGecko), initializes ingestion state for each
2. **Market chart ingestion**: Backfills ~2 years of hourly data per asset, storing price, market cap, and volume
3. **Incremental runs**: Refreshes the top 350 list, adds new assets, marks dropped assets, and fetches only new data since the last collection

### Rate Limiting

CoinGecko's free tier has rate limits (~30 calls/minute). The pipeline handles this automatically with exponential backoff (60s, 120s, 240s...) and persists state before sleeping so progress is never lost.

For faster ingestion, set `COINGECKO_API_KEY` for a paid tier or use the pro base URL:

```python
secrets = {
    "base_url": "https://pro-api.coingecko.com/api/v3/",
    ...
}
```

### Resuming After Interruption

Simply re-run `python ingestion.py`. The system will skip assets that are already up-to-date, resume from the last collected timestamp for each asset, and continue with any unprocessed assets.

### Querying the Data

```python
import duckdb

conn = duckdb.connect("market_data.duckdb")

df = conn.execute("""
    SELECT
        datetime(timestamp_unix, 'unixepoch') as timestamp,
        price_usd,
        market_cap_usd,
        volume_usd
    FROM market_data
    WHERE asset_id = 'bitcoin'
    ORDER BY timestamp_unix DESC
    LIMIT 100
""").fetchdf()

print(df)
conn.close()
```

## Project Structure

```
market_breadth_proj/
├── README.md                          # This file
├── requirements.txt                   # Main dependencies
├── requirements-test.txt              # Test dependencies (pytest)
├── activate.zsh                       # Quick venv setup script
│
├── source/                            # Data ingestion & portfolio management
│   ├── __init__.py
│   ├── coingecko_api.py               # CoinGecko API client
│   ├── duckdb_store.py                # DuckDB persistence layer
│   ├── ingestion.py                   # Ingestion orchestrator
│   ├── portfolio_store.py             # Portfolio position & trade tracking
│   ├── parameters.json                # Default API parameters
│   ├── add_sol_trade.py               # Example trade recording script
│   ├── test_coingecko_api.py
│   ├── test_duckdb_store.py
│   └── test_ingestion.py
│
├── agents/                            # Multi-agent investment system
│   ├── __init__.py
│   ├── README.md                      # Detailed agent system documentation
│   ├── crew.py                        # Core 8-agent crew definition
│   ├── post_mortem.py                 # Post-Mortem Architect
│   ├── tools/
│   │   ├── portfolio_tools.py         # Portfolio data access + snapshot
│   │   ├── market_data_tools.py       # Price/market data queries
│   │   ├── technical_tools.py         # SMA, RSI, correlations
│   │   ├── serper_tools.py            # Web search via Serper API
│   │   └── market_breadth.py          # Market breadth calculations
│   ├── utils/
│   │   ├── db_connection.py           # Shared DuckDB connection manager
│   │   ├── prompt_loader.py           # Prompty file parsing
│   │   ├── report_generator.py        # Investment report builder (v5.0)
│   │   └── meta_report_generator.py   # Meta-learning report builder
│   └── tests/
│       ├── conftest.py
│       ├── test_enforcement_contract.py
│       ├── test_market_breadth.py
│       ├── test_portfolio_snapshot.py
│       ├── test_prompt_loader.py
│       ├── test_report_from_json.py
│       ├── test_report_generator.py
│       ├── test_schemas.py
│       ├── test_tools.py
│       ├── test_trading_plan_enforcement.py
│       └── test_validation.py
│
├── schemas/                           # Pydantic schemas for agent outputs
│   ├── __init__.py
│   ├── base.py                        # Common enums and base models
│   ├── macro_cycle.py                 # MacroCycleSchema
│   ├── portfolio_context.py           # PortfolioContextSchema
│   ├── qa_review.py                   # QAReviewSchema
│   ├── recommendations.py             # RecommendationsSchema
│   ├── technical_analysis.py          # TechnicalAnalysisSchema
│   └── token_research.py              # TokenResearchSchema
│
├── validation/                        # Output validation & enforcement
│   ├── __init__.py
│   └── task_validation.py             # Task output validation, enforcement rules
│
├── prompts/                           # Prompty files for agent configuration
│   ├── token_screener.prompty
│   ├── fundamentals_analyst.prompty
│   ├── research_synthesizer.prompty
│   ├── token_research.prompty         # Legacy (backward compat)
│   ├── technical_analyst.prompty
│   ├── macro_cycle.prompty
│   ├── portfolio_context.prompty
│   ├── orchestrator.prompty
│   ├── qa_risk.prompty
│   └── post_mortem.prompty
│
├── reports/                           # Generated reports (git-tracked samples)
│   └── debug/                         # Debug output (when DEBUG_VALIDATION=1)
│
├── run_crew.py                        # CLI: run investment analysis
├── run_post_mortem.py                 # CLI: run meta-learning analysis
├── schedule_crew.py                   # Cron: scheduled investment analysis
├── schedule_post_mortem.py            # Cron: scheduled meta-learning
│
├── INVESTMENT_FRAMEWORK.md            # Personal investment framework rules
└── _devresources/                     # Development notebooks and sample data
```

## Database Schema

The DuckDB database (`market_data.duckdb`) contains tables for both the data pipeline and the agent system.

### Data Ingestion Tables

| Table | Purpose |
|-------|---------|
| `ingestion_state` | Tracks global ingestion process state (singleton row) |
| `asset_metadata` | Asset information (id, symbol, name, market cap rank) |
| `asset_ingestion_state` | Per-asset ingestion progress (last collected timestamp, backfill status) |
| `market_data` | Hourly price, market cap, and volume data (PK: asset_id + timestamp_unix) |

### Portfolio Tables

| Table | Purpose |
|-------|---------|
| `positions` | Current holdings (asset_id, quantity, avg cost basis) |
| `trades` | Immutable trade ledger (buy/sell records with realized P&L) |

### Agent System Tables

| Table | Purpose |
|-------|---------|
| `audit_investment_report` | Tracks prompt versions used for each investment report |
| `audit_meta_learning_report` | Tracks prompt versions and analysis period for meta-learning reports |
| `technical_indicators_cache` | Cached technical indicator values for agent analysis |

## Running Tests

### Data Ingestion Tests

```bash
cd source
python -m pytest test_duckdb_store.py test_coingecko_api.py test_ingestion.py -v
```

### Agent System Tests

```bash
# From project root
python -m pytest agents/tests/ -v

# Run specific test categories
python -m pytest agents/tests/test_report_generator.py -v      # Report generation
python -m pytest agents/tests/test_schemas.py -v                # Pydantic schemas
python -m pytest agents/tests/test_validation.py -v             # Task validation
python -m pytest agents/tests/test_enforcement_contract.py -v   # Trading plan enforcement
python -m pytest agents/tests/test_trading_plan_enforcement.py -v
python -m pytest agents/tests/test_report_from_json.py -v       # JSON report rendering
python -m pytest agents/tests/test_portfolio_snapshot.py -v     # Portfolio snapshot
python -m pytest agents/tests/test_prompt_loader.py -v          # Prompty file parsing
python -m pytest agents/tests/test_tools.py -v                  # Tool imports
python -m pytest agents/tests/test_market_breadth.py -v         # Market breadth
```

### All Tests

```bash
python -m pytest source/test_*.py agents/tests/ -v
```

## Troubleshooting

### "No module named 'dacite'" or similar import errors

Ensure all dependencies are installed:

```bash
pip install -r requirements.txt
```

### "OPENAI_API_KEY environment variable not set"

```bash
export OPENAI_API_KEY="your-key-here"
```

### "SERPER_API_KEY environment variable not set"

```bash
export SERPER_API_KEY="your-key-here"
```

### CoinGecko rate limit errors persist

The free CoinGecko API is heavily rate-limited. Options:
1. Wait and retry (the pipeline does this automatically with exponential backoff)
2. Set `COINGECKO_API_KEY` for a paid tier
3. Reduce the number of tracked assets

### Database locked error

Ensure no other process is accessing the DuckDB file. Only one connection can write at a time.

### "No price data found for asset"

Ensure market data has been ingested before running the agent system:

```bash
cd source
python ingestion.py
```

### Report marked NOT ACTIONABLE

Check the Actionability section in the report header for specific reasons:
- Missing critical inputs (portfolio_context, orchestration, qa_risk)
- `data_quality == "invalid"` for critical inputs
- `contradictions_detected == true` in portfolio
- QA `overall_status == "reject"`

### Validation errors for orchestration or qa_risk

The validation system automatically normalizes common LLM output variations (e.g., `"High"` to `"high"`, `stop_loss: 3000` to `stop_loss: "3000"`). If validation still fails, enable debug mode:

```bash
DEBUG_VALIDATION=1 python run_crew.py
```

This writes debug files to `reports/debug/` showing raw output, parsed JSON, preprocessed JSON, and error details at each validation stage.

### Tests fail with import errors

Run data ingestion tests from the `source/` directory:

```bash
cd source
python -m pytest test_*.py -v
```

Run agent tests from the project root:

```bash
python -m pytest agents/tests/ -v
```

## Important Notes

1. **Human Approval Required**: The system generates recommendations but does not execute trades. All recommendations require human review and approval.

2. **Fail-Fast Design**: Reports are marked NOT ACTIONABLE if critical inputs are missing or invalid, preventing execution of unreliable recommendations.

3. **Evidence Traceability**: Every recommendation links to evidence in the appendix via stable anchors, supporting learning and accountability.

4. **Not Financial Advice**: This is a research and learning tool. Always do your own research before making investment decisions.

5. **API Costs**: Running the full crew makes multiple LLM and search API calls. Monitor your OpenAI and Serper usage and costs.

6. **Data Freshness**: Ensure market data is up-to-date before running analysis. Run `python source/ingestion.py` to refresh.

## License

MIT License - see LICENSE file for details.
