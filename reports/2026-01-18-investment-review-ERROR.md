# INVESTMENT REVIEW - RUN FAILED

**Generated:** 2026-01-18 17:46:41
**Report ID:** a750d3ce-4a41-44bd-b3a6-0a854101f3c4

---

## Validation Failures

The following tasks failed JSON schema validation. No recommendations were generated.

### Task: token_research

**Errors:**
- Validation error at 'candidates.0.sources.0.type': Input should be 'url', 'paper', 'dashboard', 'dataset', 'article', 'blog', 'docs' or 'api'
- Validation error at 'candidates.3.sources.0.type': Input should be 'url', 'paper', 'dashboard', 'dataset', 'article', 'blog', 'docs' or 'api'

### Task: macro_analysis

**Errors:**
- JSON parse error: Expecting ',' delimiter: line 1 column 2628 (char 2627)
- Output starts with: {"meta":{"agent_name":"macro_cycle","schema_version":"1.0","as_of_timestamp_utc":"2026-01-18T17:00:0...

### Task: orchestration

**Errors:**
- Validation error at 'recommendations.0.conviction': Input should be 'high', 'medium' or 'low'
- Validation error at 'recommendations.0.trading_plan.take_profit_targets.0.target': Input should be a valid string
- Validation error at 'recommendations.0.trading_plan.take_profit_targets.1.target': Input should be a valid string
- Validation error at 'recommendations.0.trading_plan.stop_loss': Input should be a valid string
- Validation error at 'recommendations.1.conviction': Input should be 'high', 'medium' or 'low'
- Validation error at 'recommendations.1.trading_plan.take_profit_targets.0.target': Input should be a valid string
- Validation error at 'recommendations.1.trading_plan.take_profit_targets.1.target': Input should be a valid string
- Validation error at 'recommendations.1.trading_plan.stop_loss': Input should be a valid string

### Task: qa_risk

**Errors:**
- Validation error at 'compliance_checklist.6.status': Input should be 'pass', 'fail', 'unknown' or 'not_applicable'

---

## Required Actions

1. Review the validation errors above
2. Check agent prompts for JSON output compliance
3. Ensure all required fields are present in agent outputs
4. Re-run the crew after fixing the issues

---

## Debug Information

### token_screening (first 500 chars)

```
```
{
  "meta": {
    "agent_name": "token_screener",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-17T17:00:00Z",
    "data_quality": "ok",
    "warnings": []
  },
  "screening_summary": {
    "total_candidates": 18,
    "standard_candidates": 15,
    "exception_candidates": 3
  },
  "standard_candidates": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "mcap_rank": 1,
      "category": "L1",
      "screening_reason": "Top cryptocurrency by market cap and volume
... (2450 more characters)
```

### fundamentals_analysis (first 500 chars)

```
{
  "meta": {
    "agent_name": "fundamentals_analyst",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-18T17:00:00Z",
    "data_quality": "partial",
    "warnings": ["Some metrics from third-party sources might vary. Verify with primary sources such as official dashboards, token analytics platforms, and project reports."]
  },
  "analysis_summary": {
    "candidates_analyzed": 4,
    "high_quality_count": 3,
    "flagged_concerns": 1
  },
  "candidates": [
    {
      "symbol":
... (5641 more characters)
```

### news_sentiment (first 500 chars)

```
{
  "meta": {
    "agent_name": "token_research",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-18T17:00:00Z",
    "data_quality": "ok",
    "warnings": []
  },
  "news_summary": {
    "BTC": {
      "recent_developments": [
        "Analysts predict Bitcoin could reach $200,000 in 2026.",
        "U.S. states like Texas are considering adding Bitcoin to strategic reserves.",
        "Market cycles and increased liquidity expected to drive Bitcoin's 2026 phase."
      ],
     
... (2389 more characters)
```

### token_research (first 500 chars)

```
{"meta": {"agent_name": "token_research", "schema_version": "1.0", "as_of_timestamp_utc": "2026-01-18T17:00:00Z", "data_quality": "ok", "warnings": []}, "universe": {"constraints": {"max_mcap_rank": 200, "exclude_memecoins": true}}, "candidates": [{"symbol": "BTC", "name": "Bitcoin", "mcap_rank": 1, "category": "Store of Value", "thesis": {"problem": "Digital store of value for inflation hedge and financial sovereignty.", "why_it_wins": "Widespread recognition and adoption as digital gold.", "ne
... (3433 more characters)
```

### technical_analysis (first 500 chars)

```
```
{
  "meta": {
    "agent_name": "technical_analysis",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-18T17:00:00Z",
    "data_quality": "ok",
    "warnings": []
  },
  "assets": [
    {
      "symbol": "BTC",
      "timeframes": {
        "d1": {
          "sma_50": 34000.00,
          "sma_200": 30000.00,
          "rsi_14": 65.0,
          "pct_change_7d": 2.5,
          "pct_change_30d": 10.0
        },
        "w1": {
          "sma_20": 33500.00,
          "rsi_14": 64
... (2541 more characters)
```

### macro_analysis (first 500 chars)

```
{"meta":{"agent_name":"macro_cycle","schema_version":"1.0","as_of_timestamp_utc":"2026-01-18T17:00:00Z","data_quality":"ok","warnings":[]},"regime":{"stance":"risk_on","confidence":"high"},"macro":{"liquidity":{"summary":"Global liquidity is expanding as central banks ease monetary policy, providing favorable conditions for risk assets.","signals":["Central banks announcing QE programs","Rise in money supply growth rates"]},"fed_policy":{"summary":"The Federal Reserve is pursuing a dovish stance
... (2127 more characters)
```

### portfolio_context (first 500 chars)

```
{
  "meta": {
    "agent_name": "portfolio_context",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-18T23:45:59.742485+00:00",
    "pricing_source": "market_data_table",
    "data_quality": "ok",
    "missing_fields": [],
    "warnings": []
  },
  "portfolio_totals": {
    "total_cost_basis_usd": 1241.77,
    "total_current_value_usd": 4484.94,
    "total_realized_pnl_usd": -12.5,
    "drawdown_from_peak_pct": null
  },
  "positions": [
    {
      "symbol": "btc",
      "asset
... (1116 more characters)
```

### orchestration (first 500 chars)

```
```json
{
  "meta": {
    "agent_name": "orchestrator",
    "schema_version": "4.0",
    "as_of_timestamp_utc": "2026-01-18T17:00:00Z",
    "data_quality": "ok",
    "warnings": []
  },
  "executive_summary": "Market conditions suggest expanding the portfolio beyond 100% BTC, capitalizing on the risk-on regime and favorable altcoin performance.",
  "market_context": {
    "macro_regime": "risk_on",
    "technical_env": "bullish",
    "key_considerations": ["Risk-on regime supports diversificatio
... (3352 more characters)
```

### qa_risk (first 500 chars)

```
```json
{
  "meta": {
    "agent_name": "qa_risk",
    "schema_version": "4.0",
    "as_of_timestamp_utc": "2026-01-18T23:45:59.742485+00:00",
    "data_quality": "ok",
    "warnings": []
  },
  "overall_status": "reject",
  "recommendations_reviewed": 2,
  "issues_found": 3,
  "compliance_checklist": [
    {
      "check": "data_quality_ok",
      "status": "pass",
      "notes": "Portfolio data quality is ok"
    },
    {
      "check": "assets_top_200",
      "status": "pass",
      "notes": 
... (2241 more characters)
```

---

*This is an error report. No investment recommendations were generated.*