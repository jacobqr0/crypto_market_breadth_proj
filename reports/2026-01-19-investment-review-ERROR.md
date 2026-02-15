# INVESTMENT REVIEW - RUN FAILED

**Generated:** 2026-01-19 14:56:30
**Report ID:** 942165f1-26f5-43ac-86cd-89af8abedb18

---

## Validation Failures

The following tasks failed JSON schema validation. No recommendations were generated.

### Task: token_research

**Errors:**
- Validation error at 'universe': Field required
- Validation error at 'candidates': Field required
- Validation error at 'ranked_shortlist': Field required

### Task: qa_risk

**Errors:**
- Validation error at 'per_recommendation.0.risk.correlation_with_portfolio': Input should be 'high', 'medium', 'low' or 'unknown'

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
{
  "meta": {
    "agent_name": "token_screener",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-19T12:00:00Z",
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
      "screening_reason": "Top asset by market cap with significant price 
... (5138 more characters)
```

### fundamentals_analysis (first 500 chars)

```
I will continue gathering the necessary data for the remaining candidates. After that, I will consolidate everything into the final detailed analysis report as specified. Next, I will proceed to gather information on the remaining tokens like TRON (TRX), Lido Staked Ether (STETH), Dogecoin (DOGE), and so on. This process will be repeated for each candidate to ensure a comprehensive analysis is performed for all identified tokens. 

Meanwhile, I'll ensure all necessary metrics and fundamental asp
... (189 more characters)
```

### news_sentiment (first 500 chars)

```
```json
{
  "meta": {
    "agent_name": "token_research",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2023-11-01T12:00:00Z",
    "data_quality": "invalid",
    "warnings": ["schema_violation"]
  }
}
```
```

### token_research (first 500 chars)

```
```json
{
  "meta": {
    "agent_name": "token_research",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2023-11-01T12:00:00Z",
    "data_quality": "invalid",
    "warnings": ["schema_violation"]
  }
}
```
```

### technical_analysis (first 500 chars)

```
{
  "meta": {
    "agent_name": "technical_analysis",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2023-11-01T12:00:00Z",
    "data_quality": "invalid",
    "warnings": ["schema_violation"]
  },
  "assets": [],
  "breadth": {
    "universe": "custom",
    "pct_above_200d": null,
    "pct_golden_cross": null,
    "median_rsi_14": null,
    "correlation": []
  }
}
```

### macro_analysis (first 500 chars)

```
{"meta":{"agent_name":"macro_cycle","schema_version":"1.0","as_of_timestamp_utc":"2026-01-19T12:00:00Z","data_quality":"ok","warnings":[]},"regime":{"stance":"risk_on","confidence":"high"},"macro":{"liquidity":{"summary":"Global liquidity is expanding as central banks implement supportive measures, leading to increased capital flows into risk assets.","signals":["Fed balance sheet growing","M2 growth positive"]},"fed_policy":{"summary":"Fed has adopted a dovish stance with indications of potenti
... (1555 more characters)
```

### portfolio_context (first 500 chars)

```
{
  "meta": {
    "agent_name": "portfolio_context",
    "schema_version": "1.0",
    "as_of_timestamp_utc": "2026-01-19T20:56:02.830047+00:00",
    "pricing_source": "market_data_table",
    "data_quality": "ok",
    "missing_fields": [],
    "warnings": []
  },
  "portfolio_totals": {
    "total_cost_basis_usd": 1241.77,
    "total_current_value_usd": 4372.2,
    "total_realized_pnl_usd": -12.5,
    "drawdown_from_peak_pct": null
  },
  "positions": [
    {
      "symbol": "btc",
      "asset_
... (1114 more characters)
```

### orchestration (first 500 chars)

```
```json
{
  "meta": {
    "agent_name": "orchestrator",
    "schema_version": "4.0",
    "as_of_timestamp_utc": "2026-01-19T20:56:02.830047+00:00",
    "data_quality": "invalid",
    "warnings": ["token_research and technical_analysis modules reported schema violation"]
  },
  "executive_summary": "Due to invalid data quality from key inputs, no new buy recommendations are made. Current holdings should be maintained while data issues are investigated.",
  "market_context": {
    "macro_regime": 
... (1591 more characters)
```

### qa_risk (first 500 chars)

```
```json
{
    "meta": {
        "agent_name": "qa_risk",
        "schema_version": "4.0",
        "as_of_timestamp_utc": "2026-01-19T20:56:02.830047+00:00",
        "data_quality": "invalid",
        "warnings": ["Data quality is invalid due to schema violations in token research and technical analysis"]
    },
    "overall_status": "reject",
    "recommendations_reviewed": 1,
    "issues_found": 1,
    "compliance_checklist": [
        {
            "check": "data_quality_ok",
            "stat
... (2143 more characters)
```

---

*This is an error report. No investment recommendations were generated.*