# Crypto Market Breadth Project

A resilient, restartable data ingestion pipeline for cryptocurrency market data using the CoinGecko API. The system collects hourly market chart data for the top 300 assets by market cap, with support for historical backfill and incremental updates.

## Features

- **Restartable Ingestion**: Resume from where you left off after failures or rate limits
- **Idempotent Operations**: Safe to re-run without creating duplicate data
- **DuckDB Persistence**: Local-first storage with transactional guarantees
- **Rate Limit Handling**: Automatic exponential backoff on API throttling
- **Historical Backfill**: Fetch ~1 year of historical data per asset
- **Incremental Updates**: Continue forward with hourly data after backfill

## Prerequisites

- Python 3.9+ (tested with Python 3.11)
- pip package manager

## Installation

1. **Clone the repository**

```bash
git clone <repository-url>
cd market_breadth_proj
```

2. **Create a virtual environment** (recommended)

```bash
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Install test dependencies** (optional, for running tests)

```bash
pip install -r requirements-test.txt
```

## Quick Start

### Run a Full Ingestion Cycle

The simplest way to start collecting data:

```bash
cd source
python ingestion.py
```

This will:
1. Fetch the top 300 cryptocurrencies by market cap
2. Backfill ~1 year of historical hourly data for each asset
3. Store everything in a local DuckDB database (`market_data.duckdb`)

### Programmatic Usage

```python
from source.ingestion import run_ingestion, IngestionOrchestrator

# Simple one-liner
result = run_ingestion(db_path="my_crypto_data.duckdb")
print(f"Collected {result['total_data_points']} data points for {result['total_assets']} assets")

# Or with more control
orchestrator = IngestionOrchestrator(db_path="my_crypto_data.duckdb")
try:
    result = orchestrator.run()
    print(orchestrator.get_status())
finally:
    orchestrator.close()
```

### Check Ingestion Status

```python
from source.duckdb_store import DuckDBStore

store = DuckDBStore("market_data.duckdb")
summary = store.get_ingestion_summary()

print(f"Total assets tracked: {summary['total_assets']}")
print(f"Assets with data: {summary['assets_with_data']}")
print(f"Assets pending: {summary['assets_pending']}")
print(f"Total data points: {summary['total_data_points']}")
print(f"Current status: {summary['run_status']}")

store.close()
```

### Query the Data

```python
import duckdb

conn = duckdb.connect("market_data.duckdb")

# Get Bitcoin's price history
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

## Configuration

### Custom API Parameters

You can customize the ingestion behavior by providing a secrets configuration:

```python
import time

secrets = {
    "base_url": "https://api.coingecko.com/api/v3/",
    "parameters": {
        "coinmarkets": {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "300"  # Number of top assets to track
        },
        "marketchart": {
            "vs_currency": "usd",
            "initial_query_from": int(time.time()) - (365 * 24 * 3600),  # 1 year ago
            "query_to": int(time.time())
        }
    }
}

result = run_ingestion(db_path="market_data.duckdb", secrets=secrets)
```

### Using a CoinGecko API Key (Paid Tiers)

If you have a CoinGecko Demo or Basic subscription, set your API key as an environment variable:

**On macOS/Linux:**
```bash
export COINGECKO_API_KEY="your-api-key-here"
python source/ingestion.py
```

**On Windows (Command Prompt):**
```cmd
set COINGECKO_API_KEY=your-api-key-here
python source/ingestion.py
```

**On Windows (PowerShell):**
```powershell
$env:COINGECKO_API_KEY="your-api-key-here"
python source/ingestion.py
```

**Permanent Setup (recommended):**

Add to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.):
```bash
export COINGECKO_API_KEY="your-api-key-here"
```

Or create a `.env` file in the project root and use a library like `python-dotenv`:
```
COINGECKO_API_KEY=your-api-key-here
```

The API key will be automatically included in request headers when the environment variable is set. Without it, the free tier rate limits apply.

**For CoinGecko Pro tier**, you'll also need to update the base URL:
```python
secrets = {
    "base_url": "https://pro-api.coingecko.com/api/v3/",
    # ... rest of config
}
```
And change the header in `coingecko_api.py` from `x-cg-demo-api-key` to `x-cg-pro-api-key`.

## Running Tests

Run the full test suite:

```bash
cd source
python -m pytest test_duckdb_store.py test_coingecko_api.py test_ingestion.py -v
```

Run specific test files:

```bash
# DuckDB store tests (schema, CRUD, idempotency)
python -m pytest test_duckdb_store.py -v

# API client tests
python -m pytest test_coingecko_api.py -v

# Integration tests
python -m pytest test_ingestion.py -v
```

## Project Structure

```
market_breadth_proj/
├── README.md
├── requirements.txt          # Main dependencies
├── requirements-test.txt     # Test dependencies
├── source/
│   ├── __init__.py          # Package exports
│   ├── coingecko_api.py     # CoinGecko API client
│   ├── duckdb_store.py      # DuckDB persistence layer
│   ├── ingestion.py         # Orchestration logic
│   ├── parameters.json      # Default API parameters
│   ├── test_coingecko_api.py
│   ├── test_duckdb_store.py
│   └── test_ingestion.py
└── _devresources/           # Development notebooks and sample data
```

## Database Schema

The DuckDB database contains four tables:

| Table | Purpose |
|-------|---------|
| `ingestion_state` | Tracks global ingestion process state |
| `asset_metadata` | Asset information (id, symbol, name, rank) |
| `asset_ingestion_state` | Per-asset progress tracking |
| `market_data` | Hourly price, market cap, and volume data |

## Rate Limiting

CoinGecko's free tier has rate limits (~30 calls/minute). The ingestion pipeline handles this automatically:

- Detects HTTP 429 (Too Many Requests) responses
- Implements exponential backoff (60s, 120s, 240s, ...)
- Persists state before sleeping so progress isn't lost
- Resumes from the last successful point on restart

**Tip**: For faster ingestion, consider a CoinGecko Pro subscription with higher rate limits.

## Resuming After Interruption

The pipeline is designed to be restartable. If interrupted (Ctrl+C, error, rate limit):

1. Simply run `python ingestion.py` again
2. The system will:
   - Skip assets that are already up-to-date
   - Resume fetching from the last collected timestamp for each asset
   - Continue with any assets that haven't been processed yet

## Troubleshooting

### "No module named 'dacite'" or similar

Ensure you've installed all dependencies:
```bash
pip install -r requirements.txt
```

### Rate limit errors persist

The free CoinGecko API is heavily rate-limited. Options:
1. Wait and retry (the system does this automatically)
2. Upgrade to CoinGecko Pro
3. Reduce `per_page` in configuration to track fewer assets

### Database locked error

Ensure no other process is accessing the DuckDB file. Only one connection can write at a time.

### Tests fail with import errors

Run tests from the `source/` directory:
```bash
cd source
python -m pytest test_*.py -v
```

## Future Enhancements

- [ ] Prefect/Airflow orchestration integration
- [ ] Prometheus metrics export
- [ ] REST API for querying data
- [ ] Support for additional exchanges/data sources

## License

MIT License - see LICENSE file for details.
