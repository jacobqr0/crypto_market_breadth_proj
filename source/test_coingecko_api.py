import pytest 
from coingecko_api import CoinGeckoAPI
from typing import Optional, List, Dict

BASIC_PARAMETERS = {
    "coinmarkets": {
        "vs_currency": "usd",
        "order": "market_cap_des",
        "per_page": "250"
    },

    "marketchart": {
        "vs_currency": "usd",
        "initial_query_from": 1739340000,
        "query_to": 1754888400
    }
}

BASIC_SECRETS = {
    "base_url": "https://api.coingecko.com/api/v3/",
    "parameters": BASIC_PARAMETERS
}

BASIC_STATE = {
    "marketchart_state": {
        "already_fetched": {}
    }
}

BASIC_MARKETCHART_STATE = {
    "already_fetched": {"bitcoin": {"last_fetched": 1739340000}, "ethereum": {"last_fetched": 1739340000}},
    "to_query_asset_id": ["bitcoin", "ethereum"]
}

def test_parse_request():

    api = CoinGeckoAPI(BASIC_STATE, BASIC_SECRETS)

    assert api.secrets.base_url == "https://api.coingecko.com/api/v3/"
    assert api.secrets.parameters.coinmarkets.vs_currency == "usd"
    assert api.secrets.parameters.coinmarkets.order == "market_cap_des"
    assert api.secrets.parameters.coinmarkets.per_page == "250"
    assert api.secrets.parameters.marketchart.vs_currency == "usd"
    assert api.original_state.marketchart_state.already_fetched == {}

def test_parse_request_nonblank_api():

    api_state = {
        "api": "marketchart",
        "marketchart_state": {
            "already_fetched": {}
        }
        }

    api = CoinGeckoAPI(api_state, BASIC_SECRETS)

    assert api.original_state.api == "marketchart"

@pytest.mark.parametrize("input_api, assets, already_fetched, expected_endpoint", [
    (None, None, {}, "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_des&per_page=250&page=1"),
    ("marketchart", ["bitcoin", "ethereum"], {}, "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from=1739340000&to=1754888400"),
    ("marketchart", ["bitcoin", "ethereum"], {"bitcoin": {"last_fetched": 1739349999}, "ethereum": {"last_fetched": 1739340000}}, "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from=1739349999&to=1754888400"),
])
def test_build_api_multiple_states(input_api: Optional[str], assets: Optional[List], already_fetched: Optional[Dict], expected_endpoint: str):

    state = {

        "api": input_api,
        "marketchart_state": {
            "to_query_asset_id": assets,
            "already_fetched": already_fetched
            }
        }
    
    api = CoinGeckoAPI(state, BASIC_SECRETS)
    actual_endpoint = api.build_api()
    
    assert actual_endpoint == expected_endpoint