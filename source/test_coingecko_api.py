import pytest 
from unittest.mock import Mock
import requests

try:
    from .coingecko_api import CoinGeckoAPI
except ImportError:
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


def test_build_api_with_page_parameter():
    """Test that page parameter is correctly included in endpoint URL."""
    state = {
        "api": None,
        "marketchart_state": {
            "already_fetched": {},
            "to_query_asset_id": None
        }
    }
    
    secrets_with_page = {
        "base_url": "https://api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "250",
                "page": "2"
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": 1739340000,
                "query_to": 1754888400
            }
        }
    }
    
    api = CoinGeckoAPI(state, secrets_with_page)
    endpoint = api.build_api()
    
    assert "&page=2" in endpoint
    assert "per_page=250" in endpoint


def test_build_api_defaults_to_page_1():
    """Test that page defaults to 1 if not specified."""
    state = {
        "api": None,
        "marketchart_state": {
            "already_fetched": {},
            "to_query_asset_id": None
        }
    }
    
    secrets_no_page = {
        "base_url": "https://api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "250"
                # page not specified
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": 1739340000,
                "query_to": 1754888400
            }
        }
    }
    
    api = CoinGeckoAPI(state, secrets_no_page)
    endpoint = api.build_api()
    
    assert "&page=1" in endpoint


def test_build_api_page2_with_per_page_250_for_350_tokens():
    """
    Test that page 2 with per_page=250 generates correct endpoint URL.
    
    This tests the correct pagination approach for fetching top 350 tokens:
    - Page 1: per_page=250 → tokens 1-250
    - Page 2: per_page=250 → tokens 251-500 (caller slices to get 251-350)
    
    Note: Using per_page=100 with page=2 would incorrectly return tokens 101-200.
    """
    state = {
        "api": None,
        "marketchart_state": {
            "already_fetched": {},
            "to_query_asset_id": None
        }
    }
    
    # Configuration for fetching page 2 with per_page=250
    secrets_page2 = {
        "base_url": "https://pro-api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "250",
                "page": "2"
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": 1739340000,
                "query_to": 1754888400
            }
        }
    }
    
    api = CoinGeckoAPI(state, secrets_page2)
    endpoint = api.build_api()
    
    # Verify page 2 is specified
    assert "&page=2" in endpoint
    # Verify per_page=250 is used (not 100)
    assert "per_page=250" in endpoint
    # Verify PRO API URL is used
    assert "pro-api.coingecko.com" in endpoint
    # Verify it's the coins/markets endpoint
    assert "coins/markets?" in endpoint


def test_build_api_page1_and_page2_both_use_per_page_250():
    """
    Test that both page 1 and page 2 use per_page=250 for consistent pagination.
    
    This verifies the correct approach for fetching top 350 tokens:
    - CoinGecko pagination: (page-1)*per_page+1 to page*per_page
    - Page 1 with per_page=250: tokens 1-250
    - Page 2 with per_page=250: tokens 251-500
    """
    state = {
        "api": None,
        "marketchart_state": {
            "already_fetched": {},
            "to_query_asset_id": None
        }
    }
    
    # Test page 1
    secrets_page1 = {
        "base_url": "https://pro-api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "250",
                "page": "1"
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": 1739340000,
                "query_to": 1754888400
            }
        }
    }
    
    api1 = CoinGeckoAPI(state, secrets_page1)
    endpoint1 = api1.build_api()
    
    assert "&page=1" in endpoint1
    assert "per_page=250" in endpoint1
    
    # Test page 2 - must also use per_page=250
    secrets_page2 = {
        "base_url": "https://pro-api.coingecko.com/api/v3/",
        "parameters": {
            "coinmarkets": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": "250",
                "page": "2"
            },
            "marketchart": {
                "vs_currency": "usd",
                "initial_query_from": 1739340000,
                "query_to": 1754888400
            }
        }
    }
    
    api2 = CoinGeckoAPI(state, secrets_page2)
    endpoint2 = api2.build_api()
    
    assert "&page=2" in endpoint2
    assert "per_page=250" in endpoint2


# ==================== Tests for build_response() ====================

def create_mock_response(json_data, status_code=200):
    """Helper to create mock Response objects."""
    mock_resp = Mock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    return mock_resp


class TestBuildResponseCoinMarkets:
    """Tests for build_response with coins/markets endpoint."""
    
    def test_build_response_coin_markets_success(self):
        """Verify coins/markets response is parsed correctly."""
        state = {
            "api": None,
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        mock_json = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
        ]
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        response = create_mock_response(mock_json)
        
        result = api.build_response(response)
        
        assert result is not None
        assert result["type"] == "coinmarkets"
        assert len(result["assets"]) == 2
        assert result["assets"][0]["asset_id"] == "bitcoin"
        assert result["assets"][1]["asset_id"] == "ethereum"
    
    def test_build_response_coin_markets_error(self):
        """Verify error response returns None."""
        state = {
            "api": None,
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        response = create_mock_response({}, status_code=500)
        
        result = api.build_response(response)
        
        assert result is None


class TestBuildResponseMarketChart:
    """Tests for build_response with market_chart/range endpoint."""
    
    def test_build_response_market_chart_success(self):
        """Verify market_chart response is parsed correctly."""
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": ["bitcoin"]
            }
        }
        
        mock_json = {
            "prices": [[1700000000000, 35000.0], [1700003600000, 35100.0]],
            "market_caps": [[1700000000000, 680e9], [1700003600000, 681e9]],
            "total_volumes": [[1700000000000, 15e9], [1700003600000, 16e9]]
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        api.build_api()  # Sets current_asset_id
        response = create_mock_response(mock_json)
        
        result = api.build_response(response)
        
        assert result is not None
        assert result["type"] == "marketchart"
        assert result["asset_id"] == "bitcoin"
        assert len(result["data_points"]) == 2
        
        # Verify first data point
        dp = result["data_points"][0]
        assert dp["timestamp_unix"] == 1700000000  # Converted from ms
        assert dp["price_usd"] == 35000.0
        assert dp["market_cap_usd"] == 680e9
        assert dp["volume_usd"] == 15e9
    
    def test_build_response_market_chart_empty(self):
        """Verify empty market_chart response is handled."""
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": ["bitcoin"]
            }
        }
        
        mock_json = {
            "prices": [],
            "market_caps": [],
            "total_volumes": []
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        api.build_api()
        response = create_mock_response(mock_json)
        
        result = api.build_response(response)
        
        assert result is not None
        assert result["type"] == "marketchart"
        assert len(result["data_points"]) == 0


# ==================== Tests for update_state() ====================

class TestUpdateStateCoinMarkets:
    """Tests for update_state after coins/markets response."""
    
    def test_update_state_switches_to_marketchart(self):
        """Verify state switches to marketchart after coins/markets."""
        state = {
            "api": None,
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        parsed_response = {
            "type": "coinmarkets",
            "assets": [
                {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
                {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2},
            ]
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        updated = api.update_state(parsed_response)
        
        assert updated["api"] == "marketchart"
        assert updated["marketchart_state"]["to_query_asset_id"] == ["bitcoin", "ethereum"]
    
    def test_update_state_handles_none_response(self):
        """Verify None response returns unchanged state."""
        state = {
            "api": None,
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": None
            }
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        updated = api.update_state(None)
        
        assert updated["api"] is None


class TestUpdateStateMarketChart:
    """Tests for update_state after market_chart response."""
    
    def test_update_state_updates_already_fetched(self):
        """Verify already_fetched is updated with latest timestamp."""
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": ["bitcoin", "ethereum"]
            }
        }
        
        parsed_response = {
            "type": "marketchart",
            "asset_id": "bitcoin",
            "data_points": [
                {"timestamp_unix": 1700000000, "price_usd": 35000.0},
                {"timestamp_unix": 1700003600, "price_usd": 35100.0},
            ]
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        api.build_api()  # Sets current_asset_id
        updated = api.update_state(parsed_response)
        
        assert "bitcoin" in updated["marketchart_state"]["already_fetched"]
        assert updated["marketchart_state"]["already_fetched"]["bitcoin"]["last_fetched"] == 1700003600
    
    def test_update_state_removes_from_queue(self):
        """Verify processed asset is removed from queue."""
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": ["bitcoin", "ethereum"]
            }
        }
        
        parsed_response = {
            "type": "marketchart",
            "asset_id": "bitcoin",
            "data_points": [
                {"timestamp_unix": 1700000000, "price_usd": 35000.0},
            ]
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        api.build_api()
        updated = api.update_state(parsed_response)
        
        assert "bitcoin" not in updated["marketchart_state"]["to_query_asset_id"]
        assert "ethereum" in updated["marketchart_state"]["to_query_asset_id"]
    
    def test_update_state_sets_api_none_when_complete(self):
        """Verify api is set to None when queue is empty."""
        state = {
            "api": "marketchart",
            "marketchart_state": {
                "already_fetched": {},
                "to_query_asset_id": ["bitcoin"]
            }
        }
        
        parsed_response = {
            "type": "marketchart",
            "asset_id": "bitcoin",
            "data_points": [
                {"timestamp_unix": 1700000000, "price_usd": 35000.0},
            ]
        }
        
        api = CoinGeckoAPI(state, BASIC_SECRETS)
        api.build_api()
        updated = api.update_state(parsed_response)
        
        assert updated["api"] is None
        assert updated["marketchart_state"]["to_query_asset_id"] == []