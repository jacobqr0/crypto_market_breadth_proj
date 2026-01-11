import pytest 
from unittest.mock import Mock
import requests
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