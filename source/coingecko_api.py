from dataclasses import asdict, dataclass
from dacite import from_dict
from typing import Optional

import requests 
import json
import pandas as pd 

@dataclass 
class CoinGeckoEndpoint:
    api: str
    endpoint: str

@dataclass
class CoinGeckoMarketChartState:
    asset_id: Optional[list[str]]

@dataclass
class CoinGeckoState:
    api: Optional[str]
    marketchart_state: Optional[CoinGeckoMarketChartState]
    last_query_time: Optional[str]

@dataclass 
class CoinMarketParameters:
    vs_currency: str
    order: str
    per_page: str

@dataclass 
class MarketChartParameters:
    vs_currency: str

@dataclass 
class APIBaseParameters:
    coinmarkets: CoinMarketParameters
    marketchart: MarketChartParameters

@dataclass
class CoinGeckoSecrets:
    base_url: str

class CoinGeckoAPI:

    API_KEY_COIN_MARKETS = "coinmarkets"
    API_KEY_MARKET_CHART = "marketchart"

    API_ENDPOINTS: dict[str, CoinGeckoEndpoint] = {
        API_KEY_COIN_MARKETS: CoinGeckoEndpoint(API_KEY_COIN_MARKETS, "coinmarkets"),
        API_KEY_MARKET_CHART: CoinGeckoEndpoint(API_KEY_MARKET_CHART, "marketchart")
    }

    def __init__(self, state: dict, secrets: dict) -> None:
        self.parse_request(state, secrets)

    def parse_request(self, state: dict, secrets: dict):
        self.original_state: CoinGeckoState = from_dict(data_class=CoinGeckoState, data=state)
        self.updated_state: dict = asdict(self.original_state)
        self.secrets: CoinGeckoSecrets = from_dict(data_class=CoinGeckoSecrets, data=secrets)

    def build_api(self) -> str:
        api = self.API_ENDPOINTS[self.API_KEY_COIN_MARKETS] if self.original_state.api is None else self.API_ENDPOINTS[self.original_state.api]

        # TODO - build default parameters for coin markets endpoint
        # Parameters for Coin Markets endpoint will not be configurable 

        if api.api == self.API_KEY_COIN_MARKETS:
            pass
        
        elif api.api == self.API_KEY_MARKET_CHART:
            if self.original_state.marketchart_state is None:
                existing_asset_id = None 
            else:
                existing_asset_id = self.original_state.marketchart_state.asset_id