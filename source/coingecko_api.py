from dataclasses import asdict, dataclass
from dacite import from_dict
from typing import Optional, Dict, List
from datetime import datetime, timedelta

import requests 
import json
import pandas as pd 

@dataclass 
class CoinGeckoEndpoint:
    api: str
    endpoint: str

@dataclass
class CoinGeckoMarketChartState:
    already_fetched: Dict[str, Dict]
    to_query_asset_id: Optional[List[str]]

@dataclass
class CoinGeckoState:
    api: Optional[str]
    marketchart_state: CoinGeckoMarketChartState

@dataclass 
class CoinMarketParameters:
    vs_currency: str
    order: str
    per_page: str

@dataclass 
class MarketChartParameters:
    vs_currency: str
    initial_query_from: int
    query_to: int

@dataclass 
class APIBaseParameters:
    coinmarkets: CoinMarketParameters
    marketchart: MarketChartParameters

@dataclass
class CoinGeckoSecrets:
    base_url: str
    parameters: APIBaseParameters

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

        if api.api == self.API_KEY_COIN_MARKETS:

            # TODO: Figure out if we need to paginate 
            api_endpoint = (f"{self.secrets.base_url}"
                            f"coins/markets?vs_currency={self.secrets.parameters.coinmarkets.vs_currency}"
                            f"&order={self.secrets.parameters.coinmarkets.order}"
                            f"&per_page={self.secrets.parameters.coinmarkets.per_page}"
                            f"&page=1"
                            )
        
        elif api.api == self.API_KEY_MARKET_CHART:
            self.current_asset_id = self.original_state.marketchart_state.to_query_asset_id[0]
            query_to = self.secrets.parameters.marketchart.query_to
            
            if self.current_asset_id in self.original_state.marketchart_state.already_fetched.keys():
                query_from = self.original_state.marketchart_state.already_fetched[self.current_asset_id]["last_fetched"]
            
            else:
                query_from = self.secrets.parameters.marketchart.initial_query_from

            api_endpoint = (f"{self.secrets.base_url}"
                            f"coins/{self.current_asset_id}/market_chart/range"
                            f"?vs_currency={self.secrets.parameters.marketchart.vs_currency}"
                            f"&from={query_from}"
                            f"&to={query_to}"
                            )

        return api_endpoint