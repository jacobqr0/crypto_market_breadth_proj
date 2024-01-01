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

class CoinGeckoAPI:

    API_KEY_COIN_MARKETS = "coinmarkets"
    API_KEY_MARKET_CHART = "marketchart"

    API_ENDPOINTS: dict[str, CoinGeckoEndpoint] = {
        API_KEY_COIN_MARKETS: CoinGeckoEndpoint(API_KEY_COIN_MARKETS, "coinmarkets"),
        API_KEY_MARKET_CHART: CoinGeckoEndpoint(API_KEY_MARKET_CHART, "marketchart")
    }

    def __init__(self, state: dict) -> None:
        self.parse_request(state)

    def parse_request(self, state: dict):
        self.original_state: CoinGeckoState = from_dict(data_class=CoinGeckoState, data=state)
        self.updated_state: dict = asdict(self.original_state)