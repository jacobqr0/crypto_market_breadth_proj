from dataclasses import asdict, dataclass
from dacite import from_dict
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import time
import logging
import os

import requests 
import json
import pandas as pd 

logger = logging.getLogger(__name__)

# Environment variable name for CoinGecko API key
COINGECKO_API_KEY_ENV = "COINGECKO_API_KEY"

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

    def _get_api_headers(self) -> Dict[str, str]:
        """
        Build request headers, including API key if available.
        
        Reads API key from COINGECKO_API_KEY environment variable.
        For CoinGecko Demo/Basic tier, uses x-cg-demo-api-key header.
        
        :return: Dictionary of headers
        """
        headers = {
            "Accept": "application/json"
        }
        
        api_key = os.environ.get(COINGECKO_API_KEY_ENV)
        
        if api_key:
            # CoinGecko Demo/Basic tier uses x-cg-demo-api-key header
            headers["x-cg-pro-api-key"] = api_key
            logger.debug("Using API key from environment variable")
        else:
            logger.debug("No API key found - using free tier (rate limits apply)")
        
        return headers

    def make_request(self, endpoint: str, max_retries: int = 5) -> requests.Response:
        """
        Perform a GET request to the given endpoint with retry logic.

        :param endpoint: API endpoint URL
        :param max_retries: Maximum number of retry attempts for rate limiting
        :return: Response object
        
        Note: Set COINGECKO_API_KEY environment variable for paid tier access.
        """
        base_delay = 60  # CoinGecko recommends 60s wait on rate limit
        headers = self._get_api_headers()

        for attempt in range(max_retries):
            response = requests.get(endpoint, headers=headers)
            
            if response.status_code == 200:
                logger.info("Request successful")
                return response
            elif response.status_code == 400:
                logger.error("Bad Request - The server could not understand the request.")
                return response
            elif response.status_code == 401:
                logger.error("Unauthorized - Invalid API key or authentication required.")
                return response
            elif response.status_code == 403:
                logger.error("Forbidden - You do not have permission to access this resource.")
                return response
            elif response.status_code == 429:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Rate Limit Exceeded. Waiting {delay}s before retry (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                continue
            elif response.status_code == 500:
                logger.error("Server Error - The problem is on the server side.")
                return response
            elif response.status_code == 503:
                logger.error("Service Unavailable - The server is temporarily unable to handle the request.")
                return response
            else:
                logger.warning(f"Unexpected status code: {response.status_code}")
                return response
        
        # If we exhausted retries due to rate limiting
        logger.error(f"Max retries ({max_retries}) exceeded due to rate limiting")
        return response

    def build_response(self, response: requests.Response) -> Optional[Dict]:
        """
        Parse API response into normalized format for storage.
        
        :param response: Response object from make_request
        :return: Normalized dictionary or None if error
        """
        if response.status_code != 200:
            return None
        
        api = self.API_ENDPOINTS[self.API_KEY_COIN_MARKETS] if self.original_state.api is None else self.API_ENDPOINTS[self.original_state.api]
        response_json = response.json()
        
        if api.api == self.API_KEY_COIN_MARKETS:
            # Extract asset metadata from coins/markets response
            assets = []
            for asset in response_json:
                assets.append({
                    "asset_id": asset.get("id"),
                    "symbol": asset.get("symbol"),
                    "name": asset.get("name"),
                    "market_cap_rank": asset.get("market_cap_rank")
                })
            return {"type": "coinmarkets", "assets": assets}
        
        elif api.api == self.API_KEY_MARKET_CHART:
            # Extract time series data from market_chart/range response
            prices = response_json.get("prices", [])
            market_caps = response_json.get("market_caps", [])
            total_volumes = response_json.get("total_volumes", [])
            
            data_points = []
            for i, price_entry in enumerate(prices):
                timestamp_ms = price_entry[0]
                timestamp_unix = int(timestamp_ms / 1000)  # Convert ms to seconds
                
                data_points.append({
                    "timestamp_unix": timestamp_unix,
                    "price_usd": price_entry[1] if len(price_entry) > 1 else None,
                    "market_cap_usd": market_caps[i][1] if i < len(market_caps) and len(market_caps[i]) > 1 else None,
                    "volume_usd": total_volumes[i][1] if i < len(total_volumes) and len(total_volumes[i]) > 1 else None
                })
            
            return {
                "type": "marketchart",
                "asset_id": self.current_asset_id,
                "data_points": data_points
            }
        
        return None

    def update_state(self, parsed_response: Optional[Dict]) -> dict:
        """
        Update internal state based on parsed API response.
        
        :param parsed_response: Parsed response from build_response
        :return: Updated state dictionary
        """
        if parsed_response is None:
            return self.updated_state
        
        if parsed_response.get("type") == "coinmarkets":
            # After fetching coin markets, switch to market chart endpoint
            asset_ids = [asset["asset_id"] for asset in parsed_response.get("assets", [])]
            self.updated_state["api"] = self.API_KEY_MARKET_CHART
            self.updated_state["marketchart_state"]["to_query_asset_id"] = asset_ids
            
        elif parsed_response.get("type") == "marketchart":
            asset_id = parsed_response.get("asset_id")
            data_points = parsed_response.get("data_points", [])
            
            if data_points:
                # Update last fetched timestamp
                max_ts = max(dp["timestamp_unix"] for dp in data_points)
                self.updated_state["marketchart_state"]["already_fetched"][asset_id] = {
                    "last_fetched": max_ts
                }
            
            # Remove current asset from queue
            to_query = self.updated_state["marketchart_state"].get("to_query_asset_id", [])
            if asset_id in to_query:
                to_query.remove(asset_id)
            
            # If queue is empty, mark as complete
            if not to_query:
                self.updated_state["api"] = None
        
        return self.updated_state
