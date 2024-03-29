import pytest 
import coingecko_api

BASIC_SECRETS = {
    'base_url': 'https://api.coingecko.com/api/v3/'
}

BASIC_PARAMETERS = {
    "coinmarkets": {
        "vs_currency": "usd",
        "order": "market_cap_des",
        "per_page": "300"
    },

    "marketchart": {
        "vs_currency": "usd"
    }
}

BASIC_STATE = {}

