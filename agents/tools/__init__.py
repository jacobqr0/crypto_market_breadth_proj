"""
Agent tools for the CrewAI investment system.

Tools wrap existing data access layers to provide safe, read-only
access for agents to query portfolio state, market data, and
technical indicators.
"""

__all__ = []

# Lazy imports to avoid import errors if crewai isn't installed
try:
    from agents.tools.portfolio_tools import (
        get_open_positions,
        get_position,
        get_trade_history,
        get_realized_pnl_summary,
        get_portfolio_summary,
    )
    __all__.extend([
        "get_open_positions",
        "get_position",
        "get_trade_history",
        "get_realized_pnl_summary",
        "get_portfolio_summary",
    ])
except ImportError:
    pass

try:
    from agents.tools.market_data_tools import (
        get_price_history,
        get_btc_relative_price,
        get_market_cap_rankings,
    )
    __all__.extend([
        "get_price_history",
        "get_btc_relative_price",
        "get_market_cap_rankings",
    ])
except ImportError:
    pass

try:
    from agents.tools.technical_tools import (
        get_sma,
        get_rsi,
        get_price_correlation,
    )
    __all__.extend([
        "get_sma",
        "get_rsi",
        "get_price_correlation",
    ])
except ImportError:
    pass

try:
    from agents.tools.serper_tools import search_web
    __all__.append("search_web")
except ImportError:
    pass
