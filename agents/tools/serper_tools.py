"""
Serper web search tools for CrewAI agents.

Provides web search capabilities for agents to research crypto news,
market conditions, and asset-specific information.
"""

import os
import json
from typing import Optional
from datetime import datetime
import requests
from crewai.tools import tool


def _get_recent_years() -> str:
    """Get current year and previous year for search queries."""
    current_year = datetime.now().year
    return f"{current_year} {current_year - 1}"


def _get_current_year() -> int:
    """Get the current year."""
    return datetime.now().year


def _get_serper_api_key() -> str:
    """Get Serper API key from environment."""
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        raise ValueError("SERPER_API_KEY environment variable not set")
    return api_key


def _serper_search(query: str, num_results: int = 10) -> dict:
    """Execute a Serper search request."""
    api_key = _get_serper_api_key()
    
    url = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "q": query,
        "num": num_results
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


@tool
def search_web(query: str) -> str:
    """
    Search the web for information using Serper (Google Search API).
    
    Args:
        query: Search query string. Be specific for better results.
               Example: "Ethereum 2024 adoption metrics TVL growth"
    
    Returns search results with titles, snippets, and URLs.
    Use this to research current market conditions, news, and asset fundamentals.
    """
    try:
        results = _serper_search(query, num_results=10)
    except ValueError as e:
        return f"Search error: {str(e)}"
    except requests.RequestException as e:
        return f"Search request failed: {str(e)}"
    
    organic = results.get("organic", [])
    
    if not organic:
        return f"No results found for: {query}"
    
    lines = [f"Search Results for: {query}", "=" * 60, ""]
    
    for i, result in enumerate(organic[:10], 1):
        title = result.get("title", "No title")
        snippet = result.get("snippet", "No description")
        link = result.get("link", "")
        
        lines.append(f"{i}. {title}")
        lines.append(f"   {snippet}")
        lines.append(f"   URL: {link}")
        lines.append("")
    
    return "\n".join(lines)


@tool
def search_crypto_news(asset_name: str) -> str:
    """
    Search for recent news about a cryptocurrency.
    
    Args:
        asset_name: Name of the cryptocurrency (e.g., "Bitcoin", "Ethereum", "Solana")
    
    Returns recent news articles and developments related to the asset.
    Use this to understand current sentiment and recent events.
    """
    query = f"{asset_name} cryptocurrency news {_get_recent_years()}"
    
    try:
        results = _serper_search(query, num_results=10)
    except Exception as e:
        return f"News search failed: {str(e)}"
    
    organic = results.get("organic", [])
    news = results.get("news", [])
    
    # Combine organic and news results
    all_results = news + organic
    
    if not all_results:
        return f"No news found for {asset_name}"
    
    lines = [f"Recent News for {asset_name}:", "=" * 60, ""]
    
    for i, result in enumerate(all_results[:8], 1):
        title = result.get("title", "No title")
        snippet = result.get("snippet", result.get("description", ""))
        date = result.get("date", "")
        link = result.get("link", "")
        
        date_str = f" ({date})" if date else ""
        lines.append(f"{i}. {title}{date_str}")
        if snippet:
            lines.append(f"   {snippet[:200]}...")
        lines.append(f"   {link}")
        lines.append("")
    
    return "\n".join(lines)


@tool
def search_market_metrics(asset_name: str) -> str:
    """
    Search for on-chain metrics and market data for a cryptocurrency.
    
    Args:
        asset_name: Name of the cryptocurrency (e.g., "Ethereum", "Solana")
    
    Returns information about TVL, daily active users, protocol revenue,
    and other adoption metrics. Use this for fundamental analysis.
    """
    query = f"{asset_name} TVL daily active users protocol revenue metrics {_get_current_year()}"
    
    try:
        results = _serper_search(query, num_results=8)
    except Exception as e:
        return f"Metrics search failed: {str(e)}"
    
    organic = results.get("organic", [])
    
    if not organic:
        return f"No metrics data found for {asset_name}"
    
    lines = [f"Market Metrics Search for {asset_name}:", "=" * 60, ""]
    
    for i, result in enumerate(organic[:6], 1):
        title = result.get("title", "No title")
        snippet = result.get("snippet", "")
        link = result.get("link", "")
        
        lines.append(f"{i}. {title}")
        if snippet:
            lines.append(f"   {snippet}")
        lines.append(f"   Source: {link}")
        lines.append("")
    
    lines.append("Note: Verify metrics from primary sources like DefiLlama, Token Terminal, or official dashboards.")
    
    return "\n".join(lines)


@tool
def search_macro_conditions() -> str:
    """
    Search for current macroeconomic conditions affecting crypto markets.
    
    Returns information about Federal Reserve policy, inflation, liquidity conditions,
    and other macro factors that influence crypto market cycles.
    """
    query = f"cryptocurrency market macro conditions Federal Reserve liquidity {_get_recent_years()}"
    
    try:
        results = _serper_search(query, num_results=10)
    except Exception as e:
        return f"Macro search failed: {str(e)}"
    
    organic = results.get("organic", [])
    news = results.get("news", [])
    
    all_results = news + organic
    
    if not all_results:
        return "No macro condition information found."
    
    lines = ["Macro Economic Conditions (Crypto Impact):", "=" * 60, ""]
    
    for i, result in enumerate(all_results[:8], 1):
        title = result.get("title", "No title")
        snippet = result.get("snippet", result.get("description", ""))
        date = result.get("date", "")
        
        date_str = f" [{date}]" if date else ""
        lines.append(f"{i}. {title}{date_str}")
        if snippet:
            lines.append(f"   {snippet[:250]}")
        lines.append("")
    
    lines.append("Key factors to consider:")
    lines.append("- Interest rate expectations")
    lines.append("- Liquidity conditions (QE/QT)")
    lines.append("- Risk appetite indicators")
    lines.append("- Regulatory developments")
    
    return "\n".join(lines)


@tool
def search_asset_fundamentals(asset_name: str) -> str:
    """
    Search for fundamental information about a cryptocurrency project.
    
    Args:
        asset_name: Name of the cryptocurrency
    
    Returns information about the project's use case, team, technology,
    partnerships, and competitive position.
    """
    query = f"{asset_name} cryptocurrency fundamentals use case technology team {_get_current_year()}"
    
    try:
        results = _serper_search(query, num_results=8)
    except Exception as e:
        return f"Fundamentals search failed: {str(e)}"
    
    organic = results.get("organic", [])
    
    if not organic:
        return f"No fundamental information found for {asset_name}"
    
    lines = [f"Fundamentals Research for {asset_name}:", "=" * 60, ""]
    
    for i, result in enumerate(organic[:6], 1):
        title = result.get("title", "No title")
        snippet = result.get("snippet", "")
        link = result.get("link", "")
        
        lines.append(f"{i}. {title}")
        if snippet:
            lines.append(f"   {snippet}")
        lines.append(f"   {link}")
        lines.append("")
    
    lines.append("Research checklist:")
    lines.append("- What problem does this solve?")
    lines.append("- What are the network effects?")
    lines.append("- Who is the team and what is their track record?")
    lines.append("- What is the competitive landscape?")
    
    return "\n".join(lines)
