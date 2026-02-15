"""
Base schema definitions shared across all agent schemas.

This module defines:
- Common enums (DataQuality, Confidence, Trend, etc.)
- SchemaMeta: Required metadata block for all agent outputs
- Source: Citation/reference structure
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


# =============================================================================
# Common Enums
# =============================================================================

class DataQuality(str, Enum):
    """Data quality status for schema meta."""
    OK = "ok"
    PARTIAL = "partial"
    INVALID = "invalid"


class Confidence(str, Enum):
    """Confidence level for assessments."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Trend(str, Enum):
    """Price/market trend direction."""
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"


class Signal(str, Enum):
    """Technical signal direction."""
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"


class Regime(str, Enum):
    """Macro market regime."""
    RISK_ON = "risk_on"
    RISK_OFF = "risk_off"
    NEUTRAL = "neutral"


class Action(str, Enum):
    """Investment action recommendation."""
    BUY = "buy"
    HOLD = "hold"
    REDUCE = "reduce"
    SELL = "sell"
    WATCH = "watch"


class QAStatus(str, Enum):
    """QA review status for individual items."""
    PASS = "pass"
    FLAG = "flag"
    REJECT = "reject"


class Verdict(str, Enum):
    """QA verdict for recommendations."""
    PROCEED = "proceed"
    MODIFY = "modify"
    REJECT = "reject"


class CycleStage(str, Enum):
    """Market cycle stage."""
    EARLY = "early"
    MID = "mid"
    LATE = "late"
    UNKNOWN = "unknown"


class Momentum(str, Enum):
    """Narrative momentum direction."""
    RISING = "rising"
    STABLE = "stable"
    FALLING = "falling"


class Substance(str, Enum):
    """Narrative substance level."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class BTCRelativeTrend(str, Enum):
    """BTC-relative performance trend."""
    OUTPERFORMING = "outperforming"
    NEUTRAL = "neutral"
    UNDERPERFORMING = "underperforming"
    UNKNOWN = "unknown"


class TimeHorizon(str, Enum):
    """Investment time horizon."""
    SHORT = "3-6m"
    MEDIUM = "6-12m"
    LONG = "12m+"
    UNKNOWN = "unknown"


class SourceType(str, Enum):
    """Type of source/citation."""
    URL = "url"
    PAPER = "paper"
    DASHBOARD = "dashboard"
    DATASET = "dataset"
    ARTICLE = "article"
    BLOG = "blog"
    DOCS = "docs"
    API = "api"


class CorrelationLevel(str, Enum):
    """Correlation level assessment."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class ConvictionStrength(str, Enum):
    """Conviction strength assessment."""
    STRONG = "strong"
    ADEQUATE = "adequate"
    WEAK = "weak"


class OverallStatus(str, Enum):
    """Overall QA status for the run."""
    PASS = "pass"
    FLAG = "flag"
    REJECT = "reject"


class CheckStatus(str, Enum):
    """Status for individual compliance checks."""
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class DefaultAction(str, Enum):
    """Default recommendation action."""
    HOLD_BTC = "hold_btc"
    DCA_BTC = "dca_btc"
    DO_NOTHING = "do_nothing"


# =============================================================================
# Base Models
# =============================================================================

class SchemaMeta(BaseModel):
    """
    Required metadata block for all agent outputs.
    
    Every schema must include this as the 'meta' field.
    """
    model_config = {"use_enum_values": True}
    
    agent_name: str = Field(..., description="Name of the agent producing this output")
    schema_version: str = Field(..., description="Schema version (e.g., '1.0')")
    as_of_timestamp_utc: str = Field(
        ..., 
        description="ISO-8601 timestamp when this data was produced"
    )
    data_quality: DataQuality = Field(
        ..., 
        description="Data quality status: ok, partial, or invalid"
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="List of warnings or issues encountered"
    )


class Source(BaseModel):
    """Citation/reference for data sources."""
    model_config = {"use_enum_values": True}
    
    name: str = Field(..., description="Name of the source")
    type: SourceType = Field(..., description="Type: url, paper, dashboard, dataset")
    ref: str = Field(..., description="Reference (URL, DOI, etc.)")
    as_of: Optional[str] = Field(None, description="Date when source was accessed")
