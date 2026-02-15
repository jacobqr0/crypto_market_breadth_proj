"""
Pydantic schema definitions for structured agent outputs.

This module defines strict JSON schemas for all agents in the
multi-agent crypto investing system. Every agent must output
JSON conforming to its schema - no prose, no markdown.

Schemas:
- PortfolioContextSchema: Deterministic portfolio state from tool
- TokenResearchSchema: Fundamentals research with candidates
- TechnicalAnalysisSchema: Momentum and breadth indicators
- MacroCycleSchema: Market regime and cycle assessment
- RecommendationsSchema: Orchestrator output with trading plans
- QAReviewSchema: Compliance gatekeeper result
"""

from schemas.base import (
    # Enums
    DataQuality,
    Confidence,
    Trend,
    Signal,
    Regime,
    Action,
    QAStatus,
    Verdict,
    CycleStage,
    Momentum,
    Substance,
    BTCRelativeTrend,
    TimeHorizon,
    # Base models
    SchemaMeta,
    Source,
)

from schemas.portfolio_context import (
    PortfolioContextSchema,
    PortfolioTotals,
    Position,
    DerivedMetrics,
    FrameworkConfig,
    FrameworkChecks,
    Framework,
    PositionOverLimit,
)

from schemas.token_research import (
    TokenResearchSchema,
    UniverseConstraints,
    Universe,
    Thesis,
    AdoptionMetrics,
    Candidate,
    ScoreBreakdown,
    RankedCandidate,
)

from schemas.technical_analysis import (
    TechnicalAnalysisSchema,
    DailyTimeframe,
    WeeklyTimeframe,
    Timeframes,
    BTCRelative,
    KeyLevels,
    AssetTechnical,
    CorrelationPair,
    Breadth,
)

from schemas.macro_cycle import (
    MacroCycleSchema,
    RegimeAssessment,
    MacroFactor,
    MacroFactors,
    CycleAssessment,
    Narrative,
    Implications,
)

from schemas.recommendations import (
    RecommendationsSchema,
    MarketContext,
    TakeProfitTarget,
    TradingPlan,
    Rubric,
    Dependencies,
    Recommendation,
    DefaultRecommendation,
)

from schemas.qa_review import (
    QAReviewSchema,
    ComplianceCheck,
    RiskAssessment,
    PerRecommendationReview,
)

__all__ = [
    # Enums
    "DataQuality",
    "Confidence",
    "Trend",
    "Signal",
    "Regime",
    "Action",
    "QAStatus",
    "Verdict",
    "CycleStage",
    "Momentum",
    "Substance",
    "BTCRelativeTrend",
    "TimeHorizon",
    # Base
    "SchemaMeta",
    "Source",
    # Portfolio Context
    "PortfolioContextSchema",
    "PortfolioTotals",
    "Position",
    "DerivedMetrics",
    "FrameworkConfig",
    "FrameworkChecks",
    "Framework",
    "PositionOverLimit",
    # Token Research
    "TokenResearchSchema",
    "UniverseConstraints",
    "Universe",
    "Thesis",
    "AdoptionMetrics",
    "Candidate",
    "ScoreBreakdown",
    "RankedCandidate",
    # Technical Analysis
    "TechnicalAnalysisSchema",
    "DailyTimeframe",
    "WeeklyTimeframe",
    "Timeframes",
    "BTCRelative",
    "KeyLevels",
    "AssetTechnical",
    "CorrelationPair",
    "Breadth",
    # Macro Cycle
    "MacroCycleSchema",
    "RegimeAssessment",
    "MacroFactor",
    "MacroFactors",
    "CycleAssessment",
    "Narrative",
    "Implications",
    # Recommendations
    "RecommendationsSchema",
    "MarketContext",
    "TakeProfitTarget",
    "TradingPlan",
    "Rubric",
    "Dependencies",
    "Recommendation",
    "DefaultRecommendation",
    # QA Review
    "QAReviewSchema",
    "ComplianceCheck",
    "RiskAssessment",
    "PerRecommendationReview",
]
