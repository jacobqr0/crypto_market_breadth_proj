"""
Token Research schema definition.

This schema defines the output format for the Token Research/Synthesizer Agent,
which provides decision-useful fundamentals research with ranked candidates.
"""

from typing import List, Optional, Literal

from pydantic import BaseModel, Field

from schemas.base import SchemaMeta, Source, Confidence


class UniverseConstraints(BaseModel):
    """Constraints for the research universe."""
    max_mcap_rank: int = Field(200, description="Maximum market cap rank to consider")
    exclude_memecoins: bool = Field(True, description="Exclude meme coins")


class Universe(BaseModel):
    """Research universe definition."""
    constraints: UniverseConstraints = Field(..., description="Universe constraints")


class Thesis(BaseModel):
    """Investment thesis for a candidate."""
    problem: str = Field(..., description="What problem does this solve?")
    why_it_wins: str = Field(..., description="Why does this project win vs alternatives?")
    network_effects: str = Field(..., description="What network effects exist or are emerging?")


class AdoptionMetrics(BaseModel):
    """On-chain and adoption metrics for a candidate."""
    tvl_usd: Optional[float] = Field(None, description="Total Value Locked in USD")
    tvl_change_90d_pct: Optional[float] = Field(
        None, 
        description="TVL change over 90 days (%)"
    )
    fees_30d_usd: Optional[float] = Field(
        None, 
        description="Protocol fees over 30 days in USD"
    )
    revenue_30d_usd: Optional[float] = Field(
        None, 
        description="Protocol revenue over 30 days in USD"
    )
    dau: Optional[int] = Field(None, description="Daily Active Users")
    tx_count_30d: Optional[int] = Field(
        None, 
        description="Transaction count over 30 days"
    )


class Candidate(BaseModel):
    """Research candidate with full analysis."""
    model_config = {"use_enum_values": True}
    
    symbol: str = Field(..., description="Asset symbol (e.g., ETH, SOL)")
    name: str = Field(..., description="Full asset name")
    mcap_rank: Optional[int] = Field(None, description="Market cap rank")
    category: Optional[str] = Field(
        None, 
        description="Category: L1, L2, DeFi, Infra, RWA, etc."
    )
    thesis: Thesis = Field(..., description="Investment thesis")
    adoption_metrics: AdoptionMetrics = Field(..., description="Adoption metrics")
    catalysts: List[str] = Field(
        default_factory=list,
        description="Upcoming catalysts"
    )
    risks: List[str] = Field(default_factory=list, description="Key risks")
    sources: List[Source] = Field(
        default_factory=list,
        description="Data sources and citations"
    )
    tier_suggestion: Optional[Literal[1, 2, 3]] = Field(
        None, 
        description="Suggested tier: 1=large-cap, 2=emerging, 3=tactical"
    )
    confidence: Confidence = Field(..., description="Research confidence level")


class ScoreBreakdown(BaseModel):
    """Breakdown of composite score by dimension."""
    adoption: float = Field(..., ge=0, le=10, description="Adoption metrics score (0-10)")
    moat: float = Field(..., ge=0, le=10, description="Network effects/moat score (0-10)")
    catalyst: float = Field(..., ge=0, le=10, description="Catalyst score (0-10)")
    risk: float = Field(..., ge=0, le=10, description="Risk-adjusted score (0-10)")


class RankedCandidate(BaseModel):
    """Ranked candidate in the shortlist."""
    symbol: str = Field(..., description="Asset symbol")
    score: float = Field(..., ge=0, le=10, description="Composite score (0-10)")
    score_breakdown: ScoreBreakdown = Field(..., description="Score breakdown by dimension")


class TokenResearchSchema(BaseModel):
    """
    Complete token research schema.
    
    This is the output format for the Token Research Synthesizer Agent.
    Contains detailed analysis of candidates and a ranked shortlist.
    """
    model_config = {"use_enum_values": True}
    
    meta: SchemaMeta = Field(..., description="Schema metadata")
    universe: Universe = Field(..., description="Research universe definition")
    candidates: List[Candidate] = Field(..., description="Analyzed candidates")
    ranked_shortlist: List[RankedCandidate] = Field(
        ..., 
        description="Ranked shortlist (top 5-8)"
    )
