"""
Macro Cycle schema definition.

This schema defines the output format for the Macro/Cycle Agent,
which provides market regime assessment, cycle positioning, and implications.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from schemas.base import (
    SchemaMeta, 
    Source, 
    Regime, 
    Confidence, 
    CycleStage, 
    Momentum, 
    Substance
)


class RegimeAssessment(BaseModel):
    """Market regime classification."""
    model_config = {"use_enum_values": True}
    
    stance: Regime = Field(
        ..., 
        description="Current regime: risk_on, risk_off, neutral"
    )
    confidence: Confidence = Field(..., description="Confidence in regime assessment")


class MacroFactor(BaseModel):
    """Analysis of a single macro factor."""
    summary: str = Field(..., description="Summary of current conditions")
    signals: List[str] = Field(
        default_factory=list,
        description="Key signals observed"
    )


class MacroFactors(BaseModel):
    """Collection of macro factors."""
    liquidity: MacroFactor = Field(..., description="Global liquidity conditions")
    fed_policy: MacroFactor = Field(..., description="Federal Reserve policy stance")
    inflation: MacroFactor = Field(..., description="Inflation trends")
    risk_appetite: MacroFactor = Field(..., description="Risk appetite indicators")


class CycleAssessment(BaseModel):
    """Market cycle positioning."""
    model_config = {"use_enum_values": True}
    
    stage: CycleStage = Field(
        ..., 
        description="Cycle stage: early, mid, late, unknown"
    )
    evidence: List[str] = Field(
        default_factory=list,
        description="Evidence supporting cycle assessment"
    )
    halving_context: Optional[str] = Field(
        None, 
        description="Bitcoin halving cycle context"
    )


class Narrative(BaseModel):
    """Analysis of a market narrative."""
    model_config = {"use_enum_values": True}
    
    name: str = Field(..., description="Narrative name (e.g., AI, RWA, DeFi)")
    momentum: Momentum = Field(
        ..., 
        description="Narrative momentum: rising, stable, falling"
    )
    substance: Substance = Field(
        ..., 
        description="Narrative substance: high, medium, low"
    )
    notes: str = Field(..., description="Additional notes on the narrative")


class Implications(BaseModel):
    """Investment implications from macro analysis."""
    favor: List[str] = Field(
        default_factory=list,
        description="Asset types/strategies to favor"
    )
    avoid: List[str] = Field(
        default_factory=list,
        description="Asset types/strategies to avoid"
    )


class MacroCycleSchema(BaseModel):
    """
    Complete macro cycle schema.
    
    This is the output format for the Macro/Cycle Agent.
    Contains regime assessment, macro factors, cycle position, and implications.
    """
    model_config = {"use_enum_values": True}
    
    meta: SchemaMeta = Field(..., description="Schema metadata")
    regime: RegimeAssessment = Field(..., description="Market regime assessment")
    macro: MacroFactors = Field(..., description="Macro factor analysis")
    cycle: CycleAssessment = Field(..., description="Cycle positioning")
    narratives: List[Narrative] = Field(
        default_factory=list,
        description="Active market narratives"
    )
    implications: Implications = Field(..., description="Investment implications")
    sources: List[Source] = Field(
        default_factory=list,
        description="Data sources and citations"
    )
