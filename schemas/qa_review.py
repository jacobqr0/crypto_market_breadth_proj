"""
QA Review schema definition.

This schema defines the output format for the QA/Risk Agent,
which validates recommendations for compliance and risk management.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from schemas.base import (
    SchemaMeta,
    OverallStatus,
    CheckStatus,
    QAStatus,
    Verdict,
    CorrelationLevel,
    ConvictionStrength,
)


class ComplianceCheck(BaseModel):
    """Individual compliance check result."""
    model_config = {"use_enum_values": True}
    
    check: str = Field(..., description="Name of the compliance check")
    status: CheckStatus = Field(
        ..., 
        description="Check status: pass, fail, unknown"
    )
    notes: str = Field(..., description="Notes or details about the check")


class RiskAssessment(BaseModel):
    """Risk assessment for a recommendation."""
    model_config = {"use_enum_values": True}
    
    correlation_with_portfolio: CorrelationLevel = Field(
        ..., 
        description="Correlation with existing portfolio: high, medium, low, unknown"
    )
    sector_concentration: str = Field(
        ..., 
        description="Sector concentration impact"
    )
    conviction: ConvictionStrength = Field(
        ..., 
        description="Conviction assessment: strong, adequate, weak"
    )


class PerRecommendationReview(BaseModel):
    """QA review for a single recommendation."""
    model_config = {"use_enum_values": True}
    
    symbol: str = Field(..., description="Asset symbol")
    original_action: str = Field(..., description="Original recommended action")
    qa_status: QAStatus = Field(
        ..., 
        description="QA status: pass, flag, reject"
    )
    issues: List[str] = Field(
        default_factory=list,
        description="Issues found"
    )
    risk: RiskAssessment = Field(..., description="Risk assessment")
    verdict: Verdict = Field(
        ..., 
        description="Final verdict: proceed, modify, reject"
    )


class QAReviewSchema(BaseModel):
    """
    Complete QA review schema.
    
    This is the output format for the QA/Risk Agent.
    Contains compliance checks, per-recommendation reviews, and final verdict.
    """
    model_config = {"use_enum_values": True}
    
    meta: SchemaMeta = Field(..., description="Schema metadata")
    overall_status: OverallStatus = Field(
        ..., 
        description="Overall QA status: pass, flag, reject"
    )
    recommendations_reviewed: int = Field(
        ..., 
        ge=0, 
        description="Number of recommendations reviewed"
    )
    issues_found: int = Field(..., ge=0, description="Total issues found")
    compliance_checklist: List[ComplianceCheck] = Field(
        ..., 
        description="Compliance checklist results"
    )
    per_recommendation: List[PerRecommendationReview] = Field(
        ..., 
        description="Per-recommendation reviews"
    )
    final_verdict: str = Field(
        ..., 
        description="Summary final verdict with required actions"
    )
