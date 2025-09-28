"""ESG Risk Integration Service.

This service provides:
- Unified carbon and risk metrics for ESG-adjusted risk assessment
- Portfolio-level ESG scoring and risk analysis
- Carbon-adjusted risk metrics and scenario analysis
- Integration of carbon pricing into risk calculations
- ESG compliance monitoring and reporting
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4
from enum import Enum

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..dao.experimental import TrinoDAO
from .carbon_rec_service import (
    CarbonRecService,
    CarbonInstrument,
    CarbonPricing,
    PortfolioCarbonExposure,
    get_carbon_rec_service
)
from .risk_engine_service import (
    RiskEngineService,
    PortfolioPosition,
    RiskDistributionConfig,
    RiskScenario,
    RiskCalculationResult,
    get_risk_engine_service
)


class ESGScore(str, Enum):
    """ESG performance scores."""
    AAA = "AAA"  # Excellent
    AA = "AA"    # Very Good
    A = "A"      # Good
    BBB = "BBB"  # Adequate
    BB = "BB"    # Below Average
    B = "B"      # Poor
    CCC = "CCC"  # Very Poor


class ESGRiskCategory(str, Enum):
    """ESG risk categories."""
    ENVIRONMENTAL = "environmental"
    SOCIAL = "social"
    GOVERNANCE = "governance"
    CLIMATE = "climate"
    TRANSITION = "transition"
    PHYSICAL = "physical"


class ESGRiskMetric(BaseModel):
    """ESG risk metric definition."""

    metric_name: str
    category: ESGRiskCategory
    value: float
    unit: str
    percentile: Optional[float] = None  # 0-100 percentile ranking
    benchmark_value: Optional[float] = None
    risk_level: str = "medium"  # "low", "medium", "high", "critical"
    description: str = ""
    calculation_method: str = "weighted_average"


class ESGPortfolioAnalysis(BaseModel):
    """ESG analysis results for a portfolio."""

    portfolio_id: str
    analysis_date: datetime
    overall_esg_score: ESGScore
    overall_risk_score: float  # 0-100, higher = more risk
    carbon_intensity: float  # tons CO2 per MWh
    carbon_cost_impact: float  # $ per MWh
    transition_risk_score: float
    physical_risk_score: float
    governance_risk_score: float
    social_risk_score: float
    esg_metrics: List[ESGRiskMetric] = field(default_factory=list)
    risk_adjustments: Dict[str, float] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class ESGAdjustedRiskResult(BaseModel):
    """ESG-adjusted risk calculation result."""

    portfolio_id: str
    base_risk_result: RiskCalculationResult
    esg_adjustments: Dict[str, float]
    adjusted_var: float
    adjusted_cvar: float
    adjusted_volatility: float
    esg_risk_premium: float  # Additional risk due to ESG factors
    carbon_cost_impact: float
    transition_risk_impact: float
    physical_risk_impact: float
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)


class ESGRiskService:
    """ESG Risk Integration Service."""

    def __init__(self):
        """Initialize ESG risk service."""
        self.carbon_service = get_carbon_rec_service()
        self.risk_service = get_risk_engine_service()
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # ESG scoring weights
        self.esg_weights = {
            ESGRiskCategory.ENVIRONMENTAL: 0.4,
            ESGRiskCategory.SOCIAL: 0.2,
            ESGRiskCategory.GOVERNANCE: 0.2,
            ESGRiskCategory.CLIMATE: 0.2
        }

        # Risk adjustment factors
        self.risk_adjustment_factors = {
            "carbon_intensity_high": 1.2,  # 20% higher risk for high carbon intensity
            "transition_risk_high": 1.15,  # 15% higher risk for high transition risk
            "physical_risk_high": 1.1,    # 10% higher risk for high physical risk
            "governance_risk_high": 1.05   # 5% higher risk for poor governance
        }

        self.logger = logging.getLogger(__name__)

    async def calculate_esg_adjusted_risk(
        self,
        portfolio_id: str,
        risk_config: RiskDistributionConfig,
        carbon_pricing_scenario: str = "moderate"
    ) -> ESGAdjustedRiskResult:
        """Calculate ESG-adjusted risk metrics for a portfolio.

        Args:
            portfolio_id: Portfolio identifier
            risk_config: Risk calculation configuration
            carbon_pricing_scenario: Carbon pricing scenario ("low", "moderate", "high")

        Returns:
            ESG-adjusted risk calculation result
        """
        try:
            # Get base risk calculation
            base_risk_result = await self.risk_service.calculate_portfolio_risk_metrics(
                portfolio_id, risk_config
            )

            # Get ESG analysis
            esg_analysis = await self.calculate_portfolio_esg_analysis(portfolio_id)

            # Calculate ESG adjustments
            esg_adjustments = await self._calculate_esg_risk_adjustments(
                esg_analysis, carbon_pricing_scenario
            )

            # Apply ESG adjustments to base risk metrics
            adjusted_var = base_risk_result.var_95 * (1 + esg_adjustments.get("total_adjustment", 0))
            adjusted_cvar = base_risk_result.cvar_95 * (1 + esg_adjustments.get("total_adjustment", 0))
            adjusted_volatility = base_risk_result.volatility * (1 + esg_adjustments.get("volatility_adjustment", 0))

            # Calculate ESG risk premium
            esg_risk_premium = await self._calculate_esg_risk_premium(esg_analysis)

            # Calculate carbon cost impact
            carbon_cost_impact = esg_analysis.carbon_cost_impact

            # Calculate transition and physical risk impacts
            transition_risk_impact = esg_adjustments.get("transition_risk_adjustment", 0)
            physical_risk_impact = esg_adjustments.get("physical_risk_adjustment", 0)

            # Calculate confidence intervals
            confidence_intervals = await self._calculate_confidence_intervals(
                adjusted_var, adjusted_cvar, adjusted_volatility, esg_analysis
            )

            result = ESGAdjustedRiskResult(
                portfolio_id=portfolio_id,
                base_risk_result=base_risk_result,
                esg_adjustments=esg_adjustments,
                adjusted_var=adjusted_var,
                adjusted_cvar=adjusted_cvar,
                adjusted_volatility=adjusted_volatility,
                esg_risk_premium=esg_risk_premium,
                carbon_cost_impact=carbon_cost_impact,
                transition_risk_impact=transition_risk_impact,
                physical_risk_impact=physical_risk_impact,
                confidence_intervals=confidence_intervals
            )

            # Cache result
            cache_key = f"esg_risk:{portfolio_id}:{datetime.utcnow().strftime('%Y%m%d')}"
            await self.cache_manager.set(
                cache_key,
                result.dict(),
                ttl_seconds=3600  # 1 hour cache
            )

            self.telemetry.info(
                "ESG-adjusted risk calculated",
                portfolio_id=portfolio_id,
                adjusted_var=adjusted_var,
                esg_risk_premium=esg_risk_premium
            )

            return result

        except Exception as e:
            self.telemetry.error("ESG risk calculation failed", portfolio_id=portfolio_id, error=str(e))
            raise

    async def calculate_portfolio_esg_analysis(self, portfolio_id: str) -> ESGPortfolioAnalysis:
        """Calculate comprehensive ESG analysis for a portfolio.

        Args:
            portfolio_id: Portfolio identifier

        Returns:
            ESG portfolio analysis
        """
        try:
            # Get carbon exposure data
            carbon_exposure = await self.carbon_service.calculate_portfolio_carbon_exposure(portfolio_id)

            # Calculate ESG metrics
            esg_metrics = await self._calculate_esg_metrics(portfolio_id, carbon_exposure)

            # Calculate overall ESG score
            overall_score = self._calculate_overall_esg_score(esg_metrics)

            # Calculate risk scores
            overall_risk_score = self._calculate_overall_risk_score(esg_metrics)
            carbon_intensity = carbon_exposure.get("carbon_intensity", 0)
            carbon_cost_impact = carbon_exposure.get("carbon_cost_per_mwh", 0)

            # Calculate category-specific risk scores
            transition_risk = self._calculate_transition_risk_score(esg_metrics, carbon_exposure)
            physical_risk = self._calculate_physical_risk_score(esg_metrics)
            governance_risk = self._calculate_governance_risk_score(esg_metrics)
            social_risk = self._calculate_social_risk_score(esg_metrics)

            # Generate recommendations
            recommendations = await self._generate_esg_recommendations(
                esg_metrics, carbon_exposure
            )

            analysis = ESGPortfolioAnalysis(
                portfolio_id=portfolio_id,
                analysis_date=datetime.utcnow(),
                overall_esg_score=overall_score,
                overall_risk_score=overall_risk_score,
                carbon_intensity=carbon_intensity,
                carbon_cost_impact=carbon_cost_impact,
                transition_risk_score=transition_risk,
                physical_risk_score=physical_risk,
                governance_risk_score=governance_risk,
                social_risk_score=social_risk,
                esg_metrics=esg_metrics,
                recommendations=recommendations
            )

            self.telemetry.info(
                "ESG analysis completed",
                portfolio_id=portfolio_id,
                overall_score=overall_score.value,
                risk_score=overall_risk_score
            )

            return analysis

        except Exception as e:
            self.telemetry.error("ESG analysis failed", portfolio_id=portfolio_id, error=str(e))
            raise

    async def _calculate_esg_metrics(
        self,
        portfolio_id: str,
        carbon_exposure: Dict[str, Any]
    ) -> List[ESGRiskMetric]:
        """Calculate detailed ESG metrics."""
        metrics = []

        # Carbon intensity metric
        carbon_intensity = carbon_exposure.get("carbon_intensity", 0)
        metrics.append(ESGRiskMetric(
            metric_name="carbon_intensity",
            category=ESGRiskCategory.CLIMATE,
            value=carbon_intensity,
            unit="tons_CO2_per_MWh",
            percentile=self._calculate_percentile(carbon_intensity, "carbon_intensity"),
            risk_level=self._get_risk_level(carbon_intensity, "carbon_intensity"),
            description="Carbon emissions intensity of portfolio",
            calculation_method="weighted_average"
        ))

        # Carbon cost impact metric
        carbon_cost = carbon_exposure.get("carbon_cost_per_mwh", 0)
        metrics.append(ESGRiskMetric(
            metric_name="carbon_cost_impact",
            category=ESGRiskCategory.CLIMATE,
            value=carbon_cost,
            unit="usd_per_MWh",
            risk_level=self._get_risk_level(carbon_cost, "carbon_cost"),
            description="Financial impact of carbon pricing",
            calculation_method="carbon_pricing_model"
        ))

        # Transition risk metric
        transition_risk = self._calculate_transition_risk_value(carbon_exposure)
        metrics.append(ESGRiskMetric(
            metric_name="transition_risk",
            category=ESGRiskCategory.TRANSITION,
            value=transition_risk,
            unit="risk_score",
            risk_level=self._get_risk_level(transition_risk, "transition_risk"),
            description="Risk from transition to low-carbon economy"
        ))

        # Physical risk metric (simplified)
        physical_risk = 25.0  # Mock calculation
        metrics.append(ESGRiskMetric(
            metric_name="physical_risk",
            category=ESGRiskCategory.PHYSICAL,
            value=physical_risk,
            unit="risk_score",
            risk_level=self._get_risk_level(physical_risk, "physical_risk"),
            description="Risk from physical climate impacts"
        ))

        # Governance risk metric (simplified)
        governance_risk = 30.0  # Mock calculation
        metrics.append(ESGRiskMetric(
            metric_name="governance_risk",
            category=ESGRiskCategory.GOVERNANCE,
            value=governance_risk,
            unit="risk_score",
            risk_level=self._get_risk_level(governance_risk, "governance_risk"),
            description="Corporate governance risk score"
        ))

        # Social risk metric (simplified)
        social_risk = 20.0  # Mock calculation
        metrics.append(ESGRiskMetric(
            metric_name="social_risk",
            category=ESGRiskCategory.SOCIAL,
            value=social_risk,
            unit="risk_score",
            risk_level=self._get_risk_level(social_risk, "social_risk"),
            description="Social responsibility risk score"
        ))

        return metrics

    def _calculate_overall_esg_score(self, metrics: List[ESGRiskMetric]) -> ESGScore:
        """Calculate overall ESG score from metrics."""
        # Simplified scoring - in reality would be more sophisticated
        total_score = 0
        for metric in metrics:
            # Convert risk scores to ESG scores (lower risk = higher ESG score)
            esg_component = max(0, 100 - metric.value)
            total_score += esg_component * self.esg_weights.get(metric.category, 0.2)

        avg_score = total_score / len(metrics) if metrics else 50

        # Map to ESG score categories
        if avg_score >= 90:
            return ESGScore.AAA
        elif avg_score >= 80:
            return ESGScore.AA
        elif avg_score >= 70:
            return ESGScore.A
        elif avg_score >= 60:
            return ESGScore.BBB
        elif avg_score >= 50:
            return ESGScore.BB
        elif avg_score >= 40:
            return ESGScore.B
        else:
            return ESGScore.CCC

    def _calculate_overall_risk_score(self, metrics: List[ESGRiskMetric]) -> float:
        """Calculate overall risk score from ESG metrics."""
        total_risk = 0
        for metric in metrics:
            total_risk += metric.value * self.esg_weights.get(metric.category, 0.2)

        return min(total_risk, 100.0)  # Cap at 100

    def _calculate_transition_risk_score(self, metrics: List[ESGRiskMetric], carbon_exposure: Dict[str, Any]) -> float:
        """Calculate transition risk score."""
        carbon_metric = next((m for m in metrics if m.metric_name == "carbon_intensity"), None)
        carbon_cost_metric = next((m for m in metrics if m.metric_name == "carbon_cost_impact"), None)

        transition_risk = 0
        if carbon_metric:
            transition_risk += carbon_metric.value * 0.6
        if carbon_cost_metric:
            transition_risk += carbon_cost_metric.value * 0.4

        return min(transition_risk, 100.0)

    def _calculate_physical_risk_score(self, metrics: List[ESGRiskMetric]) -> float:
        """Calculate physical risk score."""
        physical_metric = next((m for m in metrics if m.metric_name == "physical_risk"), None)
        return physical_metric.value if physical_metric else 25.0

    def _calculate_governance_risk_score(self, metrics: List[ESGRiskMetric]) -> float:
        """Calculate governance risk score."""
        governance_metric = next((m for m in metrics if m.metric_name == "governance_risk"), None)
        return governance_metric.value if governance_metric else 30.0

    def _calculate_social_risk_score(self, metrics: List[ESGRiskMetric]) -> float:
        """Calculate social risk score."""
        social_metric = next((m for m in metrics if m.metric_name == "social_risk"), None)
        return social_metric.value if social_metric else 20.0

    def _calculate_transition_risk_value(self, carbon_exposure: Dict[str, Any]) -> float:
        """Calculate transition risk value from carbon exposure."""
        carbon_intensity = carbon_exposure.get("carbon_intensity", 0)
        carbon_cost = carbon_exposure.get("carbon_cost_per_mwh", 0)

        # Simple transition risk calculation
        transition_risk = (carbon_intensity * 0.3) + (carbon_cost * 0.7)
        return min(transition_risk, 100.0)

    def _calculate_percentile(self, value: float, metric_type: str) -> float:
        """Calculate percentile ranking for a metric."""
        # Mock percentile calculation - in reality would use historical data
        if metric_type == "carbon_intensity":
            return max(0, min(100, 100 - (value / 5) * 10))  # Lower carbon intensity = higher percentile
        else:
            return 50.0  # Default to median

    def _get_risk_level(self, value: float, metric_type: str) -> str:
        """Determine risk level for a metric value."""
        if metric_type == "carbon_intensity":
            if value > 0.8:
                return "critical"
            elif value > 0.6:
                return "high"
            elif value > 0.4:
                return "medium"
            else:
                return "low"
        elif metric_type == "carbon_cost":
            if value > 10:
                return "high"
            elif value > 5:
                return "medium"
            else:
                return "low"
        else:
            if value > 70:
                return "high"
            elif value > 40:
                return "medium"
            else:
                return "low"

    async def _calculate_esg_risk_adjustments(
        self,
        esg_analysis: ESGPortfolioAnalysis,
        carbon_pricing_scenario: str
    ) -> Dict[str, float]:
        """Calculate ESG risk adjustments."""
        adjustments = {}

        # Carbon intensity adjustment
        carbon_intensity = esg_analysis.carbon_intensity
        if carbon_intensity > 0.5:
            adjustments["carbon_intensity_adjustment"] = 0.2  # 20% increase
        elif carbon_intensity > 0.3:
            adjustments["carbon_intensity_adjustment"] = 0.1  # 10% increase
        else:
            adjustments["carbon_intensity_adjustment"] = 0.0

        # Transition risk adjustment
        if esg_analysis.transition_risk_score > 60:
            adjustments["transition_risk_adjustment"] = 0.15
        elif esg_analysis.transition_risk_score > 40:
            adjustments["transition_risk_adjustment"] = 0.08
        else:
            adjustments["transition_risk_adjustment"] = 0.0

        # Physical risk adjustment
        if esg_analysis.physical_risk_score > 50:
            adjustments["physical_risk_adjustment"] = 0.1
        else:
            adjustments["physical_risk_adjustment"] = 0.0

        # Carbon pricing scenario adjustment
        if carbon_pricing_scenario == "high":
            adjustments["carbon_pricing_adjustment"] = 0.25
        elif carbon_pricing_scenario == "moderate":
            adjustments["carbon_pricing_adjustment"] = 0.15
        else:
            adjustments["carbon_pricing_adjustment"] = 0.05

        # Total adjustment
        adjustments["total_adjustment"] = sum(adjustments.values())

        # Volatility adjustment based on ESG factors
        adjustments["volatility_adjustment"] = adjustments["total_adjustment"] * 0.5

        return adjustments

    async def _calculate_esg_risk_premium(self, esg_analysis: ESGPortfolioAnalysis) -> float:
        """Calculate ESG risk premium."""
        # Risk premium based on ESG risk score
        base_premium = esg_analysis.overall_risk_score * 0.02  # 2% per risk point

        # Additional premium for high carbon intensity
        if esg_analysis.carbon_intensity > 0.5:
            base_premium += 0.05  # Additional 5%

        return base_premium

    async def _calculate_confidence_intervals(
        self,
        adjusted_var: float,
        adjusted_cvar: float,
        adjusted_volatility: float,
        esg_analysis: ESGPortfolioAnalysis
    ) -> Dict[str, Tuple[float, float]]:
        """Calculate confidence intervals for adjusted risk metrics."""
        # Simplified confidence interval calculation
        confidence_level = 0.95
        z_score = 1.96  # For 95% confidence

        intervals = {}

        # VaR confidence interval
        var_std_error = adjusted_volatility * 0.1  # Simplified
        var_margin = z_score * var_std_error
        intervals["var_95"] = (adjusted_var - var_margin, adjusted_var + var_margin)

        # CVaR confidence interval
        cvar_std_error = adjusted_volatility * 0.15  # Simplified
        cvar_margin = z_score * cvar_std_error
        intervals["cvar_95"] = (adjusted_cvar - cvar_margin, adjusted_cvar + cvar_margin)

        # Volatility confidence interval
        vol_std_error = adjusted_volatility * 0.05  # Simplified
        vol_margin = z_score * vol_std_error
        intervals["volatility"] = (adjusted_volatility - vol_margin, adjusted_volatility + vol_margin)

        return intervals

    async def _generate_esg_recommendations(
        self,
        esg_metrics: List[ESGRiskMetric],
        carbon_exposure: Dict[str, Any]
    ) -> List[str]:
        """Generate ESG improvement recommendations."""
        recommendations = []

        # Carbon-related recommendations
        carbon_metric = next((m for m in esg_metrics if m.metric_name == "carbon_intensity"), None)
        if carbon_metric and carbon_metric.value > 0.5:
            recommendations.append(
                "Reduce portfolio carbon intensity through asset reallocation or carbon offset purchases"
            )
            recommendations.append(
                "Implement carbon pricing stress testing to assess transition risk exposure"
            )

        # Risk-based recommendations
        high_risk_metrics = [m for m in esg_metrics if m.risk_level in ["high", "critical"]]
        if high_risk_metrics:
            recommendations.append(
                f"Address {len(high_risk_metrics)} high-risk ESG factors through targeted risk mitigation"
            )

        # General ESG recommendations
        if not recommendations:
            recommendations.append(
                "Continue monitoring ESG factors and consider ESG-focused investment strategies"
            )

        return recommendations

    async def get_portfolio_dashboard_data(self, portfolio_id: str) -> Dict[str, Any]:
        """Get comprehensive ESG dashboard data for a portfolio.

        Args:
            portfolio_id: Portfolio identifier

        Returns:
            Dashboard data including ESG metrics and risk analysis
        """
        try:
            # Get ESG analysis
            esg_analysis = await self.calculate_portfolio_esg_analysis(portfolio_id)

            # Get ESG-adjusted risk metrics
            risk_config = RiskDistributionConfig(
                distribution_type="normal",
                parameters={"mu": 0.0, "sigma": 0.2}
            )
            risk_result = await self.calculate_esg_adjusted_risk(
                portfolio_id, risk_config
            )

            # Compile dashboard data
            dashboard_data = {
                "portfolio_id": portfolio_id,
                "esg_analysis": esg_analysis.dict(),
                "risk_metrics": {
                    "base_var": risk_result.base_risk_result.var_95,
                    "adjusted_var": risk_result.adjusted_var,
                    "base_cvar": risk_result.base_risk_result.cvar_95,
                    "adjusted_cvar": risk_result.adjusted_cvar,
                    "base_volatility": risk_result.base_risk_result.volatility,
                    "adjusted_volatility": risk_result.adjusted_volatility,
                    "esg_risk_premium": risk_result.esg_risk_premium,
                    "carbon_cost_impact": risk_result.carbon_cost_impact
                },
                "esg_breakdown": {
                    "environmental_score": self._calculate_category_score(
                        esg_analysis.esg_metrics, ESGRiskCategory.ENVIRONMENTAL
                    ),
                    "social_score": self._calculate_category_score(
                        esg_analysis.esg_metrics, ESGRiskCategory.SOCIAL
                    ),
                    "governance_score": self._calculate_category_score(
                        esg_analysis.esg_metrics, ESGRiskCategory.GOVERNANCE
                    ),
                    "climate_score": self._calculate_category_score(
                        esg_analysis.esg_metrics, ESGRiskCategory.CLIMATE
                    )
                },
                "risk_adjustments": risk_result.esg_adjustments,
                "recommendations": esg_analysis.recommendations,
                "last_updated": datetime.utcnow().isoformat()
            }

            return dashboard_data

        except Exception as e:
            self.telemetry.error("Dashboard data generation failed", portfolio_id=portfolio_id, error=str(e))
            raise

    def _calculate_category_score(self, metrics: List[ESGRiskMetric], category: ESGRiskCategory) -> float:
        """Calculate ESG score for a specific category."""
        category_metrics = [m for m in metrics if m.category == category]
        if not category_metrics:
            return 50.0  # Default score

        # Average the category metrics (inverted risk scores)
        avg_risk = sum(m.value for m in category_metrics) / len(category_metrics)
        return max(0, 100 - avg_risk)

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "carbon_service_available": True,  # Would check actual service health
            "risk_service_available": True,
            "last_check": datetime.utcnow(),
            "esg_weights": self.esg_weights,
            "risk_adjustment_factors": self.risk_adjustment_factors
        }


def get_esg_risk_service() -> ESGRiskService:
    """Get the global ESG risk service instance."""
    return ESGRiskService()


async def get_portfolio_esg_dashboard(portfolio_id: str) -> Dict[str, Any]:
    """Get ESG dashboard data for a portfolio."""
    service = get_esg_risk_service()
    return await service.get_portfolio_dashboard_data(portfolio_id)


async def calculate_esg_adjusted_risk(
    portfolio_id: str,
    carbon_pricing_scenario: str = "moderate"
) -> ESGAdjustedRiskResult:
    """Calculate ESG-adjusted risk metrics."""
    service = get_esg_risk_service()

    risk_config = RiskDistributionConfig(
        distribution_type="normal",
        parameters={"mu": 0.0, "sigma": 0.2}
    )

    return await service.calculate_esg_adjusted_risk(
        portfolio_id, risk_config, carbon_pricing_scenario
    )
