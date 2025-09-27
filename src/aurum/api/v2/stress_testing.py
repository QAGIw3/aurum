"""v2 Stress Testing API for extreme scenario analysis and P&L views.

This module provides REST endpoints for:
- Managing stress test templates and scenarios
- Running batch stress test analysis
- Viewing P&L impact analysis and comparisons
- Historical shock pack analysis
- Interactive scenario configuration
- Risk scenario impact assessment
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ...scenarios.stress_testing import (
    get_stress_test_engine,
    StressTestTemplate,
    StressTestConfig,
    ScenarioImpact
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/stress-testing", tags=["stress-testing"])


class StressTestTemplateResponse(BaseModel):
    """Response containing stress test template information."""

    template_id: str
    name: str
    description: str
    category: str
    parameters: Dict[str, any]
    default_config: Dict[str, any]
    created_by: str
    created_at: datetime
    version: str


class StressTestTemplateListResponse(BaseModel):
    """Response for listing stress test templates."""

    data: List[StressTestTemplateResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class ScenarioRunRequest(BaseModel):
    """Request to run a stress test scenario."""

    template_id: str = Field(..., description="Template to use for the scenario")
    scenario_name: str = Field(..., description="Name for this scenario run")
    severity: str = Field("medium", description="Severity level")
    duration_hours: int = Field(24, description="Duration in hours")
    affected_regions: List[str] = Field(default_factory=list, description="Affected regions")
    probability: float = Field(0.01, description="Scenario probability")
    impact_multiplier: float = Field(1.0, description="Impact multiplier")


class ScenarioImpactResponse(BaseModel):
    """Response containing scenario impact analysis."""

    scenario_id: str
    scenario_name: str
    affected_curves: List[str]
    price_impact: Dict[str, float]
    volume_impact: Dict[str, float]
    confidence_level: float
    risk_metrics: Dict[str, any]
    affected_positions: List[str]
    portfolio_impact: float
    recovery_timeline: List[Dict[str, any]]


class StressTestRunResponse(BaseModel):
    """Response containing stress test run results."""

    run_id: str
    scenario_name: str
    template_id: str
    status: str
    progress: float
    impact_analysis: List[ScenarioImpactResponse]
    summary_metrics: Dict[str, any]
    execution_time: float
    created_at: datetime


class StressTestBatchResponse(BaseModel):
    """Response containing batch stress test results."""

    batch_id: str
    scenarios: List[StressTestRunResponse]
    comparison_metrics: Dict[str, any]
    portfolio_impact_summary: Dict[str, any]
    risk_assessment: Dict[str, any]


class PLImpactResponse(BaseModel):
    """Response containing P&L impact analysis."""

    portfolio_id: str
    scenario_id: str
    baseline_pnl: float
    stressed_pnl: float
    pnl_impact: float
    pnl_impact_percent: float
    risk_factors: Dict[str, float]
    recommendations: List[str]
    analysis_date: datetime


@router.get("/templates", response_model=StressTestTemplateListResponse)
async def list_stress_test_templates(
    request: Request,
    response: Response,
    category: Optional[str] = Query(None, description="Filter by template category"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
) -> StressTestTemplateListResponse:
    """List available stress test templates with filtering."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()

        # Get templates (mock implementation)
        templates = await engine.list_templates(category=category)

        # Apply pagination
        paginated_templates = templates[offset:offset + limit]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        template_responses = [
            StressTestTemplateResponse(
                template_id=template.template_id,
                name=template.name,
                description=template.description,
                category=template.category,
                parameters=template.parameters,
                default_config=template.default_config.dict() if hasattr(template.default_config, 'dict') else template.default_config,
                created_by=template.created_by,
                created_at=template.created_at,
                version=template.version
            )
            for template in paginated_templates
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_stress_test_templates",
            query_time_ms=query_time_ms,
            record_count=len(template_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return StressTestTemplateListResponse(
            data=template_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_stress_test_templates",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list stress test templates: {str(exc)}"
        )


@router.get("/templates/{template_id}", response_model=StressTestTemplateResponse)
async def get_stress_test_template(
    request: Request,
    template_id: str
) -> StressTestTemplateResponse:
    """Get a specific stress test template."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()
        template = await engine.get_template(template_id)

        if not template:
            raise HTTPException(status_code=404, detail="Template not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_stress_test_template",
            query_time_ms=query_time_ms
        )

        return StressTestTemplateResponse(
            template_id=template.template_id,
            name=template.name,
            description=template.description,
            category=template.category,
            parameters=template.parameters,
            default_config=template.default_config.dict() if hasattr(template.default_config, 'dict') else template.default_config,
            created_by=template.created_by,
            created_at=template.created_at,
            version=template.version
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_stress_test_template",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get stress test template: {str(exc)}"
        )


@router.post("/scenarios", response_model=StressTestRunResponse, status_code=201)
async def run_stress_test_scenario(
    request: Request,
    scenario_data: ScenarioRunRequest
) -> StressTestRunResponse:
    """Run a stress test scenario."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()

        # Get template
        template = await engine.get_template(scenario_data.template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")

        # Create config from request
        config = StressTestConfig(
            scenario_type=template.category,
            severity=scenario_data.severity,
            duration_hours=scenario_data.duration_hours,
            affected_regions=scenario_data.affected_regions,
            probability=scenario_data.probability,
            impact_multiplier=scenario_data.impact_multiplier
        )

        # Run stress test (mock implementation)
        impact = await engine.run_stress_test(
            template=template,
            config=config,
            scenario_name=scenario_data.scenario_name
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_stress_test_scenario",
            query_time_ms=query_time_ms
        )

        # Convert impact to response format
        impact_response = ScenarioImpactResponse(
            scenario_id=impact.scenario_id,
            scenario_name=scenario_data.scenario_name,
            affected_curves=impact.affected_curves,
            price_impact=impact.price_impact,
            volume_impact=impact.volume_impact,
            confidence_level=impact.confidence_level,
            risk_metrics=impact.risk_metrics,
            affected_positions=impact.affected_positions,
            portfolio_impact=impact.portfolio_impact,
            recovery_timeline=[
                {"timestamp": t[0], "recovery_percentage": t[1]}
                for t in impact.recovery_timeline
            ]
        )

        return StressTestRunResponse(
            run_id=str(uuid4()),
            scenario_name=scenario_data.scenario_name,
            template_id=scenario_data.template_id,
            status="completed",
            progress=1.0,
            impact_analysis=[impact_response],
            summary_metrics={
                "total_affected_curves": len(impact.affected_curves),
                "max_price_impact": max(impact.price_impact.values()) if impact.price_impact else 0,
                "portfolio_impact": impact.portfolio_impact
            },
            execution_time=query_time_ms / 1000,
            created_at=datetime.utcnow()
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_stress_test_scenario",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run stress test scenario: {str(exc)}"
        )


@router.post("/batch", response_model=StressTestBatchResponse, status_code=201)
async def run_batch_stress_tests(
    request: Request,
    scenarios: List[ScenarioRunRequest] = Field(..., description="List of scenarios to run"),
    compare_results: bool = Query(True, description="Compare results across scenarios")
) -> StressTestBatchResponse:
    """Run multiple stress test scenarios in batch."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()
        batch_results = []

        for scenario in scenarios:
            # Run each scenario
            result = await run_stress_test_scenario(request, scenario)
            batch_results.append(result)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_batch_stress_tests",
            query_time_ms=query_time_ms
        )

        # Generate comparison metrics if requested
        comparison_metrics = {}
        if compare_results and len(batch_results) > 1:
            portfolio_impacts = [r.summary_metrics["portfolio_impact"] for r in batch_results]
            comparison_metrics = {
                "max_impact": max(portfolio_impacts),
                "min_impact": min(portfolio_impacts),
                "avg_impact": sum(portfolio_impacts) / len(portfolio_impacts),
                "impact_range": max(portfolio_impacts) - min(portfolio_impacts)
            }

        # Generate portfolio impact summary
        portfolio_summary = {
            "total_scenarios": len(scenarios),
            "worst_case_impact": max([r.summary_metrics["portfolio_impact"] for r in batch_results]),
            "best_case_impact": min([r.summary_metrics["portfolio_impact"] for r in batch_results]),
            "average_impact": sum([r.summary_metrics["portfolio_impact"] for r in batch_results]) / len(batch_results)
        }

        return StressTestBatchResponse(
            batch_id=str(uuid4()),
            scenarios=batch_results,
            comparison_metrics=comparison_metrics,
            portfolio_impact_summary=portfolio_summary,
            risk_assessment={
                "overall_risk_level": "high" if portfolio_summary["average_impact"] > 0.3 else "medium",
                "risk_diversity": len(set([r.template_id for r in batch_results])),
                "temporal_distribution": "concentrated"  # Mock analysis
            }
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_batch_stress_tests",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run batch stress tests: {str(exc)}"
        )


@router.get("/scenarios/{scenario_id}/impact", response_model=ScenarioImpactResponse)
async def get_scenario_impact(
    request: Request,
    scenario_id: str
) -> ScenarioImpactResponse:
    """Get detailed impact analysis for a scenario."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()

        # Get scenario impact (mock implementation)
        # In real implementation, would retrieve from database
        impact = ScenarioImpact(
            scenario_id=scenario_id,
            affected_curves=["load_curve", "price_curve"],
            price_impact={"load_curve": 1.5, "price_curve": 2.0},
            volume_impact={"load_curve": 0.8, "price_curve": 0.9},
            confidence_level=0.85,
            risk_metrics={"volatility_increase": 0.3},
            affected_positions=["portfolio_123"],
            portfolio_impact=0.25,
            recovery_timeline=[
                (datetime.utcnow() + timedelta(hours=24), 0.5),
                (datetime.utcnow() + timedelta(hours=48), 0.8),
                (datetime.utcnow() + timedelta(hours=72), 1.0)
            ]
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_scenario_impact",
            query_time_ms=query_time_ms
        )

        return ScenarioImpactResponse(
            scenario_id=impact.scenario_id,
            scenario_name="Mock Scenario",  # Would be retrieved
            affected_curves=impact.affected_curves,
            price_impact=impact.price_impact,
            volume_impact=impact.volume_impact,
            confidence_level=impact.confidence_level,
            risk_metrics=impact.risk_metrics,
            affected_positions=impact.affected_positions,
            portfolio_impact=impact.portfolio_impact,
            recovery_timeline=[
                {"timestamp": t[0], "recovery_percentage": t[1]}
                for t in impact.recovery_timeline
            ]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_scenario_impact",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario impact: {str(exc)}"
        )


@router.get("/pnl/{portfolio_id}/{scenario_id}", response_model=PLImpactResponse)
async def get_pnl_impact(
    request: Request,
    portfolio_id: str,
    scenario_id: str,
    baseline_date: Optional[datetime] = Query(None, description="Baseline date for comparison")
) -> PLImpactResponse:
    """Get P&L impact analysis for a portfolio under a scenario."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()

        # Calculate P&L impact (mock implementation)
        baseline_pnl = 1000000.0  # $1M baseline
        stressed_pnl = 750000.0   # $750K stressed
        pnl_impact = baseline_pnl - stressed_pnl

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_pnl_impact",
            query_time_ms=query_time_ms
        )

        return PLImpactResponse(
            portfolio_id=portfolio_id,
            scenario_id=scenario_id,
            baseline_pnl=baseline_pnl,
            stressed_pnl=stressed_pnl,
            pnl_impact=pnl_impact,
            pnl_impact_percent=(pnl_impact / baseline_pnl) * 100,
            risk_factors={
                "price_volatility": 0.3,
                "demand_shock": 0.2,
                "correlation_breakdown": 0.1
            },
            recommendations=[
                "Consider hedging strategies for high-impact scenarios",
                "Diversify across geographic regions",
                "Implement dynamic risk limits"
            ],
            analysis_date=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_pnl_impact",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get P&L impact: {str(exc)}"
        )


@router.get("/health")
async def get_stress_testing_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get stress testing service health status."""
    start_time = time.perf_counter()

    try:
        engine = get_stress_test_engine()

        # Get service health (mock implementation)
        health = {
            "status": "healthy",
            "templates_available": 10,  # Mock count
            "active_scenarios": 0,      # Mock count
            "last_execution": datetime.utcnow()
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_stress_testing_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_stress_testing_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get stress testing health: {str(exc)}"
        )
