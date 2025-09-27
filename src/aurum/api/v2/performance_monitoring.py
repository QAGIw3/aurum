"""v2 Performance Monitoring API for budgets and regression harness.

This module provides REST endpoints for:
- Performance budget management and compliance checking
- k6 load testing scenario execution and monitoring
- Prometheus metrics collection and comparison
- CI/CD integration with performance gating
- Regression detection and alerting
- Performance trend analysis and reporting
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.performance_monitoring_service import (
    get_performance_monitoring_service,
    PerformanceBudget,
    LoadTestScenario,
    PerformanceTestResult,
    PerformanceComparison
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/performance", tags=["performance"])


class BudgetCreateRequest(BaseModel):
    """Request to create a performance budget."""

    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field(..., description="HTTP method")
    p95_latency_ms: float = Field(..., description="P95 latency budget in ms")
    p99_latency_ms: float = Field(..., description="P99 latency budget in ms")
    max_error_rate: float = Field(..., description="Maximum error rate (0-1)")
    min_throughput_rps: float = Field(..., description="Minimum throughput in RPS")
    max_memory_mb: float = Field(..., description="Maximum memory usage in MB")
    max_cpu_percent: float = Field(..., description="Maximum CPU usage percentage")
    description: str = Field(..., description="Budget description")
    enabled: bool = Field(True, description="Whether budget is enabled")


class ScenarioCreateRequest(BaseModel):
    """Request to create a load test scenario."""

    scenario_id: str = Field(..., description="Scenario identifier")
    name: str = Field(..., description="Scenario name")
    description: str = Field(..., description="Scenario description")
    script_path: str = Field(..., description="Path to k6 script")
    duration_seconds: int = Field(300, description="Test duration in seconds")
    virtual_users: int = Field(50, description="Number of virtual users")
    ramp_up_seconds: int = Field(30, description="Ramp up time in seconds")
    ramp_down_seconds: int = Field(30, description="Ramp down time in seconds")
    environment: str = Field("staging", description="Target environment")
    thresholds: Dict[str, str] = Field(default_factory=dict, description="k6 thresholds")
    enabled: bool = Field(True, description="Whether scenario is enabled")


class BudgetResponse(BaseModel):
    """Response containing budget information."""

    endpoint: str
    method: str
    p95_latency_ms: float
    p99_latency_ms: float
    max_error_rate: float
    min_throughput_rps: float
    max_memory_mb: float
    max_cpu_percent: float
    description: str
    enabled: bool
    created_at: datetime
    compliance_status: str  # "compliant", "violated", "unknown"


class ScenarioResponse(BaseModel):
    """Response containing scenario information."""

    scenario_id: str
    name: str
    description: str
    script_path: str
    duration_seconds: int
    virtual_users: int
    ramp_up_seconds: int
    ramp_down_seconds: int
    environment: str
    thresholds: Dict[str, str]
    enabled: bool
    created_at: datetime


class TestResultResponse(BaseModel):
    """Response containing test result information."""

    test_id: str
    scenario_id: str
    status: str
    metrics: Dict[str, float]
    start_time: datetime
    end_time: Optional[datetime]
    execution_time: Optional[float]
    errors: List[str]


class PerformanceDashboardResponse(BaseModel):
    """Response containing performance dashboard."""

    budget_compliance: Dict[str, bool]
    recent_tests: List[Dict[str, any]]
    performance_trends: Dict[str, any]
    recommendations: List[str]
    last_updated: datetime


class ComparisonResponse(BaseModel):
    """Response containing performance comparison."""

    comparison_id: str
    baseline_test_id: str
    current_test_id: str
    comparison_metrics: Dict[str, float]
    regression_detected: bool
    improvement_areas: List[str]
    degradation_areas: List[str]
    recommendations: List[str]


@router.get("/budgets", response_model=Dict[str, any])
async def list_performance_budgets(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """List all performance budgets with compliance status."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Get budgets with compliance status
        budgets = []
        for budget_key, budget in service._budgets.items():
            compliance_status = "unknown"

            # Check compliance (mock implementation)
            compliance_status = "compliant"  # Would check actual metrics

            budgets.append({
                "endpoint": budget.endpoint,
                "method": budget.method,
                "p95_latency_ms": budget.p95_latency_ms,
                "p99_latency_ms": budget.p99_latency_ms,
                "max_error_rate": budget.max_error_rate,
                "min_throughput_rps": budget.min_throughput_rps,
                "max_memory_mb": budget.max_memory_mb,
                "max_cpu_percent": budget.max_cpu_percent,
                "description": budget.description,
                "enabled": budget.enabled,
                "compliance_status": compliance_status
            })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_performance_budgets",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": budgets
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_performance_budgets",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list performance budgets: {str(exc)}"
        )


@router.post("/budgets", response_model=Dict[str, any], status_code=201)
async def create_performance_budget(
    request: Request,
    budget_data: BudgetCreateRequest
) -> Dict[str, any]:
    """Create a new performance budget."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Create budget
        budget = PerformanceBudget(
            endpoint=budget_data.endpoint,
            method=budget_data.method,
            p95_latency_ms=budget_data.p95_latency_ms,
            p99_latency_ms=budget_data.p99_latency_ms,
            max_error_rate=budget_data.max_error_rate,
            min_throughput_rps=budget_data.min_throughput_rps,
            max_memory_mb=budget_data.max_memory_mb,
            max_cpu_percent=budget_data.max_cpu_percent,
            description=budget_data.description,
            enabled=budget_data.enabled
        )

        # Store budget (mock implementation)
        budget_key = f"{budget.method}:{budget.endpoint}"
        service._budgets[budget_key] = budget

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_performance_budget",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="create_performance_budget",
                query_time_ms=query_time_ms
            ),
            "data": {
                "budget_key": budget_key,
                "message": "Performance budget created successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_performance_budget",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create performance budget: {str(exc)}"
        )


@router.get("/scenarios", response_model=Dict[str, any])
async def list_load_test_scenarios(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """List available load test scenarios."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Get scenarios
        scenarios = []
        for scenario_id, scenario in service._scenarios.items():
            scenarios.append({
                "scenario_id": scenario.scenario_id,
                "name": scenario.name,
                "description": scenario.description,
                "script_path": scenario.script_path,
                "duration_seconds": scenario.duration_seconds,
                "virtual_users": scenario.virtual_users,
                "environment": scenario.environment,
                "enabled": scenario.enabled
            })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_load_test_scenarios",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": scenarios
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_load_test_scenarios",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list load test scenarios: {str(exc)}"
        )


@router.post("/scenarios/{scenario_id}/run", response_model=Dict[str, any], status_code=202)
async def run_load_test_scenario(
    request: Request,
    scenario_id: str
) -> Dict[str, any]:
    """Execute a load test scenario."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Run scenario
        test_id = await service.run_k6_scenario(scenario_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_load_test_scenario",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="run_load_test_scenario",
                query_time_ms=query_time_ms
            ),
            "data": {
                "test_id": test_id,
                "status": "running",
                "message": "Load test scenario started successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_load_test_scenario",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run load test scenario: {str(exc)}"
        )


@router.get("/tests/{test_id}", response_model=TestResultResponse)
async def get_test_result(
    request: Request,
    test_id: str
) -> TestResultResponse:
    """Get test execution result."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()
        test_result = service._test_results.get(test_id)

        if not test_result:
            raise HTTPException(status_code=404, detail="Test result not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_test_result",
            query_time_ms=query_time_ms
        )

        return TestResultResponse(
            test_id=test_result.test_id,
            scenario_id=test_result.scenario_id,
            status=test_result.status,
            metrics=test_result.metrics,
            start_time=test_result.start_time,
            end_time=test_result.end_time,
            execution_time=(test_result.end_time - test_result.start_time).total_seconds() if test_result.end_time else None,
            errors=test_result.errors
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_test_result",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get test result: {str(exc)}"
        )


@router.get("/dashboard", response_model=PerformanceDashboardResponse)
async def get_performance_dashboard(
    request: Request,
    response: Response
) -> PerformanceDashboardResponse:
    """Get comprehensive performance dashboard."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Get dashboard data
        dashboard = await service.get_performance_dashboard()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_performance_dashboard",
            query_time_ms=query_time_ms
        )

        return PerformanceDashboardResponse(
            budget_compliance=dashboard["budget_compliance"],
            recent_tests=dashboard["recent_tests"],
            performance_trends=dashboard["performance_trends"],
            recommendations=dashboard["recommendations"],
            last_updated=dashboard["last_updated"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_performance_dashboard",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get performance dashboard: {str(exc)}"
        )


@router.post("/compare", response_model=ComparisonResponse, status_code=201)
async def compare_performance_runs(
    request: Request,
    baseline_test_id: str = Query(..., description="Baseline test ID"),
    current_test_id: str = Query(..., description="Current test ID")
) -> ComparisonResponse:
    """Compare two performance test runs for regression detection."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Compare performance
        comparison_id = await service.compare_performance(baseline_test_id, current_test_id)

        comparison = service._comparisons[comparison_id]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="compare_performance_runs",
            query_time_ms=query_time_ms
        )

        return ComparisonResponse(
            comparison_id=comparison.comparison_id,
            baseline_test_id=comparison.baseline_test_id,
            current_test_id=comparison.current_test_id,
            comparison_metrics=comparison.comparison_metrics,
            regression_detected=comparison.regression_detected,
            improvement_areas=comparison.improvement_areas,
            degradation_areas=comparison.degradation_areas,
            recommendations=comparison.recommendations
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="compare_performance_runs",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare performance runs: {str(exc)}"
        )


@router.post("/ci-check", response_model=Dict[str, any], status_code=202)
async def run_ci_performance_check(
    request: Request
) -> Dict[str, any]:
    """Run performance check for CI/CD pipeline."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Run CI check
        result = await service.run_ci_performance_check()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_ci_performance_check",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="run_ci_performance_check",
                query_time_ms=query_time_ms
            ),
            "data": result
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_ci_performance_check",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run CI performance check: {str(exc)}"
        )


@router.get("/metrics/prometheus", response_model=Dict[str, any])
async def get_prometheus_metrics(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get Prometheus metrics for comparison."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()

        # Collect Prometheus metrics
        metrics = await service.collect_prometheus_metrics()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_prometheus_metrics",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": metrics
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_prometheus_metrics",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get Prometheus metrics: {str(exc)}"
        )


@router.get("/health")
async def get_performance_monitoring_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get performance monitoring service health status."""
    start_time = time.perf_counter()

    try:
        service = get_performance_monitoring_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_performance_monitoring_health",
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
            operation="get_performance_monitoring_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get performance monitoring health: {str(exc)}"
        )
