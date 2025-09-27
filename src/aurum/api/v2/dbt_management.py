"""v2 DBT Management API for model hardening and data mart management.

This module provides REST endpoints for:
- DBT model testing and validation
- Data mart creation and management
- Seed fixture generation for local development
- Lineage documentation and freshness monitoring
- Model dependency analysis and impact assessment
- Automated testing and validation workflows
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.dbt_management_service import (
    get_dbt_management_service,
    DBTModel,
    DataMart,
    TestFixture
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/dbt", tags=["dbt-management"])


class ModelTestRequest(BaseModel):
    """Request to run DBT tests."""

    model_names: List[str] = Field(..., description="Models to test")
    test_type: str = Field("all", description="Test type (all, unit, integration)")


class FixtureGenerationRequest(BaseModel):
    """Request to generate test fixtures."""

    fixture_names: List[str] = Field(..., description="Fixtures to generate")
    generation_strategy: str = Field("sample", description="Generation strategy")


class MartCreateRequest(BaseModel):
    """Request to create a data mart."""

    mart_name: str = Field(..., description="Mart name")
    description: str = Field(..., description="Mart description")
    business_domain: str = Field(..., description="Business domain")
    data_sources: List[str] = Field(..., description="Data source models")
    key_dimensions: List[str] = Field(..., description="Key dimensions")
    metrics: List[str] = Field(..., description="Metrics to aggregate")
    refresh_schedule: str = Field("daily", description="Refresh schedule")
    retention_days: int = Field(365, description="Data retention days")


class ModelResponse(BaseModel):
    """Response containing model information."""

    model_name: str
    model_path: str
    model_type: str
    schema_name: str
    materialization: str
    dependencies: List[str]
    tags: List[str]
    description: str
    status: str
    last_run: Optional[datetime]
    last_success: Optional[datetime]
    error_count: int


class MartResponse(BaseModel):
    """Response containing mart information."""

    mart_name: str
    description: str
    business_domain: str
    data_sources: List[str]
    key_dimensions: List[str]
    metrics: List[str]
    refresh_schedule: str
    retention_days: int
    documentation_url: Optional[str]


class TestResultResponse(BaseModel):
    """Response containing test results."""

    status: str
    total_tests: int
    passed_tests: int
    failed_tests: int
    test_details: List[Dict[str, any]]
    execution_time: float


class FixtureResponse(BaseModel):
    """Response containing fixture generation results."""

    fixture_name: str
    status: str
    records_generated: int
    file_path: str
    error: Optional[str]


class DependencyAnalysisResponse(BaseModel):
    """Response containing dependency analysis."""

    model_name: str
    upstream_dependencies: List[str]
    downstream_dependents: List[str]
    impact_score: int
    recommendations: List[str]


class LineageDocumentationResponse(BaseModel):
    """Response containing lineage documentation."""

    generated_at: datetime
    total_models: int
    total_marts: int
    lineage_graph: Dict[str, any]
    critical_paths: List[str]
    data_quality_checks: List[str]


class FreshnessCheckResponse(BaseModel):
    """Response containing freshness check results."""

    table_name: str
    last_updated: datetime
    freshness_hours: float
    threshold_hours: int
    status: str


@router.post("/test", response_model=TestResultResponse, status_code=202)
async def run_dbt_tests(
    request: Request,
    test_data: ModelTestRequest
) -> TestResultResponse:
    """Run DBT tests for specified models."""
    start_time = time.perf_counter()

    try:
        from ..services.dbt_management_service import run_model_tests

        # Run tests
        result = await run_model_tests(test_data.model_names)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_dbt_tests",
            query_time_ms=query_time_ms
        )

        return TestResultResponse(
            status=result["status"],
            total_tests=result["total_tests"],
            passed_tests=result["passed_tests"],
            failed_tests=result["failed_tests"],
            test_details=result["test_details"],
            execution_time=result["execution_time"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_dbt_tests",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run DBT tests: {str(exc)}"
        )


@router.post("/build", response_model=Dict[str, any], status_code=202)
async def run_dbt_build(
    request: Request,
    models: Optional[List[str]] = Query(None, description="Models to build")
) -> Dict[str, any]:
    """Run DBT build for models."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Run build
        result = await service.run_dbt_build(models)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_dbt_build",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="run_dbt_build",
                query_time_ms=query_time_ms
            ),
            "data": result
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_dbt_build",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run DBT build: {str(exc)}"
        )


@router.post("/fixtures/generate", response_model=Dict[str, any], status_code=201)
async def generate_test_fixtures(
    request: Request,
    fixture_data: FixtureGenerationRequest
) -> Dict[str, any]:
    """Generate test fixtures for local development."""
    start_time = time.perf_counter()

    try:
        from ..services.dbt_management_service import generate_development_fixtures

        # Generate fixtures
        results = await generate_development_fixtures(fixture_data.fixture_names)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        fixture_responses = [
            FixtureResponse(
                fixture_name=name,
                status=result["status"],
                records_generated=result.get("records_generated", 0),
                file_path=result.get("file_path", ""),
                error=result.get("error")
            )
            for name, result in results.items()
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="generate_test_fixtures",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="generate_test_fixtures",
                query_time_ms=query_time_ms
            ),
            "data": {
                "fixtures": fixture_responses,
                "total_generated": len([f for f in fixture_responses if f.status == "success"])
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="generate_test_fixtures",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate test fixtures: {str(exc)}"
        )


@router.post("/marts", response_model=Dict[str, any], status_code=201)
async def create_data_mart(
    request: Request,
    mart_data: MartCreateRequest
) -> Dict[str, any]:
    """Create a new data mart."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Create mart configuration
        mart = DataMart(
            mart_name=mart_data.mart_name,
            description=mart_data.description,
            business_domain=mart_data.business_domain,
            data_sources=mart_data.data_sources,
            key_dimensions=mart_data.key_dimensions,
            metrics=mart_data.metrics,
            refresh_schedule=mart_data.refresh_schedule,
            retention_days=mart_data.retention_days
        )

        # Create mart
        mart_name = await service.create_mart(mart)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_data_mart",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="create_data_mart",
                query_time_ms=query_time_ms
            ),
            "data": {
                "mart_name": mart_name,
                "message": "Data mart created successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_data_mart",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create data mart: {str(exc)}"
        )


@router.get("/models", response_model=List[ModelResponse])
async def list_dbt_models(
    request: Request,
    response: Response,
    model_type: Optional[str] = Query(None, description="Filter by model type"),
    status: Optional[str] = Query(None, description="Filter by status")
) -> List[ModelResponse]:
    """List DBT models with filtering."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Get models
        models = list(service._models.values())

        # Apply filters
        if model_type:
            models = [m for m in models if m.model_type == model_type]
        if status:
            models = [m for m in models if m.status == status]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        model_responses = [
            ModelResponse(
                model_name=model.model_name,
                model_path=model.model_path,
                model_type=model.model_type,
                schema_name=model.schema_name,
                materialization=model.materialization,
                dependencies=model.dependencies,
                tags=model.tags,
                description=model.description,
                status=model.status,
                last_run=model.last_run,
                last_success=model.last_success,
                error_count=model.error_count
            )
            for model in models
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="list_dbt_models",
            query_time_ms=query_time_ms
        )

        return model_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_dbt_models",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list DBT models: {str(exc)}"
        )


@router.get("/marts", response_model=List[MartResponse])
async def list_data_marts(
    request: Request,
    response: Response
) -> List[MartResponse]:
    """List available data marts."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Get marts
        marts = await service.get_mart_definitions()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        mart_responses = [
            MartResponse(
                mart_name=mart.mart_name,
                description=mart.description,
                business_domain=mart.business_domain,
                data_sources=mart.data_sources,
                key_dimensions=mart.key_dimensions,
                metrics=mart.metrics,
                refresh_schedule=mart.refresh_schedule,
                retention_days=mart.retention_days,
                documentation_url=mart.documentation_url
            )
            for mart in marts
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="list_data_marts",
            query_time_ms=query_time_ms
        )

        return mart_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_data_marts",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list data marts: {str(exc)}"
        )


@router.get("/models/{model_name}/dependencies", response_model=DependencyAnalysisResponse)
async def analyze_model_dependencies(
    request: Request,
    model_name: str
) -> DependencyAnalysisResponse:
    """Analyze model dependencies and impact."""
    start_time = time.perf_counter()

    try:
        from ..services.dbt_management_service import analyze_model_impact

        # Analyze dependencies
        analysis = await analyze_model_impact(model_name)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="analyze_model_dependencies",
            query_time_ms=query_time_ms
        )

        return DependencyAnalysisResponse(
            model_name=analysis["model_name"],
            upstream_dependencies=analysis["upstream_dependencies"],
            downstream_dependents=analysis["downstream_dependents"],
            impact_score=analysis["impact_score"],
            recommendations=analysis["recommendations"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="analyze_model_dependencies",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze model dependencies: {str(exc)}"
        )


@router.get("/lineage", response_model=LineageDocumentationResponse)
async def get_lineage_documentation(
    request: Request,
    response: Response
) -> LineageDocumentationResponse:
    """Get data lineage documentation."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Generate lineage documentation
        lineage = await service.generate_lineage_documentation()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_lineage_documentation",
            query_time_ms=query_time_ms
        )

        return LineageDocumentationResponse(
            generated_at=lineage["generated_at"],
            total_models=lineage["total_models"],
            total_marts=lineage["total_marts"],
            lineage_graph=lineage["lineage_graph"],
            critical_paths=lineage["critical_paths"],
            data_quality_checks=lineage["data_quality_checks"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_lineage_documentation",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get lineage documentation: {str(exc)}"
        )


@router.get("/freshness", response_model=List[FreshnessCheckResponse])
async def check_data_freshness(
    request: Request,
    response: Response
) -> List[FreshnessCheckResponse]:
    """Check data freshness across all tables."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()

        # Check freshness
        freshness_results = await service.check_data_freshness()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        freshness_responses = [
            FreshnessCheckResponse(
                table_name=table_name,
                last_updated=result["last_updated"],
                freshness_hours=result["freshness_hours"],
                threshold_hours=result["threshold_hours"],
                status=result["status"]
            )
            for table_name, result in freshness_results.items()
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="check_data_freshness",
            query_time_ms=query_time_ms
        )

        return freshness_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="check_data_freshness",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check data freshness: {str(exc)}"
        )


@router.get("/health")
async def get_dbt_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get DBT management service health status."""
    start_time = time.perf_counter()

    try:
        service = get_dbt_management_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_dbt_health",
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
            operation="get_dbt_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get DBT health: {str(exc)}"
        )
