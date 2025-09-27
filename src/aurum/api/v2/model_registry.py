"""v2 Model Registry API for ML model management and retrain pipelines.

This module provides comprehensive REST endpoints for:

Core Model Management:
- Model registration and versioning
- Training job management and monitoring  
- Model performance tracking and validation

Champion/Challenger Workflows:
- Automated champion model selection based on performance criteria
- Manual champion promotion with version control
- Advanced model comparison with statistical significance testing
- Multi-criteria recommendation engine

Training Job Operations:
- Job progress tracking with real-time updates
- Training completion handling with model registration
- Job cancellation for long-running operations
- Comprehensive job listing and filtering

Enhanced Features:
- Champion/challenger testing and promotion
- Scheduled retrain job configuration  
- Model deployment and serving integration
- Business impact assessment for model changes

New Endpoints Added:
- POST /models/{model_name}/select-champion - Automated champion selection
- POST /models/{model_name}/promote-champion - Manual champion promotion  
- PUT /jobs/{job_id}/progress - Update training job progress
- POST /jobs/{job_id}/complete - Complete training jobs
- DELETE /jobs/{job_id} - Cancel training jobs
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.model_registry_service import (
    get_model_registry_service,
    ModelVersion,
    ModelConfig,
    TrainingJob,
    ModelComparison,
    RetrainSchedule
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/models", tags=["model-registry"])


class ModelCreateRequest(BaseModel):
    """Request to register a new model."""

    model_name: str = Field(..., description="Model name")
    model_type: str = Field(..., description="Model type (linear_regression, random_forest, xgboost, etc.)")
    description: str = Field("", description="Model description")
    config: Dict[str, any] = Field(..., description="Model configuration")
    tags: Dict[str, str] = Field(default_factory=dict, description="Model tags")


class ModelResponse(BaseModel):
    """Response containing model information."""

    model_name: str
    model_type: str
    description: str
    latest_version: Optional[str]
    total_versions: int
    status: str
    created_at: datetime
    updated_at: datetime
    tags: Dict[str, str]


class ModelListResponse(BaseModel):
    """Response for listing models."""

    data: List[ModelResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class ModelVersionResponse(BaseModel):
    """Response containing model version information."""

    version_id: str
    model_name: str
    version_number: str
    description: str
    config: Dict[str, any]
    training_start_date: datetime
    training_end_date: datetime
    model_path: str
    model_size_bytes: int
    performance_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    validation_results: Dict[str, any]
    status: str
    created_at: datetime
    created_by: str
    tags: Dict[str, str]


class ModelVersionListResponse(BaseModel):
    """Response for listing model versions."""

    data: List[ModelVersionResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class TrainingJobResponse(BaseModel):
    """Response containing training job information."""

    job_id: str
    model_name: str
    config: Dict[str, any]
    status: str
    progress: float
    current_stage: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]
    model_version_id: Optional[str]
    created_at: datetime
    scheduled_for: Optional[datetime]


class TrainingJobListResponse(BaseModel):
    """Response for listing training jobs."""

    data: List[TrainingJobResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class ComparisonResponse(BaseModel):
    """Response containing model comparison information."""

    comparison_id: str
    champion_version: str
    challenger_version: str
    comparison_metrics: Dict[str, float]
    statistical_significance: Dict[str, float]
    business_impact: Dict[str, float]
    recommendation: str
    comparison_date: datetime
    notes: str


class ComparisonListResponse(BaseModel):
    """Response for listing model comparisons."""

    data: List[ComparisonResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class RetrainScheduleResponse(BaseModel):
    """Response containing retrain schedule information."""

    schedule_id: str
    model_name: str
    cron_expression: str
    enabled: bool
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class RetrainScheduleListResponse(BaseModel):
    """Response for listing retrain schedules."""

    data: List[RetrainScheduleResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


@router.get("/models", response_model=ModelListResponse)
async def list_registered_models(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by model status")
) -> ModelListResponse:
    """List registered ML models with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Get models (mock implementation)
        models = await service.list_models(
            status=status,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        model_responses = [
            ModelResponse(
                model_name=model.name,
                model_type=model.model_type,
                description=model.description,
                latest_version=model.latest_version,
                total_versions=model.total_versions,
                status=model.status,
                created_at=model.created_at,
                updated_at=model.updated_at,
                tags=model.tags
            )
            for model in models
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_registered_models",
            query_time_ms=query_time_ms,
            record_count=len(model_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return ModelListResponse(
            data=model_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_registered_models",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list registered models: {str(exc)}"
        )


@router.post("/models", response_model=ModelResponse, status_code=201)
async def register_model(
    request: Request,
    model_data: ModelCreateRequest
) -> ModelResponse:
    """Register a new ML model."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Create initial model version
        model_config = ModelConfig(**model_data.config)
        initial_version = ModelVersion(
            version_id=str(uuid4()),
            model_name=model_data.model_name,
            version_number="v1.0",
            description=model_data.description,
            config=model_config,
            training_start_date=datetime.utcnow(),
            training_end_date=datetime.utcnow(),
            model_path=f"models/{model_data.model_name}/v1.0",
            model_size_bytes=0,
            performance_metrics={},
            feature_importance={},
            validation_results={},
            status="active",
            created_by="system",
            tags=model_data.tags
        )

        # Register model (mock implementation)
        registered_model = await service.register_model_version(initial_version)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="register_model",
            query_time_ms=query_time_ms
        )

        return ModelResponse(
            model_name=registered_model.model_name,
            model_type=registered_model.config.model_type,
            description=registered_model.description,
            latest_version=registered_model.version_number,
            total_versions=1,
            status=registered_model.status,
            created_at=registered_model.created_at,
            updated_at=registered_model.created_at,
            tags=registered_model.tags
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="register_model",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to register model: {str(exc)}"
        )


@router.get("/models/{model_name}/versions", response_model=ModelVersionListResponse)
async def list_model_versions(
    request: Request,
    response: Response,
    model_name: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by version status")
) -> ModelVersionListResponse:
    """List model versions with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Get versions (mock implementation)
        versions = await service.list_model_versions(
            model_name=model_name,
            status=status,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        version_responses = [
            ModelVersionResponse(
                version_id=version.version_id,
                model_name=version.model_name,
                version_number=version.version_number,
                description=version.description,
                config=version.config.dict() if hasattr(version.config, 'dict') else version.config,
                training_start_date=version.training_start_date,
                training_end_date=version.training_end_date,
                model_path=version.model_path,
                model_size_bytes=version.model_size_bytes,
                performance_metrics=version.performance_metrics,
                feature_importance=version.feature_importance,
                validation_results=version.validation_results,
                status=version.status,
                created_at=version.created_at,
                created_by=version.created_by,
                tags=version.tags
            )
            for version in versions
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_model_versions",
            query_time_ms=query_time_ms,
            record_count=len(version_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return ModelVersionListResponse(
            data=version_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_model_versions",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list model versions: {str(exc)}"
        )


@router.post("/models/{model_name}/train", response_model=Dict[str, any], status_code=202)
async def start_model_training(
    request: Request,
    model_name: str,
    config: Optional[Dict[str, any]] = None
) -> Dict[str, any]:
    """Start model training job."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Start training job
        job_id = await service.start_training_job(
            model_name=model_name,
            config=ModelConfig(**config) if config else None
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="start_model_training",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="start_model_training",
                query_time_ms=query_time_ms
            ),
            "data": {
                "job_id": job_id,
                "status": "pending",
                "message": "Model training started successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="start_model_training",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start model training: {str(exc)}"
        )


@router.get("/jobs", response_model=TrainingJobListResponse)
async def list_training_jobs(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by job status"),
    model_name: Optional[str] = Query(None, description="Filter by model name")
) -> TrainingJobListResponse:
    """List training jobs with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Get jobs (mock implementation)
        jobs = await service.list_training_jobs(
            status=status,
            model_name=model_name,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        job_responses = [
            TrainingJobResponse(
                job_id=job.job_id,
                model_name=job.model_name,
                config=job.config.dict() if hasattr(job.config, 'dict') else job.config,
                status=job.status,
                progress=job.progress,
                current_stage=job.current_stage,
                started_at=job.started_at,
                completed_at=job.completed_at,
                error_message=job.error_message,
                model_version_id=job.model_version_id,
                created_at=job.created_at,
                scheduled_for=job.scheduled_for
            )
            for job in jobs
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_training_jobs",
            query_time_ms=query_time_ms,
            record_count=len(job_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return TrainingJobListResponse(
            data=job_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_training_jobs",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list training jobs: {str(exc)}"
        )


@router.get("/jobs/{job_id}", response_model=TrainingJobResponse)
async def get_training_job(
    request: Request,
    job_id: str
) -> TrainingJobResponse:
    """Get a specific training job."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()
        job = await service.get_training_job(job_id)

        if not job:
            raise HTTPException(status_code=404, detail="Training job not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_training_job",
            query_time_ms=query_time_ms
        )

        return TrainingJobResponse(
            job_id=job.job_id,
            model_name=job.model_name,
            config=job.config.dict() if hasattr(job.config, 'dict') else job.config,
            status=job.status,
            progress=job.progress,
            current_stage=job.current_stage,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_message=job.error_message,
            model_version_id=job.model_version_id,
            created_at=job.created_at,
            scheduled_for=job.scheduled_for
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_training_job",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get training job: {str(exc)}"
        )


@router.get("/models/{model_name}/champion", response_model=Optional[ModelVersionResponse])
async def get_champion_model(
    request: Request,
    model_name: str
) -> Optional[ModelVersionResponse]:
    """Get the champion model version for a model."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()
        champion = service.get_current_champion_model(model_name)

        if not champion:
            return None

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_champion_model",
            query_time_ms=query_time_ms
        )

        return ModelVersionResponse(
            version_id=champion.version_id,
            model_name=champion.model_name,
            version_number=champion.version_number,
            description=champion.description,
            config=champion.config.dict() if hasattr(champion.config, 'dict') else champion.config,
            training_start_date=champion.training_start_date,
            training_end_date=champion.training_end_date,
            model_path=champion.model_path,
            model_size_bytes=champion.model_size_bytes,
            performance_metrics=champion.performance_metrics,
            feature_importance=champion.feature_importance,
            validation_results=champion.validation_results,
            status=champion.status,
            created_at=champion.created_at,
            created_by=champion.created_by,
            tags=champion.tags
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_champion_model",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get champion model: {str(exc)}"
        )


@router.post("/models/{model_name}/compare", response_model=ComparisonResponse, status_code=201)
async def compare_model_versions(
    request: Request,
    model_name: str,
    champion_version: str = Query(..., description="Champion model version"),
    challenger_version: str = Query(..., description="Challenger model version")
) -> ComparisonResponse:
    """Compare two model versions."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Perform comparison (mock implementation)
        comparison = await service.compare_models(
            model_name=model_name,
            champion_version=champion_version,
            challenger_version=challenger_version
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="compare_model_versions",
            query_time_ms=query_time_ms
        )

        return ComparisonResponse(
            comparison_id=comparison.comparison_id,
            champion_version=comparison.champion_version,
            challenger_version=comparison.challenger_version,
            comparison_metrics=comparison.comparison_metrics,
            statistical_significance=comparison.statistical_significance,
            business_impact=comparison.business_impact,
            recommendation=comparison.recommendation,
            comparison_date=comparison.comparison_date,
            notes=comparison.notes
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="compare_model_versions",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare model versions: {str(exc)}"
        )


@router.get("/comparisons", response_model=ComparisonListResponse)
async def list_model_comparisons(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
) -> ComparisonListResponse:
    """List model comparisons with pagination."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Get comparisons (mock implementation)
        comparisons = await service.list_champion_challenger_comparisons(limit=limit + offset)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        comparison_responses = [
            ComparisonResponse(
                comparison_id=comp.comparison_id,
                champion_version=comp.champion_version,
                challenger_version=comp.challenger_version,
                comparison_metrics=comp.comparison_metrics,
                statistical_significance=comp.statistical_significance,
                business_impact=comp.business_impact,
                recommendation=comp.recommendation,
                comparison_date=comp.comparison_date,
                notes=comp.notes
            )
            for comp in comparisons[offset:offset + limit]
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_model_comparisons",
            query_time_ms=query_time_ms,
            record_count=len(comparison_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return ComparisonListResponse(
            data=comparison_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_model_comparisons",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list model comparisons: {str(exc)}"
        )


@router.get("/schedules", response_model=RetrainScheduleListResponse)
async def list_retrain_schedules(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    enabled_only: bool = Query(False, description="Only return enabled schedules")
) -> RetrainScheduleListResponse:
    """List retrain schedules with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        # Get schedules (mock implementation)
        schedules = await service.list_retrain_schedules(
            enabled_only=enabled_only,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        schedule_responses = [
            RetrainScheduleResponse(
                schedule_id=schedule.schedule_id,
                model_name=schedule.model_name,
                cron_expression=schedule.cron_expression,
                enabled=schedule.enabled,
                last_run=schedule.last_run,
                next_run=schedule.next_run,
                created_at=schedule.created_at,
                updated_at=schedule.updated_at
            )
            for schedule in schedules
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_retrain_schedules",
            query_time_ms=query_time_ms,
            record_count=len(schedule_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return RetrainScheduleListResponse(
            data=schedule_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_retrain_schedules",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list retrain schedules: {str(exc)}"
        )


@router.post("/models/{model_name}/select-champion", response_model=Dict[str, any], status_code=200)
async def select_champion_model(
    request: Request,
    model_name: str,
    selection_criteria: Optional[Dict[str, any]] = None
) -> Dict[str, any]:
    """Automatically select champion model based on performance criteria."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        champion = await service.select_champion_model(
            model_name=model_name,
            selection_criteria=selection_criteria
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="select_champion_model",
            query_time_ms=query_time_ms
        )

        if champion:
            return {
                "champion_selected": True,
                "champion_version": champion.version_number,
                "champion_version_id": champion.version_id,
                "performance_metrics": champion.performance_metrics,
                "selection_criteria": selection_criteria
            }
        else:
            return {
                "champion_selected": False,
                "message": "No suitable champion model found",
                "selection_criteria": selection_criteria
            }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="select_champion_model",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to select champion model: {str(exc)}"
        )


@router.post("/models/{model_name}/promote-champion", response_model=Dict[str, any], status_code=200)
async def promote_model_to_champion(
    request: Request,
    model_name: str,
    version_id: str = Query(..., description="Version ID to promote to champion")
) -> Dict[str, any]:
    """Promote a specific model version to champion status."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        success = await service.promote_to_champion(
            model_name=model_name,
            version_id=version_id
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="promote_model_to_champion",
            query_time_ms=query_time_ms
        )

        return {
            "promotion_successful": success,
            "model_name": model_name,
            "promoted_version_id": version_id,
            "message": "Model promoted to champion" if success else "Promotion failed"
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="promote_model_to_champion",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to promote model to champion: {str(exc)}"
        )


@router.put("/jobs/{job_id}/progress", response_model=Dict[str, any], status_code=200)
async def update_training_job_progress(
    request: Request,
    job_id: str,
    progress: float = Query(..., ge=0.0, le=1.0, description="Progress percentage (0.0 to 1.0)"),
    stage: str = Query(..., description="Current training stage"),
    metrics: Optional[Dict[str, float]] = None
) -> Dict[str, any]:
    """Update training job progress and metrics."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        success = await service.update_training_job_progress(
            job_id=job_id,
            progress=progress,
            stage=stage,
            metrics=metrics
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="update_training_job_progress",
            query_time_ms=query_time_ms
        )

        return {
            "update_successful": success,
            "job_id": job_id,
            "progress": progress,
            "stage": stage,
            "metrics": metrics
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="update_training_job_progress",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update training job progress: {str(exc)}"
        )


@router.post("/jobs/{job_id}/complete", response_model=Dict[str, any], status_code=200)
async def complete_training_job(
    request: Request,
    job_id: str,
    success: bool = Query(..., description="Whether the job completed successfully"),
    error_message: Optional[str] = Query(None, description="Error message if job failed")
) -> Dict[str, any]:
    """Complete a training job with success or failure status."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        completion_success = await service.complete_training_job(
            job_id=job_id,
            model_version=None,  # In real implementation, this would come from the request
            error_message=error_message if not success else None
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="complete_training_job",
            query_time_ms=query_time_ms
        )

        return {
            "completion_successful": completion_success,
            "job_id": job_id,
            "job_success": success,
            "error_message": error_message
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="complete_training_job",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete training job: {str(exc)}"
        )


@router.delete("/jobs/{job_id}", response_model=Dict[str, any], status_code=200)
async def cancel_training_job(
    request: Request,
    job_id: str
) -> Dict[str, any]:
    """Cancel a pending or running training job."""
    start_time = time.perf_counter()

    try:
        service = get_model_registry_service()

        success = await service.cancel_training_job(job_id=job_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="cancel_training_job",
            query_time_ms=query_time_ms
        )

        return {
            "cancellation_successful": success,
            "job_id": job_id,
            "message": "Training job cancelled" if success else "Failed to cancel job"
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="cancel_training_job",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel training job: {str(exc)}"
        )
