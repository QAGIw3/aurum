"""v2 Renewables data ingestion API.

This module provides REST endpoints for:
- Managing renewable data sources (satellite, weather stations)
- Monitoring ingestion jobs and data quality
- Viewing available datasets and metadata
- Configuring data validation and lineage tracking
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.renewables_ingestion_service import (
    get_renewables_ingestion_service,
    DataSourceConfig,
    IngestionJob,
    RenewablesDataPoint,
    RenewablesDataset,
    DataQualityCheck
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/renewables", tags=["renewables"])


class DataSourceCreateRequest(BaseModel):
    """Request to create a renewable data source."""

    name: str = Field(..., description="Data source name")
    source_type: str = Field(..., description="Source type (satellite, weather_station, nwp, reanalysis)")
    provider: str = Field(..., description="Data provider")
    api_endpoint: Optional[str] = Field(None, description="API endpoint")
    api_key: Optional[str] = Field(None, description="API key")
    credentials_file: Optional[str] = Field(None, description="Credentials file path")
    data_format: str = Field("json", description="Data format")
    temporal_resolution: str = Field("hourly", description="Temporal resolution")
    spatial_resolution: str = Field("1km", description="Spatial resolution")
    coverage_area: Dict[str, any] = Field(..., description="Geographic coverage")
    variables: List[str] = Field(..., description="Variables to ingest")
    quality_threshold: float = Field(0.8, description="Quality threshold")
    enabled: bool = Field(True, description="Whether source is enabled")


class DataSourceResponse(BaseModel):
    """Response containing data source information."""

    name: str
    source_type: str
    provider: str
    api_endpoint: Optional[str]
    data_format: str
    temporal_resolution: str
    spatial_resolution: str
    coverage_area: Dict[str, any]
    variables: List[str]
    quality_threshold: float
    enabled: bool
    created_at: datetime
    updated_at: datetime


class DataSourceListResponse(BaseModel):
    """Response for listing data sources."""

    data: List[DataSourceResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class JobResponse(BaseModel):
    """Response containing job information."""

    job_id: str
    data_source: str
    geography: str
    start_date: datetime
    end_date: datetime
    variables: List[str]
    status: str
    progress: float
    records_processed: int
    records_failed: int
    error_message: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


class JobListResponse(BaseModel):
    """Response for listing jobs."""

    data: List[JobResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


class DatasetResponse(BaseModel):
    """Response containing dataset information."""

    dataset_id: str
    name: str
    description: str
    geography: str
    data_source: str
    temporal_range: Dict[str, datetime]
    variables: List[str]
    record_count: int
    quality_score: float
    created_at: datetime
    metadata: Dict[str, any]


class DatasetListResponse(BaseModel):
    """Response for listing datasets."""

    data: List[DatasetResponse]
    meta: Dict[str, any]
    links: Dict[str, any]


@router.get("/data-sources", response_model=DataSourceListResponse)
async def list_renewable_data_sources(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    source_type: Optional[str] = Query(None, description="Filter by source type")
) -> DataSourceListResponse:
    """List renewable data sources with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()

        # Get data sources (mock implementation)
        sources = await service.list_data_sources(
            source_type=source_type,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        source_responses = [
            DataSourceResponse(
                name=source.name,
                source_type=source.source_type,
                provider=source.provider,
                api_endpoint=source.api_endpoint,
                data_format=source.data_format,
                temporal_resolution=source.temporal_resolution,
                spatial_resolution=source.spatial_resolution,
                coverage_area=source.coverage_area,
                variables=source.variables,
                quality_threshold=source.quality_threshold,
                enabled=source.enabled,
                created_at=datetime.utcnow(),  # Mock
                updated_at=datetime.utcnow()   # Mock
            )
            for source in sources
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_renewable_data_sources",
            query_time_ms=query_time_ms,
            record_count=len(source_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return DataSourceListResponse(
            data=source_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_renewable_data_sources",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list renewable data sources: {str(exc)}"
        )


@router.post("/data-sources", response_model=DataSourceResponse, status_code=201)
async def create_renewable_data_source(
    request: Request,
    source_data: DataSourceCreateRequest
) -> DataSourceResponse:
    """Create a new renewable data source."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()

        # Convert to service format
        source_config = DataSourceConfig(
            name=source_data.name,
            source_type=source_data.source_type,
            provider=source_data.provider,
            api_endpoint=source_data.api_endpoint,
            api_key=source_data.api_key,
            credentials_file=source_data.credentials_file,
            data_format=source_data.data_format,
            temporal_resolution=source_data.temporal_resolution,
            spatial_resolution=source_data.spatial_resolution,
            coverage_area=source_data.coverage_area,
            variables=source_data.variables,
            quality_threshold=source_data.quality_threshold,
            enabled=source_data.enabled
        )

        # Save data source (mock implementation)
        saved_source = await service.create_data_source(source_config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_renewable_data_source",
            query_time_ms=query_time_ms
        )

        return DataSourceResponse(
            name=saved_source.name,
            source_type=saved_source.source_type,
            provider=saved_source.provider,
            api_endpoint=saved_source.api_endpoint,
            data_format=saved_source.data_format,
            temporal_resolution=saved_source.temporal_resolution,
            spatial_resolution=saved_source.spatial_resolution,
            coverage_area=saved_source.coverage_area,
            variables=saved_source.variables,
            quality_threshold=saved_source.quality_threshold,
            enabled=saved_source.enabled,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_renewable_data_source",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create renewable data source: {str(exc)}"
        )


@router.get("/data-sources/{source_name}", response_model=DataSourceResponse)
async def get_renewable_data_source(
    request: Request,
    source_name: str
) -> DataSourceResponse:
    """Get a specific renewable data source."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()
        source = await service.get_data_source(source_name)

        if not source:
            raise HTTPException(status_code=404, detail="Data source not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_renewable_data_source",
            query_time_ms=query_time_ms
        )

        return DataSourceResponse(
            name=source.name,
            source_type=source.source_type,
            provider=source.provider,
            api_endpoint=source.api_endpoint,
            data_format=source.data_format,
            temporal_resolution=source.temporal_resolution,
            spatial_resolution=source.spatial_resolution,
            coverage_area=source.coverage_area,
            variables=source.variables,
            quality_threshold=source.quality_threshold,
            enabled=source.enabled,
            created_at=datetime.utcnow(),  # Mock
            updated_at=datetime.utcnow()   # Mock
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_renewable_data_source",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get renewable data source: {str(exc)}"
        )


@router.post("/ingest", response_model=Dict[str, any], status_code=202)
async def trigger_renewables_ingestion(
    request: Request,
    data_source: str = Query(..., description="Data source name"),
    geography: str = Query("US", description="Geographic scope"),
    start_date: datetime = Query(..., description="Start date for ingestion"),
    end_date: datetime = Query(..., description="End date for ingestion"),
    variables: Optional[List[str]] = Query(None, description="Variables to ingest")
) -> Dict[str, any]:
    """Trigger renewables data ingestion job."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()

        # Create ingestion job
        job = IngestionJob(
            job_id=str(uuid4()),
            data_source=data_source,
            geography=geography,
            start_date=start_date,
            end_date=end_date,
            variables=variables or [],
            status="pending"
        )

        # Start ingestion (mock implementation)
        job_id = await service.start_ingestion_job(job)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="trigger_renewables_ingestion",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="trigger_renewables_ingestion",
                query_time_ms=query_time_ms
            ),
            "data": {
                "job_id": job_id,
                "status": "pending",
                "message": "Ingestion job started successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="trigger_renewables_ingestion",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger renewables ingestion: {str(exc)}"
        )


@router.get("/jobs", response_model=JobListResponse)
async def list_renewables_jobs(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by job status"),
    data_source: Optional[str] = Query(None, description="Filter by data source")
) -> JobListResponse:
    """List renewables ingestion jobs with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()

        # Get jobs (mock implementation)
        jobs = await service.list_ingestion_jobs(
            status=status,
            data_source=data_source,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        job_responses = [
            JobResponse(
                job_id=job.job_id,
                data_source=job.data_source,
                geography=job.geography,
                start_date=job.start_date,
                end_date=job.end_date,
                variables=job.variables,
                status=job.status,
                progress=job.progress,
                records_processed=job.records_processed,
                records_failed=job.records_failed,
                error_message=job.error_message,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at
            )
            for job in jobs
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_renewables_jobs",
            query_time_ms=query_time_ms,
            record_count=len(job_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return JobListResponse(
            data=job_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_renewables_jobs",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list renewables jobs: {str(exc)}"
        )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_renewables_job(
    request: Request,
    job_id: str
) -> JobResponse:
    """Get a specific renewables ingestion job."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()
        job = await service.get_ingestion_job(job_id)

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_renewables_job",
            query_time_ms=query_time_ms
        )

        return JobResponse(
            job_id=job.job_id,
            data_source=job.data_source,
            geography=job.geography,
            start_date=job.start_date,
            end_date=job.end_date,
            variables=job.variables,
            status=job.status,
            progress=job.progress,
            records_processed=job.records_processed,
            records_failed=job.records_failed,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_renewables_job",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get renewables job: {str(exc)}"
        )


@router.get("/datasets", response_model=DatasetListResponse)
async def list_renewables_datasets(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    geography: Optional[str] = Query(None, description="Filter by geography"),
    data_source: Optional[str] = Query(None, description="Filter by data source")
) -> DatasetListResponse:
    """List available renewables datasets with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_renewables_ingestion_service()

        # Get datasets (mock implementation)
        datasets = await service.list_datasets(
            geography=geography,
            data_source=data_source,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        dataset_responses = [
            DatasetResponse(
                dataset_id=dataset.dataset_id,
                name=dataset.name,
                description=dataset.description,
                geography=dataset.geography,
                data_source=dataset.data_source,
                temporal_range=dataset.temporal_range,
                variables=dataset.variables,
                record_count=dataset.record_count,
                quality_score=dataset.quality_score,
                created_at=dataset.created_at,
                metadata=dataset.metadata
            )
            for dataset in datasets
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_renewables_datasets",
            query_time_ms=query_time_ms,
            record_count=len(dataset_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return DatasetListResponse(
            data=dataset_responses,
            meta=meta,
            links={}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_renewables_datasets",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list renewables datasets: {str(exc)}"
        )
