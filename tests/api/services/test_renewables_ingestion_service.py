"""Tests for the renewable ingestion service pipelines."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from aurum.api.services.renewables_ingestion_service import (
    IngestionJob,
    RenewablesIngestionService,
)


async def _wait_for_completion(service: RenewablesIngestionService, job_id: str, timeout: float = 2.0) -> IngestionJob:
    """Poll the service until the job finishes or timeout expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await service.get_ingestion_job(job_id)
        assert job is not None, "job disappeared from service"
        if job.status not in {"pending", "running"}:
            return job
        await asyncio.sleep(0.05)
    raise AssertionError("ingestion job did not finish within timeout")


@pytest.mark.asyncio
async def test_satellite_ingestion_pipeline_succeeds():
    service = RenewablesIngestionService(enable_parallel_processing=False)
    now = datetime.utcnow()
    job = IngestionJob(
        data_source="satellite_nasa",
        geography="US",
        start_date=now - timedelta(hours=3),
        end_date=now,
        variables=["ghi", "dni", "wind_speed"],
    )

    job_id = await service.start_ingestion_job(job=job)
    completed_job = await _wait_for_completion(service, job_id)

    assert completed_job.status == "completed"
    assert completed_job.records_processed > 0
    assert completed_job.metadata["quality_results"]

    datasets = await service.list_datasets(data_source="satellite_nasa", limit=1)
    assert datasets and datasets[0].data_points


@pytest.mark.asyncio
async def test_weather_station_ingestion_pipeline_succeeds():
    service = RenewablesIngestionService(enable_parallel_processing=False)
    now = datetime.utcnow()
    job = IngestionJob(
        data_source="weather_station_noaa",
        geography="US",
        start_date=now - timedelta(hours=2),
        end_date=now,
        variables=["temperature", "humidity", "wind_speed"],
    )

    job_id = await service.start_ingestion_job(job=job)
    completed_job = await _wait_for_completion(service, job_id)

    assert completed_job.status == "completed"
    assert completed_job.records_processed > 0
    datasets = await service.list_datasets(data_source="weather_station_noaa", limit=1)
    assert datasets and datasets[0].data_source == "weather_station_noaa"


@pytest.mark.asyncio
async def test_quality_check_failure_blocks_dataset():
    service = RenewablesIngestionService(enable_parallel_processing=False)

    # Tighten the irradiance range to force a failure (values ~850 so raise minimum above that)
    irradiance_check = service.quality_checks["irradiance_range"]
    irradiance_check.parameters["min_value"] = 900.0
    irradiance_check.threshold = 1.0

    now = datetime.utcnow()
    job = IngestionJob(
        data_source="satellite_nasa",
        geography="US",
        start_date=now - timedelta(hours=1),
        end_date=now,
        variables=["ghi", "dni"],
    )

    job_id = await service.start_ingestion_job(job=job)
    completed_job = await _wait_for_completion(service, job_id)

    assert completed_job.status == "failed"
    assert "Quality checks" in (completed_job.error_message or "")

    datasets = await service.list_datasets(data_source="satellite_nasa")
    assert not datasets, "dataset should not be stored when quality checks fail"
