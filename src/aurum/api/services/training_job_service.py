"""Training Job Service for ML model training lifecycle management.

This service handles:
- Training job lifecycle (start, progress, completion, cancellation)
- Real-time status tracking and updates
- Integration with MLflow for experiment tracking
- Background task management for long-running jobs
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, get_user_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from .feature_store_shim import get_feature_store_service

logger = logging.getLogger(__name__)


class TrainingJob(BaseModel):
    """Model training job configuration."""

    job_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    model_type: str
    hyperparameters: Dict[str, Any]
    feature_selection: List[str]
    target_variable: str
    status: str = "pending"
    progress: float = 0.0
    current_stage: str = "initialization"
    metrics: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None


class TrainingJobService:
    """Service for managing ML model training jobs."""

    def __init__(self) -> None:
        self.telemetry = get_telemetry_facade() or _NoOpTelemetry()
        self.cache = get_unified_cache_manager()
        self.feature_store = get_feature_store_service()
        self._active_jobs: Dict[str, asyncio.Task] = {}
        self._training_jobs: Dict[str, TrainingJob] = {}

    async def start_training_job(
        self,
        model_name: str,
        config: Dict[str, Any]
    ) -> str:
        """Start a new training job."""
        job_id = str(uuid4())

        job = TrainingJob(
            job_id=job_id,
            model_name=model_name,
            model_type=config.get("model_type", "unknown"),
            hyperparameters=config.get("hyperparameters", {}),
            feature_selection=config.get("feature_selection", []),
            target_variable=config.get("target_variable", "unknown")
        )

        self._training_jobs[job_id] = job

        # Start training in background
        task = asyncio.create_task(self._run_training_job(job))
        self._active_jobs[job_id] = task

        logger.info(f"Started training job {job_id} for model {model_name}")
        return job_id

    async def update_training_job_progress(
        self,
        job_id: str,
        progress: float,
        stage: str,
        metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update training job progress."""
        if job_id not in self._training_jobs:
            return False

        job = self._training_jobs[job_id]
        job.progress = progress
        job.current_stage = stage

        if metrics:
            job.metrics.update(metrics)

        logger.info(f"Updated training job {job_id}: {progress:.1%} complete, stage: {stage}")
        return True

    async def complete_training_job(
        self,
        job_id: str,
        model_version: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Mark training job as completed."""
        if job_id not in self._training_jobs:
            return False

        job = self._training_jobs[job_id]
        job.status = "completed"
        job.completed_at = datetime.utcnow()

        # Clean up background task
        if job_id in self._active_jobs:
            task = self._active_jobs.pop(job_id)
            if not task.done():
                task.cancel()

        logger.info(f"Completed training job {job_id}")
        return True

    async def cancel_training_job(self, job_id: str) -> bool:
        """Cancel a running training job."""
        if job_id not in self._training_jobs:
            return False

        job = self._training_jobs[job_id]
        if job.status not in ["pending", "running"]:
            return False

        job.status = "cancelled"
        job.cancelled_at = datetime.utcnow()

        # Cancel background task
        if job_id in self._active_jobs:
            task = self._active_jobs.pop(job_id)
            if not task.done():
                task.cancel()

        logger.info(f"Cancelled training job {job_id}")
        return True

    async def get_training_job_status(self, job_id: str) -> Optional[TrainingJob]:
        """Get training job status."""
        return self._training_jobs.get(job_id)

    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[TrainingJob]:
        """List training jobs with optional filtering."""
        jobs = list(self._training_jobs.values())

        if model_name:
            jobs = [job for job in jobs if job.model_name == model_name]

        if status:
            jobs = [job for job in jobs if job.status == status]

        return sorted(jobs, key=lambda x: x.created_at, reverse=True)

    async def _run_training_job(self, job: TrainingJob) -> None:
        """Run training job in background."""
        try:
            job.status = "running"
            job.started_at = datetime.utcnow()

            # Simulate training process
            stages = ["data_loading", "feature_engineering", "model_training", "validation"]

            for i, stage in enumerate(stages):
                await self.update_training_job_progress(job.job_id, (i + 1) / len(stages), stage)

                # Simulate work
                await asyncio.sleep(1.0)

            # Complete job
            await self.complete_training_job(job.job_id)

        except asyncio.CancelledError:
            job.status = "cancelled"
            job.cancelled_at = datetime.utcnow()
            logger.info(f"Training job {job.job_id} was cancelled")
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            logger.error(f"Training job {job.job_id} failed: {e}")


class _NoOpTelemetry:
    """Fallback telemetry implementation for offline contexts."""

    def info(self, *_: Any, **__: Any) -> None:
        pass

    def warning(self, *_: Any, **__: Any) -> None:
        pass

    def error(self, *_: Any, **__: Any) -> None:
        pass

    def increment_counter(self, *_: Any, **__: Any) -> None:
        pass

    def record_histogram(self, *_: Any, **__: Any) -> None:
        pass

    def record_success(self, *_: Any, **__: Any) -> None:
        pass

    def record_error(self, *_: Any, **__: Any) -> None:
        pass

    def create_response_metadata(self, **kwargs: Any) -> Dict[str, Any]:
        return {
            "operation": kwargs.get("operation"),
            "query_time_ms": kwargs.get("query_time_ms", 0),
            "record_count": kwargs.get("record_count", 0),
        }
