"""Model Training Service - Handles model training job execution and progress tracking."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

try:
    from aurum.telemetry.context import get_request_id, get_tenant_id, get_user_id
    from aurum.observability.telemetry_facade import get_telemetry_facade
except ImportError:
    # Fallback for demo
    def get_telemetry_facade():
        class MockTelemetry:
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
        return MockTelemetry()
    def get_request_id(): return "demo-request"
    def get_tenant_id(): return "demo-tenant"
    def get_user_id(): return "demo-user"
try:
    from aurum.dao.experimental import TrinoDAO
except ImportError:
    # Mock DAO for demo
    class TrinoDAO:
        pass
from .models import TrainingJob, ModelConfig, ModelVersion
from .interfaces import IModelTrainingService


class TrainingJobDAO(TrinoDAO):
    """DAO for training job operations."""

    async def save_training_job(self, job: TrainingJob) -> bool:
        """Save training job to database."""
        # Implementation would persist to database
        return True

    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get training job by ID."""
        # Implementation would query database
        return None

    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """List training jobs with optional filtering."""
        # Implementation would query database
        return []


class ModelTrainingService(IModelTrainingService):
    """Service for managing model training jobs."""

    def __init__(self, management_service=None):
        self.logger = logging.getLogger(__name__)
        self.dao = TrainingJobDAO()
        self.telemetry = get_telemetry_facade()
        self.management_service = management_service
        self.training_jobs: Dict[str, TrainingJob] = {}

    async def start_training_job(
        self,
        model_name: str,
        config: ModelConfig,
        created_by: str
    ) -> str:
        """Start a new training job."""
        try:
            job_id = str(uuid4())

            # Create training job record
            job = TrainingJob(
                job_id=job_id,
                model_name=model_name,
                config=config,
                status="pending",
                created_by=created_by
            )

            # Store job locally and persist
            self.training_jobs[job_id] = job
            await self.dao.save_training_job(job)

            self.telemetry.info(
                "model_training.training_job_started",
                job_id=job_id,
                model_name=model_name,
                model_type=config.model_type,
                created_by=created_by
            )

            # Start background training execution
            asyncio.create_task(self._execute_training_job(job))

            return job_id

        except Exception as exc:
            self.telemetry.error("Failed to start training job", error=str(exc))
            raise

    async def update_training_job_progress(
        self,
        job_id: str,
        progress: float,
        stage: str,
        metrics: Optional[Dict[str, Any]] = None,
        updated_by: str = "system"
    ) -> bool:
        """Update progress of a training job."""
        try:
            job = self.training_jobs.get(job_id)
            if not job:
                return False

            # Update job progress
            job.progress = min(max(progress, 0.0), 1.0)  # Clamp between 0 and 1
            job.current_stage = stage
            job.metadata["last_updated"] = datetime.utcnow().isoformat()

            if metrics:
                # Store intermediate metrics
                job.metadata.setdefault("intermediate_metrics", []).append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "stage": stage,
                    "metrics": metrics,
                    "updated_by": updated_by,
                })

            # Persist changes
            await self.dao.save_training_job(job)

            self.telemetry.info(
                "model_training.training_job_progress_updated",
                job_id=job_id,
                progress=progress,
                stage=stage,
                metrics=metrics,
                updated_by=updated_by
            )

            return True

        except Exception as exc:
            self.telemetry.error("Failed to update training job progress", error=str(exc))
            return False

    async def complete_training_job(
        self,
        job_id: str,
        model_version: Optional[ModelVersion] = None,
        error_message: Optional[str] = None,
        completed_by: str = "system"
    ) -> bool:
        """Mark a training job as completed."""
        try:
            job = self.training_jobs.get(job_id)
            if not job:
                return False

            # Update job status
            job.completed_at = datetime.utcnow()

            if error_message:
                job.status = "failed"
                job.error_message = error_message
                self.telemetry.error(
                    "model_training.training_job_failed",
                    job_id=job_id,
                    error_message=error_message,
                    completed_by=completed_by
                )
            elif model_version:
                job.status = "completed"
                job.model_version = model_version

                # Register the model version if management service is available
                if self.management_service:
                    registered_version = await self.management_service.register_model_version(
                        model_name=model_version.model_name,
                        version=model_version,
                        created_by=completed_by
                    )
                    job.model_version_id = registered_version.version_id

                self.telemetry.info(
                    "model_training.training_job_completed",
                    job_id=job_id,
                    model_version_id=model_version.version_id,
                    completed_by=completed_by
                )
            else:
                job.status = "completed"
                self.telemetry.info(
                    "model_training.training_job_completed",
                    job_id=job_id,
                    completed_by=completed_by
                )

            # Persist completion
            await self.dao.save_training_job(job)

            return True

        except Exception as exc:
            self.telemetry.error("Failed to complete training job", error=str(exc))
            return False

    async def cancel_training_job(
        self,
        job_id: str,
        cancelled_by: str
    ) -> bool:
        """Cancel a pending or running training job."""
        try:
            job = self.training_jobs.get(job_id)
            if not job:
                return False

            # Check if job can be cancelled
            if job.status in ["completed", "failed", "cancelled"]:
                return False  # Cannot cancel already finished jobs

            # Update job status
            job.status = "cancelled"
            job.completed_at = datetime.utcnow()
            job.metadata["cancelled_by"] = cancelled_by

            # Persist cancellation
            await self.dao.save_training_job(job)

            self.telemetry.info(
                "model_training.training_job_cancelled",
                job_id=job_id,
                cancelled_by=cancelled_by
            )

            return True

        except Exception as exc:
            self.telemetry.error("Failed to cancel training job", error=str(exc))
            return False

    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get a specific training job."""
        # First check local storage
        job = self.training_jobs.get(job_id)
        if job:
            return job

        # Fall back to DAO query
        return await self.dao.get_training_job(job_id)

    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """List training jobs with optional filtering."""
        # Get jobs from local storage first
        jobs = list(self.training_jobs.values())

        # Apply filters
        if model_name:
            jobs = [job for job in jobs if job.model_name == model_name]

        if status:
            jobs = [job for job in jobs if job.status == status]

        # Sort by creation date (newest first)
        jobs.sort(key=lambda job: job.created_at, reverse=True)

        # Apply pagination
        start = min(offset, len(jobs))
        end = min(start + limit, len(jobs))

        return jobs[start:end]

    async def _execute_training_job(self, job: TrainingJob) -> None:
        """Execute a model training job in the background."""
        try:
            # Update job status to running
            job.status = "running"
            job.started_at = datetime.utcnow()
            await self.dao.save_training_job(job)

            # Simulate training process with stages
            stages = [
                ("data_loading", 0.1),
                ("feature_engineering", 0.3),
                ("model_training", 0.8),
                ("model_validation", 0.95),
                ("model_saving", 1.0),
            ]

            for stage_name, target_progress in stages:
                # Update progress
                await self.update_training_job_progress(
                    job_id=job.job_id,
                    progress=target_progress,
                    stage=stage_name,
                    metrics={"stage": stage_name, "target_progress": target_progress}
                )

                # Simulate work time
                await asyncio.sleep(0.1)

            # Create mock model version for successful completion
            model_version = ModelVersion(
                version_id=str(uuid4()),
                model_name=job.model_name,
                version_number=f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                description=f"Training job {job.job_id} result",
                config=job.config,
                training_start_date=job.started_at or datetime.utcnow(),
                training_end_date=datetime.utcnow(),
                model_path=f"models/{job.model_name}/{job.job_id}",
                model_size_bytes=1024 * 1024,  # 1MB mock size
                performance_metrics={
                    "accuracy": 0.94,
                    "rmse": 0.06,
                    "r2_score": 0.96
                },
                feature_importance={
                    "temperature": 0.35,
                    "load_mw": 0.30,
                    "humidity": 0.20,
                    "wind_speed": 0.15
                },
                validation_results={
                    "cross_validation_scores": [0.93, 0.95, 0.92, 0.96, 0.94],
                    "mean_cv_score": 0.94
                },
                created_by=job.created_by,
                status="active"
            )

            # Complete the job successfully
            await self.complete_training_job(
                job_id=job.job_id,
                model_version=model_version,
                completed_by="training_system"
            )

        except asyncio.CancelledError:
            # Handle job cancellation
            await self.cancel_training_job(job.job_id, "system")
            raise
        except Exception as exc:
            # Handle training failure
            await self.complete_training_job(
                job_id=job.job_id,
                error_message=f"Training failed: {str(exc)}",
                completed_by="training_system"
            )

    async def health_check(self) -> bool:
        """Health check for the model training service."""
        try:
            # Check if we can access the training jobs dictionary
            if not hasattr(self, 'training_jobs') or self.training_jobs is None:
                return False

            # Check if management service is available (if configured)
            if hasattr(self, 'management_service') and self.management_service is None:
                return False

            return True

        except Exception as exc:
            self.logger.error(f"Health check failed: {exc}")
            return False

    def get_service_health(self) -> Dict[str, Any]:
        """Get detailed health information for the service."""
        return {
            "healthy": True,  # Would be determined by health_check()
            "service_name": "ModelTrainingService",
            "training_jobs_count": len(self.training_jobs),
            "management_service_available": self.management_service is not None,
            "last_health_check": datetime.utcnow().isoformat()
        }
