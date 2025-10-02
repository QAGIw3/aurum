"""Training Jobs Service.

This service handles ML model training job lifecycle management including
job creation, progress tracking, completion, and cancellation.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from uuid import uuid4

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class TrainingJobStatus(str, Enum):
    """Training job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ModelConfig(BaseModel):
    """Model training configuration."""
    
    model_type: str
    hyperparameters: Dict[str, Any] = Field(default_factory=dict)
    feature_selection: List[str] = Field(default_factory=list)
    target_variable: Optional[str] = None
    validation_split: float = 0.2
    random_state: Optional[int] = 42
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TrainingJob(BaseModel):
    """Represents a model training job."""
    
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    config: ModelConfig
    status: TrainingJobStatus = TrainingJobStatus.PENDING
    progress: float = 0.0
    current_stage: Optional[str] = None
    stages_completed: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    model_version_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_by: str = "system"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TrainingJobRepository(BaseRepository):
    """Repository interface for training job operations."""
    
    async def save_job(self, job: TrainingJob) -> TrainingJob:
        """Save or update a training job."""
        raise NotImplementedError
    
    async def get_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get a job by ID."""
        raise NotImplementedError
    
    async def list_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[TrainingJobStatus] = None,
        created_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """List training jobs with optional filters."""
        raise NotImplementedError
    
    async def update_job_status(
        self,
        job_id: str,
        status: TrainingJobStatus,
        error_message: Optional[str] = None
    ) -> Optional[TrainingJob]:
        """Update job status."""
        raise NotImplementedError


class TrainingJobsService(BaseService):
    """
    Training job lifecycle management service.
    
    This service handles the complete lifecycle of ML model training jobs
    including creation, progress tracking, completion, and cancellation.
    """
    
    def __init__(
        self,
        repository: Optional[TrainingJobRepository] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 60  # Short TTL for job status
    ):
        """
        Initialize the training jobs service.
        
        Args:
            repository: Repository for data persistence
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.logger = logging.getLogger(__name__)
        
        # In-memory job tracking
        self._active_jobs: Dict[str, TrainingJob] = {}
        self._job_tasks: Dict[str, asyncio.Task] = {}
    
    def _get_default_repository(self) -> TrainingJobRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        # For now, return a mock repository
        class MockRepository(TrainingJobRepository):
            def __init__(self):
                self.jobs = {}
            
            async def save_job(self, job: TrainingJob) -> TrainingJob:
                self.jobs[job.job_id] = job
                return job
            
            async def get_job(self, job_id: str) -> Optional[TrainingJob]:
                return self.jobs.get(job_id)
            
            async def list_jobs(self, **kwargs) -> List[TrainingJob]:
                jobs = list(self.jobs.values())
                # Apply filters
                if kwargs.get('model_name'):
                    jobs = [j for j in jobs if j.model_name == kwargs['model_name']]
                if kwargs.get('status'):
                    jobs = [j for j in jobs if j.status == kwargs['status']]
                return jobs
            
            async def update_job_status(
                self,
                job_id: str,
                status: TrainingJobStatus,
                error_message: Optional[str] = None
            ) -> Optional[TrainingJob]:
                job = self.jobs.get(job_id)
                if job:
                    job.status = status
                    if error_message:
                        job.error_message = error_message
                    job.updated_at = datetime.utcnow()
                return job
        
        return MockRepository()
    
    async def start_training_job(
        self,
        model_name: str,
        config: ModelConfig,
        created_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Start a new training job.
        
        Args:
            model_name: Name of the model to train
            config: Model training configuration
            created_by: User starting the job
            metadata: Additional job metadata
            
        Returns:
            Job ID for tracking
        """
        # Create job
        job = TrainingJob(
            model_name=model_name,
            config=config,
            created_by=created_by,
            status=TrainingJobStatus.PENDING
        )
        
        if metadata:
            job.metrics.update(metadata)
        
        # Save job
        job = await self.repository.save_job(job)
        self._active_jobs[job.job_id] = job
        
        self.logger.info(
            f"Started training job {job.job_id} for model {model_name}",
            extra={"job_id": job.job_id, "model_name": model_name}
        )
        
        # Emit metric
        await self._emit_metric(
            "training_job_started",
            tags={"model_name": model_name, "model_type": config.model_type}
        )
        
        # Start async execution (in real implementation, this would submit to job queue)
        # For now, we'll simulate with a background task
        task = asyncio.create_task(self._simulate_training(job.job_id))
        self._job_tasks[job.job_id] = task
        
        return job.job_id
    
    async def update_training_job_progress(
        self,
        job_id: str,
        progress: float,
        stage: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> TrainingJob:
        """
        Update training job progress.
        
        Args:
            job_id: Job identifier
            progress: Progress percentage (0-100)
            stage: Current training stage
            metrics: Current metrics to record
            
        Returns:
            Updated TrainingJob
            
        Raises:
            ValueError: If job not found or in terminal state
        """
        job = await self.get_training_job(job_id)
        if not job:
            raise ValueError(f"Training job {job_id} not found")
        
        if job.status in [TrainingJobStatus.COMPLETED, TrainingJobStatus.FAILED, TrainingJobStatus.CANCELLED]:
            raise ValueError(f"Cannot update progress for job in {job.status} state")
        
        # Update job
        job.progress = max(0.0, min(100.0, progress))
        job.status = TrainingJobStatus.RUNNING
        
        if stage:
            job.current_stage = stage
            if stage not in job.stages_completed:
                job.stages_completed.append(stage)
        
        if metrics:
            job.metrics.update(metrics)
        
        if not job.started_at:
            job.started_at = datetime.utcnow()
        
        job.updated_at = datetime.utcnow()
        
        # Save job
        job = await self.repository.save_job(job)
        self._active_jobs[job_id] = job
        
        self.logger.info(
            f"Updated job {job_id} progress to {progress}%",
            extra={
                "job_id": job_id,
                "progress": progress,
                "stage": stage,
                "metrics": metrics
            }
        )
        
        return job
    
    async def complete_training_job(
        self,
        job_id: str,
        model_version_id: Optional[str] = None,
        final_metrics: Optional[Dict[str, Any]] = None
    ) -> TrainingJob:
        """
        Mark a training job as completed.
        
        Args:
            job_id: Job identifier
            model_version_id: ID of the created model version
            final_metrics: Final training metrics
            
        Returns:
            Updated TrainingJob
            
        Raises:
            ValueError: If job not found
        """
        job = await self.get_training_job(job_id)
        if not job:
            raise ValueError(f"Training job {job_id} not found")
        
        # Update job
        job.status = TrainingJobStatus.COMPLETED
        job.progress = 100.0
        job.completed_at = datetime.utcnow()
        
        if model_version_id:
            job.model_version_id = model_version_id
        
        if final_metrics:
            job.metrics.update(final_metrics)
        
        job.updated_at = datetime.utcnow()
        
        # Save job
        job = await self.repository.save_job(job)
        
        # Clean up
        self._active_jobs.pop(job_id, None)
        task = self._job_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()
        
        self.logger.info(
            f"Completed training job {job_id}",
            extra={
                "job_id": job_id,
                "model_version_id": model_version_id,
                "duration_seconds": (job.completed_at - job.created_at).total_seconds()
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "training_job_completed",
            value=(job.completed_at - job.created_at).total_seconds(),
            tags={"model_name": job.model_name, "status": "success"}
        )
        
        return job
    
    async def fail_training_job(
        self,
        job_id: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None
    ) -> TrainingJob:
        """
        Mark a training job as failed.
        
        Args:
            job_id: Job identifier
            error_message: Error description
            error_details: Additional error information
            
        Returns:
            Updated TrainingJob
        """
        job = await self.get_training_job(job_id)
        if not job:
            raise ValueError(f"Training job {job_id} not found")
        
        # Update job
        job.status = TrainingJobStatus.FAILED
        job.error_message = error_message
        job.completed_at = datetime.utcnow()
        
        if error_details:
            job.metrics["error_details"] = error_details
        
        job.updated_at = datetime.utcnow()
        
        # Save job
        job = await self.repository.save_job(job)
        
        # Clean up
        self._active_jobs.pop(job_id, None)
        task = self._job_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()
        
        self.logger.error(
            f"Training job {job_id} failed: {error_message}",
            extra={"job_id": job_id, "error_details": error_details}
        )
        
        # Emit metric
        await self._emit_metric(
            "training_job_failed",
            tags={"model_name": job.model_name, "error_type": "training_failure"}
        )
        
        return job
    
    async def cancel_training_job(
        self,
        job_id: str,
        reason: str = "User requested cancellation"
    ) -> TrainingJob:
        """
        Cancel a running training job.
        
        Args:
            job_id: Job identifier
            reason: Cancellation reason
            
        Returns:
            Updated TrainingJob
            
        Raises:
            ValueError: If job not found or not cancellable
        """
        job = await self.get_training_job(job_id)
        if not job:
            raise ValueError(f"Training job {job_id} not found")
        
        if job.status in [TrainingJobStatus.COMPLETED, TrainingJobStatus.FAILED]:
            raise ValueError(f"Cannot cancel job in {job.status} state")
        
        # Update job
        job.status = TrainingJobStatus.CANCELLED
        job.error_message = reason
        job.completed_at = datetime.utcnow()
        job.updated_at = datetime.utcnow()
        
        # Save job
        job = await self.repository.save_job(job)
        
        # Clean up
        self._active_jobs.pop(job_id, None)
        task = self._job_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()
        
        self.logger.info(
            f"Cancelled training job {job_id}: {reason}",
            extra={"job_id": job_id, "reason": reason}
        )
        
        # Emit metric
        await self._emit_metric(
            "training_job_cancelled",
            tags={"model_name": job.model_name}
        )
        
        return job
    
    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """
        Get a training job by ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            TrainingJob if found, None otherwise
        """
        # Check active jobs first
        if job_id in self._active_jobs:
            return self._active_jobs[job_id]
        
        # Check cache
        cache_key = f"training_job:{job_id}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                return TrainingJob(**cached)
        
        # Load from repository
        job = await self.repository.get_job(job_id)
        if job and self.cache_enabled:
            await self._set_cache(cache_key, job.dict(), ttl=self.cache_ttl)
        
        return job
    
    async def get_training_job_status(self, job_id: str) -> Optional[TrainingJobStatus]:
        """
        Get the current status of a training job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            TrainingJobStatus if job found, None otherwise
        """
        job = await self.get_training_job(job_id)
        return job.status if job else None
    
    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[TrainingJobStatus] = None,
        created_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """
        List training jobs with optional filters.
        
        Args:
            model_name: Filter by model name
            status: Filter by status
            created_by: Filter by creator
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of TrainingJob instances
        """
        return await self.repository.list_jobs(
            model_name=model_name,
            status=status,
            created_by=created_by,
            limit=limit,
            offset=offset
        )
    
    async def get_active_jobs(self) -> List[TrainingJob]:
        """
        Get all currently active (running or pending) jobs.
        
        Returns:
            List of active TrainingJob instances
        """
        active_statuses = [TrainingJobStatus.PENDING, TrainingJobStatus.RUNNING]
        active_jobs = []
        
        for status in active_statuses:
            jobs = await self.list_training_jobs(status=status)
            active_jobs.extend(jobs)
        
        return active_jobs
    
    async def _simulate_training(self, job_id: str):
        """Simulate a training job for testing purposes."""
        try:
            # Simulate training stages
            stages = [
                ("data_loading", 10),
                ("preprocessing", 25),
                ("feature_engineering", 40),
                ("model_training", 70),
                ("validation", 85),
                ("finalization", 100)
            ]
            
            for stage, progress in stages:
                await asyncio.sleep(2)  # Simulate work
                
                # Check if cancelled
                if job_id not in self._active_jobs:
                    return
                
                # Update progress
                await self.update_training_job_progress(
                    job_id=job_id,
                    progress=progress,
                    stage=stage,
                    metrics={"current_loss": 0.5 * (100 - progress) / 100}
                )
            
            # Complete job
            await self.complete_training_job(
                job_id=job_id,
                model_version_id=str(uuid4()),
                final_metrics={
                    "accuracy": 0.95,
                    "loss": 0.05,
                    "training_time_seconds": 120
                }
            )
            
        except asyncio.CancelledError:
            # Job was cancelled
            pass
        except Exception as e:
            # Job failed
            await self.fail_training_job(
                job_id=job_id,
                error_message=str(e),
                error_details={"exception_type": type(e).__name__}
            )
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
