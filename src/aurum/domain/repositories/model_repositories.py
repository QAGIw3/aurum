"""Repository interfaces for model management domain."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from aurum.domain.models.model_models import ModelVersion, TrainingJob, RetrainSchedule, RegisteredModel


class IModelRepository(ABC):
    """Repository interface for model data access."""

    @abstractmethod
    async def save_model_version(self, version: ModelVersion) -> bool:
        """Save a model version."""
        pass

    @abstractmethod
    async def get_model_version(self, model_name: str, version: str) -> Optional[ModelVersion]:
        """Get a specific model version."""
        pass

    @abstractmethod
    async def list_model_versions(
        self,
        model_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """List model versions."""
        pass

    @abstractmethod
    async def save_registered_model(self, model: RegisteredModel) -> bool:
        """Save a registered model."""
        pass

    @abstractmethod
    async def get_registered_model(self, model_name: str) -> Optional[RegisteredModel]:
        """Get a registered model."""
        pass

    @abstractmethod
    async def list_registered_models(self) -> List[RegisteredModel]:
        """List all registered models."""
        pass


class ITrainingJobRepository(ABC):
    """Repository interface for training job data access."""

    @abstractmethod
    async def save_training_job(self, job: TrainingJob) -> bool:
        """Save a training job."""
        pass

    @abstractmethod
    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get a training job by ID."""
        pass

    @abstractmethod
    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """List training jobs with optional filtering."""
        pass


class ISchedulingRepository(ABC):
    """Repository interface for scheduling data access."""

    @abstractmethod
    async def save_retrain_schedule(self, schedule: RetrainSchedule) -> bool:
        """Save a retrain schedule."""
        pass

    @abstractmethod
    async def get_retrain_schedule(self, schedule_id: str) -> Optional[RetrainSchedule]:
        """Get a retrain schedule by ID."""
        pass

    @abstractmethod
    async def list_retrain_schedules(
        self,
        model_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RetrainSchedule]:
        """List retrain schedules with optional filtering."""
        pass

    @abstractmethod
    async def delete_retrain_schedule(self, schedule_id: str) -> bool:
        """Delete a retrain schedule."""
        pass
