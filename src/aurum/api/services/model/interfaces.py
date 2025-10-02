"""SOLID-compliant service interfaces for model management domain."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol

# Remove ServiceExecutionResult import for now - will be added back when needed
# from aurum.libs.services.contracts import ServiceExecutionResult
from .models import (
    ModelConfig,
    ModelVersion,
    TrainingJob,
    ModelComparison,
    RetrainSchedule,
    RegisteredModel,
    ChampionChallengerSelection
)


# Base interfaces for dependency inversion
class IAuditLogger(Protocol):
    """Protocol for audit logging functionality."""

    async def log_action(
        self,
        action: str,
        model_name: str,
        reference: Dict[str, Any],
        user_id: str
    ) -> None:
        """Log an audit action."""
        ...


class ITelemetryProvider(Protocol):
    """Protocol for telemetry and metrics."""

    async def record_metric(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Record a metric."""
        ...

    async def increment_counter(
        self,
        counter_name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Increment a counter."""
        ...


# Segregated interfaces following ISP
class IModelRegistry(ABC):
    """Interface for model registry operations (SRP: model registration)."""

    @abstractmethod
    async def register_model(
        self,
        model_name: str,
        description: str,
        model_type: str,
        created_by: str,
        **metadata
    ) -> RegisteredModel:
        """Register a new model in the registry."""
        pass

    @abstractmethod
    async def list_models(self) -> List[RegisteredModel]:
        """List all registered models."""
        pass


class IModelVersionManager(ABC):
    """Interface for model version management (SRP: version lifecycle)."""

    @abstractmethod
    async def register_model_version(
        self,
        model_name: str,
        version: ModelVersion,
        created_by: str
    ) -> ModelVersion:
        """Register a new version of an existing model."""
        pass

    @abstractmethod
    async def get_model_version(
        self,
        model_name: str,
        version: str
    ) -> Optional[ModelVersion]:
        """Get a specific model version."""
        pass

    @abstractmethod
    async def list_model_versions(
        self,
        model_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """List all versions of a model."""
        pass

    @abstractmethod
    async def update_model_version_status(
        self,
        model_name: str,
        version: str,
        status: str,
        updated_by: str
    ) -> bool:
        """Update the status of a model version."""
        pass


# Combined interface that composes the segregated interfaces (OCP: extensible)
class IModelManagementService(IModelRegistry, IModelVersionManager):
    """Interface for model registry management operations.

    This interface composes smaller, focused interfaces following ISP.
    It can be extended without modification through composition.
    """
    pass


# Segregated training interfaces following ISP
class ITrainingJobManager(ABC):
    """Interface for training job lifecycle management (SRP: job lifecycle)."""

    @abstractmethod
    async def start_training_job(
        self,
        model_name: str,
        config: ModelConfig,
        created_by: str
    ) -> str:
        """Start a new training job."""
        pass

    @abstractmethod
    async def cancel_training_job(
        self,
        job_id: str,
        cancelled_by: str
    ) -> bool:
        """Cancel a pending or running training job."""
        pass

    @abstractmethod
    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get a specific training job."""
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


class ITrainingJobMonitor(ABC):
    """Interface for training job monitoring (SRP: progress tracking)."""

    @abstractmethod
    async def update_training_job_progress(
        self,
        job_id: str,
        progress: float,
        stage: str,
        metrics: Optional[Dict[str, Any]] = None,
        updated_by: str = "system"
    ) -> bool:
        """Update progress of a training job."""
        pass

    @abstractmethod
    async def complete_training_job(
        self,
        job_id: str,
        model_version: Optional[ModelVersion] = None,
        error_message: Optional[str] = None,
        completed_by: str = "system"
    ) -> bool:
        """Mark a training job as completed."""
        pass


# Combined interface following OCP (extensible through composition)
class IModelTrainingService(ITrainingJobManager, ITrainingJobMonitor):
    """Interface for model training operations.

    This interface composes smaller, focused interfaces following ISP.
    It can be extended without modification through composition.
    """
    pass


# Segregated comparison interfaces following ISP
class IModelComparator(ABC):
    """Interface for model comparison operations (SRP: model comparison)."""

    @abstractmethod
    async def compare_models(
        self,
        champion_version_id: str,
        challenger_version_id: str,
        compared_by: str
    ) -> ModelComparison:
        """Compare two model versions."""
        pass

    @abstractmethod
    async def create_champion_challenger_selection(
        self,
        model_name: str,
        champion_version: str,
        challenger_versions: List[str],
        selection_criteria: Dict[str, float],
        created_by: str
    ) -> ChampionChallengerSelection:
        """Create a champion/challenger comparison configuration."""
        pass

    @abstractmethod
    async def list_champion_challenger_comparisons(
        self,
        model_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ChampionChallengerSelection]:
        """List champion/challenger comparisons."""
        pass


class IChampionSelector(ABC):
    """Interface for champion selection operations (SRP: champion selection)."""

    @abstractmethod
    async def select_champion_model(
        self,
        model_name: str,
        selection_criteria: Optional[Dict[str, float]] = None,
        selected_by: str = "system"
    ) -> Optional[ModelVersion]:
        """Select the best model version as champion."""
        pass

    @abstractmethod
    async def promote_to_champion(
        self,
        model_name: str,
        version_id: str,
        promoted_by: str
    ) -> bool:
        """Promote a model version to champion."""
        pass


# Combined interface following OCP (extensible through composition)
class IModelComparisonService(IModelComparator, IChampionSelector):
    """Interface for model comparison and champion selection.

    This interface composes smaller, focused interfaces following ISP.
    It can be extended without modification through composition.
    """
    pass


# Segregated scheduling interfaces following ISP
class IScheduleManager(ABC):
    """Interface for schedule management operations (SRP: schedule CRUD)."""

    @abstractmethod
    async def create_retrain_schedule(
        self,
        model_name: str,
        cron_expression: str,
        config: ModelConfig,
        created_by: str
    ) -> RetrainSchedule:
        """Create a new retraining schedule."""
        pass

    @abstractmethod
    async def update_retrain_schedule(
        self,
        schedule_id: str,
        enabled: Optional[bool] = None,
        cron_expression: Optional[str] = None,
        config: Optional[ModelConfig] = None,
        updated_by: str = "system"
    ) -> bool:
        """Update an existing retraining schedule."""
        pass

    @abstractmethod
    async def delete_retrain_schedule(
        self,
        schedule_id: str,
        deleted_by: str
    ) -> bool:
        """Delete a retraining schedule."""
        pass

    @abstractmethod
    async def list_retrain_schedules(
        self,
        model_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RetrainSchedule]:
        """List retraining schedules with optional filtering."""
        pass


class IScheduleExecutor(ABC):
    """Interface for schedule execution (SRP: schedule execution)."""

    @abstractmethod
    async def trigger_scheduled_retrain(
        self,
        schedule_id: str,
        triggered_by: str = "scheduler"
    ) -> Optional[str]:
        """Trigger a scheduled retrain manually."""
        pass


# Combined interface following OCP (extensible through composition)
class IModelSchedulingService(IScheduleManager, IScheduleExecutor):
    """Interface for model retraining scheduling.

    This interface composes smaller, focused interfaces following ISP.
    It can be extended without modification through composition.
    """
    pass
