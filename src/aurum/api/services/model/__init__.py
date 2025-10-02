"""Model management domain services and models.

This package contains the decomposed model registry functionality,
organized into focused, single-responsibility services.
"""

from .models import ModelConfig, ModelVersion, TrainingJob, ModelComparison
from .interfaces import (
    IModelManagementService,
    IModelTrainingService,
    IModelComparisonService,
    IModelSchedulingService
)
from .management_service import ModelManagementService
from .training_service import ModelTrainingService
from .comparison_service import ModelComparisonService
from .scheduling_service import ModelSchedulingService
from .service_factory import (
    ModelServiceFactory,
    get_model_service_factory,
    get_model_management_service,
    get_model_training_service,
    get_model_comparison_service,
    get_model_scheduling_service
)

__all__ = [
    # Models
    "ModelConfig",
    "ModelVersion",
    "TrainingJob",
    "ModelComparison",

    # Interfaces
    "IModelManagementService",
    "IModelTrainingService",
    "IModelComparisonService",
    "IModelSchedulingService",

    # Services
    "ModelManagementService",
    "ModelTrainingService",
    "ModelComparisonService",
    "ModelSchedulingService",

    # Factories
    "ModelServiceFactory",
    "get_model_service_factory",
    "get_model_management_service",
    "get_model_training_service",
    "get_model_comparison_service",
    "get_model_scheduling_service",
]
