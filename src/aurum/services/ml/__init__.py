"""ML Services Module.

This module contains machine learning related services extracted from
monolithic implementations as part of the service layer decomposition.

Services:
- ModelRegistryService: Core model registration and versioning
- TrainingJobsService: Training job lifecycle management
- ModelComparisonService: Champion/challenger comparison
- ModelSelectionService: Champion selection algorithms
- RetrainSchedulerService: Scheduled retraining
- MLflowIntegrationService: MLflow integration
"""

from .model_registry import (
    ModelRegistryService,
    RegisteredModel,
    ModelVersion,
    ModelRegistryRepository
)

from .training_jobs import (
    TrainingJobsService,
    TrainingJob,
    TrainingJobStatus,
    ModelConfig,
    TrainingJobRepository
)

from .model_comparison import (
    ModelComparisonService,
    ModelComparison,
    ComparisonRepository
)

from .model_selection import (
    ModelSelectionService,
    ChampionSelectionCriteria,
    ChampionChallengerSelection,
    ChampionHistory,
    SelectionRepository
)

from .retrain_scheduler import (
    RetrainSchedulerService,
    RetrainSchedule,
    RetrainTrigger,
    SchedulerRepository
)

from .mlflow_integration import (
    MLflowIntegrationService,
    MLflowConfig,
    ExperimentRun
)

__all__ = [
    # Model Registry
    "ModelRegistryService",
    "RegisteredModel",
    "ModelVersion",
    "ModelRegistryRepository",
    
    # Training Jobs
    "TrainingJobsService",
    "TrainingJob",
    "TrainingJobStatus",
    "ModelConfig",
    "TrainingJobRepository",
    
    # Model Comparison
    "ModelComparisonService",
    "ModelComparison",
    "ComparisonRepository",
    
    # Model Selection
    "ModelSelectionService",
    "ChampionSelectionCriteria",
    "ChampionChallengerSelection",
    "ChampionHistory",
    "SelectionRepository",
    
    # Retrain Scheduler
    "RetrainSchedulerService",
    "RetrainSchedule",
    "RetrainTrigger",
    "SchedulerRepository",
    
    # MLflow Integration
    "MLflowIntegrationService",
    "MLflowConfig",
    "ExperimentRun",
]