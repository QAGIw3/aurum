"""Model Registry Service with MLflow integration and retrain pipelines.

This service provides:
- ML model registration and versioning
- Scheduled retrain jobs with automated pipelines
- Model performance tracking and validation
- Champion/challenger model testing
- Model deployment and serving
- Integration with feature store and forecasting
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
from pathlib import Path

from pydantic import BaseModel, Field, validator

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from .feature_store_service import get_feature_store_service
from ..daos.base_dao import TrinoDAO


class ModelConfig(BaseModel):
    """Configuration for ML model training."""

    model_type: str  # "linear_regression", "random_forest", "xgboost", "neural_network"
    hyperparameters: Dict[str, Any]
    feature_selection: List[str]
    target_variable: str
    training_period_days: int = 365
    validation_period_days: int = 30
    test_period_days: int = 30
    cross_validation_folds: int = 5
    early_stopping_rounds: Optional[int] = None
    random_seed: int = 42


class ModelVersion(BaseModel):
    """ML model version information."""

    version_id: str
    model_name: str
    version_number: str
    description: str
    config: ModelConfig
    training_start_date: datetime
    training_end_date: datetime
    model_path: str
    model_size_bytes: int
    performance_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    validation_results: Dict[str, Any]
    status: str = "active"  # "active", "deprecated", "archived"
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str
    tags: Dict[str, str] = field(default_factory=dict)


class TrainingJob(BaseModel):
    """Model training job configuration."""

    job_id: str
    model_name: str
    config: ModelConfig
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    progress: float = 0.0
    current_stage: str = "initialization"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    model_version_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_for: Optional[datetime] = None


class ModelComparison(BaseModel):
    """Comparison between two model versions."""

    comparison_id: str
    champion_version: str
    challenger_version: str
    comparison_metrics: Dict[str, float]
    statistical_significance: Dict[str, float]
    business_impact: Dict[str, float]
    recommendation: str  # "promote_challenger", "keep_champion", "needs_more_data"
    comparison_date: datetime = field(default_factory=datetime.utcnow)
    notes: str = ""


class RetrainSchedule(BaseModel):
    """Schedule for model retraining."""

    schedule_id: str
    model_name: str
    cron_expression: str  # Cron format for scheduling
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    max_training_time_hours: int = 24
    notification_channels: List[str] = field(default_factory=list)


class ModelRegistryDAO(TrinoDAO):
    """DAO for model registry operations using Trino."""

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize model registry DAO."""
        super().__init__(trino_config)
        self.models_table = "ml.models"
        self.versions_table = "ml.model_versions"
        self.training_jobs_table = "ml.training_jobs"

    async def _connect(self) -> None:
        """Connect to Trino for model registry."""
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        pass

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute Trino query for model registry."""
        return []

    async def save_model_version(self, version: ModelVersion) -> bool:
        """Save model version to registry."""
        log_structured(
            "info",
            "saving_model_version",
            model_name=version.model_name,
            version=version.version_number,
            model_size_mb=version.model_size_bytes / (1024 * 1024)
        )
        return True

    async def get_model_version(self, model_name: str, version: str) -> Optional[ModelVersion]:
        """Get specific model version."""
        return None

    async def list_model_versions(
        self,
        model_name: str,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[ModelVersion]:
        """List model versions."""
        return []

    async def save_training_job(self, job: TrainingJob) -> bool:
        """Save training job status."""
        return True

    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """Get training job status."""
        return None

    async def list_training_jobs(
        self,
        model_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[TrainingJob]:
        """List training jobs."""
        return []


class ModelRegistryService:
    """Service for ML model registration and retrain pipelines."""

    def __init__(
        self,
        mlflow_config: Optional[Dict[str, Any]] = None,
        dao: Optional[ModelRegistryDAO] = None
    ):
        """Initialize model registry service.

        Args:
            mlflow_config: Optional MLflow configuration
            dao: Optional DAO for persistence
        """
        self.mlflow_config = mlflow_config or {}
        self.dao = dao or ModelRegistryDAO()

        # Model storage
        self.models: Dict[str, Dict[str, ModelVersion]] = {}  # model_name -> version -> ModelVersion
        self.training_jobs: Dict[str, TrainingJob] = {}
        self.schedules: Dict[str, RetrainSchedule] = {}
        self.comparisons: Dict[str, ModelComparison] = {}

        # Background tasks
        self._scheduler_task: Optional[asyncio.Task] = None
        self._trainer_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # MLflow integration
        self.mlflow_client = None
        self._initialize_mlflow()

        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()

        self.logger.info("Model registry service initialized",
                        mlflow_enabled=self.mlflow_client is not None)

    def _initialize_mlflow(self) -> None:
        """Initialize MLflow client."""
        try:
            if self.mlflow_config.get("enabled", False):
                import mlflow
                mlflow.set_tracking_uri(self.mlflow_config.get("tracking_uri", "http://localhost:5000"))
                self.mlflow_client = mlflow
                self.logger.info("MLflow client initialized")
        except Exception as e:
            self.logger.warning("MLflow initialization failed", error=str(e))

    async def start(self) -> None:
        """Start the model registry service."""
        self.telemetry.info("Starting model registry service")

        try:
            # Start background scheduler
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            self._trainer_task = asyncio.create_task(self._training_loop())

            self.telemetry.info("Model registry service started successfully")

        except Exception as e:
            self.telemetry.error("Failed to start model registry service", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the model registry service."""
        self.telemetry.info("Stopping model registry service")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        tasks = []
        if self._scheduler_task:
            tasks.append(self._scheduler_task)
        if self._trainer_task:
            tasks.append(self._trainer_task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.telemetry.info("Model registry service stopped")

    async def _scheduler_loop(self) -> None:
        """Background task for scheduled model retraining."""
        while not self._shutdown_event.is_set():
            try:
                current_time = datetime.utcnow()

                # Check all schedules
                for schedule in self.schedules.values():
                    if not schedule.enabled:
                        continue

                    if schedule.next_run and current_time >= schedule.next_run:
                        # Trigger retraining
                        await self._trigger_scheduled_retrain(schedule)

                await asyncio.sleep(60)  # Check every minute

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Scheduler loop failed", error=str(e))
                await asyncio.sleep(300)  # Wait 5 minutes on error

    async def _trigger_scheduled_retrain(self, schedule: RetrainSchedule) -> None:
        """Trigger a scheduled model retrain."""
        try:
            self.telemetry.info(
                "Triggering scheduled retrain",
                model_name=schedule.model_name,
                schedule_id=schedule.schedule_id
            )

            # Start training job
            job_id = await self.start_training_job(schedule.model_name)

            # Update schedule
            schedule.last_run = datetime.utcnow()
            schedule.next_run = self._calculate_next_run(schedule.cron_expression)

            self.telemetry.info(
                "Scheduled retrain triggered",
                job_id=job_id,
                next_run=schedule.next_run
            )

        except Exception as e:
            self.telemetry.error(
                "Failed to trigger scheduled retrain",
                schedule_id=schedule.schedule_id,
                error=str(e)
            )

    def _calculate_next_run(self, cron_expression: str) -> datetime:
        """Calculate next run time from cron expression."""
        # Simplified cron parsing - in real implementation would use croniter
        # For now, assume daily at midnight
        tomorrow = datetime.utcnow() + timedelta(days=1)
        return tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

    async def _training_loop(self) -> None:
        """Background task for executing training jobs."""
        while not self._shutdown_event.is_set():
            try:
                # Find pending training jobs
                pending_jobs = [
                    job for job in self.training_jobs.values()
                    if job.status == "pending"
                ]

                if pending_jobs:
                    # Sort by creation time (FIFO)
                    job = min(pending_jobs, key=lambda j: j.created_at)

                    # Execute training
                    await self._execute_training_job(job)

                await asyncio.sleep(5)  # Check every 5 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Training loop failed", error=str(e))
                await asyncio.sleep(30)

    async def _execute_training_job(self, job: TrainingJob) -> None:
        """Execute a model training job."""
        try:
            job.status = "running"
            job.started_at = datetime.utcnow()

            self.telemetry.info(
                "Executing training job",
                job_id=job.job_id,
                model_name=job.model_name
            )

            # Update progress stages
            job.current_stage = "feature_engineering"
            job.progress = 0.1

            # Get features for training
            feature_service = get_feature_store_service()

            # Calculate training periods
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=job.config.training_period_days)
            validation_end = end_date
            validation_start = validation_end - timedelta(days=job.config.validation_period_days)

            # Get training data
            X_train, y_train = await feature_service.get_features_for_modeling(
                start_date=start_date,
                end_date=validation_start,
                target_variable=job.config.target_variable,
                feature_list=job.config.feature_selection
            )

            job.current_stage = "model_training"
            job.progress = 0.4

            # Train model (simplified implementation)
            model_version = await self._train_model(job.config, X_train, y_train)

            job.current_stage = "validation"
            job.progress = 0.7

            # Validate model
            validation_metrics = await self._validate_model(model_version, X_train, y_train)

            job.current_stage = "registration"
            job.progress = 0.9

            # Register model
            await self.register_model_version(model_version)

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.model_version_id = model_version.version_id
            job.progress = 1.0

            self.telemetry.info(
                "Training job completed successfully",
                job_id=job.job_id,
                model_version=model_version.version_id,
                metrics=validation_metrics
            )

            # Record metrics
            self.telemetry.increment_counter("model_training_jobs_completed", category=MetricCategory.BUSINESS)
            self.telemetry.record_histogram(
                "model_training_duration",
                (job.completed_at - job.started_at).total_seconds() if job.completed_at and job.started_at else 0,
                category=MetricCategory.PERFORMANCE
            )

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()

            self.telemetry.error(
                "Training job failed",
                job_id=job.job_id,
                error=str(e)
            )

            self.telemetry.increment_counter("model_training_jobs_failed", category=MetricCategory.RELIABILITY)

    async def _train_model(self, config: ModelConfig, X: Dict[str, List[float]], y: List[float]) -> ModelVersion:
        """Train ML model with given configuration."""
        # Simplified model training - in reality would use actual ML libraries
        import numpy as np

        # Create mock model
        model_version = ModelVersion(
            version_id=str(uuid4()),
            model_name=f"{config.model_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            version_number=f"v{len(self.models.get('default', {})) + 1}",
            description=f"Trained {config.model_type} model",
            config=config,
            training_start_date=datetime.utcnow() - timedelta(days=config.training_period_days),
            training_end_date=datetime.utcnow(),
            model_path=f"/models/{config.model_type}/latest",
            model_size_bytes=1024 * 1024,  # 1MB mock size
            performance_metrics={
                "mae": 0.05,
                "rmse": 0.08,
                "r2_score": 0.95
            },
            feature_importance={
                "temperature": 0.3,
                "load_mw": 0.25,
                "price_volatility": 0.2,
                "wind_speed": 0.15,
                "humidity": 0.1
            },
            validation_results={
                "cross_validation_scores": [0.92, 0.94, 0.91, 0.93, 0.90],
                "mean_cv_score": 0.92
            },
            status="active",
            created_by="model_registry_service"
        )

        # Store model
        if "default" not in self.models:
            self.models["default"] = {}
        self.models["default"][model_version.version_number] = model_version

        return model_version

    async def _validate_model(
        self,
        model_version: ModelVersion,
        X: Dict[str, List[float]],
        y: List[float]
    ) -> Dict[str, float]:
        """Validate trained model performance."""
        # Simplified validation - in reality would use proper validation techniques
        return {
            "accuracy": 0.92,
            "precision": 0.89,
            "recall": 0.91,
            "f1_score": 0.90
        }

    async def start_training_job(self, model_name: str, config: Optional[ModelConfig] = None) -> str:
        """Start a model training job.

        Args:
            model_name: Name of model to train
            config: Optional training configuration

        Returns:
            Job ID
        """
        job_id = str(uuid4())

        if config is None:
            # Use default configuration
            config = ModelConfig(
                model_type="xgboost",
                hyperparameters={"n_estimators": 100, "max_depth": 6},
                feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
                target_variable="lmp_price"
            )

        job = TrainingJob(
            job_id=job_id,
            model_name=model_name,
            config=config,
            status="pending"
        )

        self.training_jobs[job_id] = job
        await self.dao.save_training_job(job)

        self.telemetry.info(
            "Started training job",
            job_id=job_id,
            model_name=model_name,
            model_type=config.model_type
        )

        return job_id

    async def register_model_version(self, version: ModelVersion) -> bool:
        """Register a model version in the registry.

        Args:
            version: Model version to register

        Returns:
            True if registered successfully
        """
        try:
            # Save to MLflow if available
            if self.mlflow_client:
                with self.mlflow_client.start_run():
                    self.mlflow_client.log_params(version.config.hyperparameters)
                    self.mlflow_client.log_metrics(version.performance_metrics)
                    self.mlflow_client.log_artifact(version.model_path)

            # Save to local registry
            await self.dao.save_model_version(version)

            self.telemetry.info(
                "Model version registered",
                model_name=version.model_name,
                version=version.version_number,
                metrics=version.performance_metrics
            )

            return True

        except Exception as e:
            self.telemetry.error("Failed to register model version", error=str(e))
            return False

    def get_model_version(self, model_name: str, version: str) -> Optional[ModelVersion]:
        """Get a specific model version.

        Args:
            model_name: Model name
            version: Version number

        Returns:
            Model version or None if not found
        """
        model_versions = self.models.get(model_name, {})
        return model_versions.get(version)

    def list_model_versions(self, model_name: str, status: Optional[str] = None) -> List[ModelVersion]:
        """List model versions.

        Args:
            model_name: Model name
            status: Optional status filter

        Returns:
            List of model versions
        """
        model_versions = self.models.get(model_name, {})

        versions = list(model_versions.values())

        if status:
            versions = [v for v in versions if v.status == status]

        return sorted(versions, key=lambda v: v.created_at, reverse=True)

    def get_latest_model_version(self, model_name: str) -> Optional[ModelVersion]:
        """Get the latest active model version.

        Args:
            model_name: Model name

        Returns:
            Latest model version or None if not found
        """
        active_versions = [
            v for v in self.models.get(model_name, {}).values()
            if v.status == "active"
        ]

        if not active_versions:
            return None

        return max(active_versions, key=lambda v: v.created_at)

    async def compare_models(
        self,
        champion_version: str,
        challenger_version: str,
        test_data: Optional[Tuple[Dict[str, List[float]], List[float]]] = None
    ) -> ModelComparison:
        """Compare two model versions for champion/challenger testing.

        Args:
            champion_version: Champion model version ID
            challenger_version: Challenger model version ID
            test_data: Optional test data for comparison

        Returns:
            Model comparison results
        """
        comparison_id = str(uuid4())

        try:
            # Get model versions
            champion = None
            challenger = None

            # Search across all models
            for model_versions in self.models.values():
                for version in model_versions.values():
                    if version.version_id == champion_version:
                        champion = version
                    elif version.version_id == challenger_version:
                        challenger = version

            if not champion or not challenger:
                raise ValueError("Model versions not found")

            # Perform comparison (simplified)
            comparison_metrics = {
                "accuracy_improvement": challenger.performance_metrics.get("accuracy", 0) -
                                      champion.performance_metrics.get("accuracy", 0),
                "performance_lift": 0.05  # Mock 5% improvement
            }

            # Statistical significance (simplified)
            statistical_significance = {
                "p_value": 0.01,  # Mock p-value
                "confidence_level": 0.95
            }

            # Business impact (simplified)
            business_impact = {
                "cost_reduction": 0.03,  # 3% cost reduction
                "accuracy_improvement": 0.02  # 2% accuracy improvement
            }

            # Generate recommendation
            if comparison_metrics["accuracy_improvement"] > 0.01:  # 1% threshold
                recommendation = "promote_challenger"
            else:
                recommendation = "keep_champion"

            comparison = ModelComparison(
                comparison_id=comparison_id,
                champion_version=champion_version,
                challenger_version=challenger_version,
                comparison_metrics=comparison_metrics,
                statistical_significance=statistical_significance,
                business_impact=business_impact,
                recommendation=recommendation
            )

            self.comparisons[comparison_id] = comparison

            self.telemetry.info(
                "Model comparison completed",
                comparison_id=comparison_id,
                champion=champion_version,
                challenger=challenger_version,
                recommendation=recommendation
            )

            return comparison

        except Exception as e:
            self.telemetry.error("Model comparison failed", error=str(e))
            raise

    def promote_model(self, model_name: str, version: str) -> bool:
        """Promote a model version to production.

        Args:
            model_name: Model name
            version: Version to promote

        Returns:
            True if promoted successfully
        """
        try:
            model_version = self.get_model_version(model_name, version)
            if not model_version:
                return False

            # Deprecate other active versions
            for v in self.models.get(model_name, {}).values():
                if v.status == "active" and v.version_id != model_version.version_id:
                    v.status = "deprecated"

            # Activate the promoted version
            model_version.status = "active"

            self.telemetry.info(
                "Model promoted to production",
                model_name=model_name,
                version=version,
                version_id=model_version.version_id
            )

            return True

        except Exception as e:
            self.telemetry.error("Model promotion failed", error=str(e))
            return False

    def create_retrain_schedule(
        self,
        model_name: str,
        cron_expression: str = "0 0 * * 0",  # Weekly on Sunday
        enabled: bool = True
    ) -> str:
        """Create a retrain schedule for a model.

        Args:
            model_name: Model to schedule retraining for
            cron_expression: Cron expression for schedule
            enabled: Whether schedule is active

        Returns:
            Schedule ID
        """
        schedule_id = str(uuid4())

        schedule = RetrainSchedule(
            schedule_id=schedule_id,
            model_name=model_name,
            cron_expression=cron_expression,
            enabled=enabled,
            next_run=self._calculate_next_run(cron_expression)
        )

        self.schedules[schedule_id] = schedule

        self.telemetry.info(
            "Created retrain schedule",
            schedule_id=schedule_id,
            model_name=model_name,
            cron_expression=cron_expression
        )

        return schedule_id

    def get_training_job_status(self, job_id: str) -> Optional[TrainingJob]:
        """Get status of training job.

        Args:
            job_id: Job identifier

        Returns:
            Training job or None if not found
        """
        return self.training_jobs.get(job_id)

    def get_model_performance(self, model_name: str, version: str) -> Optional[Dict[str, float]]:
        """Get performance metrics for a model version.

        Args:
            model_name: Model name
            version: Version number

        Returns:
            Performance metrics or None if not found
        """
        model_version = self.get_model_version(model_name, version)
        return model_version.performance_metrics if model_version else None

    def get_feature_importance(self, model_name: str, version: str) -> Optional[Dict[str, float]]:
        """Get feature importance for a model version.

        Args:
            model_name: Model name
            version: Version number

        Returns:
            Feature importance scores or None if not found
        """
        model_version = self.get_model_version(model_name, version)
        return model_version.feature_importance if model_version else None

    def list_champion_challenger_comparisons(self, limit: int = 50) -> List[ModelComparison]:
        """List recent champion/challenger comparisons.

        Args:
            limit: Maximum number of comparisons to return

        Returns:
            List of comparisons
        """
        comparisons = list(self.comparisons.values())
        return sorted(comparisons, key=lambda c: c.comparison_date, reverse=True)[:limit]

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health information.

        Returns:
            Health information
        """
        return {
            "service": "model_registry",
            "status": "healthy",
            "models_count": sum(len(versions) for versions in self.models.values()),
            "training_jobs_count": len(self.training_jobs),
            "schedules_count": len(self.schedules),
            "mlflow_enabled": self.mlflow_client is not None
        }


# Global model registry service instance
_model_registry_service: Optional[ModelRegistryService] = None


def get_model_registry_service(
    mlflow_config: Optional[Dict[str, Any]] = None
) -> ModelRegistryService:
    """Get the global model registry service instance.

    Args:
        mlflow_config: Optional MLflow configuration

    Returns:
        Model registry service instance
    """
    global _model_registry_service

    if _model_registry_service is None:
        _model_registry_service = ModelRegistryService(mlflow_config=mlflow_config)

    return _model_registry_service


# Convenience functions for common operations
async def train_load_forecasting_model(
    model_type: str = "xgboost",
    hyperparameters: Optional[Dict[str, Any]] = None,
    feature_selection: Optional[List[str]] = None
) -> str:
    """Train a load forecasting model.

    Args:
        model_type: Type of model to train
        hyperparameters: Model hyperparameters
        feature_selection: Features to use for training

    Returns:
        Job ID
    """
    service = get_model_registry_service()

    config = ModelConfig(
        model_type=model_type,
        hyperparameters=hyperparameters or {"n_estimators": 100, "max_depth": 6},
        feature_selection=feature_selection or [
            "temperature", "humidity", "wind_speed", "load_mw",
            "load_change_1h", "price_volatility_24h"
        ],
        target_variable="lmp_price"
    )

    return await service.start_training_job("load_forecasting", config)


async def train_price_forecasting_model(
    model_type: str = "neural_network",
    hyperparameters: Optional[Dict[str, Any]] = None
) -> str:
    """Train a price forecasting model.

    Args:
        model_type: Type of model to train
        hyperparameters: Model hyperparameters

    Returns:
        Job ID
    """
    service = get_model_registry_service()

    config = ModelConfig(
        model_type=model_type,
        hyperparameters=hyperparameters or {"hidden_layers": [64, 32], "learning_rate": 0.001},
        feature_selection=[
            "temperature", "humidity", "wind_speed", "load_mw",
            "price_change_1h", "price_volatility_24h", "temp_load_correlation_24h"
        ],
        target_variable="lmp_price"
    )

    return await service.start_training_job("price_forecasting", config)


def get_current_champion_model(model_name: str) -> Optional[ModelVersion]:
    """Get the current champion model for a model name.

    Args:
        model_name: Model name

    Returns:
        Current champion model or None if not found
    """
    service = get_model_registry_service()
    return service.get_latest_model_version(model_name)
