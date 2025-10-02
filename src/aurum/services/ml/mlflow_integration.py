"""MLflow Integration Service.

This service handles MLflow experiment tracking, model artifact storage,
metrics logging, and integration with the MLflow model registry.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from contextlib import contextmanager

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService


class MLflowConfig(BaseModel):
    """Configuration for MLflow integration."""
    
    enabled: bool = False
    tracking_uri: str = "http://localhost:5000"
    registry_uri: Optional[str] = None
    experiment_name: Optional[str] = "Default"
    artifact_location: Optional[str] = None
    default_tags: Dict[str, str] = Field(default_factory=dict)
    auto_log_metrics: bool = True
    auto_log_params: bool = True
    auto_log_artifacts: bool = True


class ExperimentRun(BaseModel):
    """Represents an MLflow experiment run."""
    
    run_id: str
    experiment_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    metrics: Dict[str, float] = Field(default_factory=dict)
    params: Dict[str, str] = Field(default_factory=dict)
    tags: Dict[str, str] = Field(default_factory=dict)
    artifacts: List[str] = Field(default_factory=list)


class MLflowIntegrationService(BaseService):
    """
    MLflow experiment tracking integration service.
    
    This service provides a clean interface to MLflow functionality,
    handling experiment management, run tracking, and model logging.
    """
    
    def __init__(
        self,
        config: Optional[MLflowConfig] = None,
        cache_enabled: bool = False  # MLflow has its own caching
    ):
        """
        Initialize the MLflow integration service.
        
        Args:
            config: MLflow configuration
            cache_enabled: Whether to cache MLflow data (usually not needed)
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=0)
        self.config = config or MLflowConfig()
        self.logger = logging.getLogger(__name__)
        
        # MLflow client
        self.mlflow_client = None
        self.mlflow = None
        
        # Current run tracking
        self._active_run = None
        self._run_stack = []
        
        # Initialize MLflow if enabled
        if self.config.enabled:
            self._initialize_mlflow()
    
    def _initialize_mlflow(self):
        """Initialize MLflow client and configuration."""
        try:
            import mlflow
            from mlflow.tracking import MlflowClient
            
            self.mlflow = mlflow
            
            # Set tracking URI
            mlflow.set_tracking_uri(self.config.tracking_uri)
            
            # Set registry URI if specified
            if self.config.registry_uri:
                mlflow.set_registry_uri(self.config.registry_uri)
            
            # Create MLflow client
            self.mlflow_client = MlflowClient(tracking_uri=self.config.tracking_uri)
            
            # Set or create experiment
            if self.config.experiment_name:
                try:
                    experiment = self.mlflow_client.get_experiment_by_name(
                        self.config.experiment_name
                    )
                    if experiment:
                        mlflow.set_experiment(experiment_name=self.config.experiment_name)
                    else:
                        experiment_id = mlflow.create_experiment(
                            name=self.config.experiment_name,
                            artifact_location=self.config.artifact_location
                        )
                        mlflow.set_experiment(experiment_id=experiment_id)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to set experiment {self.config.experiment_name}: {e}"
                    )
            
            self.logger.info(
                f"MLflow initialized with tracking URI: {self.config.tracking_uri}"
            )
            
        except ImportError:
            self.logger.error("MLflow not installed. Install with: pip install mlflow")
            self.config.enabled = False
        except Exception as e:
            self.logger.error(f"Failed to initialize MLflow: {e}")
            self.config.enabled = False
    
    def update_config(self, config: MLflowConfig):
        """Update MLflow configuration and reinitialize if needed."""
        if config == self.config:
            return
        
        self.config = config
        
        if config.enabled and not self.mlflow_client:
            self._initialize_mlflow()
        elif not config.enabled and self.mlflow_client:
            self.mlflow_client = None
            self.mlflow = None
            self.logger.info("MLflow integration disabled")
    
    @contextmanager
    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        nested: bool = False
    ):
        """
        Context manager to start and manage an MLflow run.
        
        Args:
            run_name: Name for the run
            tags: Additional tags for the run
            nested: Whether this is a nested run
            
        Yields:
            Active MLflow run object
        """
        if not self.config.enabled or not self.mlflow:
            yield None
            return
        
        run = None
        try:
            # Merge tags
            all_tags = self.config.default_tags.copy()
            if tags:
                all_tags.update(tags)
            if run_name:
                all_tags["mlflow.runName"] = run_name
            
            # Start run
            run = self.mlflow.start_run(nested=nested, tags=all_tags)
            self._active_run = run
            self._run_stack.append(run)
            
            self.logger.debug(f"Started MLflow run: {run.info.run_id}")
            
            yield run
            
        except Exception as e:
            self.logger.error(f"Failed to start MLflow run: {e}")
            yield None
        finally:
            if run:
                try:
                    self.mlflow.end_run()
                    self._run_stack.pop()
                    self._active_run = self._run_stack[-1] if self._run_stack else None
                except Exception as e:
                    self.logger.error(f"Failed to end MLflow run: {e}")
    
    async def log_params(
        self,
        params: Dict[str, Any],
        prefix: Optional[str] = None
    ) -> bool:
        """
        Log parameters to the current MLflow run.
        
        Args:
            params: Parameters to log
            prefix: Optional prefix for parameter names
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled or not self.mlflow or not self._active_run:
            return False
        
        if not self.config.auto_log_params:
            return True
        
        try:
            # Flatten nested params and convert to strings
            flat_params = self._flatten_dict(params, prefix)
            
            # Log each parameter
            for key, value in flat_params.items():
                self.mlflow.log_param(key, str(value))
            
            self.logger.debug(f"Logged {len(flat_params)} parameters to MLflow")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log parameters to MLflow: {e}")
            return False
    
    async def log_metrics(
        self,
        metrics: Dict[str, float],
        step: Optional[int] = None,
        prefix: Optional[str] = None
    ) -> bool:
        """
        Log metrics to the current MLflow run.
        
        Args:
            metrics: Metrics to log
            step: Optional step number for metrics
            prefix: Optional prefix for metric names
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled or not self.mlflow or not self._active_run:
            return False
        
        if not self.config.auto_log_metrics:
            return True
        
        try:
            # Flatten nested metrics
            flat_metrics = self._flatten_dict(metrics, prefix)
            
            # Log each metric
            for key, value in flat_metrics.items():
                if isinstance(value, (int, float)):
                    self.mlflow.log_metric(key, float(value), step=step)
                else:
                    self.logger.warning(f"Skipping non-numeric metric: {key}={value}")
            
            self.logger.debug(f"Logged {len(flat_metrics)} metrics to MLflow")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log metrics to MLflow: {e}")
            return False
    
    async def log_model(
        self,
        model_path: Union[str, Path],
        model_type: str,
        artifact_path: str = "model",
        registered_model_name: Optional[str] = None,
        signature: Optional[Any] = None,
        input_example: Optional[Any] = None,
        **kwargs
    ) -> bool:
        """
        Log a model to MLflow.
        
        Args:
            model_path: Path to model file or directory
            model_type: Type of model (sklearn, tensorflow, pytorch, etc.)
            artifact_path: Path within the run's artifact directory
            registered_model_name: Name to register model under
            signature: Model signature for inference
            input_example: Example model input
            **kwargs: Additional model-specific arguments
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled or not self.mlflow or not self._active_run:
            return False
        
        if not self.config.auto_log_artifacts:
            return True
        
        try:
            # Convert to Path object
            model_path = Path(model_path)
            
            if not model_path.exists():
                self.logger.error(f"Model path does not exist: {model_path}")
                return False
            
            # Log based on model type
            if model_type == "sklearn":
                import mlflow.sklearn
                # Assume model is already loaded
                # In real impl, would load from path
                mlflow.sklearn.log_model(
                    sk_model=kwargs.get("model"),
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example
                )
            elif model_type == "tensorflow":
                import mlflow.tensorflow
                mlflow.tensorflow.log_model(
                    tf_saved_model_dir=str(model_path),
                    tf_meta_graph_tags=kwargs.get("meta_graph_tags"),
                    tf_signature_def_key=kwargs.get("signature_def_key"),
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example
                )
            elif model_type == "pytorch":
                import mlflow.pytorch
                mlflow.pytorch.log_model(
                    pytorch_model=kwargs.get("model"),
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example
                )
            elif model_type == "xgboost":
                import mlflow.xgboost
                mlflow.xgboost.log_model(
                    xgb_model=kwargs.get("model"),
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example
                )
            else:
                # Generic model logging
                self.mlflow.log_artifact(str(model_path), artifact_path)
                self.logger.warning(
                    f"Model type {model_type} not specifically supported, "
                    "logged as generic artifact"
                )
            
            self.logger.info(
                f"Logged {model_type} model to MLflow",
                extra={"model_path": str(model_path), "artifact_path": artifact_path}
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log model to MLflow: {e}")
            return False
    
    async def log_artifact(
        self,
        local_path: Union[str, Path],
        artifact_path: Optional[str] = None
    ) -> bool:
        """
        Log a local file or directory as an artifact.
        
        Args:
            local_path: Path to local file or directory
            artifact_path: Destination path within run's artifact directory
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled or not self.mlflow or not self._active_run:
            return False
        
        if not self.config.auto_log_artifacts:
            return True
        
        try:
            local_path = Path(local_path)
            
            if not local_path.exists():
                self.logger.error(f"Artifact path does not exist: {local_path}")
                return False
            
            if local_path.is_file():
                self.mlflow.log_artifact(str(local_path), artifact_path)
            else:
                self.mlflow.log_artifacts(str(local_path), artifact_path)
            
            self.logger.debug(f"Logged artifact: {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log artifact to MLflow: {e}")
            return False
    
    async def log_figure(
        self,
        figure: Any,
        artifact_file: str
    ) -> bool:
        """
        Log a matplotlib figure to MLflow.
        
        Args:
            figure: Matplotlib figure object
            artifact_file: Name for the artifact file
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled or not self.mlflow or not self._active_run:
            return False
        
        try:
            self.mlflow.log_figure(figure, artifact_file)
            self.logger.debug(f"Logged figure: {artifact_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to log figure to MLflow: {e}")
            return False
    
    async def get_experiment_runs(
        self,
        experiment_name: Optional[str] = None,
        filter_string: Optional[str] = None,
        max_results: int = 100
    ) -> List[ExperimentRun]:
        """
        Get runs from an experiment.
        
        Args:
            experiment_name: Name of experiment (uses default if not specified)
            filter_string: MLflow filter string
            max_results: Maximum number of runs to return
            
        Returns:
            List of ExperimentRun objects
        """
        if not self.config.enabled or not self.mlflow_client:
            return []
        
        try:
            # Get experiment
            exp_name = experiment_name or self.config.experiment_name
            experiment = self.mlflow_client.get_experiment_by_name(exp_name)
            
            if not experiment:
                self.logger.warning(f"Experiment not found: {exp_name}")
                return []
            
            # Search runs
            runs = self.mlflow_client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=filter_string,
                max_results=max_results
            )
            
            # Convert to our model
            result = []
            for run in runs:
                result.append(ExperimentRun(
                    run_id=run.info.run_id,
                    experiment_id=run.info.experiment_id,
                    status=run.info.status,
                    start_time=datetime.fromtimestamp(run.info.start_time / 1000),
                    end_time=datetime.fromtimestamp(run.info.end_time / 1000) if run.info.end_time else None,
                    metrics=run.data.metrics,
                    params=run.data.params,
                    tags=run.data.tags
                ))
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to get experiment runs: {e}")
            return []
    
    async def get_run_artifacts(
        self,
        run_id: str,
        path: Optional[str] = None
    ) -> List[str]:
        """
        List artifacts for a run.
        
        Args:
            run_id: MLflow run ID
            path: Optional path within artifacts
            
        Returns:
            List of artifact paths
        """
        if not self.config.enabled or not self.mlflow_client:
            return []
        
        try:
            artifacts = self.mlflow_client.list_artifacts(run_id, path)
            return [a.path for a in artifacts]
        except Exception as e:
            self.logger.error(f"Failed to list artifacts: {e}")
            return []
    
    async def download_artifacts(
        self,
        run_id: str,
        path: str,
        dst_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Download artifacts from a run.
        
        Args:
            run_id: MLflow run ID
            path: Path to artifact
            dst_path: Optional destination path
            
        Returns:
            Local path to downloaded artifacts
        """
        if not self.config.enabled or not self.mlflow_client:
            return None
        
        try:
            return self.mlflow_client.download_artifacts(run_id, path, dst_path)
        except Exception as e:
            self.logger.error(f"Failed to download artifacts: {e}")
            return None
    
    def _flatten_dict(
        self,
        d: Dict[str, Any],
        prefix: Optional[str] = None,
        sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten nested dictionary with optional prefix."""
        items = []
        for k, v in d.items():
            new_key = f"{prefix}{sep}{k}" if prefix else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    async def create_model_version(
        self,
        name: str,
        source: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        """
        Create a new model version in MLflow model registry.
        
        Args:
            name: Registered model name
            source: Source run ID or artifact URI
            description: Version description
            tags: Version tags
            
        Returns:
            Model version number if successful
        """
        if not self.config.enabled or not self.mlflow_client:
            return None
        
        try:
            # Create registered model if it doesn't exist
            try:
                self.mlflow_client.create_registered_model(name)
            except Exception:
                # Model already exists
                pass
            
            # Create model version
            mv = self.mlflow_client.create_model_version(
                name=name,
                source=source,
                description=description,
                tags=tags
            )
            
            self.logger.info(
                f"Created model version {mv.version} for model {name}"
            )
            
            return mv.version
            
        except Exception as e:
            self.logger.error(f"Failed to create model version: {e}")
            return None
    
    async def transition_model_version_stage(
        self,
        name: str,
        version: str,
        stage: str,
        archive_existing_versions: bool = False
    ) -> bool:
        """
        Transition a model version to a new stage.
        
        Args:
            name: Registered model name
            version: Model version
            stage: Target stage (Staging, Production, Archived)
            archive_existing_versions: Archive other versions in target stage
            
        Returns:
            True if successful
        """
        if not self.config.enabled or not self.mlflow_client:
            return False
        
        try:
            self.mlflow_client.transition_model_version_stage(
                name=name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing_versions
            )
            
            self.logger.info(
                f"Transitioned model {name} version {version} to {stage}"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to transition model stage: {e}")
            return False
