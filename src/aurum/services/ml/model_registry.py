"""Model registry service for ML model management.

Implements business logic for model versioning, training job management,
model comparison, and deployment lifecycle.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from uuid import uuid4

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError

logger = logging.getLogger(__name__)


class ModelRegistryService(BaseService):
    """Service for ML model registry operations.
    
    Model registry provides:
    - Model versioning and lifecycle management
    - Training job tracking and metrics
    - Model comparison and A/B testing
    - Deployment management
    - Model performance monitoring
    
    This service:
    - Manages model definitions and versions
    - Tracks training jobs and experiments
    - Provides model comparison analytics
    - Implements model promotion workflows
    - Enforces model governance policies
    """
    
    def __init__(self):
        """Initialize service with default configuration."""
        super().__init__()
        self._models: Dict[str, Dict[str, Any]] = {}
        self._versions: Dict[str, List[Dict[str, Any]]] = {}
        self._training_jobs: Dict[str, Dict[str, Any]] = {}
    
    async def register_model(
        self,
        name: str,
        model_type: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Register a new model in the registry.
        
        Args:
            name: Model name (unique identifier)
            model_type: Type of model (e.g., "forecasting", "classification")
            description: Model description
            tags: Model tags for categorization
            context: Service context
            
        Returns:
            ServiceResult with registered model
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If registration fails
        """
        self._log_operation("register_model", context=context, model_name=name)
        
        try:
            # Validate inputs
            self._validate_model_name(name)
            self._validate_model_type(model_type)
            
            # Check if model already exists
            if name in self._models:
                raise ValidationError(f"Model '{name}' already registered", field="name")
            
            # Create model entry
            model = {
                "name": name,
                "model_type": model_type,
                "description": description or "",
                "tags": tags or [],
                "created_at": datetime.now().isoformat(),
                "version_count": 0,
                "latest_version": None,
                "status": "registered"
            }
            
            self._models[name] = model
            self._versions[name] = []
            
            return ServiceResult.ok(
                data=model,
                metadata={"model_name": name, "registered": True}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "register_model", context)
    
    async def create_model_version(
        self,
        model_name: str,
        version: str,
        model_path: str,
        metrics: Optional[Dict[str, float]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a new version of a model.
        
        Args:
            model_name: Name of the model
            version: Version identifier (e.g., "v1.0.0")
            model_path: Path to model artifacts
            metrics: Model performance metrics
            parameters: Model hyperparameters
            context: Service context
            
        Returns:
            ServiceResult with created model version
            
        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If model not found
            ServiceError: If creation fails
        """
        self._log_operation(
            "create_model_version",
            context=context,
            model_name=model_name,
            version=version
        )
        
        try:
            # Validate inputs
            self._validate_model_name(model_name)
            self._validate_version(version)
            self._validate_model_path(model_path)
            
            # Check if model exists
            if model_name not in self._models:
                raise NotFoundError("model", model_name)
            
            # Check if version already exists
            existing_versions = self._versions.get(model_name, [])
            if any(v["version"] == version for v in existing_versions):
                raise ValidationError(f"Version '{version}' already exists", field="version")
            
            # Create version entry
            model_version = {
                "model_name": model_name,
                "version": version,
                "model_path": model_path,
                "metrics": metrics or {},
                "parameters": parameters or {},
                "created_at": datetime.now().isoformat(),
                "status": "active"
            }
            
            self._versions[model_name].append(model_version)
            
            # Update model info
            self._models[model_name]["version_count"] = len(self._versions[model_name])
            self._models[model_name]["latest_version"] = version
            
            return ServiceResult.ok(
                data=model_version,
                metadata={
                    "model_name": model_name,
                    "version": version,
                    "created": True
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "create_model_version", context)
    
    async def get_model(
        self,
        model_name: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get model information.
        
        Args:
            model_name: Model name
            context: Service context
            
        Returns:
            ServiceResult with model information
            
        Raises:
            ValidationError: If name invalid
            NotFoundError: If model not found
            ServiceError: If retrieval fails
        """
        self._log_operation("get_model", context=context, model_name=model_name)
        
        try:
            self._validate_model_name(model_name)
            
            if model_name not in self._models:
                raise NotFoundError("model", model_name)
            
            model = self._models[model_name]
            
            return ServiceResult.ok(
                data=model,
                metadata={"model_name": model_name}
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_model", context)
    
    async def list_models(
        self,
        model_type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """List models with optional filtering.
        
        Args:
            model_type: Filter by model type
            tags: Filter by tags
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with list of models
        """
        self._log_operation("list_models", context=context, model_type=model_type)
        
        try:
            if limit < 1 or limit > 1000:
                raise ValidationError("Limit must be between 1 and 1000", field="limit")
            
            # Filter models
            models = list(self._models.values())
            
            if model_type:
                self._validate_model_type(model_type)
                models = [m for m in models if m["model_type"] == model_type]
            
            if tags:
                models = [m for m in models if any(tag in m["tags"] for tag in tags)]
            
            # Apply limit
            models = models[:limit]
            
            return ServiceResult.ok(
                data=models,
                metadata={
                    "model_count": len(models),
                    "limit": limit,
                    "model_type": model_type
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "list_models", context)
    
    async def get_model_versions(
        self,
        model_name: str,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get all versions of a model.
        
        Args:
            model_name: Model name
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with list of versions
            
        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If model not found
            ServiceError: If retrieval fails
        """
        self._log_operation("get_model_versions", context=context, model_name=model_name)
        
        try:
            self._validate_model_name(model_name)
            
            if model_name not in self._models:
                raise NotFoundError("model", model_name)
            
            versions = self._versions.get(model_name, [])
            
            return ServiceResult.ok(
                data=versions[:limit],
                metadata={
                    "model_name": model_name,
                    "version_count": len(versions),
                    "limit": limit
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_model_versions", context)
    
    async def compare_models(
        self,
        model_name1: str,
        version1: str,
        model_name2: str,
        version2: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Compare two model versions.
        
        Args:
            model_name1: First model name
            version1: First model version
            model_name2: Second model name
            version2: Second model version
            context: Service context
            
        Returns:
            ServiceResult with comparison results
            
        Raises:
            ValidationError: If parameters invalid
            NotFoundError: If models/versions not found
            ServiceError: If comparison fails
        """
        self._log_operation(
            "compare_models",
            context=context,
            model1=f"{model_name1}:{version1}",
            model2=f"{model_name2}:{version2}"
        )
        
        try:
            # Validate inputs
            self._validate_model_name(model_name1)
            self._validate_model_name(model_name2)
            self._validate_version(version1)
            self._validate_version(version2)
            
            # Get model versions
            version1_data = self._get_version(model_name1, version1)
            version2_data = self._get_version(model_name2, version2)
            
            if not version1_data:
                raise NotFoundError("model_version", f"{model_name1}:{version1}")
            if not version2_data:
                raise NotFoundError("model_version", f"{model_name2}:{version2}")
            
            # Perform comparison
            comparison = self._compute_comparison(version1_data, version2_data)
            
            return ServiceResult.ok(
                data=comparison,
                metadata={
                    "model1": model_name1,
                    "version1": version1,
                    "model2": model_name2,
                    "version2": version2
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "compare_models", context)
    
    # Private helper methods
    
    def _validate_model_name(self, name: str) -> None:
        """Validate model name."""
        if not name or not name.strip():
            raise ValidationError("Model name is required", field="name")
        
        if len(name) > 100:
            raise ValidationError("Model name too long", field="name")
        
        invalid_chars = ["<", ">", "&", "\"", "'", ";"]
        if any(char in name for char in invalid_chars):
            raise ValidationError("Model name contains invalid characters", field="name")
    
    def _validate_model_type(self, model_type: str) -> None:
        """Validate model type."""
        valid_types = ["forecasting", "classification", "regression", "clustering", "anomaly_detection"]
        if model_type not in valid_types:
            raise ValidationError(
                f"Invalid model type. Must be one of: {', '.join(valid_types)}",
                field="model_type"
            )
    
    def _validate_version(self, version: str) -> None:
        """Validate version string."""
        if not version or not version.strip():
            raise ValidationError("Version is required", field="version")
        
        if len(version) > 50:
            raise ValidationError("Version string too long", field="version")
    
    def _validate_model_path(self, path: str) -> None:
        """Validate model path."""
        if not path or not path.strip():
            raise ValidationError("Model path is required", field="model_path")
    
    def _get_version(self, model_name: str, version: str) -> Optional[Dict[str, Any]]:
        """Get a specific model version."""
        versions = self._versions.get(model_name, [])
        for v in versions:
            if v["version"] == version:
                return v
        return None
    
    def _compute_comparison(
        self,
        version1: Dict[str, Any],
        version2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute comparison between two model versions."""
        metrics1 = version1.get("metrics", {})
        metrics2 = version2.get("metrics", {})
        
        # Calculate metric differences
        metric_diffs = {}
        all_metrics = set(metrics1.keys()) | set(metrics2.keys())
        
        for metric in all_metrics:
            val1 = metrics1.get(metric, 0)
            val2 = metrics2.get(metric, 0)
            metric_diffs[metric] = {
                "version1_value": val1,
                "version2_value": val2,
                "difference": val2 - val1,
                "percent_change": ((val2 - val1) / val1 * 100) if val1 != 0 else 0
            }
        
        return {
            "version1": version1["version"],
            "version2": version2["version"],
            "metric_comparisons": metric_diffs,
            "recommendation": self._generate_recommendation(metric_diffs)
        }
    
    def _generate_recommendation(self, metric_diffs: Dict[str, Any]) -> str:
        """Generate recommendation based on metric comparison."""
        # Simplified logic
        improvements = sum(1 for m in metric_diffs.values() if m["difference"] > 0)
        total = len(metric_diffs)
        
        if improvements / total > 0.7:
            return "version2_recommended"
        elif improvements / total < 0.3:
            return "version1_recommended"
        else:
            return "inconclusive"

