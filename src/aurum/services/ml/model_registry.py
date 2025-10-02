"""Core Model Registry Service.

This service handles model registration, versioning, and metadata management
following SOLID principles and clean architecture patterns.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class RegisteredModel(BaseModel):
    """Represents a registered ML model in the registry."""
    
    model_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    model_type: str
    description: str = ""
    status: str = "active"
    versions: List[str] = Field(default_factory=list)
    latest_version: Optional[str] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    owners: Set[str] = Field(default_factory=set)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ModelVersion(BaseModel):
    """Represents a specific version of a registered model."""
    
    version_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    version_number: str
    description: str = ""
    status: str = "registered"
    model_path: Optional[str] = None
    model_size_bytes: Optional[int] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    performance_metrics: Dict[str, float] = Field(default_factory=dict)
    feature_importance: Dict[str, float] = Field(default_factory=dict)
    validation_results: Dict[str, Any] = Field(default_factory=dict)
    training_start_date: Optional[datetime] = None
    training_end_date: Optional[datetime] = None
    deployment_date: Optional[datetime] = None
    retirement_date: Optional[datetime] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_by: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ModelRegistryRepository(BaseRepository):
    """Repository interface for model registry operations."""
    
    async def save_model(self, model: RegisteredModel) -> RegisteredModel:
        """Save or update a registered model."""
        raise NotImplementedError
    
    async def get_model(self, model_name: str) -> Optional[RegisteredModel]:
        """Get a model by name."""
        raise NotImplementedError
    
    async def list_models(
        self,
        status: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RegisteredModel]:
        """List registered models with optional filters."""
        raise NotImplementedError
    
    async def save_version(self, version: ModelVersion) -> ModelVersion:
        """Save a model version."""
        raise NotImplementedError
    
    async def get_version(self, version_id: str) -> Optional[ModelVersion]:
        """Get a specific model version."""
        raise NotImplementedError
    
    async def list_versions(
        self,
        model_name: str,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """List versions for a model."""
        raise NotImplementedError


class ModelRegistryService(BaseService):
    """
    Core model registration and versioning service.
    
    This service handles the fundamental operations of model lifecycle management
    including registration, versioning, and metadata management.
    """
    
    def __init__(
        self,
        repository: Optional[ModelRegistryRepository] = None,
        cache_enabled: bool = True,
        cache_ttl: int = 300
    ):
        """
        Initialize the model registry service.
        
        Args:
            repository: Repository for data persistence
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.logger = logging.getLogger(__name__)
        
        # In-memory indexes for fast lookups
        self._models_by_name: Dict[str, RegisteredModel] = {}
        self._versions_by_id: Dict[str, ModelVersion] = {}
    
    def _get_default_repository(self) -> ModelRegistryRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        # For now, return a mock repository
        class MockRepository(ModelRegistryRepository):
            async def save_model(self, model: RegisteredModel) -> RegisteredModel:
                return model
            
            async def get_model(self, model_name: str) -> Optional[RegisteredModel]:
                return None
            
            async def list_models(self, **kwargs) -> List[RegisteredModel]:
                return []
            
            async def save_version(self, version: ModelVersion) -> ModelVersion:
                return version
            
            async def get_version(self, version_id: str) -> Optional[ModelVersion]:
                return None
            
            async def list_versions(self, **kwargs) -> List[ModelVersion]:
                return []
        
        return MockRepository()
    
    async def register_model(
        self,
        model_name: str,
        model_type: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owners: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> RegisteredModel:
        """
        Register a new model or update existing registration.
        
        Args:
            model_name: Unique name for the model
            model_type: Type of model (e.g., 'xgboost', 'tensorflow')
            description: Human-readable description
            tags: Key-value tags for categorization
            owners: List of model owners
            metadata: Additional metadata
            
        Returns:
            RegisteredModel instance
        """
        # Check cache first
        cache_key = f"model:{model_name}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                self.logger.debug(f"Model {model_name} found in cache")
                return RegisteredModel(**cached)
        
        # Check if model exists
        existing = await self.repository.get_model(model_name)
        
        if existing:
            # Update existing model
            if description:
                existing.description = description
            if tags:
                existing.tags.update(tags)
            if owners:
                existing.owners.update(owners)
            if metadata:
                existing.metadata.update(metadata)
            existing.updated_at = datetime.utcnow()
            
            model = await self.repository.save_model(existing)
            self.logger.info(f"Updated existing model: {model_name}")
        else:
            # Create new model
            model = RegisteredModel(
                model_name=model_name,
                model_type=model_type,
                description=description,
                tags=tags or {},
                owners=set(owners) if owners else set(),
                metadata=metadata or {}
            )
            
            model = await self.repository.save_model(model)
            self.logger.info(f"Registered new model: {model_name}")
        
        # Update cache and index
        self._models_by_name[model_name] = model
        if self.cache_enabled:
            await self._set_cache(cache_key, model.dict(), ttl=self.cache_ttl)
        
        # Emit metrics
        await self._emit_metric(
            "model_registered",
            tags={"model_type": model_type, "action": "create" if not existing else "update"}
        )
        
        return model
    
    async def register_model_version(
        self,
        model_name: str,
        version_number: str,
        description: str = "",
        model_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        performance_metrics: Optional[Dict[str, float]] = None,
        created_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ) -> ModelVersion:
        """
        Register a new version of a model.
        
        Args:
            model_name: Name of the parent model
            version_number: Version identifier (e.g., 'v1.0', '2023.10.1')
            description: Version description
            model_path: Path to model artifacts
            config: Model configuration used for training
            performance_metrics: Performance metrics dict
            created_by: User or system that created the version
            metadata: Additional metadata
            
        Returns:
            ModelVersion instance
            
        Raises:
            ValueError: If model is not registered
        """
        # Ensure model exists
        model = await self.get_model(model_name)
        if not model:
            raise ValueError(f"Model {model_name} not found. Register model first.")
        
        # Create version
        version = ModelVersion(
            model_name=model_name,
            version_number=version_number,
            description=description,
            model_path=model_path,
            config=config or {},
            performance_metrics=performance_metrics or {},
            created_by=created_by,
            metadata=metadata or {}
        )
        
        # Save version
        version = await self.repository.save_version(version)
        
        # Update model
        model.versions.append(version.version_id)
        model.latest_version = version.version_id
        model.updated_at = datetime.utcnow()
        await self.repository.save_model(model)
        
        # Update indexes
        self._versions_by_id[version.version_id] = version
        
        self.logger.info(f"Registered version {version_number} for model {model_name}")
        
        # Emit metrics
        await self._emit_metric(
            "model_version_registered",
            tags={"model_name": model_name, "version": version_number}
        )
        
        return version
    
    async def get_model(self, model_name: str) -> Optional[RegisteredModel]:
        """
        Get a registered model by name.
        
        Args:
            model_name: Name of the model
            
        Returns:
            RegisteredModel if found, None otherwise
        """
        # Check memory index first
        if model_name in self._models_by_name:
            return self._models_by_name[model_name]
        
        # Check cache
        cache_key = f"model:{model_name}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                model = RegisteredModel(**cached)
                self._models_by_name[model_name] = model
                return model
        
        # Load from repository
        model = await self.repository.get_model(model_name)
        if model:
            self._models_by_name[model_name] = model
            if self.cache_enabled:
                await self._set_cache(cache_key, model.dict(), ttl=self.cache_ttl)
        
        return model
    
    async def get_model_version(self, version_id: str) -> Optional[ModelVersion]:
        """
        Get a specific model version.
        
        Args:
            version_id: Version identifier
            
        Returns:
            ModelVersion if found, None otherwise
        """
        # Check memory index
        if version_id in self._versions_by_id:
            return self._versions_by_id[version_id]
        
        # Load from repository
        version = await self.repository.get_version(version_id)
        if version:
            self._versions_by_id[version_id] = version
        
        return version
    
    async def get_latest_model_version(self, model_name: str) -> Optional[ModelVersion]:
        """
        Get the latest version of a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Latest ModelVersion if found, None otherwise
        """
        model = await self.get_model(model_name)
        if not model or not model.latest_version:
            return None
        
        return await self.get_model_version(model.latest_version)
    
    async def list_models(
        self,
        status: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RegisteredModel]:
        """
        List registered models with optional filters.
        
        Args:
            status: Filter by status
            tags: Filter by tags (all must match)
            owner: Filter by owner
            limit: Maximum results to return
            offset: Pagination offset
            
        Returns:
            List of RegisteredModel instances
        """
        # For now, use repository directly
        # TODO: Add caching strategy for list operations
        models = await self.repository.list_models(
            status=status,
            tags=tags,
            limit=limit,
            offset=offset
        )
        
        # Apply additional filters in memory if needed
        if owner:
            models = [m for m in models if owner in m.owners]
        
        return models
    
    async def list_model_versions(
        self,
        model_name: str,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """
        List versions for a model.
        
        Args:
            model_name: Name of the model
            status: Filter by status
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of ModelVersion instances
        """
        return await self.repository.list_versions(
            model_name=model_name,
            status=status,
            limit=limit,
            offset=offset
        )
    
    async def update_model_metadata(
        self,
        model_name: str,
        description: Optional[str] = None,
        status: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        owners: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> RegisteredModel:
        """
        Update model metadata.
        
        Args:
            model_name: Name of the model
            description: New description
            status: New status
            tags: Tags to add/update
            owners: Owners to add
            metadata: Metadata to add/update
            
        Returns:
            Updated RegisteredModel
            
        Raises:
            ValueError: If model not found
        """
        model = await self.get_model(model_name)
        if not model:
            raise ValueError(f"Model {model_name} not found")
        
        # Update fields
        if description is not None:
            model.description = description
        if status is not None:
            model.status = status
        if tags:
            model.tags.update(tags)
        if owners:
            model.owners.update(owners)
        if metadata:
            model.metadata.update(metadata)
        
        model.updated_at = datetime.utcnow()
        
        # Save and update cache
        model = await self.repository.save_model(model)
        self._models_by_name[model_name] = model
        
        cache_key = f"model:{model_name}"
        if self.cache_enabled:
            await self._set_cache(cache_key, model.dict(), ttl=self.cache_ttl)
        
        self.logger.info(f"Updated metadata for model {model_name}")
        
        return model
    
    async def retire_model(self, model_name: str, reason: str = "") -> RegisteredModel:
        """
        Retire a model from active use.
        
        Args:
            model_name: Name of the model
            reason: Retirement reason
            
        Returns:
            Updated RegisteredModel
        """
        return await self.update_model_metadata(
            model_name=model_name,
            status="retired",
            metadata={"retirement_reason": reason, "retired_at": datetime.utcnow().isoformat()}
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