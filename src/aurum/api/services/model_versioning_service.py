"""Model Versioning Service for ML model registration and lifecycle management.

This service handles:
- Model registration and versioning
- Model metadata management
- Model archiving and cleanup
- Version number generation
- Model validation and quality checks
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ModelVersion(BaseModel):
    """Model version metadata."""

    version_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    version_number: str
    description: str
    model_type: str
    hyperparameters: Dict[str, Any]
    feature_selection: List[str]
    target_variable: str
    model_path: str
    model_size_bytes: int
    performance_metrics: Dict[str, Any]
    feature_importance: Dict[str, Any]
    validation_results: Dict[str, Any]
    created_by: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_champion: bool = False
    is_archived: bool = False


class RegisteredModel(BaseModel):
    """Registered ML model with all versions."""

    model_name: str
    description: str
    model_type: str
    versions: List[ModelVersion] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def add_version(self, version: ModelVersion) -> None:
        """Add a new version to the model."""
        self.versions.append(version)
        self.updated_at = datetime.utcnow()

    def name(self) -> str:
        """Get model name."""
        return self.model_name

    def total_versions(self) -> int:
        """Get total number of versions."""
        return len(self.versions)


class ModelVersioningService:
    """Service for managing ML model versions and registration."""

    def __init__(self) -> None:
        self._models: Dict[str, RegisteredModel] = {}

    async def register_model(
        self,
        model_name: str,
        description: str,
        model_type: str
    ) -> RegisteredModel:
        """Register a new ML model."""
        if model_name in self._models:
            return self._models[model_name]

        model = RegisteredModel(
            model_name=model_name,
            description=description,
            model_type=model_type
        )

        self._models[model_name] = model
        logger.info(f"Registered new model: {model_name}")
        return model

    async def register_model_version(
        self,
        model_name: str,
        version: ModelVersion
    ) -> bool:
        """Register a new version of a model."""
        if model_name not in self._models:
            await self.register_model(model_name, f"Model: {model_name}", version.model_type)

        model = self._models[model_name]
        model.add_version(version)

        logger.info(f"Registered version {version.version_number} for model {model_name}")
        return True

    async def get_model(self, model_name: str) -> Optional[RegisteredModel]:
        """Get model by name."""
        return self._models.get(model_name)

    async def get_model_version(
        self,
        model_name: str,
        version_number: str
    ) -> Optional[ModelVersion]:
        """Get specific model version."""
        model = self._models.get(model_name)
        if not model:
            return None

        for version in model.versions:
            if version.version_number == version_number:
                return version

        return None

    async def get_latest_model_version(self, model_name: str) -> Optional[ModelVersion]:
        """Get latest version of a model."""
        model = self._models.get(model_name)
        if not model or not model.versions:
            return None

        return max(model.versions, key=lambda v: v.created_at)

    async def get_current_champion_model(self, model_name: str) -> Optional[ModelVersion]:
        """Get current champion model version."""
        model = self._models.get(model_name)
        if not model:
            return None

        for version in model.versions:
            if version.is_champion:
                return version

        return None

    async def update_model_metadata(
        self,
        model_name: str,
        version_number: str,
        metadata: Dict[str, Any]
    ) -> bool:
        """Update model version metadata."""
        version = await self.get_model_version(model_name, version_number)
        if not version:
            return False

        # Update allowed metadata fields
        for key, value in metadata.items():
            if hasattr(version, key):
                setattr(version, key, value)

        logger.info(f"Updated metadata for {model_name} version {version_number}")
        return True

    async def archive_model_version(
        self,
        model_name: str,
        version_number: str
    ) -> bool:
        """Archive a model version."""
        version = await self.get_model_version(model_name, version_number)
        if not version:
            return False

        version.is_archived = True
        logger.info(f"Archived model version {version_number} for {model_name}")
        return True

    async def promote_to_champion(
        self,
        model_name: str,
        version_number: str
    ) -> bool:
        """Promote a model version to champion."""
        model = self._models.get(model_name)
        if not model:
            return False

        # Demote current champion
        for version in model.versions:
            version.is_champion = False

        # Promote new champion
        version = await self.get_model_version(model_name, version_number)
        if version:
            version.is_champion = True
            logger.info(f"Promoted {model_name} version {version_number} to champion")
            return True

        return False

    async def get_next_version_number(self, model_name: str) -> str:
        """Generate next version number for a model."""
        model = self._models.get(model_name)
        if not model:
            return "v1.0"

        if not model.versions:
            return "v1.0"

        # Simple versioning: increment minor version
        latest_version = max(model.versions, key=lambda v: v.created_at)
        current_number = latest_version.version_number

        try:
            parts = current_number.split('.')
            if len(parts) == 2 and parts[0].startswith('v'):
                major = int(parts[0][1:])
                minor = int(parts[1])
                return f"v{major}.{minor + 1}"
        except (ValueError, IndexError):
            pass

        # Fallback to timestamp-based versioning
        return f"v{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
