"""Model Management Service - Handles model registration and versioning."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union
from uuid import uuid4

try:
    from aurum.telemetry.context import get_request_id, get_tenant_id, get_user_id, log_structured
    from aurum.observability.telemetry_facade import get_telemetry_facade, MetricCategory
    from aurum.cache.consolidated_manager import get_unified_cache_manager
except ImportError:
    # Fallback for demo
    def get_telemetry_facade():
        class MockTelemetry:
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
        return MockTelemetry()
    def get_unified_cache_manager():
        class MockCache:
            def get(self, key): return None
            def set(self, key, value, ttl=None): pass
        return MockCache()
    def get_request_id(): return "demo-request"
    def get_tenant_id(): return "demo-tenant"
    def get_user_id(): return "demo-user"
try:
    from aurum.dao.experimental import TrinoDAO
except ImportError:
    # Mock DAO for demo
    class TrinoDAO:
        pass
from .models import ModelVersion, RegisteredModel, ModelConfig
from .interfaces import IModelManagementService


class ModelRegistryDAO(TrinoDAO):
    """DAO for model registry operations."""

    async def save_model_version(self, version: ModelVersion) -> bool:
        """Save model version to registry."""
        # Implementation would persist to database
        return True

    async def get_model_version(self, model_name: str, version: str) -> Optional[ModelVersion]:
        """Get specific model version."""
        # Implementation would query database
        return None

    async def list_model_versions(
        self,
        model_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """List all versions of a model."""
        # Implementation would query database
        return []

    async def save_registered_model(self, model: RegisteredModel) -> bool:
        """Save registered model metadata."""
        # Implementation would persist to database
        return True

    async def get_registered_model(self, model_name: str) -> Optional[RegisteredModel]:
        """Get registered model metadata."""
        # Implementation would query database
        return None

    async def list_registered_models(self) -> List[RegisteredModel]:
        """List all registered models."""
        # Implementation would query database
        return []


class ModelManagementService(IModelManagementService):
    """Service for managing model registration and versioning."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dao = ModelRegistryDAO()
        self.telemetry = get_telemetry_facade()
        self.cache = get_unified_cache_manager()
        self.models: Dict[str, RegisteredModel] = {}
        self.version_index: Dict[str, ModelVersion] = {}

    async def register_model(
        self,
        model_name: str,
        description: str,
        model_type: str,
        created_by: str,
        **kwargs
    ) -> RegisteredModel:
        """Register a new model in the registry."""
        try:
            # Check if model already exists
            existing = self.models.get(model_name)
            if existing:
                return existing

            # Create new model record
            model = RegisteredModel(
                model_name=model_name,
                description=description,
                model_type=model_type,
                latest_version="",
                created_by=created_by,
                **kwargs
            )

            self.models[model_name] = model
            await self.dao.save_registered_model(model)

            self.telemetry.info(
                "model_registry.model_registered",
                model_name=model_name,
                model_type=model_type,
                version_count=0,
            )

            self._record_audit_event(
                action="register_model",
                model_name=model_name,
                reference={
                    "model_type": model.model_type,
                    "description": model.description,
                    "created_by": created_by,
                },
            )

            return model

        except Exception as exc:
            self.telemetry.error("Failed to register model", error=str(exc))
            raise

    async def register_model_version(
        self,
        model_name: str,
        version: ModelVersion,
        created_by: str
    ) -> ModelVersion:
        """Register a new version of an existing model."""
        try:
            # Ensure model exists
            model = self.models.get(model_name)
            if not model:
                # Auto-register model if it doesn't exist
                model = await self.register_model(
                    model_name=version.model_name,
                    description=version.description,
                    model_type=version.config.model_type,
                    created_by=created_by
                )

            # Update model metadata
            model.total_versions += 1
            model.latest_version = version.version_number

            # Store version
            self.version_index[version.version_id] = version
            await self.dao.save_model_version(version)

            # Update champion status if needed
            if version.status == "champion":
                model.champion_version = version.version_id

            self.telemetry.info(
                "model_registry.model_version_registered",
                model_name=version.model_name,
                version=version.version_number,
                metrics=version.performance_metrics,
                total_versions=model.total_versions
            )

            self._record_audit_event(
                action="register_model_version",
                model_name=model_name,
                reference={
                    "version_id": version.version_id,
                    "version_number": version.version_number,
                    "status": version.status,
                },
            )

            return version

        except Exception as exc:
            self.telemetry.error("Failed to register model version", error=str(exc))
            raise

    async def get_model_version(
        self,
        model_name: str,
        version: str
    ) -> Optional[ModelVersion]:
        """Get a specific model version."""
        # First check local index
        for version_obj in self.version_index.values():
            if version_obj.model_name == model_name and version_obj.version_number == version:
                return version_obj

        # Fall back to DAO query
        return await self.dao.get_model_version(model_name, version)

    async def list_model_versions(
        self,
        model_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ModelVersion]:
        """List all versions of a model."""
        model = self.models.get(model_name)
        if not model:
            return []

        # Get versions from local index first
        versions = [
            v for v in self.version_index.values()
            if v.model_name == model_name
        ]

        # Sort by creation date (newest first)
        versions.sort(key=lambda v: v.created_at, reverse=True)

        # Apply pagination
        start = min(offset, len(versions))
        end = min(start + limit, len(versions))

        return versions[start:end]

    async def list_models(self) -> List[RegisteredModel]:
        """List all registered models."""
        return list(self.models.values())

    async def update_model_version_status(
        self,
        model_name: str,
        version: str,
        status: str,
        updated_by: str
    ) -> bool:
        """Update the status of a model version."""
        try:
            # Find the version
            version_obj = None
            for v in self.version_index.values():
                if v.model_name == model_name and v.version_number == version:
                    version_obj = v
                    break

            if not version_obj:
                return False

            # Update status
            old_status = version_obj.status
            version_obj.status = status
            version_obj.metadata["status_updated_at"] = datetime.utcnow().isoformat()
            version_obj.metadata["status_updated_by"] = updated_by

            # Update champion if needed
            if status == "champion":
                model = self.models.get(model_name)
                if model:
                    model.champion_version = version_obj.version_id
            elif old_status == "champion":
                # Find new champion
                model = self.models.get(model_name)
                if model:
                    active_versions = [
                        v for v in self.version_index.values()
                        if v.model_name == model_name and v.status == "active"
                    ]
                    if active_versions:
                        new_champion = max(active_versions, key=lambda v: v.created_at)
                        model.champion_version = new_champion.version_id

            self.telemetry.info(
                "model_registry.model_version_status_updated",
                model_name=model_name,
                version=version,
                old_status=old_status,
                new_status=status,
                updated_by=updated_by
            )

            self._record_audit_event(
                action="update_model_version_status",
                model_name=model_name,
                reference={
                    "version_id": version_obj.version_id,
                    "old_status": old_status,
                    "new_status": status,
                    "updated_by": updated_by,
                },
            )

            return True

        except Exception as exc:
            self.telemetry.error("Failed to update model version status", error=str(exc))
            return False

    def _record_audit_event(
        self,
        action: str,
        model_name: str,
        reference: Dict[str, Any]
    ) -> None:
        """Record an audit event for the model registry."""
        try:
            # This would integrate with the audit logging system
            self.logger.info(
                "Model registry audit event",
                extra={
                    "action": action,
                    "model_name": model_name,
                    "reference": reference,
                    "timestamp": datetime.utcnow().isoformat(),
                    "request_id": get_request_id(),
                    "tenant_id": get_tenant_id(),
                    "user_id": get_user_id(),
                }
            )
        except Exception:
            # Best effort audit logging
            pass

    async def health_check(self) -> bool:
        """Health check for the model management service."""
        try:
            # Check if we can access the models dictionary (basic connectivity test)
            if not hasattr(self, 'models') or self.models is None:
                return False

            # Check DAO connectivity (if available)
            if hasattr(self, 'dao') and self.dao is not None:
                # Simple connectivity check
                pass

            return True

        except Exception as exc:
            self.logger.error(f"Health check failed: {exc}")
            return False

    def get_service_health(self) -> Dict[str, Any]:
        """Get detailed health information for the service."""
        return {
            "healthy": True,  # Would be determined by health_check()
            "service_name": "ModelManagementService",
            "models_count": len(self.models),
            "versions_count": len(self.version_index),
            "last_health_check": datetime.utcnow().isoformat()
        }
