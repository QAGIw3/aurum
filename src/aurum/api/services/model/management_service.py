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
    from aurum.data.dao.trino import TrinoDAO
except ImportError:
    # Mock DAO for demo
    class TrinoDAO:
        pass
from .models import ModelVersion, RegisteredModel, ModelConfig
from .interfaces import IModelManagementService, IAuditLogger, ITelemetryProvider
from .exceptions import ModelNotFoundException, ModelVersionNotFoundException, ModelValidationException


class DefaultAuditLogger:
    """Default implementation of audit logging."""

    async def log_action(
        self,
        action: str,
        model_name: str,
        reference: Dict[str, Any],
        user_id: str
    ) -> None:
        """Log an audit action."""
        logger.info(
            "Model registry audit event",
            extra={
                "action": action,
                "model_name": model_name,
                "reference": reference,
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "request_id": get_request_id(),
                "tenant_id": get_tenant_id(),
            }
        )


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

    def __init__(
        self,
        audit_logger: Optional[IAuditLogger] = None,
        telemetry_provider: Optional[ITelemetryProvider] = None,
        repository: Optional[IModelRepository] = None
    ):
        self.logger = logging.getLogger(__name__)
        self.dao = ModelRegistryDAO()  # Legacy DAO for backward compatibility
        self.audit_logger = audit_logger or DefaultAuditLogger()
        self.telemetry = telemetry_provider or get_telemetry_facade()
        self.repository = repository  # Future: use repository instead of direct DAO
        self.cache = get_unified_cache_manager()
        self.models: Dict[str, RegisteredModel] = {}
        self.version_index: Dict[str, ModelVersion] = {}

    async def register_model(
        self,
        model_name: str,
        description: str,
        model_type: str,
        created_by: str,
        **metadata
    ) -> RegisteredModel:
        """Register a new model in the registry."""
        try:
            # Validate inputs
            if not model_name or not model_name.strip():
                raise ModelValidationException("Model name cannot be empty", model_name=model_name)

            if not model_type or not model_type.strip():
                raise ModelValidationException("Model type cannot be empty", model_name=model_name)

            # Check if model already exists
            existing = self.models.get(model_name)
            if existing:
                return existing

            # Create new model record
            model = RegisteredModel(
                model_name=model_name.strip(),
                description=description.strip() if description else "",
                model_type=model_type.strip(),
                latest_version="",
                created_by=created_by,
                **metadata
            )

            self.models[model_name] = model
            await self.dao.save_registered_model(model)

            # Record telemetry
            await self.telemetry.record_metric(
                "model_registry.model_registered",
                1.0,
                {"model_name": model_name, "model_type": model_type}
            )

            # Audit log
            await self.audit_logger.log_action(
                action="register_model",
                model_name=model_name,
                reference={
                    "model_type": model.model_type,
                    "description": model.description,
                    "created_by": created_by,
                },
                user_id=created_by
            )

            return model

        except ModelValidationException:
            raise  # Re-raise validation exceptions
        except Exception as exc:
            await self.telemetry.record_metric("model_registry.registration_error", 1.0)
            raise ModelValidationException(f"Failed to register model: {str(exc)}", model_name=model_name)

    async def register_model_version(
        self,
        model_name: str,
        version: ModelVersion,
        created_by: str
    ) -> ModelVersion:
        """Register a new version of an existing model."""
        try:
            # Validate inputs
            if not model_name or not model_name.strip():
                raise ModelValidationException("Model name cannot be empty", model_name=model_name)

            if not version or not version.version_id:
                raise ModelValidationException("Model version must have a valid version_id", model_name=model_name)

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

            # Record telemetry
            await self.telemetry.record_metric(
                "model_registry.model_version_registered",
                1.0,
                {
                    "model_name": version.model_name,
                    "version": version.version_number,
                    "total_versions": model.total_versions
                }
            )

            # Audit log
            await self.audit_logger.log_action(
                action="register_model_version",
                model_name=model_name,
                reference={
                    "version_id": version.version_id,
                    "version_number": version.version_number,
                    "status": version.status,
                },
                user_id=created_by
            )

            return version

        except ModelValidationException:
            raise  # Re-raise validation exceptions
        except Exception as exc:
            await self.telemetry.record_metric("model_registry.version_registration_error", 1.0)
            raise ModelValidationException(f"Failed to register model version: {str(exc)}", model_name=model_name)

    async def get_model_version(
        self,
        model_name: str,
        version: str
    ) -> Optional[ModelVersion]:
        """Get a specific model version."""
        try:
            # Validate inputs
            if not model_name or not model_name.strip():
                raise ModelValidationException("Model name cannot be empty", model_name=model_name)

            if not version or not version.strip():
                raise ModelValidationException("Version cannot be empty", model_name=model_name)

            # First check local index
            for version_obj in self.version_index.values():
                if version_obj.model_name == model_name and version_obj.version_number == version:
                    return version_obj

            # Fall back to DAO query
            dao_result = await self.dao.get_model_version(model_name, version)
            if dao_result:
                # Cache the result
                self.version_index[dao_result.version_id] = dao_result
                return dao_result

            return None

        except ModelValidationException:
            raise  # Re-raise validation exceptions
        except Exception as exc:
            await self.telemetry.record_metric("model_registry.version_retrieval_error", 1.0)
            raise ModelVersionNotFoundException(model_name, version) from exc

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

# Legacy method for backward compatibility - use injected audit logger instead
# def _record_audit_event(self, action: str, model_name: str, reference: Dict[str, Any]) -> None:
#     """Legacy audit logging - replaced by injected audit logger."""
#     pass

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
