"""Runtime configuration management for rate limits, concurrency, and feature flags.

Exposes admin endpoints under `/v1/admin/config/*` to:
- Update per-tenant rate limits for specific paths
- Update per-tenant concurrency guardrails
- Update per-tenant feature flags and configuration
- Retrieve an in-memory audit log of changes

Notes:
- Persists to in-memory settings and the scenario store; wire to durable storage in production.
- All changes are logged with `tenant_id`, `user_id`, and `request_id`.
- See docs/runtime-config.md for examples and operational guidance.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from .telemetry.context import (
    TenantIdValidationError,
    get_request_id,
    get_user_id,
    log_structured,
    normalize_tenant_id,
)
from .deps import get_principal, require_admin
from aurum.core import AurumSettings
from aurum.core.settings import validate_migration_health
from ..observability.metrics import increment_runtime_override_updates

try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False


class RateLimitConfigUpdate(BaseModel):
    """Rate limit configuration update."""
    requests_per_second: int = Field(..., gt=0, description="Requests per second")
    burst: int = Field(..., gt=0, description="Burst capacity")
    enabled: bool = Field(True, description="Whether rate limiting is enabled")


class FeatureFlagUpdate(BaseModel):
    """Feature flag configuration update."""
    enabled: bool = Field(..., description="Whether the feature is enabled")
    configuration: Dict[str, Any] = Field(default_factory=dict, description="Feature configuration")


class ConcurrencyOverrideUpdate(BaseModel):
    """Tenant-specific concurrency override payload."""

    enabled: bool = Field(True, description="Whether the override is active")
    max_requests_per_tenant: Optional[int] = Field(None, gt=0)
    tenant_queue_limit: Optional[int] = Field(None, ge=0)
    queue_timeout_seconds: Optional[float] = Field(None, ge=0.0)
    burst_refill_per_second: Optional[float] = Field(None, ge=0.0)
    slow_start_initial_limit: Optional[int] = Field(None, gt=0)
    slow_start_step_seconds: Optional[float] = Field(None, ge=0.0)
    slow_start_step_size: Optional[int] = Field(None, gt=0)
    slow_start_cooldown_seconds: Optional[float] = Field(None, ge=0.0)
    max_request_duration_seconds: Optional[float] = Field(None, ge=0.0)
    max_requests_per_second: Optional[float] = Field(None, ge=0.0)


class AuditLogEntry(BaseModel):
    """Audit log entry for configuration changes."""
    id: str
    tenant_id: str
    user_id: str
    action: str
    resource_type: str  # "rate_limit", "concurrency", or "feature_flag"
    resource_id: str  # path or feature name
    old_value: Dict[str, Any]
    new_value: Dict[str, Any]
    timestamp: datetime
    request_id: str


class RuntimeConfigService:
    """Service for managing runtime configuration with audit logging."""

    def __init__(self):
        self._audit_log: List[AuditLogEntry] = []
        self._lock = asyncio.Lock()

    async def update_tenant_rate_limit(
        self,
        tenant_id: str,
        path: str,
        config: RateLimitConfigUpdate,
        user_id: str,
        request_id: str
    ) -> Dict[str, Any]:
        """Update rate limit configuration for a tenant and path."""
        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        store = get_scenario_store()

        try:
            token = set_tenant_id(tenant_id)
            try:
                old_config = await store.get_rate_limit_override(path, tenant_id)
            finally:
                reset_tenant_id(token)

            token = set_tenant_id(tenant_id)
            try:
                updated_config = await store.set_rate_limit_override(
                    path_prefix=path,
                    requests_per_second=config.requests_per_second,
                    burst_capacity=config.burst,
                    daily_cap=100000,  # default daily cap
                    enabled=config.enabled,
                    tenant_id=tenant_id
                )
            finally:
                reset_tenant_id(token)
        except Exception:
            await increment_runtime_override_updates("rate_limit", result="error")
            raise

        # Create audit log entry
        async with self._lock:
            audit_entry = AuditLogEntry(
                id=str(uuid4()),
                tenant_id=tenant_id,
                user_id=user_id,
                action="update_rate_limit",
                resource_type="rate_limit",
                resource_id=path,
                old_value={
                    "rps": old_config["requests_per_second"] if old_config else None,
                    "burst": old_config["burst_capacity"] if old_config else None,
                    "enabled": old_config["enabled"] if old_config else None,
                },
                new_value={"rps": config.requests_per_second, "burst": config.burst, "enabled": config.enabled},
                timestamp=datetime.utcnow(),
                request_id=request_id
            )
            self._audit_log.append(audit_entry)

        log_structured(
            "info",
            "tenant_rate_limit_updated",
            tenant_id=tenant_id,
            path=path,
            rps=config.requests_per_second,
            burst=config.burst,
            user_id=user_id,
            request_id=request_id
        )

        await increment_runtime_override_updates("rate_limit", result="success")

        return {
            "tenant_id": tenant_id,
            "path": path,
            "requests_per_second": config.requests_per_second,
            "burst": config.burst,
            "enabled": config.enabled,
            "daily_cap": updated_config.get("daily_cap", 100000),
        }

    async def update_feature_flag(
        self,
        tenant_id: str,
        feature_name: str,
        config: FeatureFlagUpdate,
        user_id: str,
        request_id: str
    ) -> Dict[str, Any]:
        """Update feature flag configuration for a tenant."""
        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        store = get_scenario_store()

        # Temporarily impersonate the target tenant for RLS-bound operations
        token = set_tenant_id(tenant_id)
        try:
            old_config = await store.get_feature_flag(feature_name)

            # Update feature flag
            updated_config = await store.set_feature_flag(
                feature_name=feature_name,
                enabled=config.enabled,
                configuration=config.configuration
            )
        finally:
            reset_tenant_id(token)

        # Create audit log entry
        async with self._lock:
            audit_entry = AuditLogEntry(
                id=str(uuid4()),
                tenant_id=tenant_id,
                user_id=user_id,
                action="update_feature_flag",
                resource_type="feature_flag",
                resource_id=feature_name,
                old_value=old_config or {},
                new_value=updated_config,
                timestamp=datetime.utcnow(),
                request_id=request_id
            )
            self._audit_log.append(audit_entry)

        log_structured(
            "info",
            "feature_flag_updated",
            tenant_id=tenant_id,
            feature_name=feature_name,
            enabled=config.enabled,
            user_id=user_id,
            request_id=request_id
        )

        return updated_config

    async def update_tenant_concurrency(
        self,
        tenant_id: str,
        payload: ConcurrencyOverrideUpdate,
        user_id: str,
        request_id: str,
    ) -> Dict[str, Any]:
        """Persist a tenant-specific concurrency override."""

        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        store = get_scenario_store()
        configuration = {
            key: value
            for key, value in payload.model_dump().items()
            if key not in {"enabled"} and value is not None
        }

        try:
            token = set_tenant_id(tenant_id)
            try:
                previous = await store.get_concurrency_override(tenant_id)
            finally:
                reset_tenant_id(token)

            token = set_tenant_id(tenant_id)
            try:
                updated = await store.set_concurrency_override(
                    tenant_id=tenant_id,
                    configuration=configuration,
                    enabled=payload.enabled,
                )
            finally:
                reset_tenant_id(token)
        except Exception:
            await increment_runtime_override_updates("concurrency", result="error")
            raise

        async with self._lock:
            audit_entry = AuditLogEntry(
                id=str(uuid4()),
                tenant_id=tenant_id,
                user_id=user_id,
                action="update_concurrency",
                resource_type="concurrency",
                resource_id=tenant_id,
                old_value=previous or {},
                new_value=updated,
                timestamp=datetime.utcnow(),
                request_id=request_id,
            )
            self._audit_log.append(audit_entry)

        log_structured(
            "info",
            "tenant_concurrency_override_updated",
            tenant_id=tenant_id,
            enabled=payload.enabled,
            override_keys=sorted(configuration.keys()),
            user_id=user_id,
            request_id=request_id,
        )

        await increment_runtime_override_updates("concurrency", result="success")

        return updated

    async def fetch_tenant_concurrency_override(
        self,
        tenant_id: str,
        *,
        settings: Optional[AurumSettings] = None,
    ) -> Dict[str, Any]:
        """Retrieve the stored concurrency override or fall back to static settings."""

        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        store = get_scenario_store()
        token = set_tenant_id(tenant_id)
        try:
            stored = await store.get_concurrency_override(tenant_id)
        finally:
            reset_tenant_id(token)

        if stored:
            stored.setdefault("tenant_id", tenant_id)
            return {**stored, "source": "database"}

        fallback_config: Dict[str, Any] = {}
        if settings is not None:
            try:
                overrides = getattr(settings.api.concurrency, "tenant_overrides", {}) or {}
                fallback_config = overrides.get(tenant_id, {})
            except Exception:
                fallback_config = {}

        return {
            "tenant_id": tenant_id,
            "configuration": fallback_config,
            "enabled": bool(fallback_config),
            "source": "settings" if fallback_config else "default",
        }

    async def fetch_tenant_rate_limit_overrides(
        self,
        tenant_id: str,
        *,
        settings: Optional[AurumSettings] = None,
    ) -> Dict[str, Dict[str, int]]:
        """Retrieve rate limit overrides for a tenant."""

        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        overrides: Dict[str, Dict[str, int]] = {}

        try:
            store = get_scenario_store()
            token = set_tenant_id(tenant_id)
            try:
                records = await store.list_rate_limit_overrides(tenant_id)
            finally:
                reset_tenant_id(token)
        except Exception:
            records = []

        for record in records:
            overrides[record["path_prefix"]] = {
                "requests_per_second": record["requests_per_second"],
                "burst": record["burst_capacity"],
                "daily_cap": record.get("daily_cap"),
                "enabled": record["enabled"],
            }

        if not overrides and settings is not None:
            try:
                tenant_overrides = settings.api.rate_limit.tenant_overrides.get(tenant_id, {})
                for path, (rps, burst) in tenant_overrides.items():
                    overrides[path] = {
                        "requests_per_second": rps,
                        "burst": burst,
                        "daily_cap": getattr(settings.api.rate_limit, "daily_cap", 100000),
                        "enabled": True,
                    }
            except Exception:
                pass

        return overrides

    async def get_audit_log(
        self,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None,
        limit: int = 100
    ) -> List[AuditLogEntry]:
        """Get audit log entries with optional filtering."""
        async with self._lock:
            filtered_log = self._audit_log

            if tenant_id:
                filtered_log = [entry for entry in filtered_log if entry.tenant_id == tenant_id]
            if user_id:
                filtered_log = [entry for entry in filtered_log if entry.user_id == user_id]

            # Sort by timestamp descending
            filtered_log.sort(key=lambda x: x.timestamp, reverse=True)

            return filtered_log[:limit]


# Global service instance
_runtime_config_service = RuntimeConfigService()


# API Router
router = APIRouter()


@router.put("/v1/admin/config/ratelimit/{tenant_id}/{path:path}")
async def update_tenant_rate_limit(
    tenant_id: str,
    path: str,
    config: RateLimitConfigUpdate,
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Update rate limit configuration for a specific tenant and path."""
    _require_admin(principal)

    try:
        normalized_tenant = normalize_tenant_id(tenant_id)
    except TenantIdValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not normalized_tenant:
        raise HTTPException(status_code=400, detail="Tenant identifier is required")

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        result = await _runtime_config_service.update_tenant_rate_limit(
            tenant_id=normalized_tenant,
            path=path,
            config=config,
            user_id=user_id,
            request_id=request_id
        )
        return {
            "meta": {
                "request_id": request_id,
            },
            "data": result
        }
    except Exception as e:
        log_structured(
            "error",
            "tenant_rate_limit_update_failed",
            tenant_id=tenant_id,
            path=path,
            error=str(e),
            user_id=user_id,
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=f"Failed to update rate limit: {str(e)}")


@router.put("/v1/admin/config/feature-flags/{tenant_id}/{feature_name}")
async def update_feature_flag(
    tenant_id: str,
    feature_name: str,
    config: FeatureFlagUpdate,
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Update feature flag configuration for a specific tenant."""
    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        result = await _runtime_config_service.update_feature_flag(
            tenant_id=tenant_id,
            feature_name=feature_name,
            config=config,
            user_id=user_id,
            request_id=request_id
        )
        return {
            "meta": {
                "request_id": request_id,
            },
            "data": result
        }
    except Exception as e:
        log_structured(
            "error",
            "feature_flag_update_failed",
            tenant_id=tenant_id,
            feature_name=feature_name,
            error=str(e),
            user_id=user_id,
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=f"Failed to update feature flag: {str(e)}")


@router.get("/v1/admin/config/audit-log")
async def get_audit_log(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of entries to return"),
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Get audit log for configuration changes."""
    _require_admin(principal)

    request_id = get_request_id()

    try:
        entries = await _runtime_config_service.get_audit_log(
            tenant_id=tenant_id,
            user_id=user_id,
            limit=limit
        )

        return {
            "meta": {
                "request_id": request_id,
                "total_entries": len(entries),
                "filters": {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "limit": limit
                }
            },
            "data": [entry.model_dump() for entry in entries]
        }
    except Exception as e:
        log_structured(
            "error",
            "audit_log_retrieval_failed",
            error=str(e),
            user_id=get_user_id(),
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve audit log: {str(e)}")


@router.get("/v1/admin/config/feature-flags/{tenant_id}")
async def get_tenant_feature_flags(
    tenant_id: str,
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Get all feature flags for a tenant."""
    _require_admin(principal)

    try:
        normalized_tenant = normalize_tenant_id(tenant_id)
    except TenantIdValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not normalized_tenant:
        raise HTTPException(status_code=400, detail="Tenant identifier is required")

    request_id = get_request_id()

    try:
        from aurum.scenarios.storage import get_scenario_store
        from aurum.telemetry.context import set_tenant_id, reset_tenant_id

        store = get_scenario_store()

        # Temporarily impersonate the target tenant for RLS-bound operations
        token = set_tenant_id(normalized_tenant)
        try:
            feature_flags = await store.list_feature_flags()
        finally:
            reset_tenant_id(token)

        return {
            "meta": {
                "request_id": request_id,
                "tenant_id": normalized_tenant,
                "total_count": len(feature_flags)
            },
            "data": feature_flags
        }
    except Exception as e:
        log_structured(
            "error",
            "feature_flags_retrieval_failed",
            tenant_id=tenant_id,
            error=str(e),
            user_id=get_user_id(),
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve feature flags: {str(e)}")


@router.get("/v1/admin/config/ratelimit/{tenant_id}")
async def get_tenant_rate_limits(
    tenant_id: str,
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Get all rate limit configurations for a tenant."""
    _require_admin(principal)

    request_id = get_request_id()

    try:
        try:
            normalized_tenant = normalize_tenant_id(tenant_id)
        except TenantIdValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not normalized_tenant:
            raise HTTPException(status_code=400, detail="Tenant identifier is required")

        settings = AurumSettings.from_env()
        overrides = await _runtime_config_service.fetch_tenant_rate_limit_overrides(
            normalized_tenant,
            settings=settings,
        )

        return {
            "meta": {
                "request_id": request_id,
                "tenant_id": normalized_tenant,
                "total_count": len(overrides),
            },
            "data": {
                "overrides": overrides,
            }
        }
    except Exception as e:
        log_structured(
            "error",
            "rate_limits_retrieval_failed",
            tenant_id=tenant_id,
            error=str(e),
            user_id=get_user_id(),
            request_id=request_id
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve rate limits: {str(e)}")


@router.put("/v1/admin/config/concurrency/{tenant_id}")
async def update_tenant_concurrency_override(
    tenant_id: str,
    payload: ConcurrencyOverrideUpdate,
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Create or update concurrency limits for a tenant."""

    _require_admin(principal)

    try:
        normalized_tenant = normalize_tenant_id(tenant_id)
    except TenantIdValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not normalized_tenant:
        raise HTTPException(status_code=400, detail="Tenant identifier is required")

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        result = await _runtime_config_service.update_tenant_concurrency(
            tenant_id=normalized_tenant,
            payload=payload,
            user_id=user_id,
            request_id=request_id,
        )
        return {
            "meta": {
                "request_id": request_id,
            },
            "data": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        log_structured(
            "error",
            "tenant_concurrency_update_failed",
            tenant_id=normalized_tenant,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to update concurrency override: {str(exc)}") from exc


@router.get("/v1/admin/config/concurrency/{tenant_id}")
async def get_tenant_concurrency_override(
    tenant_id: str,
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Return the effective concurrency override for a tenant."""

    _require_admin(principal)

    request_id = get_request_id()

    try:
        try:
            normalized_tenant = normalize_tenant_id(tenant_id)
        except TenantIdValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not normalized_tenant:
            raise HTTPException(status_code=400, detail="Tenant identifier is required")

        settings = AurumSettings.from_env()
        data = await _runtime_config_service.fetch_tenant_concurrency_override(
            normalized_tenant,
            settings=settings,
        )

        return {
            "meta": {
                "request_id": request_id,
                "tenant_id": normalized_tenant,
            },
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as exc:
        log_structured(
            "error",
            "concurrency_override_retrieval_failed",
            tenant_id=tenant_id,
            error=str(exc),
            user_id=get_user_id(),
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve concurrency overrides: {str(exc)}") from exc


@router.get("/v1/admin/migration/health")
async def get_migration_health(
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Return migration health information for administrators."""

    _require_admin(principal)
    status = validate_migration_health()
    return {"data": status}


@router.post("/v1/admin/config/validate")
async def validate_configuration(
    config_data: Dict[str, Any],
    config_type: str = Query(..., description="Type of configuration to validate"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Validate configuration data against JSON schema."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        if not JSONSCHEMA_AVAILABLE:
            raise HTTPException(
                status_code=503,
                detail="JSON schema validation not available - jsonschema package not installed"
            )

        # Load schema for the config type
        schema = _load_config_schema(config_type)
        if not schema:
            raise HTTPException(
                status_code=400,
                detail=f"No schema available for configuration type: {config_type}"
            )

        # Validate configuration
        jsonschema.validate(instance=config_data, schema=schema)

        log_structured(
            "info",
            "configuration_validation_success",
            config_type=config_type,
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {"request_id": request_id, "valid": True},
            "data": {"validation_result": "Configuration is valid"}
        }

    except jsonschema.ValidationError as exc:
        log_structured(
            "warning",
            "configuration_validation_failed",
            config_type=config_type,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {"request_id": request_id, "valid": False},
            "data": {
                "validation_errors": [
                    {
                        "field": ".".join(str(p) for p in exc.absolute_path),
                        "message": exc.message,
                        "value": exc.instance,
                    }
                ]
            }
        }
    except Exception as exc:
        log_structured(
            "error",
            "configuration_validation_error",
            config_type=config_type,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Configuration validation failed: {str(exc)}")


@router.get("/v1/admin/config/schemas")
async def list_configuration_schemas(
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """List available configuration schemas."""

    _require_admin(principal)

    request_id = get_request_id()

    try:
        schemas = _list_available_schemas()

        return {
            "meta": {"request_id": request_id, "total_count": len(schemas)},
            "data": {"schemas": schemas}
        }
    except Exception as exc:
        log_structured(
            "error",
            "schema_listing_failed",
            error=str(exc),
            user_id=get_user_id(),
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(exc)}")


def _load_config_schema(config_type: str) -> Optional[Dict[str, Any]]:
    """Load JSON schema for a configuration type."""
    import os
    from pathlib import Path

    config_dir = Path(__file__).resolve().parents[3] / "config"

    # Map config types to schema files
    schema_files = {
        "eia_ingest_datasets": "eia_ingest_datasets.schema.json",
        "cpi_ingest_datasets": "cpi_ingest_datasets.schema.json",
        "fred_ingest_datasets": "fred_ingest_datasets.schema.json",
        "iso_ingest_datasets": "iso_ingest_datasets.schema.json",
        "noaa_ingest_datasets": "noaa_ingest_datasets.json",
    }

    schema_file = schema_files.get(config_type)
    if not schema_file:
        return None

    schema_path = config_dir / schema_file
    if not schema_path.exists():
        return None

    try:
        import json
        with open(schema_path, 'r') as f:
            return json.load(f)
    except Exception:
        return None


def _list_available_schemas() -> List[str]:
    """List all available configuration schema types."""
    import os
    from pathlib import Path

    config_dir = Path(__file__).resolve().parents[3] / "config"

    schema_files = []
    for file_path in config_dir.glob("*.schema.json"):
        schema_type = file_path.stem.replace("_ingest_datasets", "").replace("_", "")
        schema_files.append(schema_type)

    return sorted(schema_files)


@router.post("/v1/admin/config/backup")
async def create_configuration_backup(
    config_types: List[str] = Query(..., description="Configuration types to backup"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Create a backup of specified configuration types."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        backup_data = {}

        for config_type in config_types:
            backup_data[config_type] = _backup_configuration_type(config_type)

        log_structured(
            "info",
            "configuration_backup_created",
            config_types=config_types,
            backup_size=len(str(backup_data)),
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {"request_id": request_id, "backed_up_types": config_types},
            "data": {"backup": backup_data}
        }
    except Exception as exc:
        log_structured(
            "error",
            "configuration_backup_failed",
            config_types=config_types,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Configuration backup failed: {str(exc)}")


@router.post("/v1/admin/config/restore")
async def restore_configuration(
    backup_data: Dict[str, Any],
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Restore configuration from backup data."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        restored_types = []

        for config_type, config_data in backup_data.items():
            _restore_configuration_type(config_type, config_data)
            restored_types.append(config_type)

        log_structured(
            "info",
            "configuration_restore_completed",
            restored_types=restored_types,
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {"request_id": request_id, "restored_types": restored_types},
            "data": {"message": f"Successfully restored {len(restored_types)} configuration types"}
        }
    except Exception as exc:
        log_structured(
            "error",
            "configuration_restore_failed",
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Configuration restore failed: {str(exc)}")


def _backup_configuration_type(config_type: str) -> Dict[str, Any]:
    """Backup a specific configuration type."""
    import json
    from pathlib import Path

    config_dir = Path(__file__).resolve().parents[3] / "config"

    # Map config types to actual files
    config_files = {
        "eia": ["eia_catalog.json", "eia_ingest_datasets.json", "eia_bulk_datasets.json"],
        "cpi": ["cpi_catalog.json", "cpi_ingest_datasets.json"],
        "fred": ["fred_catalog.json", "fred_ingest_datasets.json"],
        "iso": ["iso_catalog.json", "iso_ingest_datasets.json", "iso_nodes.csv"],
        "noaa": ["noaa_ingest_datasets.json", "noaa_stations_expanded.csv"],
    }

    files = config_files.get(config_type, [])
    backup = {}

    for file_name in files:
        file_path = config_dir / file_name
        if file_path.exists():
            try:
                if file_name.endswith('.json'):
                    with open(file_path, 'r') as f:
                        backup[file_name] = json.load(f)
                else:
                    with open(file_path, 'r') as f:
                        backup[file_name] = f.read()
            except Exception as e:
                backup[file_name] = {"error": f"Failed to read: {str(e)}"}

    return backup


def _restore_configuration_type(config_type: str, config_data: Dict[str, Any]) -> None:
    """Restore a specific configuration type from backup data."""
    import json
    from pathlib import Path

    config_dir = Path(__file__).resolve().parents[3] / "config"

    for file_name, content in config_data.items():
        file_path = config_dir / file_name
        try:
            if file_name.endswith('.json') and isinstance(content, dict):
                with open(file_path, 'w') as f:
                    json.dump(content, f, indent=2)
            elif isinstance(content, str):
                with open(file_path, 'w') as f:
                    f.write(content)
        except Exception as e:
            raise Exception(f"Failed to restore {file_name}: {str(e)}")


@router.get("/v1/admin/schemas")
async def list_schemas(
    schema_type: Optional[str] = Query(None, description="Filter by schema type"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """List all registered schemas."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        from .models.schema_manager import get_schema_manager, SchemaType

        schema_manager = get_schema_manager()

        if schema_type:
            try:
                type_enum = SchemaType(schema_type)
                schemas = schema_manager.list_schemas(type_enum)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid schema type: {schema_type}")
        else:
            schemas = schema_manager.list_schemas()

        schema_data = [schema.to_dict() for schema in schemas]

        log_structured(
            "info",
            "schemas_listed",
            schema_count=len(schemas),
            schema_type=schema_type,
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {
                "request_id": request_id,
                "total_count": len(schemas),
                "schema_type": schema_type,
            },
            "data": {"schemas": schema_data}
        }
    except Exception as exc:
        log_structured(
            "error",
            "schema_listing_failed",
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(exc)}")


@router.get("/v1/admin/schemas/{schema_name}")
async def get_schema(
    schema_name: str,
    version: str = Query("latest", description="Schema version"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Get a specific schema."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        from .models.schema_manager import get_schema_manager

        schema_manager = get_schema_manager()
        schema = schema_manager.get_schema(schema_name, version)

        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema {schema_name}:{version} not found")

        metadata = schema_manager.get_schema_metadata(schema_name, version)

        log_structured(
            "info",
            "schema_retrieved",
            schema_name=schema_name,
            version=version,
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {"request_id": request_id, "schema_name": schema_name, "version": version},
            "data": {
                "schema": schema,
                "metadata": metadata.to_dict() if metadata else None
            }
        }
    except HTTPException:
        raise
    except Exception as exc:
        log_structured(
            "error",
            "schema_retrieval_failed",
            schema_name=schema_name,
            version=version,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve schema: {str(exc)}")


@router.post("/v1/admin/schemas/{schema_name}/validate")
async def validate_schema_data(
    schema_name: str,
    data: Dict[str, Any],
    version: str = Query("latest", description="Schema version to validate against"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Validate data against a schema."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        from .models.schema_manager import validate_api_schema

        validation_result = validate_api_schema(schema_name, data, version)

        log_structured(
            "info",
            "schema_validation_completed",
            schema_name=schema_name,
            version=version,
            valid=validation_result.valid,
            error_count=len(validation_result.errors),
            warning_count=len(validation_result.warnings),
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {
                "request_id": request_id,
                "schema_name": schema_name,
                "version": version,
                "validation_time": validation_result.validated_at.isoformat(),
            },
            "data": {
                "valid": validation_result.valid,
                "errors": validation_result.errors,
                "warnings": validation_result.warnings,
                "schema_version": validation_result.schema_version,
            }
        }
    except Exception as exc:
        log_structured(
            "error",
            "schema_validation_failed",
            schema_name=schema_name,
            version=version,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Schema validation failed: {str(exc)}")


@router.get("/v1/admin/schemas/{schema_name}/compatibility")
async def check_schema_compatibility(
    schema_name: str,
    from_version: str = Query(..., description="Source version"),
    to_version: str = Query(..., description="Target version"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Check compatibility between two schema versions."""

    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        from .models.schema_manager import get_schema_manager

        schema_manager = get_schema_manager()
        compatibility = schema_manager.check_compatibility(from_version, to_version, schema_name)

        log_structured(
            "info",
            "schema_compatibility_checked",
            schema_name=schema_name,
            from_version=from_version,
            to_version=to_version,
            compatible=compatibility.get("compatible", False),
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {
                "request_id": request_id,
                "schema_name": schema_name,
                "from_version": from_version,
                "to_version": to_version,
            },
            "data": compatibility
        }
    except Exception as exc:
        log_structured(
            "error",
            "schema_compatibility_check_failed",
            schema_name=schema_name,
            from_version=from_version,
            to_version=to_version,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Schema compatibility check failed: {str(exc)}")
