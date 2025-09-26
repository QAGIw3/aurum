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
from .routes import _get_principal, _require_admin
from aurum.core import AurumSettings
from aurum.core.settings import validate_migration_health
from ..observability.metrics import increment_runtime_override_updates


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
