"""Runtime configuration management for rate limits and feature flags."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request, Depends, Query
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_user_id, log_structured
from .routes import _get_principal, _require_admin
from .ratelimit import RateLimitConfig
from aurum.core import AurumSettings


class RateLimitConfigUpdate(BaseModel):
    """Rate limit configuration update."""
    requests_per_second: int = Field(..., gt=0, description="Requests per second")
    burst: int = Field(..., gt=0, description="Burst capacity")
    enabled: bool = Field(True, description="Whether rate limiting is enabled")


class FeatureFlagUpdate(BaseModel):
    """Feature flag configuration update."""
    enabled: bool = Field(..., description="Whether the feature is enabled")
    configuration: Dict[str, Any] = Field(default_factory=dict, description="Feature configuration")


class AuditLogEntry(BaseModel):
    """Audit log entry for configuration changes."""
    id: str
    tenant_id: str
    user_id: str
    action: str
    resource_type: str  # "rate_limit" or "feature_flag"
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
        # Get current config
        settings = AurumSettings.from_env()
        old_config = settings.api.rate_limit.tenant_overrides.get(tenant_id, {}).get(path)

        # Update configuration
        async with self._lock:
            if tenant_id not in settings.api.rate_limit.tenant_overrides:
                settings.api.rate_limit.tenant_overrides[tenant_id] = {}

            settings.api.rate_limit.tenant_overrides[tenant_id][path] = (
                config.requests_per_second,
                config.burst
            )

            # Save configuration (this would persist to database/file in production)
            # For now, we'll just log the change

            # Create audit log entry
            audit_entry = AuditLogEntry(
                id=str(uuid4()),
                tenant_id=tenant_id,
                user_id=user_id,
                action="update_rate_limit",
                resource_type="rate_limit",
                resource_id=path,
                old_value={"rps": old_config[0] if old_config else None, "burst": old_config[1] if old_config else None},
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

        return {
            "tenant_id": tenant_id,
            "path": path,
            "requests_per_second": config.requests_per_second,
            "burst": config.burst,
            "enabled": config.enabled,
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
        from aurum.scenarios.storage import ScenarioStore

        store = ScenarioStore()
        old_config = await store.get_feature_flag(tenant_id, feature_name)

        # Update feature flag
        updated_config = await store.set_feature_flag(
            tenant_id=tenant_id,
            feature_name=feature_name,
            enabled=config.enabled,
            configuration=config.configuration
        )

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
    request: Request,
    principal: dict = Depends(_get_principal)
) -> Dict[str, Any]:
    """Update rate limit configuration for a specific tenant and path."""
    _require_admin(principal)

    user_id = get_user_id()
    request_id = get_request_id()

    try:
        result = await _runtime_config_service.update_tenant_rate_limit(
            tenant_id=tenant_id,
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
    request: Request,
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
            "data": [entry.dict() for entry in entries]
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

    request_id = get_request_id()

    try:
        from aurum.scenarios.storage import ScenarioStore
        store = ScenarioStore()

        # Get all feature flags for the tenant (this would need to be implemented in storage)
        # For now, return empty list
        feature_flags = []

        return {
            "meta": {
                "request_id": request_id,
                "tenant_id": tenant_id
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
        settings = AurumSettings.from_env()
        tenant_overrides = settings.api.rate_limit.tenant_overrides.get(tenant_id, {})

        return {
            "meta": {
                "request_id": request_id,
                "tenant_id": tenant_id
            },
            "data": {
                "overrides": {
                    path: {"requests_per_second": rps, "burst": burst}
                    for path, (rps, burst) in tenant_overrides.items()
                }
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
