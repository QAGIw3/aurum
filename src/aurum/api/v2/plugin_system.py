"""v2 Plugin System API for extension management and security guardrails.

This module provides REST endpoints for:
- Plugin discovery and contract management
- Plugin loading, validation, and lifecycle management
- Tenant-based plugin authorization and security
- Plugin execution with sandboxing and resource limits
- Plugin health monitoring and error handling
- Plugin statistics and usage analytics
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.plugin_system_shim import (
    get_plugin_system_service,
    PluginContract,
    PluginInstance,
    PluginSecurityLevel,
    PluginStatus
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/plugins", tags=["plugin-system"])


class PluginExecutionRequest(BaseModel):
    """Request to execute a plugin method."""

    instance_id: str = Field(..., description="Plugin instance ID")
    method_name: str = Field(..., description="Method to execute")
    parameters: Dict[str, any] = Field(default_factory=dict, description="Method parameters")


class TenantPluginConfigRequest(BaseModel):
    """Request to configure tenant plugin permissions."""

    tenant_id: str = Field(..., description="Tenant identifier")
    allowed_plugins: List[str] = Field(..., description="List of allowed plugin names")


class PluginContractResponse(BaseModel):
    """Response containing plugin contract information."""

    name: str
    version: str
    description: str
    author: str
    entry_point: str
    security_level: str
    required_permissions: List[str]
    dependencies: List[str]
    configuration_schema: Dict[str, any]
    lifecycle_hooks: List[str]


class PluginInstanceResponse(BaseModel):
    """Response containing plugin instance information."""

    instance_id: str
    plugin_name: str
    plugin_version: str
    status: str
    security_level: str
    tenant_id: Optional[str]
    configuration: Dict[str, any]
    loaded_at: datetime
    last_health_check: Optional[datetime]
    error_count: int
    metadata: Dict[str, any]


class PluginStatisticsResponse(BaseModel):
    """Response containing plugin system statistics."""

    total_plugins: int
    active_plugins: int
    failed_plugins: int
    security_distribution: Dict[str, int]
    tenant_distribution: Dict[str, int]
    error_rate: float


@router.get("/discover", response_model=List[PluginContractResponse])
async def discover_plugins(
    request: Request,
    response: Response,
    entry_point_group: str = Query("aurum.plugins", description="Entry point group to search")
) -> List[PluginContractResponse]:
    """Discover available plugins via entry points."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Discover plugins
        contracts = await service.discover_plugins(entry_point_group)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        contract_responses = [
            PluginContractResponse(
                name=contract.name,
                version=contract.version,
                description=contract.description,
                author=contract.author,
                entry_point=contract.entry_point,
                security_level=contract.security_level.value,
                required_permissions=contract.required_permissions,
                dependencies=contract.dependencies,
                configuration_schema=contract.configuration_schema,
                lifecycle_hooks=contract.lifecycle_hooks
            )
            for contract in contracts
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="discover_plugins",
            query_time_ms=query_time_ms
        )

        return contract_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="discover_plugins",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to discover plugins: {str(exc)}"
        )


@router.post("/tenants/{tenant_id}/config", response_model=Dict[str, any])
async def configure_tenant_plugins(
    request: Request,
    tenant_id: str,
    config: TenantPluginConfigRequest
) -> Dict[str, any]:
    """Configure which plugins are allowed for a tenant."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Configure tenant plugins
        await service.configure_tenant_plugins(tenant_id, config.allowed_plugins)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="configure_tenant_plugins",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="configure_tenant_plugins",
                query_time_ms=query_time_ms
            ),
            "data": {
                "tenant_id": tenant_id,
                "allowed_plugins": config.allowed_plugins,
                "message": "Tenant plugin configuration updated successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="configure_tenant_plugins",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to configure tenant plugins: {str(exc)}"
        )


@router.post("/load", response_model=Dict[str, any], status_code=201)
async def load_plugin(
    request: Request,
    plugin_name: str = Query(..., description="Plugin name to load"),
    tenant_id: str = Query(..., description="Tenant for plugin loading"),
    configuration: Dict[str, any] = Field(default_factory=dict, description="Plugin configuration")
) -> Dict[str, any]:
    """Load and activate a plugin for a tenant."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Load plugin
        instance_id = await service.load_plugin(plugin_name, tenant_id, configuration)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="load_plugin",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="load_plugin",
                query_time_ms=query_time_ms
            ),
            "data": {
                "instance_id": instance_id,
                "plugin_name": plugin_name,
                "tenant_id": tenant_id,
                "message": "Plugin loaded successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="load_plugin",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load plugin: {str(exc)}"
        )


@router.post("/execute", response_model=Dict[str, any])
async def execute_plugin_method(
    request: Request,
    execution_data: PluginExecutionRequest
) -> Dict[str, any]:
    """Execute a method on a loaded plugin."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Execute plugin method
        result = await service.execute_plugin_method(
            execution_data.instance_id,
            execution_data.method_name,
            execution_data.parameters
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="execute_plugin_method",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="execute_plugin_method",
                query_time_ms=query_time_ms
            ),
            "data": {
                "result": result,
                "message": "Plugin method executed successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="execute_plugin_method",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute plugin method: {str(exc)}"
        )


@router.delete("/instances/{instance_id}", response_model=Dict[str, any])
async def unload_plugin(
    request: Request,
    instance_id: str
) -> Dict[str, any]:
    """Unload a plugin instance."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Unload plugin
        success = await service.unload_plugin(instance_id)

        if not success:
            raise HTTPException(status_code=404, detail="Plugin instance not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="unload_plugin",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="unload_plugin",
                query_time_ms=query_time_ms
            ),
            "data": {
                "instance_id": instance_id,
                "message": "Plugin unloaded successfully"
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="unload_plugin",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unload plugin: {str(exc)}"
        )


@router.get("/instances", response_model=List[PluginInstanceResponse])
async def list_plugin_instances(
    request: Request,
    response: Response,
    tenant_id: Optional[str] = Query(None, description="Filter by tenant"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
) -> List[PluginInstanceResponse]:
    """List plugin instances with filtering and pagination."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # List instances
        instances = await service.list_plugins(
            tenant_id=tenant_id,
            status=PluginStatus(status) if status else None
        )

        # Apply pagination
        paginated_instances = instances[offset:offset + limit]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        instance_responses = [
            PluginInstanceResponse(
                instance_id=instance.instance_id,
                plugin_name=instance.plugin_name,
                plugin_version=instance.plugin_version,
                status=instance.status.value,
                security_level=instance.security_level.value,
                tenant_id=instance.tenant_id,
                configuration=instance.configuration,
                loaded_at=instance.loaded_at,
                last_health_check=instance.last_health_check,
                error_count=instance.error_count,
                metadata=instance.metadata
            )
            for instance in paginated_instances
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="list_plugin_instances",
            query_time_ms=query_time_ms
        )

        return instance_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_plugin_instances",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list plugin instances: {str(exc)}"
        )


@router.get("/instances/{instance_id}/health", response_model=Dict[str, any])
async def get_plugin_health(
    request: Request,
    instance_id: str
) -> Dict[str, any]:
    """Get health status for a specific plugin instance."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Get plugin health
        health = await service.get_plugin_health(instance_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_plugin_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_plugin_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get plugin health: {str(exc)}"
        )


@router.get("/statistics", response_model=PluginStatisticsResponse)
async def get_plugin_statistics(
    request: Request,
    response: Response
) -> PluginStatisticsResponse:
    """Get plugin system statistics."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()

        # Get statistics
        stats = await service.get_plugin_statistics()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_plugin_statistics",
            query_time_ms=query_time_ms
        )

        return PluginStatisticsResponse(
            total_plugins=stats["total_plugins"],
            active_plugins=stats["active_plugins"],
            failed_plugins=stats["failed_plugins"],
            security_distribution=stats["security_distribution"],
            tenant_distribution=stats["tenant_distribution"],
            error_rate=stats["error_rate"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_plugin_statistics",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get plugin statistics: {str(exc)}"
        )


@router.get("/health")
async def get_plugin_system_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get plugin system health status."""
    start_time = time.perf_counter()

    try:
        service = get_plugin_system_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_plugin_system_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_plugin_system_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get plugin system health: {str(exc)}"
        )
