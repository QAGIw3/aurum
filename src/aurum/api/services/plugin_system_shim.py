"""Compatibility shim for plugin system service.

Provides backward compatibility for code using the old plugin_system_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

from aurum.services.platform.plugin_system import (
    PluginSystemService,
    PluginSecurityLevel as NewPluginSecurityLevel,
    PluginStatus as NewPluginStatus,
    PluginMetadata,
    PluginInterface
)

# Re-export enums
PluginSecurityLevel = NewPluginSecurityLevel
PluginStatus = NewPluginStatus


class PluginContract(BaseModel):
    """Plugin contract interface specification."""
    plugin_name: str
    version: str
    api_version: str = "v1"
    description: str
    author: str
    entry_points: Dict[str, str]  # method_name -> entry_point
    required_permissions: List[str] = Field(default_factory=list)
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    security_level: PluginSecurityLevel = PluginSecurityLevel.SANDBOXED
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PluginInstance(BaseModel):
    """Running instance of a plugin."""
    instance_id: str
    plugin_name: str
    version: str
    tenant_id: str
    config: Dict[str, Any] = Field(default_factory=dict)
    status: PluginStatus = PluginStatus.LOADING
    security_level: PluginSecurityLevel = PluginSecurityLevel.SANDBOXED
    resource_limits: Dict[str, Any] = Field(default_factory=dict)
    health_status: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_health_check: Optional[datetime] = None
    error_count: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


# Singleton instance
_service_instance = None


def get_plugin_system_service() -> PluginSystemService:
    """Get singleton plugin system service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = PluginSystemService()
    return _service_instance


async def load_plugin_instance(
    plugin_name: str,
    version: str,
    tenant_id: str,
    config: Optional[Dict[str, Any]] = None
) -> PluginInstance:
    """Load a plugin instance for a tenant."""
    service = get_plugin_system_service()
    
    # Find entry point from contract (mock for now)
    entry_point = f"{plugin_name}.plugin:Plugin"
    instance_id = f"{plugin_name}_{version}_{tenant_id}"
    
    # Load plugin using new service
    result = await service.load_plugin(
        plugin_id=instance_id,
        entry_point=entry_point,
        config=config
    )
    
    if result.success and result.data:
        metadata = result.data
        return PluginInstance(
            instance_id=instance_id,
            plugin_name=plugin_name,
            version=version,
            tenant_id=tenant_id,
            config=config or {},
            status=PluginStatus.ACTIVE,
            security_level=metadata.security_level,
            health_status={"healthy": True}
        )
    else:
        return PluginInstance(
            instance_id=instance_id,
            plugin_name=plugin_name,
            version=version,
            tenant_id=tenant_id,
            config=config or {},
            status=PluginStatus.ERROR,
            error_count=1
        )


async def execute_plugin_method(
    instance_id: str,
    method_name: str,
    args: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Execute a plugin method."""
    service = get_plugin_system_service()
    
    # Execute using new service
    result = await service.execute_plugin(
        plugin_id=instance_id,
        **args if args else {}
    )
    
    if result.success:
        return {
            "success": True,
            "result": result.data,
            "execution_time": result.execution_time_ms
        }
    else:
        return {
            "success": False,
            "error": result.error
        }


async def unload_plugin_instance(instance_id: str) -> bool:
    """Unload a plugin instance."""
    service = get_plugin_system_service()
    
    result = await service.unload_plugin(instance_id)
    return result.success


async def get_plugin_health(instance_id: str) -> Dict[str, Any]:
    """Get plugin health status."""
    service = get_plugin_system_service()
    
    result = await service.get_plugin_health(instance_id)
    
    if result.success and result.data:
        return result.data
    else:
        return {
            "plugin_id": instance_id,
            "healthy": False,
            "error": result.error
        }
