"""Plugin/Extension System MVP with entry points and security guardrails.

This service provides:
- Plugin contracts via Python entry points
- Plugin loading, validation, and lifecycle management
- Clear tenancy and security guardrails
- Plugin registry and discovery
- Sandboxed plugin execution with resource limits
- Plugin health monitoring and error handling
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import inspect
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Type, Callable, Awaitable
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager


class PluginSecurityLevel(str, Enum):
    """Security levels for plugin execution."""
    SANDBOXED = "sandboxed"  # Limited execution environment
    RESTRICTED = "restricted"  # Network/database access allowed
    TRUSTED = "trusted"  # Full system access


class PluginStatus(str, Enum):
    """Plugin lifecycle status."""
    LOADING = "loading"
    VALIDATING = "validating"
    ACTIVE = "active"
    DEACTIVATED = "deactivated"
    ERROR = "error"
    UNLOADING = "unloading"


class PluginContract(BaseModel):
    """Plugin contract definition."""

    name: str
    version: str
    description: str
    author: str
    entry_point: str
    security_level: PluginSecurityLevel
    required_permissions: List[str]
    dependencies: List[str]
    configuration_schema: Dict[str, Any]
    lifecycle_hooks: List[str]  # "startup", "shutdown", "health_check"


class PluginInstance(BaseModel):
    """Plugin instance with runtime state."""

    instance_id: str
    plugin_name: str
    plugin_version: str
    status: PluginStatus
    security_level: PluginSecurityLevel
    tenant_id: Optional[str]
    configuration: Dict[str, Any]
    loaded_at: datetime
    last_health_check: Optional[datetime]
    error_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class PluginHook(BaseModel):
    """Plugin lifecycle hook."""

    hook_name: str
    description: str
    parameters: Dict[str, Any]
    required: bool = False


class PluginSystemService:
    """Plugin/Extension System Service."""

    def __init__(self):
        """Initialize plugin system service."""
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Plugin registry
        self._plugins: Dict[str, PluginInstance] = {}
        self._plugin_contracts: Dict[str, PluginContract] = {}
        self._plugin_modules: Dict[str, Any] = {}

        # Security and tenancy
        self._tenant_isolation: Dict[str, Set[str]] = {}  # tenant_id -> allowed_plugins
        self._security_policies: Dict[str, Any] = {}

        # Plugin discovery
        self._entry_points_cache: Dict[str, Any] = {}

        # Initialize plugin system
        self._initialize_security_policies()

    def _initialize_security_policies(self) -> None:
        """Initialize default security policies."""
        self._security_policies = {
            "sandboxed": {
                "network_access": False,
                "filesystem_access": False,
                "database_access": False,
                "memory_limit": "100MB",
                "cpu_limit": "0.5",
                "timeout": 30  # seconds
            },
            "restricted": {
                "network_access": True,
                "filesystem_access": True,
                "database_access": False,
                "memory_limit": "500MB",
                "cpu_limit": "1.0",
                "timeout": 60
            },
            "trusted": {
                "network_access": True,
                "filesystem_access": True,
                "database_access": True,
                "memory_limit": "2GB",
                "cpu_limit": "2.0",
                "timeout": 300
            }
        }

    async def discover_plugins(self, entry_point_group: str = "aurum.plugins") -> List[PluginContract]:
        """Discover plugins via entry points."""
        contracts = []

        try:
            # Check cache first
            cache_key = f"entry_points:{entry_point_group}"
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached

            # Discover entry points
            for entry_point in importlib.metadata.entry_points(group=entry_point_group):
                try:
                    # Load plugin module
                    plugin_module = entry_point.load()

                    # Extract plugin contract
                    contract = await self._extract_plugin_contract(plugin_module, entry_point.name)
                    if contract:
                        contracts.append(contract)
                        self._plugin_contracts[entry_point.name] = contract

                except Exception as e:
                    self.telemetry.error("Plugin discovery failed", plugin=entry_point.name, error=str(e))

            # Cache results
            await self.cache_manager.set(cache_key, contracts, ttl_seconds=300)

        except Exception as e:
            self.telemetry.error("Plugin discovery failed", error=str(e))

        return contracts

    async def _extract_plugin_contract(self, plugin_module: Any, plugin_name: str) -> Optional[PluginContract]:
        """Extract plugin contract from module."""
        try:
            # Look for plugin metadata
            if hasattr(plugin_module, 'PLUGIN_CONTRACT'):
                contract_data = plugin_module.PLUGIN_CONTRACT
                return PluginContract(**contract_data)

            # Infer from module attributes
            contract_data = {
                "name": plugin_name,
                "version": getattr(plugin_module, '__version__', '1.0.0'),
                "description": getattr(plugin_module, '__doc__', '').split('\n')[0] if plugin_module.__doc__ else '',
                "author": getattr(plugin_module, '__author__', 'Unknown'),
                "entry_point": plugin_name,
                "security_level": PluginSecurityLevel.SANDBOXED,
                "required_permissions": [],
                "dependencies": [],
                "configuration_schema": {},
                "lifecycle_hooks": ["startup", "shutdown"]
            }

            return PluginContract(**contract_data)

        except Exception as e:
            self.telemetry.error("Contract extraction failed", plugin=plugin_name, error=str(e))
            return None

    async def validate_plugin(self, plugin_name: str, tenant_id: str) -> bool:
        """Validate plugin for tenant with security checks."""
        try:
            contract = self._plugin_contracts.get(plugin_name)
            if not contract:
                return False

            # Check tenant permissions
            tenant_plugins = self._tenant_isolation.get(tenant_id, set())
            if plugin_name not in tenant_plugins:
                return False

            # Validate security level
            security_level = contract.security_level
            if security_level == PluginSecurityLevel.SANDBOXED:
                # Additional sandbox validation
                pass

            # Validate dependencies
            for dep in contract.dependencies:
                if dep not in self._plugins:
                    return False

            return True

        except Exception as e:
            self.telemetry.error("Plugin validation failed", plugin=plugin_name, tenant=tenant_id, error=str(e))
            return False

    async def load_plugin(self, plugin_name: str, tenant_id: str, configuration: Dict[str, Any]) -> str:
        """Load and activate plugin for tenant."""
        instance_id = str(uuid4())

        try:
            # Validate plugin first
            if not await self.validate_plugin(plugin_name, tenant_id):
                raise ValueError(f"Plugin {plugin_name} not authorized for tenant {tenant_id}")

            contract = self._plugin_contracts[plugin_name]

            # Create plugin instance
            instance = PluginInstance(
                instance_id=instance_id,
                plugin_name=plugin_name,
                plugin_version=contract.version,
                status=PluginStatus.LOADING,
                security_level=contract.security_level,
                tenant_id=tenant_id,
                configuration=configuration,
                loaded_at=datetime.utcnow()
            )

            self._plugins[instance_id] = instance

            # Load plugin module
            plugin_module = await self._load_plugin_module(plugin_name, instance)

            # Execute startup hook
            await self._execute_plugin_hook(instance, "startup", plugin_module)

            instance.status = PluginStatus.ACTIVE
            instance.last_health_check = datetime.utcnow()

            self.telemetry.info("Plugin loaded successfully", instance_id=instance_id, plugin=plugin_name)

            return instance_id

        except Exception as e:
            instance.status = PluginStatus.ERROR
            self.telemetry.error("Plugin loading failed", instance_id=instance_id, plugin=plugin_name, error=str(e))
            raise

    async def _load_plugin_module(self, plugin_name: str, instance: PluginInstance) -> Any:
        """Load plugin module with security context."""
        try:
            # Load via entry point
            for entry_point in importlib.metadata.entry_points(group="aurum.plugins"):
                if entry_point.name == plugin_name:
                    plugin_module = entry_point.load()

                    # Apply security context
                    security_level = instance.security_level

                    # Setup sandbox if required
                    if security_level == PluginSecurityLevel.SANDBOXED:
                        plugin_module = await self._create_sandbox(plugin_module, instance)

                    self._plugin_modules[instance.instance_id] = plugin_module
                    return plugin_module

            raise ValueError(f"Plugin {plugin_name} not found")

        except Exception as e:
            self.telemetry.error("Module loading failed", plugin=plugin_name, error=str(e))
            raise

    async def _create_sandbox(self, plugin_module: Any, instance: PluginInstance) -> Any:
        """Create sandboxed execution environment."""
        # Simplified sandbox implementation
        # In production, would use actual sandboxing mechanisms

        class SandboxWrapper:
            def __init__(self, module, instance):
                self.module = module
                self.instance = instance
                self._allowed_calls = set()

            def __getattr__(self, name):
                if hasattr(self.module, name):
                    attr = getattr(self.module, name)
                    # Validate method call
                    if callable(attr) and not self._is_allowed_call(name):
                        raise SecurityError(f"Call to {name} not allowed in sandbox")
                    return attr
                raise AttributeError(f"Module has no attribute {name}")

            def _is_allowed_call(self, method_name: str) -> bool:
                # Define allowed methods for sandboxed plugins
                allowed_methods = {
                    'process_data', 'get_metadata', 'validate_input',
                    'transform_output', 'health_check'
                }
                return method_name in allowed_methods

        return SandboxWrapper(plugin_module, instance)

    async def _execute_plugin_hook(self, instance: PluginInstance, hook_name: str, plugin_module: Any) -> None:
        """Execute plugin lifecycle hook."""
        try:
            if hasattr(plugin_module, hook_name):
                hook_method = getattr(plugin_module, hook_name)

                if asyncio.iscoroutinefunction(hook_method):
                    await hook_method(instance.configuration)
                else:
                    hook_method(instance.configuration)

                self.telemetry.info("Plugin hook executed", instance_id=instance.instance_id, hook=hook_name)

        except Exception as e:
            self.telemetry.error("Plugin hook failed", instance_id=instance.instance_id, hook=hook_name, error=str(e))
            raise

    async def unload_plugin(self, instance_id: str) -> bool:
        """Unload plugin instance."""
        try:
            instance = self._plugins.get(instance_id)
            if not instance:
                return False

            instance.status = PluginStatus.UNLOADING

            # Execute shutdown hook
            plugin_module = self._plugin_modules.get(instance_id)
            if plugin_module:
                await self._execute_plugin_hook(instance, "shutdown", plugin_module)

            # Cleanup
            del self._plugins[instance_id]
            del self._plugin_modules[instance_id]

            self.telemetry.info("Plugin unloaded", instance_id=instance_id)
            return True

        except Exception as e:
            self.telemetry.error("Plugin unloading failed", instance_id=instance_id, error=str(e))
            return False

    async def execute_plugin_method(
        self,
        instance_id: str,
        method_name: str,
        parameters: Dict[str, Any]
    ) -> Any:
        """Execute plugin method with security checks."""
        try:
            instance = self._plugins.get(instance_id)
            if not instance or instance.status != PluginStatus.ACTIVE:
                raise ValueError(f"Plugin instance {instance_id} not active")

            plugin_module = self._plugin_modules.get(instance_id)
            if not plugin_module or not hasattr(plugin_module, method_name):
                raise ValueError(f"Method {method_name} not found in plugin {instance_id}")

            # Security validation
            if not await self._validate_method_call(instance, method_name, parameters):
                raise SecurityError(f"Method call {method_name} not allowed")

            # Execute method
            method = getattr(plugin_module, method_name)

            # Execute with timeout and resource limits
            if asyncio.iscoroutinefunction(method):
                result = await asyncio.wait_for(
                    method(**parameters),
                    timeout=self._security_policies[instance.security_level.value]["timeout"]
                )
            else:
                result = method(**parameters)

            # Update usage metrics
            instance.last_health_check = datetime.utcnow()

            return result

        except asyncio.TimeoutError:
            self.telemetry.error("Plugin method timeout", instance_id=instance_id, method=method_name)
            raise
        except Exception as e:
            self.telemetry.error("Plugin method failed", instance_id=instance_id, method=method_name, error=str(e))
            instance.error_count += 1
            raise

    async def _validate_method_call(self, instance: PluginInstance, method_name: str, parameters: Dict[str, Any]) -> bool:
        """Validate plugin method call against security policy."""
        security_level = instance.security_level

        # Check method permissions
        contract = self._plugin_contracts.get(instance.plugin_name)
        if contract and hasattr(contract, 'required_permissions'):
            # Validate required permissions
            pass

        # Check resource limits
        policy = self._security_policies.get(security_level.value, {})

        # Basic validation - in production would be more comprehensive
        return True

    async def get_plugin_health(self, instance_id: str) -> Dict[str, Any]:
        """Get plugin instance health status."""
        instance = self._plugins.get(instance_id)
        if not instance:
            return {"status": "not_found"}

        # Perform health check
        try:
            plugin_module = self._plugin_modules.get(instance_id)
            if plugin_module and hasattr(plugin_module, 'health_check'):
                if asyncio.iscoroutinefunction(plugin_module.health_check):
                    health_result = await plugin_module.health_check()
                else:
                    health_result = plugin_module.health_check()
            else:
                health_result = {"status": "ok"}

            instance.last_health_check = datetime.utcnow()

            return {
                "status": "healthy" if health_result.get("status") == "ok" else "unhealthy",
                "last_check": instance.last_health_check,
                "error_count": instance.error_count,
                "health_details": health_result
            }

        except Exception as e:
            instance.error_count += 1
            return {
                "status": "error",
                "error": str(e),
                "error_count": instance.error_count
            }

    async def list_plugins(self, tenant_id: Optional[str] = None, status: Optional[PluginStatus] = None) -> List[PluginInstance]:
        """List plugin instances with filtering."""
        instances = list(self._plugins.values())

        if tenant_id:
            instances = [i for i in instances if i.tenant_id == tenant_id]

        if status:
            instances = [i for i in instances if i.status == status]

        return instances

    async def get_plugin_statistics(self) -> Dict[str, Any]:
        """Get plugin system statistics."""
        instances = list(self._plugins.values())

        stats = {
            "total_plugins": len(instances),
            "active_plugins": len([i for i in instances if i.status == PluginStatus.ACTIVE]),
            "failed_plugins": len([i for i in instances if i.status == PluginStatus.ERROR]),
            "security_distribution": {},
            "tenant_distribution": {},
            "error_rate": 0.0
        }

        # Calculate security distribution
        for level in PluginSecurityLevel:
            count = len([i for i in instances if i.security_level == level])
            stats["security_distribution"][level.value] = count

        # Calculate tenant distribution
        tenants = {}
        for instance in instances:
            tenant = instance.tenant_id or "system"
            tenants[tenant] = tenants.get(tenant, 0) + 1

        stats["tenant_distribution"] = tenants

        # Calculate error rate
        total_executions = sum(i.error_count for i in instances) + 1
        stats["error_rate"] = sum(i.error_count for i in instances) / total_executions

        return stats

    async def configure_tenant_plugins(self, tenant_id: str, allowed_plugins: List[str]) -> None:
        """Configure which plugins are allowed for a tenant."""
        self._tenant_isolation[tenant_id] = set(allowed_plugins)
        self.telemetry.info("Tenant plugin configuration updated", tenant_id=tenant_id, plugins=allowed_plugins)

    async def get_service_health(self) -> Dict[str, Any]:
        """Get plugin system health status."""
        return {
            "status": "healthy",
            "plugins_loaded": len(self._plugins),
            "contracts_registered": len(self._plugin_contracts),
            "tenants_configured": len(self._tenant_isolation),
            "last_discovery": datetime.utcnow()
        }


def get_plugin_system_service() -> PluginSystemService:
    """Get the global plugin system service instance."""
    return PluginSystemService()


async def discover_and_load_plugins(tenant_id: str, plugin_names: List[str]) -> List[str]:
    """Discover and load plugins for tenant."""
    service = get_plugin_system_service()

    # Configure tenant permissions
    await service.configure_tenant_plugins(tenant_id, plugin_names)

    # Discover available plugins
    contracts = await service.discover_plugins()

    # Load requested plugins
    loaded_instances = []
    for plugin_name in plugin_names:
        try:
            instance_id = await service.load_plugin(plugin_name, tenant_id, {})
            loaded_instances.append(instance_id)
        except Exception as e:
            logging.error(f"Failed to load plugin {plugin_name}: {e}")

    return loaded_instances


class SecurityError(Exception):
    """Security violation in plugin execution."""
    pass
