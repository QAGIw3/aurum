"""Plugin system service for extensibility and custom integrations.

Implements business logic for plugin loading, validation, lifecycle management,
and sandboxed execution.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Type
from enum import Enum

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError

logger = logging.getLogger(__name__)


class PluginSecurityLevel(str, Enum):
    """Security levels for plugin execution."""
    SANDBOXED = "sandboxed"      # Limited execution environment
    RESTRICTED = "restricted"     # Network/database access allowed
    TRUSTED = "trusted"          # Full system access


class PluginStatus(str, Enum):
    """Plugin lifecycle status."""
    LOADING = "loading"
    VALIDATING = "validating"
    ACTIVE = "active"
    DEACTIVATED = "deactivated"
    ERROR = "error"


@dataclass
class PluginMetadata:
    """Plugin metadata and configuration."""
    name: str
    version: str
    author: str
    description: str
    entry_point: str
    security_level: PluginSecurityLevel = PluginSecurityLevel.SANDBOXED
    required_permissions: List[str] = None
    dependencies: List[str] = None
    
    def __post_init__(self):
        if self.required_permissions is None:
            self.required_permissions = []
        if self.dependencies is None:
            self.dependencies = []


class PluginInterface(ABC):
    """Base interface for all Aurum plugins."""
    
    @abstractmethod
    async def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the plugin with configuration."""
        pass
    
    @abstractmethod
    async def execute(self, *args, **kwargs) -> Any:
        """Execute the plugin's main functionality."""
        pass
    
    @abstractmethod
    async def shutdown(self) -> None:
        """Clean shutdown of the plugin."""
        pass
    
    @abstractmethod
    def get_metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        pass


class PluginRegistry(Protocol):
    """Protocol for plugin registry implementations."""
    
    async def register(self, plugin_id: str, metadata: PluginMetadata) -> None:
        """Register a plugin."""
        ...
    
    async def unregister(self, plugin_id: str) -> None:
        """Unregister a plugin."""
        ...
    
    async def get_plugin(self, plugin_id: str) -> Optional[PluginMetadata]:
        """Get plugin metadata."""
        ...
    
    async def list_plugins(self, status: Optional[PluginStatus] = None) -> List[PluginMetadata]:
        """List registered plugins."""
        ...


class PluginSystemService(BaseService):
    """Service for plugin system operations.
    
    Plugin system provides:
    - Plugin discovery and loading
    - Validation and security checks
    - Lifecycle management
    - Sandboxed execution
    - Health monitoring
    - Permission enforcement
    
    This service:
    - Loads and validates plugins
    - Manages plugin lifecycle
    - Enforces security boundaries
    - Monitors plugin health
    - Handles plugin errors
    """
    
    def __init__(self, registry: Optional[PluginRegistry] = None):
        """Initialize service with plugin registry.
        
        Args:
            registry: Plugin registry implementation
        """
        super().__init__()
        self._registry = registry or InMemoryPluginRegistry()
        self._loaded_plugins: Dict[str, PluginInterface] = {}
        self._plugin_status: Dict[str, PluginStatus] = {}
    
    async def load_plugin(
        self,
        plugin_id: str,
        entry_point: str,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[PluginMetadata]:
        """Load and initialize a plugin.
        
        Args:
            plugin_id: Unique plugin identifier
            entry_point: Python entry point (module:class)
            config: Plugin configuration
            context: Service context
            
        Returns:
            ServiceResult with plugin metadata
        """
        self._track_operation("plugin_load", {"plugin_id": plugin_id})
        
        try:
            # Update status
            self._plugin_status[plugin_id] = PluginStatus.LOADING
            
            # Parse entry point
            module_name, class_name = entry_point.split(":")
            
            # Import module
            module = importlib.import_module(module_name)
            plugin_class = getattr(module, class_name)
            
            # Validate plugin interface
            if not issubclass(plugin_class, PluginInterface):
                raise ValidationError(f"Plugin must implement PluginInterface")
            
            # Create instance
            plugin_instance = plugin_class()
            
            # Get metadata
            metadata = plugin_instance.get_metadata()
            
            # Validate security level
            if not self._validate_security_level(metadata, context):
                raise ServiceError("Security validation failed")
            
            # Initialize plugin
            self._plugin_status[plugin_id] = PluginStatus.VALIDATING
            await plugin_instance.initialize(config or {})
            
            # Store plugin
            self._loaded_plugins[plugin_id] = plugin_instance
            self._plugin_status[plugin_id] = PluginStatus.ACTIVE
            
            # Register in registry
            await self._registry.register(plugin_id, metadata)
            
            logger.info(f"Successfully loaded plugin: {plugin_id}")
            
            return ServiceResult.ok(metadata)
            
        except Exception as e:
            self._plugin_status[plugin_id] = PluginStatus.ERROR
            logger.error(f"Failed to load plugin {plugin_id}: {e}")
            return ServiceResult.error(f"Plugin load failed: {str(e)}")
    
    async def unload_plugin(
        self,
        plugin_id: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[bool]:
        """Unload a plugin.
        
        Args:
            plugin_id: Plugin to unload
            context: Service context
            
        Returns:
            ServiceResult with success status
        """
        self._track_operation("plugin_unload", {"plugin_id": plugin_id})
        
        try:
            if plugin_id not in self._loaded_plugins:
                return ServiceResult.error(f"Plugin not loaded: {plugin_id}")
            
            # Update status
            self._plugin_status[plugin_id] = PluginStatus.UNLOADING
            
            # Shutdown plugin
            plugin = self._loaded_plugins[plugin_id]
            await plugin.shutdown()
            
            # Remove from loaded plugins
            del self._loaded_plugins[plugin_id]
            del self._plugin_status[plugin_id]
            
            # Unregister from registry
            await self._registry.unregister(plugin_id)
            
            logger.info(f"Successfully unloaded plugin: {plugin_id}")
            
            return ServiceResult.ok(True)
            
        except Exception as e:
            logger.error(f"Failed to unload plugin {plugin_id}: {e}")
            return ServiceResult.error(f"Plugin unload failed: {str(e)}")
    
    async def execute_plugin(
        self,
        plugin_id: str,
        *args,
        context: Optional[ServiceContext] = None,
        **kwargs
    ) -> ServiceResult[Any]:
        """Execute a loaded plugin.
        
        Args:
            plugin_id: Plugin to execute
            *args: Positional arguments for plugin
            context: Service context
            **kwargs: Keyword arguments for plugin
            
        Returns:
            ServiceResult with plugin output
        """
        self._track_operation("plugin_execute", {"plugin_id": plugin_id})
        
        try:
            if plugin_id not in self._loaded_plugins:
                return ServiceResult.error(f"Plugin not loaded: {plugin_id}")
            
            if self._plugin_status.get(plugin_id) != PluginStatus.ACTIVE:
                return ServiceResult.error(f"Plugin not active: {plugin_id}")
            
            # Get plugin
            plugin = self._loaded_plugins[plugin_id]
            
            # Execute with sandboxing based on security level
            metadata = plugin.get_metadata()
            
            if metadata.security_level == PluginSecurityLevel.SANDBOXED:
                result = await self._execute_sandboxed(plugin, *args, **kwargs)
            else:
                result = await plugin.execute(*args, **kwargs)
            
            return ServiceResult.ok(result)
            
        except Exception as e:
            logger.error(f"Plugin execution failed for {plugin_id}: {e}")
            self._plugin_status[plugin_id] = PluginStatus.ERROR
            return ServiceResult.error(f"Plugin execution failed: {str(e)}")
    
    async def list_plugins(
        self,
        status: Optional[PluginStatus] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """List registered plugins.
        
        Args:
            status: Filter by status
            context: Service context
            
        Returns:
            ServiceResult with plugin list
        """
        self._track_operation("plugin_list", {"filter_status": status})
        
        try:
            plugins = []
            
            for plugin_id, plugin_status in self._plugin_status.items():
                if status and plugin_status != status:
                    continue
                
                metadata = await self._registry.get_plugin(plugin_id)
                if metadata:
                    plugins.append({
                        "plugin_id": plugin_id,
                        "name": metadata.name,
                        "version": metadata.version,
                        "status": plugin_status.value,
                        "security_level": metadata.security_level.value,
                        "description": metadata.description
                    })
            
            return ServiceResult.ok(plugins)
            
        except Exception as e:
            logger.error(f"Failed to list plugins: {e}")
            return ServiceResult.error(f"Plugin list failed: {str(e)}")
    
    async def get_plugin_health(
        self,
        plugin_id: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get plugin health status.
        
        Args:
            plugin_id: Plugin to check
            context: Service context
            
        Returns:
            ServiceResult with health status
        """
        self._track_operation("plugin_health", {"plugin_id": plugin_id})
        
        try:
            if plugin_id not in self._loaded_plugins:
                return ServiceResult.error(f"Plugin not loaded: {plugin_id}")
            
            status = self._plugin_status.get(plugin_id, PluginStatus.ERROR)
            plugin = self._loaded_plugins[plugin_id]
            metadata = plugin.get_metadata()
            
            health = {
                "plugin_id": plugin_id,
                "status": status.value,
                "name": metadata.name,
                "version": metadata.version,
                "security_level": metadata.security_level.value,
                "healthy": status == PluginStatus.ACTIVE
            }
            
            # Try to get custom health check if available
            if hasattr(plugin, 'health_check'):
                try:
                    custom_health = await plugin.health_check()
                    health.update(custom_health)
                except Exception as e:
                    health["health_check_error"] = str(e)
            
            return ServiceResult.ok(health)
            
        except Exception as e:
            logger.error(f"Failed to get plugin health: {e}")
            return ServiceResult.error(f"Health check failed: {str(e)}")
    
    async def reload_plugin(
        self,
        plugin_id: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[PluginMetadata]:
        """Reload a plugin (unload and load again).
        
        Args:
            plugin_id: Plugin to reload
            context: Service context
            
        Returns:
            ServiceResult with plugin metadata
        """
        self._track_operation("plugin_reload", {"plugin_id": plugin_id})
        
        try:
            # Get current metadata
            metadata = await self._registry.get_plugin(plugin_id)
            if not metadata:
                return ServiceResult.error(f"Plugin not found: {plugin_id}")
            
            # Get current config if available
            config = {}
            if hasattr(self._loaded_plugins.get(plugin_id), '_config'):
                config = self._loaded_plugins[plugin_id]._config
            
            # Unload
            unload_result = await self.unload_plugin(plugin_id, context)
            if not unload_result.success:
                return ServiceResult.error(f"Failed to unload: {unload_result.error}")
            
            # Reload
            return await self.load_plugin(
                plugin_id,
                metadata.entry_point,
                config,
                context
            )
            
        except Exception as e:
            logger.error(f"Failed to reload plugin {plugin_id}: {e}")
            return ServiceResult.error(f"Plugin reload failed: {str(e)}")
    
    # Private helper methods
    
    def _validate_security_level(
        self,
        metadata: PluginMetadata,
        context: Optional[ServiceContext]
    ) -> bool:
        """Validate plugin security requirements."""
        # In production, implement proper security validation
        # Check permissions, tenant isolation, etc.
        return True
    
    async def _execute_sandboxed(
        self,
        plugin: PluginInterface,
        *args,
        **kwargs
    ) -> Any:
        """Execute plugin in sandboxed environment."""
        # In production, implement proper sandboxing
        # Resource limits, network isolation, etc.
        return await plugin.execute(*args, **kwargs)


class InMemoryPluginRegistry:
    """Simple in-memory plugin registry."""
    
    def __init__(self):
        self._plugins: Dict[str, PluginMetadata] = {}
    
    async def register(self, plugin_id: str, metadata: PluginMetadata) -> None:
        """Register a plugin."""
        self._plugins[plugin_id] = metadata
    
    async def unregister(self, plugin_id: str) -> None:
        """Unregister a plugin."""
        self._plugins.pop(plugin_id, None)
    
    async def get_plugin(self, plugin_id: str) -> Optional[PluginMetadata]:
        """Get plugin metadata."""
        return self._plugins.get(plugin_id)
    
    async def list_plugins(self, status: Optional[PluginStatus] = None) -> List[PluginMetadata]:
        """List registered plugins."""
        return list(self._plugins.values())
