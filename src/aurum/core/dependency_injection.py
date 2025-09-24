"""Enhanced dependency injection container with lifecycle management and configuration."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar, Union
from weakref import WeakSet

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ServiceLifecycle(Enum):
    """Service lifecycle management options."""
    SINGLETON = "singleton"  # Single instance for application lifetime
    SCOPED = "scoped"       # Single instance per request/context
    TRANSIENT = "transient"  # New instance every time


@dataclass
class ServiceDescriptor:
    """Describes how to create and manage a service."""
    service_type: Type[Any]
    implementation_type: Optional[Type[Any]] = None
    factory: Optional[Callable[..., Any]] = None
    instance: Optional[Any] = None
    lifecycle: ServiceLifecycle = ServiceLifecycle.SINGLETON
    dependencies: List[Type[Any]] = field(default_factory=list)
    initialized: bool = False
    initializing: bool = False


class ServiceContext:
    """Context for scoped services (e.g., per-request)."""
    
    def __init__(self) -> None:
        self._instances: Dict[Type[Any], Any] = {}
        self._disposed: bool = False
    
    def get_instance(self, service_type: Type[T]) -> Optional[T]:
        """Get scoped instance if exists."""
        if self._disposed:
            return None
        return self._instances.get(service_type)
    
    def set_instance(self, service_type: Type[T], instance: T) -> None:
        """Set scoped instance."""
        if not self._disposed:
            self._instances[service_type] = instance
    
    async def dispose(self) -> None:
        """Dispose all scoped instances."""
        if self._disposed:
            return
            
        self._disposed = True
        for instance in self._instances.values():
            if hasattr(instance, "dispose") and callable(instance.dispose):
                try:
                    if asyncio.iscoroutinefunction(instance.dispose):
                        await instance.dispose()
                    else:
                        instance.dispose()
                except Exception as e:
                    logger.warning(f"Error disposing service instance: {e}")
        
        self._instances.clear()


class IServiceProvider(ABC):
    """Abstract service provider interface."""
    
    @abstractmethod
    def get(self, service_type: Type[T]) -> T:
        """Get service instance."""
        pass
    
    @abstractmethod
    def get_optional(self, service_type: Type[T]) -> Optional[T]:
        """Get service instance or None if not registered."""
        pass
    
    @abstractmethod
    def is_registered(self, service_type: Type[Any]) -> bool:
        """Check if service is registered."""
        pass


class EnhancedServiceContainer(IServiceProvider):
    """Enhanced dependency injection container with lifecycle management."""
    
    def __init__(self) -> None:
        self._services: Dict[Type[Any], ServiceDescriptor] = {}
        self._singletons: Dict[Type[Any], Any] = {}
        self._contexts: WeakSet[ServiceContext] = WeakSet()
        self._creation_lock = asyncio.Lock()
        self._disposed = False
    
    def register_singleton(self, service_type: Type[T], implementation: Optional[Type[T]] = None, 
                          factory: Optional[Callable[..., T]] = None, instance: Optional[T] = None) -> None:
        """Register a singleton service."""
        if self._disposed:
            raise RuntimeError("Container has been disposed")
            
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=implementation,
            factory=factory,
            instance=instance,
            lifecycle=ServiceLifecycle.SINGLETON,
            initialized=instance is not None
        )
    
    def register_scoped(self, service_type: Type[T], implementation: Optional[Type[T]] = None,
                       factory: Optional[Callable[..., T]] = None) -> None:
        """Register a scoped service."""
        if self._disposed:
            raise RuntimeError("Container has been disposed")
            
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=implementation,
            factory=factory,
            lifecycle=ServiceLifecycle.SCOPED
        )
    
    def register_transient(self, service_type: Type[T], implementation: Optional[Type[T]] = None,
                          factory: Optional[Callable[..., T]] = None) -> None:
        """Register a transient service."""
        if self._disposed:
            raise RuntimeError("Container has been disposed")
            
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=implementation,
            factory=factory,
            lifecycle=ServiceLifecycle.TRANSIENT
        )
    
    def get(self, service_type: Type[T], context: Optional[ServiceContext] = None) -> T:
        """Get service instance."""
        instance = self.get_optional(service_type, context)
        if instance is None:
            raise KeyError(f"Service {service_type} is not registered")
        return instance
    
    def get_optional(self, service_type: Type[T], context: Optional[ServiceContext] = None) -> Optional[T]:
        """Get service instance or None if not registered."""
        if self._disposed:
            return None
            
        descriptor = self._services.get(service_type)
        if descriptor is None:
            return None
        
        if descriptor.lifecycle == ServiceLifecycle.SINGLETON:
            return self._get_singleton(descriptor)
        elif descriptor.lifecycle == ServiceLifecycle.SCOPED:
            return self._get_scoped(descriptor, context)
        else:  # TRANSIENT
            return self._create_instance(descriptor)
    
    def is_registered(self, service_type: Type[Any]) -> bool:
        """Check if service is registered."""
        return service_type in self._services
    
    def create_scope(self) -> ServiceContext:
        """Create a new service scope."""
        context = ServiceContext()
        self._contexts.add(context)
        return context
    
    @asynccontextmanager
    async def scope(self):
        """Async context manager for service scope."""
        context = self.create_scope()
        try:
            yield context
        finally:
            await context.dispose()
    
    def _get_singleton(self, descriptor: ServiceDescriptor) -> Any:
        """Get or create singleton instance."""
        if descriptor.instance is not None:
            return descriptor.instance
            
        if descriptor.service_type in self._singletons:
            return self._singletons[descriptor.service_type]
        
        # Thread-safe singleton creation
        if not descriptor.initializing:
            descriptor.initializing = True
            try:
                instance = self._create_instance(descriptor)
                self._singletons[descriptor.service_type] = instance
                descriptor.instance = instance
                descriptor.initialized = True
                return instance
            finally:
                descriptor.initializing = False
        else:
            # Another thread is creating the instance, wait for it
            while descriptor.initializing and not descriptor.initialized:
                pass
            return self._singletons.get(descriptor.service_type)
    
    def _get_scoped(self, descriptor: ServiceDescriptor, context: Optional[ServiceContext]) -> Any:
        """Get or create scoped instance."""
        if context is None:
            # Fall back to singleton behavior if no context
            return self._get_singleton(descriptor)
        
        instance = context.get_instance(descriptor.service_type)
        if instance is None:
            instance = self._create_instance(descriptor)
            context.set_instance(descriptor.service_type, instance)
        
        return instance
    
    def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Create new service instance."""
        if descriptor.factory is not None:
            return descriptor.factory()
        elif descriptor.implementation_type is not None:
            return descriptor.implementation_type()
        else:
            return descriptor.service_type()
    
    async def dispose(self) -> None:
        """Dispose the container and all managed services."""
        if self._disposed:
            return
            
        self._disposed = True
        
        # Dispose all contexts
        contexts = list(self._contexts)
        for context in contexts:
            await context.dispose()
        
        # Dispose singletons
        for instance in self._singletons.values():
            if hasattr(instance, "dispose") and callable(instance.dispose):
                try:
                    if asyncio.iscoroutinefunction(instance.dispose):
                        await instance.dispose()
                    else:
                        instance.dispose()
                except Exception as e:
                    logger.warning(f"Error disposing singleton service: {e}")
        
        self._singletons.clear()
        self._services.clear()


# Global container instance
_container = EnhancedServiceContainer()


def get_container() -> EnhancedServiceContainer:
    """Get the global service container."""
    return _container


def get_service(service_type: Type[T], context: Optional[ServiceContext] = None) -> T:
    """Get service from global container."""
    return _container.get(service_type, context)


def get_optional_service(service_type: Type[T], context: Optional[ServiceContext] = None) -> Optional[T]:
    """Get optional service from global container."""
    return _container.get_optional(service_type, context)


def is_service_registered(service_type: Type[Any]) -> bool:
    """Check if service is registered in global container."""
    return _container.is_registered(service_type)


# Convenience functions for registration
def register_singleton(service_type: Type[T], implementation: Optional[Type[T]] = None, 
                      factory: Optional[Callable[..., T]] = None, instance: Optional[T] = None) -> None:
    """Register singleton service in global container."""
    _container.register_singleton(service_type, implementation, factory, instance)


def register_scoped(service_type: Type[T], implementation: Optional[Type[T]] = None,
                   factory: Optional[Callable[..., T]] = None) -> None:
    """Register scoped service in global container."""
    _container.register_scoped(service_type, implementation, factory)


def register_transient(service_type: Type[T], implementation: Optional[Type[T]] = None,
                      factory: Optional[Callable[..., T]] = None) -> None:
    """Register transient service in global container."""
    _container.register_transient(service_type, implementation, factory)