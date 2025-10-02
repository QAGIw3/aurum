"""Unified dependency injection container for the Aurum platform.

Provides centralized service registration, lifecycle management, and dependency resolution.
Consolidates patterns from src/aurum/api/container.py into a single canonical implementation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from aurum.core.settings import AurumSettings, get_settings

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceLifetime:
    """Service lifetime management options."""
    SINGLETON = "singleton"      # One instance per container
    SCOPED = "scoped"           # One instance per request/scope
    TRANSIENT = "transient"     # New instance each resolution


class DependencyContainer:
    """Unified dependency injection container.
    
    Provides:
    - Service registration with lifetime management
    - Lazy initialization
    - Singleton pattern for shared services
    - Scoped services for request isolation
    - Factory pattern for complex construction
    
    Following SOLID principles:
    - Single Responsibility: Dependency management only
    - Open/Closed: Extensible via registration
    - Liskov Substitution: All services follow contracts
    - Interface Segregation: Minimal container interface
    - Dependency Inversion: Depends on abstractions
    """
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize container with settings.
        
        Args:
            settings: Application settings (None = load from environment)
        """
        self.settings = settings or get_settings()
        self._singletons: Dict[Type, Any] = {}
        self._factories: Dict[Type, Callable] = {}
        self._lifetimes: Dict[Type, str] = {}
        self._lock = asyncio.Lock()
    
    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        """Register a pre-created singleton instance.
        
        Args:
            service_type: Service type (used as key)
            instance: Service instance
        """
        self._singletons[service_type] = instance
        self._lifetimes[service_type] = ServiceLifetime.SINGLETON
        logger.debug(f"Registered singleton: {service_type.__name__}")
    
    def register(
        self,
        service_type: Type[T],
        factory: Callable[[], T],
        lifetime: str = ServiceLifetime.SINGLETON
    ) -> None:
        """Register a service with factory function.
        
        Args:
            service_type: Service type (used as key)
            factory: Factory function to create instance
            lifetime: Service lifetime (singleton, scoped, transient)
        """
        self._factories[service_type] = factory
        self._lifetimes[service_type] = lifetime
        logger.debug(f"Registered {lifetime} service: {service_type.__name__}")
    
    async def get(self, service_type: Type[T]) -> T:
        """Get service instance.
        
        Args:
            service_type: Service type to resolve
            
        Returns:
            Service instance
            
        Raises:
            KeyError: If service not registered
        """
        # Check for pre-registered singleton
        if service_type in self._singletons:
            return self._singletons[service_type]
        
        # Check for factory
        if service_type not in self._factories:
            raise KeyError(f"Service not registered: {service_type.__name__}")
        
        lifetime = self._lifetimes.get(service_type, ServiceLifetime.SINGLETON)
        
        if lifetime == ServiceLifetime.SINGLETON:
            # Create singleton if not exists
            if service_type not in self._singletons:
                async with self._lock:
                    # Double-check after acquiring lock
                    if service_type not in self._singletons:
                        factory = self._factories[service_type]
                        instance = factory()
                        
                        # If instance is a coroutine, await it
                        if asyncio.iscoroutine(instance):
                            instance = await instance
                        
                        self._singletons[service_type] = instance
                        logger.debug(f"Created singleton: {service_type.__name__}")
            
            return self._singletons[service_type]
        
        elif lifetime == ServiceLifetime.TRANSIENT:
            # Always create new instance
            factory = self._factories[service_type]
            instance = factory()
            
            if asyncio.iscoroutine(instance):
                instance = await instance
            
            logger.debug(f"Created transient: {service_type.__name__}")
            return instance
        
        else:
            # Scoped not implemented yet - fall back to transient
            logger.warning(f"Scoped lifetime not yet implemented for {service_type.__name__}, using transient")
            return await self.get(service_type)
    
    async def close_all(self) -> None:
        """Close all singleton services that have close() methods."""
        for service_type, instance in self._singletons.items():
            if hasattr(instance, 'close'):
                try:
                    close_method = getattr(instance, 'close')
                    if asyncio.iscoroutinefunction(close_method):
                        await close_method()
                    else:
                        close_method()
                    logger.debug(f"Closed {service_type.__name__}")
                except Exception as e:
                    logger.warning(f"Error closing {service_type.__name__}: {e}")


# Global container instance
_container: Optional[DependencyContainer] = None


def get_container(settings: Optional[AurumSettings] = None) -> DependencyContainer:
    """Get or create the global dependency container.
    
    Args:
        settings: Optional settings (None = use global settings)
        
    Returns:
        Global dependency container
    """
    global _container
    if _container is None:
        _container = DependencyContainer(settings)
        _register_core_services(_container)
    return _container


def reset_container() -> None:
    """Reset the global container (for testing)."""
    global _container
    _container = None


def _register_core_services(container: DependencyContainer) -> None:
    """Register core platform services.
    
    This is called automatically when the container is first created.
    Add service registrations here for automatic wiring.
    """
    # Settings (already available as container.settings)
    container.register_singleton(AurumSettings, container.settings)
    
    # Register services as they become available
    # Example:
    # from aurum.services.core import CurveService
    # from aurum.data.repositories import CurveRepository
    # 
    # container.register(
    #     CurveService,
    #     lambda: CurveService(CurveRepository(container.settings)),
    #     lifetime=ServiceLifetime.SINGLETON
    # )
    
    logger.info("Core services registered in DI container")


# Convenience function for FastAPI dependency injection
async def get_service(service_type: Type[T]) -> T:
    """Get service instance for FastAPI dependency injection.
    
    Usage in routes:
        from aurum.core.container import get_service
        from aurum.services.core import CurveService
        
        @router.get("/curves")
        async def list_curves(
            service: CurveService = Depends(lambda: get_service(CurveService))
        ):
            result = await service.get_curves(iso="PJM")
            return result.data
    
    Args:
        service_type: Service type to resolve
        
    Returns:
        Service instance
    """
    container = get_container()
    return await container.get(service_type)


__all__ = [
    "DependencyContainer",
    "ServiceLifetime",
    "get_container",
    "reset_container",
    "get_service",
]

