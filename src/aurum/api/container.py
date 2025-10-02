"""API container compatibility layer - DEPRECATED.

This module provides backward compatibility for code using the old API container.
New code should import directly from aurum.core.container instead.

DEPRECATION NOTICE:
- DependencyInjectionContainer is deprecated, use core.container.DependencyContainer
- Most classes are now available in aurum.core.container
- This compatibility layer will be removed in a future version
"""
from __future__ import annotations

import asyncio
import logging
import warnings
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Type, TypeVar

# Import from enhanced core container
from aurum.core.container import (
    DependencyContainer,
    ServiceLifetime,
    ServiceDescriptor,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    ServiceHealth,
    ServiceHealthChecker,
)

# FastAPI deps are optional
try:  # pragma: no cover
    from fastapi import Depends, Request
except Exception:  # pragma: no cover
    Depends = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ========================================
# Compatibility Wrapper - DEPRECATED
# ========================================

class DependencyInjectionContainer(DependencyContainer):
    """Compatibility wrapper for the old API container - DEPRECATED.
    
    This class redirects to aurum.core.container.DependencyContainer.
    
    **DEPRECATED**: Use aurum.core.container.DependencyContainer instead.
    This compatibility wrapper will be removed in a future version.
    
    All functionality from the old API container is now available in the
    enhanced core container with additional features:
    - Circuit breaker protection
    - Health monitoring
    - Scoped lifetime support
    - Interface-based resolution
    """
    
    def __init__(self, settings=None):
        """Initialize with deprecation warning."""
        warnings.warn(
            "aurum.api.container.DependencyInjectionContainer is deprecated. "
            "Use aurum.core.container.DependencyContainer instead. "
            "This compatibility wrapper will be removed in a future version.",
            DeprecationWarning,
            stacklevel=2
        )
        super().__init__(settings)
        logger.debug("Using deprecated DependencyInjectionContainer (redirects to core.DependencyContainer)")
    
    async def get_service(self, service_type: Type[T], scope_id: Optional[str] = None) -> T:
        """Compatibility alias for get() method.
        
        **DEPRECATED**: Use await container.get(ServiceType) instead.
        """
        return await self.get(service_type, scope_id)
    
    def create_scope(self, scope_id: str = "default") -> "ServiceScope":
        """Create a service scope for request-scoped services.
        
        Args:
            scope_id: Unique identifier for this scope
            
        Returns:
            ServiceScope instance
        """
        return ServiceScope(self, scope_id)
    
    @staticmethod
    def from_settings(settings: "AurumSettings") -> "DependencyInjectionContainer":
        """Create container from settings - DEPRECATED.
        
        **DEPRECATED**: Use DependencyContainer(settings) instead.
        """
        warnings.warn(
            "DependencyInjectionContainer.from_settings() is deprecated. "
            "Use DependencyContainer(settings) instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return DependencyInjectionContainer(settings)


# ========================================
# Service Scope for Request-Scoped Services
# ========================================

class ServiceScope:
    """Request-scoped service container."""
    
    def __init__(self, parent: DependencyContainer, scope_id: str = "default"):
        self.parent = parent
        self.scope_id = scope_id
        self._disposed = False
    
    async def get_service(self, service_type: Type[T]) -> T:
        """Resolve service from scope."""
        if self._disposed:
            raise RuntimeError(f"ServiceScope {self.scope_id} has been disposed")
        return await self.parent.get(service_type, scope_id=self.scope_id)
    
    async def dispose(self) -> None:
        """Dispose of scoped services."""
        if self._disposed:
            return
        self._disposed = True
        logger.debug(f"Disposed service scope {self.scope_id}")


# ========================================
# Service Interfaces (API-Specific)
# ========================================

class IDataBackend(ABC):
    """Interface for data backend operations."""
    
    @abstractmethod
    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check backend health."""
        pass


class ICacheProvider(ABC):
    """Interface for caching operations."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get cached value."""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Set cached value."""
        pass


# ========================================
# Application Context (API-Specific)
# ========================================

class ApplicationContext:
    """Application context to replace global state variables."""
    
    def __init__(self):
        self.cache_manager = None
        self.settings = None
        self.admin_groups = set()
        self.drought_catalog = None
        self.tile_cache = None
        self.metadata_cache = None
        self._lock = asyncio.Lock()
    
    async def get_cache_manager(self):
        """Get cache manager with proper dependency injection."""
        if self.cache_manager is None:
            try:
                from .cache.consolidated_manager import get_unified_cache_manager
                self.cache_manager = get_unified_cache_manager()
            except Exception:
                self.cache_manager = None
        return self.cache_manager
    
    async def get_metadata_cache(self) -> Any:
        """Get metadata cache."""
        if self.metadata_cache is None:
            self.metadata_cache = await self.get_cache_manager()
        return self.metadata_cache
    
    def get_admin_groups(self):
        """Get admin groups from settings."""
        if not self.admin_groups and self.settings:
            auth_cfg = getattr(self.settings, "auth", None)
            if auth_cfg and not getattr(auth_cfg, "disabled", False):
                raw_groups = getattr(auth_cfg, "admin_groups", None)
                if raw_groups:
                    self.admin_groups = {str(item).strip().lower() for item in raw_groups if str(item).strip()}
        return self.admin_groups


# Global fallback application context
_GLOBAL_APP_CONTEXT: ApplicationContext | None = None


def get_app_context() -> ApplicationContext:
    """Return the process-wide application context."""
    global _GLOBAL_APP_CONTEXT
    if _GLOBAL_APP_CONTEXT is None:
        _GLOBAL_APP_CONTEXT = ApplicationContext()
    return _GLOBAL_APP_CONTEXT


# ========================================
# FastAPI Integration Helpers
# ========================================

def get_container_dependency(request: "Request") -> DependencyContainer:
    """FastAPI dependency to access the container from app.state.
    
    Usage:
        @app.get("/")
        async def endpoint(container: DependencyContainer = Depends(get_container_dependency)):
            service = await container.get(MyService)
    """
    if Depends is None or Request is None:
        raise RuntimeError("FastAPI is required for get_container_dependency")
    
    container = getattr(getattr(request, "app", None), "state", None) and getattr(
        request.app.state, "container", None  # type: ignore[attr-defined]
    )
    
    if not isinstance(container, DependencyContainer):
        # Also check for di_container alias
        container = getattr(request.app.state, "di_container", None) if hasattr(request.app, "state") else None  # type: ignore[attr-defined]
    
    if not isinstance(container, DependencyContainer):
        raise RuntimeError("DependencyContainer not configured on app.state.container or app.state.di_container")
    
    return container


def provide_service(service_type: Type[T]):
    """Factory that returns a FastAPI dependency to resolve a service instance.
    
    Usage:
        @app.get("/")
        async def endpoint(service: MyService = Depends(provide_service(MyService))):
            return await service.do_something()
    """
    if Depends is None:
        raise RuntimeError("FastAPI is required for provide_service")
    
    async def _resolver(container: DependencyContainer = Depends(get_container_dependency)) -> T:  # type: ignore[misc]
        return await container.get(service_type)
    
    return _resolver


def get_app_context_dependency(request: "Request") -> ApplicationContext:
    """FastAPI dependency to access application context.
    
    Usage:
        @app.get("/")
        async def endpoint(ctx: ApplicationContext = Depends(get_app_context_dependency)):
            cache = await ctx.get_cache_manager()
    """
    if Request is None:
        raise RuntimeError("FastAPI is required for get_app_context_dependency")
    
    state = getattr(getattr(request, "app", None), "state", None)
    ctx = getattr(state, "app_context", None)
    
    if isinstance(ctx, ApplicationContext):
        return ctx
    
    # Lazy init per app if missing
    ctx = ApplicationContext()
    if state is not None:
        setattr(state, "app_context", ctx)
    
    return ctx


# ========================================
# Service Registration Helpers
# ========================================

def register_core_services(container: DependencyContainer) -> None:
    """Register core services with the container.
    
    **Note**: This is now handled automatically by get_container() in core.container.
    This function is kept for backward compatibility but is mostly redundant.
    """
    # Core services are now auto-registered in core.container
    logger.debug("register_core_services called (services auto-registered in core.container)")
    
    # Register any API-specific services here if needed
    try:
        from .cache.consolidated_manager import get_unified_cache_manager, UnifiedCacheManager
        
        if not any(isinstance(container._descriptors.get(UnifiedCacheManager), ServiceDescriptor) 
                   for _ in [1]):  # Check if not already registered
            container.register(
                UnifiedCacheManager,
                lambda: get_unified_cache_manager(),
                lifetime=ServiceLifetime.SINGLETON
            )
    except Exception as e:
        logger.debug(f"Could not register UnifiedCacheManager: {e}")


# ========================================
# Legacy Compatibility Functions
# ========================================

def get_service(service_type: Type[T]) -> T:
    """Legacy helper to obtain a service instance - DEPRECATED.
    
    **DEPRECATED**: Use dependency injection via provide_service() or resolve from container.
    
    This function is kept for backward compatibility with modules that import it directly.
    """
    warnings.warn(
        "get_service() from api.container is deprecated. "
        "Use FastAPI dependency injection via provide_service() or resolve from container.",
        DeprecationWarning,
        stacklevel=2
    )
    
    try:
        from .async_service import AsyncScenarioService  # type: ignore[import]
        from aurum.core.settings import get_settings as _core_get_settings
        
        if service_type is AsyncScenarioService:  # type: ignore[name-defined]
            from .state import get_settings as _api_get_settings
            return AsyncScenarioService(_api_get_settings())  # type: ignore[return-value]
    except Exception:
        pass
    
    raise NotImplementedError(f"get_service does not support: {service_type}")


# ========================================
# Exports
# ========================================

__all__ = [
    # Core container (from aurum.core.container)
    "DependencyContainer",
    "ServiceLifetime",
    "ServiceDescriptor",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerState",
    "ServiceHealth",
    "ServiceHealthChecker",
    
    # Deprecated compatibility wrapper
    "DependencyInjectionContainer",
    "ServiceScope",
    
    # FastAPI integration
    "get_container_dependency",
    "provide_service",
    "get_app_context_dependency",
    
    # API-specific interfaces and context
    "IDataBackend",
    "ICacheProvider",
    "ApplicationContext",
    "get_app_context",
    
    # Registration helpers
    "register_core_services",
    
    # Legacy (deprecated)
    "get_service",
]
