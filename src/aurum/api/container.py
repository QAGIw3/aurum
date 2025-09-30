"""Advanced dependency injection container for the Aurum API.

Provides proper service lifecycle management, interface-based resolution,
and configuration-driven dependency wiring.
"""
from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
import os
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

# FastAPI deps are optional at import time to avoid hard dependency when running non-API code paths
try:  # pragma: no cover - import guarded for tooling contexts
    from fastapi import Depends, Request
except Exception:  # pragma: no cover
    Depends = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]

T = TypeVar("T")
ServiceKey = Union[Type[T], str]
Factory = Callable[..., Any]


class ServiceLifetime:
    """Service lifetime management options."""
    SINGLETON = "singleton"      # One instance per container
    SCOPED = "scoped"           # One instance per request/scope
    TRANSIENT = "transient"     # New instance each resolution


class ServiceDescriptor:
    """Describes a service registration."""

    def __init__(
        self,
        service_type: Type[T],
        factory: Factory[T],
        lifetime: str = ServiceLifetime.SINGLETON,
        interfaces: Optional[List[Type[T]]] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.service_type = service_type
        self.factory = factory
        self.lifetime = lifetime
        self.interfaces = interfaces or [service_type]
        self.config = config or {}
        self._instance = None
        self._lock = asyncio.Lock()


class ServiceScope:
    """Request-scoped service container."""

    def __init__(self, parent: "DependencyInjectionContainer"):
        self.parent = parent
        self._scoped_instances: Dict[ServiceKey, Any] = {}

    def get_service(self, service_type: Type[T]) -> T:
        """Resolve service from scope or parent container."""
        # Try scoped services first
        if service_type in self._scoped_instances:
            return self._scoped_instances[service_type]

        # Fall back to parent container
        return self.parent.get_service(service_type)


class DependencyInjectionContainer:
    """Advanced IoC container with interface-based resolution."""

    def __init__(self):
        self._descriptors: Dict[ServiceKey, ServiceDescriptor] = {}
        self._singleton_instances: Dict[ServiceKey, Any] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        service_type: Type[T],
        factory: Factory[T],
        lifetime: str = ServiceLifetime.SINGLETON,
        interfaces: Optional[List[Type[T]]] = None,
        **config
    ) -> None:
        """Register a service with the container."""
        descriptor = ServiceDescriptor(
            service_type=service_type,
            factory=factory,
            lifetime=lifetime,
            interfaces=interfaces or [service_type],
            config=config
        )

        # Register under service type
        self._descriptors[service_type] = descriptor

        # Register under all interfaces
        for interface in descriptor.interfaces:
            self._descriptors[interface] = descriptor

    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        """Register a pre-created singleton instance."""
        self._singleton_instances[service_type] = instance
        # Create a descriptor that returns the instance
        self._descriptors[service_type] = ServiceDescriptor(
            service_type=service_type,
            factory=lambda: instance,
            lifetime=ServiceLifetime.SINGLETON
        )

    async def get_service(self, service_type: Type[T]) -> T:
        """Resolve a service instance."""
        # Check for pre-registered singletons
        if service_type in self._singleton_instances:
            return self._singleton_instances[service_type]

        descriptor = self._descriptors.get(service_type)
        if not descriptor:
            raise KeyError(f"No service registered for {service_type}")

        if descriptor.lifetime == ServiceLifetime.SINGLETON:
            return await self._get_singleton_instance(descriptor)
        elif descriptor.lifetime == ServiceLifetime.TRANSIENT:
            return await self._create_instance(descriptor)

        raise ValueError(f"Unsupported lifetime: {descriptor.lifetime}")

    async def _get_singleton_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Get or create singleton instance."""
        async with descriptor._lock:
            if descriptor._instance is None:
                descriptor._instance = await self._create_instance(descriptor)
            return descriptor._instance

    async def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Create a new service instance."""
        # Simple factory invocation - in practice, you'd want dependency resolution here
        if inspect.iscoroutinefunction(descriptor.factory):
            return await descriptor.factory()
        return descriptor.factory()

    def create_scope(self) -> ServiceScope:
        """Create a new service scope."""
        return ServiceScope(self)

    def get_registered_services(self) -> Dict[ServiceKey, ServiceDescriptor]:
        """Get all registered services for introspection."""
        return dict(self._descriptors)

    
    # --------------------------
    # FastAPI integration helpers
    # --------------------------

    @staticmethod
    def from_settings(settings: "AurumSettings") -> "DependencyInjectionContainer":
        """Create and pre-register core services using provided settings."""
        container = DependencyInjectionContainer()
        # Register services that do not require async factories
        try:
            # Local imports to avoid heavy imports during module load
            from .services.curves_service import CurvesService
            from .services.metadata_service import MetadataService
            from .services.eia_service import EiaService
            from .services.iso_service import IsoService
            from .cache.consolidated_manager import get_unified_cache_manager, UnifiedCacheManager
            from .client import ExternalAPIClient, ClientConfig, RetryConfig

            container.register(CurvesService, lambda: CurvesService(), ServiceLifetime.SINGLETON)
            container.register(MetadataService, lambda: MetadataService(), ServiceLifetime.SINGLETON)
            container.register(EiaService, lambda: EiaService(), ServiceLifetime.SINGLETON)
            container.register(IsoService, lambda: IsoService(), ServiceLifetime.SINGLETON)
            container.register(UnifiedCacheManager, lambda: get_unified_cache_manager(), ServiceLifetime.SINGLETON)

            def _external_client_factory() -> ExternalAPIClient:
                base_url = os.getenv("AURUM_EXTERNAL_BASE_URL", os.getenv("AURUM_API_BASE_URL", "http://localhost:8001"))
                max_attempts = int(os.getenv("AURUM_EXTERNAL_RETRIES", "3") or 3)
                backoff_base = float(os.getenv("AURUM_EXTERNAL_BACKOFF_BASE", "0.3") or 0.3)
                backoff_max = float(os.getenv("AURUM_EXTERNAL_BACKOFF_MAX", "3.0") or 3.0)
                bearer = os.getenv("AURUM_EXTERNAL_BEARER")
                headers: dict[str, str] = {}
                if bearer:
                    headers["Authorization"] = f"Bearer {bearer}"
                retry = RetryConfig(max_attempts=max_attempts, base_delay_seconds=backoff_base, max_delay_seconds=backoff_max)
                return ExternalAPIClient(ClientConfig(base_url=base_url, retry=retry, headers=headers))

            container.register(ExternalAPIClient, _external_client_factory, ServiceLifetime.SINGLETON)
        except Exception:  # pragma: no cover - best effort registration
            pass
        return container


# --------------------------
# FastAPI dependency providers (no globals)
# --------------------------

def get_container_dependency(request: "Request") -> DependencyInjectionContainer:
    """FastAPI dependency to access the per-app container attached to app.state."""
    container = getattr(getattr(request, "app", None), "state", None) and getattr(request.app.state, "container", None)  # type: ignore[attr-defined]
    if not isinstance(container, DependencyInjectionContainer):
        raise RuntimeError("DependencyInjectionContainer not configured on app.state.container")
    return container


def provide_service(service_type: Type[T]):
    """Factory that returns a FastAPI dependency to resolve a service instance."""

    async def _resolver(container: DependencyInjectionContainer = Depends(get_container_dependency)) -> T:  # type: ignore[name-defined]
        return await container.get_service(service_type)

    return _resolver


# Service interfaces
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


# Application context for managing global state
class ApplicationContext:
    """Application context to replace global state variables."""

    def __init__(self):
        self.cache_manager = None
        self.settings = None
        self.admin_groups = set()
        self.drought_catalog = None
        self.tile_cache = None
        self.metadata_fallback_cache = {}
        self._lock = asyncio.Lock()

    async def get_cache_manager(self):
        if self.cache_manager is None:
            try:
                from .cache.unified_cache_manager import get_unified_cache_manager
                self.cache_manager = get_unified_cache_manager()
            except Exception:
                self.cache_manager = None
        return self.cache_manager

    def get_admin_groups(self):
        if not self.admin_groups and self.settings:
            auth_cfg = getattr(self.settings, "auth", None)
            if auth_cfg and not getattr(auth_cfg, "disabled", False):
                raw_groups = getattr(auth_cfg, "admin_groups", None)
                if raw_groups:
                    self.admin_groups = {str(item).strip().lower() for item in raw_groups if str(item).strip()}
        return self.admin_groups


# Global fallback application context for use outside of request scope
_GLOBAL_APP_CONTEXT: ApplicationContext | None = None


def get_app_context() -> ApplicationContext:
    """Return a process-wide application context.

    This provides a simple fallback for modules that need an app context during
    application initialization (when no Request object exists yet). For request
    handlers, prefer using `get_app_context_dependency` to attach context to the
    FastAPI app state.
    """
    global _GLOBAL_APP_CONTEXT
    if _GLOBAL_APP_CONTEXT is None:
        _GLOBAL_APP_CONTEXT = ApplicationContext()
    return _GLOBAL_APP_CONTEXT

# --------------------------
# Legacy shim helpers
# --------------------------
def get_service(service_type: Type[T]) -> T:
    """Legacy helper to obtain a service instance synchronously.

    Currently supports AsyncScenarioService for modules that import this
    function directly. New code should prefer dependency injection via
    `provide_service` or resolve from the app's container.
    """
    try:
        from .async_service import AsyncScenarioService  # type: ignore
        from aurum.core.settings import get_settings as _core_get_settings

        if service_type is AsyncScenarioService:  # type: ignore[name-defined]
            return AsyncScenarioService(_core_get_settings())  # type: ignore[return-value]
    except Exception:
        pass
    raise NotImplementedError(f"get_service does not support: {service_type}")

    async def get_cache_manager(self):
        """Get cache manager with proper dependency injection."""
        if self.cache_manager is None:
            from .cache.unified_cache_manager import get_unified_cache_manager
            self.cache_manager = get_unified_cache_manager()
        return self.cache_manager

    def get_admin_groups(self):
        """Get admin groups from settings."""
        if not self.admin_groups and self.settings:
            auth_cfg = getattr(self.settings, "auth", None)
            if auth_cfg and not getattr(auth_cfg, "disabled", False):
                raw_groups = getattr(auth_cfg, "admin_groups", None)
                if raw_groups:
                    self.admin_groups = {str(item).strip().lower() for item in raw_groups if str(item).strip()}
        return self.admin_groups


def get_app_context_dependency(request: "Request") -> ApplicationContext:
    """FastAPI dependency to access application context bound to the app."""
    state = getattr(getattr(request, "app", None), "state", None)
    ctx = getattr(state, "app_context", None)
    if isinstance(ctx, ApplicationContext):
        return ctx
    # Lazy init per app if missing
    ctx = ApplicationContext()
    if state is not None:
        setattr(state, "app_context", ctx)
    return ctx


# Service registration
def register_core_services(container: DependencyInjectionContainer) -> None:
    """Register core services with the provided container (no globals)."""
    from .services.curves_service import CurvesService
    from .services.metadata_service import MetadataService
    from .services.eia_service import EiaService
    from .services.iso_service import IsoService
    from .cache.consolidated_manager import get_unified_cache_manager, UnifiedCacheManager

    container.register(CurvesService, lambda: CurvesService(), ServiceLifetime.SINGLETON)
    container.register(MetadataService, lambda: MetadataService(), ServiceLifetime.SINGLETON)
    container.register(EiaService, lambda: EiaService(), ServiceLifetime.SINGLETON)
    container.register(IsoService, lambda: IsoService(), ServiceLifetime.SINGLETON)
    container.register(UnifiedCacheManager, lambda: get_unified_cache_manager(), ServiceLifetime.SINGLETON)


__all__ = [
    "DependencyInjectionContainer",
    "ServiceScope",
    "ServiceLifetime",
    "ServiceDescriptor",
    "get_container_dependency",
    "provide_service",
    "IDataBackend",
    "ICacheProvider",
    "register_core_services",
    "ApplicationContext",
    "get_app_context",
    "get_app_context_dependency",
]
