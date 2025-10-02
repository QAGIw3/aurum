"""Enhanced dependency injection container for the Aurum API.

Provides proper service lifecycle management, interface-based resolution,
configuration-driven dependency wiring, health checking, and circuit breaker patterns.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import os
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union
from collections import defaultdict

# FastAPI deps are optional at import time to avoid hard dependency when running non-API code paths
try:  # pragma: no cover - import guarded for tooling contexts
    from fastapi import Depends, Request
except Exception:  # pragma: no cover
    Depends = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

T = TypeVar("T")
ServiceKey = Union[Type[T], str]
Factory = Callable[..., Any]


class ServiceLifetime:
    """Service lifetime management options."""
    SINGLETON = "singleton"      # One instance per container
    SCOPED = "scoped"           # One instance per request/scope
    TRANSIENT = "transient"     # New instance each resolution


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing, requests rejected
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    success_threshold: int = 3
    timeout: float = 30.0  # request timeout


@dataclass
class ServiceHealth:
    """Health status of a service."""
    service_name: str
    is_healthy: bool
    last_check: float
    failure_count: int = 0
    success_count: int = 0
    last_error: Optional[str] = None
    circuit_breaker_state: CircuitBreakerState = CircuitBreakerState.CLOSED
    circuit_breaker_failures: int = 0


class ServiceHealthChecker:
    """Health checker for services."""

    def __init__(self):
        self.health_status: Dict[str, ServiceHealth] = {}
        self._lock = asyncio.Lock()

    async def check_service_health(self, service_name: str, service_instance: Any) -> bool:
        """Check if a service is healthy."""
        try:
            # Check if service has a health check method
            if hasattr(service_instance, 'health_check') and callable(service_instance.health_check):
                is_healthy = await service_instance.health_check()
            elif hasattr(service_instance, 'get_service_health') and callable(service_instance.get_service_health):
                health_info = service_instance.get_service_health()
                is_healthy = health_info.get('healthy', True)
            else:
                # Default: consider service healthy if it exists and is not None
                is_healthy = service_instance is not None

            async with self._lock:
                if service_name not in self.health_status:
                    self.health_status[service_name] = ServiceHealth(
                        service_name=service_name,
                        is_healthy=is_healthy,
                        last_check=time.time()
                    )
                else:
                    health = self.health_status[service_name]
                    health.is_healthy = is_healthy
                    health.last_check = time.time()

                    if is_healthy:
                        health.success_count += 1
                        health.last_error = None
                    else:
                        health.failure_count += 1
                        health.last_error = "Health check failed"

            return is_healthy

        except Exception as exc:
            async with self._lock:
                if service_name not in self.health_status:
                    self.health_status[service_name] = ServiceHealth(
                        service_name=service_name,
                        is_healthy=False,
                        last_check=time.time(),
                        last_error=str(exc)
                    )
                else:
                    health = self.health_status[service_name]
                    health.is_healthy = False
                    health.failure_count += 1
                    health.last_error = str(exc)
                    health.last_check = time.time()

            logger.warning(f"Health check failed for {service_name}: {exc}")
            return False

    def get_service_health(self, service_name: str) -> Optional[ServiceHealth]:
        """Get health status for a service."""
        return self.health_status.get(service_name)

    def get_all_health_status(self) -> Dict[str, ServiceHealth]:
        """Get health status for all services."""
        return dict(self.health_status)


class CircuitBreaker:
    """Circuit breaker implementation for service calls."""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.config.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker is OPEN for service")

        try:
            # Execute the function with timeout
            result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout)
            await self._record_success()
            return result

        except Exception as exc:
            await self._record_failure()
            raise exc

    async def _record_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0

    async def _record_failure(self) -> None:
        """Record a failed call."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if (self.state == CircuitBreakerState.CLOSED and
                self.failure_count >= self.config.failure_threshold):
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        return self.state


class ServiceDescriptor:
    """Describes a service registration with health checking and circuit breaker support."""

    def __init__(
        self,
        service_type: Type[T],
        factory: Factory[T],
        lifetime: str = ServiceLifetime.SINGLETON,
        interfaces: Optional[List[Type[T]]] = None,
        config: Optional[Dict[str, Any]] = None,
        health_check_enabled: bool = True,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    ):
        self.service_type = service_type
        self.factory = factory
        self.lifetime = lifetime
        self.interfaces = interfaces or [service_type]
        self.config = config or {}
        self.health_check_enabled = health_check_enabled
        self.circuit_breaker_config = circuit_breaker_config or CircuitBreakerConfig()
        self._instance = None
        self._lock = asyncio.Lock()
        self._scoped_instances: Dict[str, Any] = {}
        self.circuit_breaker = CircuitBreaker(self.circuit_breaker_config)
        self.health_checker = ServiceHealthChecker()


class ServiceScope:
    """Request-scoped service container with enhanced lifecycle management."""

    def __init__(self, parent: "DependencyInjectionContainer", scope_id: str = "default"):
        self.parent = parent
        self.scope_id = scope_id
        self._scoped_instances: Dict[ServiceKey, Any] = {}
        self._created_at = time.time()
        self._disposed = False

    async def get_service(self, service_type: Type[T]) -> T:
        """Resolve service from scope or parent container."""
        # Try scoped services first
        if service_type in self._scoped_instances:
            return self._scoped_instances[service_type]

        # Fall back to parent container with scope ID
        return await self.parent.get_service(service_type, scope_id=self.scope_id)

    async def dispose(self) -> None:
        """Dispose of scoped services."""
        if self._disposed:
            return

        self._disposed = True

        # Call dispose methods on scoped services if they exist
        for instance in self._scoped_instances.values():
            if hasattr(instance, 'dispose') and callable(instance.dispose):
                try:
                    if inspect.iscoroutinefunction(instance.dispose):
                        await instance.dispose()
                    else:
                        instance.dispose()
                except Exception as exc:
                    logger.warning(f"Error disposing service in scope {self.scope_id}: {exc}")

        # Clear scoped instances
        self._scoped_instances.clear()

        logger.debug(f"Disposed service scope {self.scope_id}")

    def get_scope_metrics(self) -> Dict[str, Any]:
        """Get metrics for this scope."""
        return {
            "scope_id": self.scope_id,
            "created_at": self._created_at,
            "scoped_services_count": len(self._scoped_instances),
            "disposed": self._disposed,
            "age_seconds": time.time() - self._created_at
        }


class DependencyInjectionContainer:
    """Advanced IoC container with interface-based resolution, health checking, and circuit breakers."""

    def __init__(self):
        self._descriptors: Dict[ServiceKey, ServiceDescriptor] = {}
        self._singleton_instances: Dict[ServiceKey, Any] = {}
        self._lock = asyncio.Lock()
        self._global_health_checker = ServiceHealthChecker()
        self._start_time = time.time()

    def register(
        self,
        service_type: Type[T],
        factory: Factory[T],
        lifetime: str = ServiceLifetime.SINGLETON,
        interfaces: Optional[List[Type[T]]] = None,
        health_check_enabled: bool = True,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        **config
    ) -> None:
        """Register a service with the container."""
        descriptor = ServiceDescriptor(
            service_type=service_type,
            factory=factory,
            lifetime=lifetime,
            interfaces=interfaces or [service_type],
            config=config,
            health_check_enabled=health_check_enabled,
            circuit_breaker_config=circuit_breaker_config
        )

        # Register under service type
        self._descriptors[service_type] = descriptor

        # Register under all interfaces
        for interface in descriptor.interfaces:
            self._descriptors[interface] = descriptor

        logger.info(f"Registered service {service_type.__name__} with lifetime {lifetime}")

    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        """Register a pre-created singleton instance."""
        self._singleton_instances[service_type] = instance
        # Create a descriptor that returns the instance
        self._descriptors[service_type] = ServiceDescriptor(
            service_type=service_type,
            factory=lambda: instance,
            lifetime=ServiceLifetime.SINGLETON
        )

    async def get_service(self, service_type: Type[T], scope_id: Optional[str] = None) -> T:
        """Resolve a service instance with health checking and circuit breaker protection."""
        # Check for pre-registered singletons
        if service_type in self._singleton_instances:
            instance = self._singleton_instances[service_type]
            descriptor = self._descriptors.get(service_type)

            # Perform health check if enabled
            if descriptor and descriptor.health_check_enabled:
                await self._perform_health_check(descriptor, instance)

            return instance

        descriptor = self._descriptors.get(service_type)
        if not descriptor:
            raise KeyError(f"No service registered for {service_type}")

        # Use circuit breaker for service creation
        async def create_service():
            if descriptor.lifetime == ServiceLifetime.SINGLETON:
                return await self._get_singleton_instance(descriptor)
            elif descriptor.lifetime == ServiceLifetime.SCOPED and scope_id:
                return await self._get_scoped_instance(descriptor, scope_id)
            elif descriptor.lifetime == ServiceLifetime.TRANSIENT:
                return await self._create_instance(descriptor)
            else:
                raise ValueError(f"Unsupported lifetime: {descriptor.lifetime}")

        # Execute with circuit breaker protection
        return await descriptor.circuit_breaker.call(create_service)

    async def _get_singleton_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Get or create singleton instance."""
        async with descriptor._lock:
            if descriptor._instance is None:
                descriptor._instance = await self._create_instance(descriptor)
                # Perform initial health check for singleton services
                if descriptor.health_check_enabled:
                    await self._perform_health_check(descriptor, descriptor._instance)
            return descriptor._instance

    async def _get_scoped_instance(self, descriptor: ServiceDescriptor, scope_id: str) -> Any:
        """Get or create scoped instance."""
        if scope_id in descriptor._scoped_instances:
            return descriptor._scoped_instances[scope_id]

        # Create new scoped instance
        instance = await self._create_instance(descriptor)
        descriptor._scoped_instances[scope_id] = instance

        # Perform health check if enabled
        if descriptor.health_check_enabled:
            await self._perform_health_check(descriptor, instance)

        return instance

    async def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Create a new service instance."""
        # Simple factory invocation - in practice, you'd want dependency resolution here
        if inspect.iscoroutinefunction(descriptor.factory):
            return await descriptor.factory()
        return descriptor.factory()

    async def _perform_health_check(self, descriptor: ServiceDescriptor, instance: Any) -> None:
        """Perform health check on a service instance."""
        service_name = descriptor.service_type.__name__
        is_healthy = await descriptor.health_checker.check_service_health(service_name, instance)

        if not is_healthy:
            logger.warning(f"Service {service_name} failed health check")

    def create_scope(self, scope_id: str) -> ServiceScope:
        """Create a new service scope."""
        return ServiceScope(self, scope_id)

    def get_service_health_status(self, service_type: Type[T]) -> Optional[ServiceHealth]:
        """Get health status for a specific service."""
        descriptor = self._descriptors.get(service_type)
        if descriptor:
            service_name = service_type.__name__
            return descriptor.health_checker.get_service_health(service_name)
        return None

    def get_all_service_health(self) -> Dict[str, ServiceHealth]:
        """Get health status for all services."""
        all_health = {}

        # Get health from individual service descriptors
        for descriptor in self._descriptors.values():
            service_name = descriptor.service_type.__name__
            health = descriptor.health_checker.get_service_health(service_name)
            if health:
                all_health[service_name] = health

        # Also include global health checker results
        all_health.update(self._global_health_checker.get_all_health_status())

        return all_health

    def get_container_metrics(self) -> Dict[str, Any]:
        """Get container performance metrics."""
        uptime = time.time() - self._start_time

        return {
            "uptime_seconds": uptime,
            "registered_services": len(self._descriptors),
            "singleton_instances": len(self._singleton_instances),
            "container_health": "healthy" if uptime > 0 else "starting"
        }

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
            from aurum.libs.services.curves_service import CurvesService
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
        self.metadata_cache = None
        self._lock = asyncio.Lock()

    async def get_cache_manager(self):
        if self.cache_manager is None:
            try:
                from .cache.consolidated_manager import get_unified_cache_manager
                self.cache_manager = get_unified_cache_manager()
            except Exception:
                self.cache_manager = None
        return self.cache_manager

    async def get_metadata_cache(self) -> Any:
        if self.metadata_cache is None:
            self.metadata_cache = await self.get_cache_manager()
        return self.metadata_cache

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
    """Return the process-wide application context.

    This uses the in-memory singleton but favors initialized FastAPI state
    when available.
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
            from aurum.api.state import get_settings as _api_get_settings
            return AsyncScenarioService(_api_get_settings())  # type: ignore[return-value]
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
    from aurum.libs.services.curves_service import CurvesService
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
