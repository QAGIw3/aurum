"""Unified dependency injection container for the Aurum platform.

Provides centralized service registration, lifecycle management, and dependency resolution.
Consolidates patterns from src/aurum/api/container.py into a single canonical implementation.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from aurum.core.settings import AurumSettings, get_settings

logger = logging.getLogger(__name__)

T = TypeVar('T')
ServiceKey = Union[Type[T], str]


class ServiceLifetime:
    """Service lifetime management options."""
    SINGLETON = "singleton"      # One instance per container
    SCOPED = "scoped"           # One instance per request/scope
    TRANSIENT = "transient"     # New instance each resolution


class CircuitBreakerState(Enum):
    """Circuit breaker states for fault tolerance."""
    CLOSED = "closed"           # Normal operation
    OPEN = "open"              # Failing, requests rejected
    HALF_OPEN = "half_open"    # Testing if service recovered


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


class CircuitBreaker:
    """Circuit breaker implementation for service resilience."""

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
                    raise RuntimeError(f"Circuit breaker is OPEN")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout)
            else:
                result = func(*args, **kwargs)
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


class ServiceHealthChecker:
    """Health checker for monitoring service status."""

    def __init__(self):
        self.health_status: Dict[str, ServiceHealth] = {}
        self._lock = asyncio.Lock()

    async def check_service_health(self, service_name: str, service_instance: Any) -> bool:
        """Check if a service is healthy."""
        try:
            # Check if service has a health check method
            if hasattr(service_instance, 'health_check') and callable(service_instance.health_check):
                is_healthy = await service_instance.health_check() if asyncio.iscoroutinefunction(service_instance.health_check) else service_instance.health_check()
            else:
                # Default: consider service healthy if it exists
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


@dataclass
class ServiceDescriptor:
    """Descriptor for registered services with advanced features."""
    service_type: Type
    factory: Callable
    lifetime: str = ServiceLifetime.SINGLETON
    interfaces: List[Type] = field(default_factory=list)
    health_check_enabled: bool = True
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    _instance: Optional[Any] = None
    _scoped_instances: Dict[str, Any] = field(default_factory=dict)
    _circuit_breaker: Optional[CircuitBreaker] = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        """Initialize circuit breaker after creation."""
        if self.circuit_breaker_config:
            self._circuit_breaker = CircuitBreaker(self.circuit_breaker_config)
        if not self.interfaces:
            self.interfaces = [self.service_type]


class DependencyContainer:
    """Unified dependency injection container with advanced features.
    
    Provides:
    - Service registration with lifetime management
    - Lazy initialization
    - Singleton pattern for shared services
    - Scoped services for request isolation
    - Factory pattern for complex construction
    - Circuit breaker protection
    - Health checking
    - Interface-based resolution
    
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
        self._descriptors: Dict[ServiceKey, ServiceDescriptor] = {}
        self._singletons: Dict[Type, Any] = {}
        self._health_checker = ServiceHealthChecker()
        self._lock = asyncio.Lock()
        self._start_time = time.time()
    
    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        """Register a pre-created singleton instance.
        
        Args:
            service_type: Service type (used as key)
            instance: Service instance
        """
        self._singletons[service_type] = instance
        descriptor = ServiceDescriptor(
            service_type=service_type,
            factory=lambda: instance,
            lifetime=ServiceLifetime.SINGLETON
        )
        self._descriptors[service_type] = descriptor
        logger.debug(f"Registered singleton: {service_type.__name__}")
    
    def register(
        self,
        service_type: Type[T],
        factory: Callable[[], T],
        lifetime: str = ServiceLifetime.SINGLETON,
        interfaces: Optional[List[Type]] = None,
        health_check_enabled: bool = False,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    ) -> None:
        """Register a service with factory function and advanced features.
        
        Args:
            service_type: Service type (used as key)
            factory: Factory function to create instance
            lifetime: Service lifetime (singleton, scoped, transient)
            interfaces: Additional interface types this service implements
            health_check_enabled: Enable health checking for this service
            circuit_breaker_config: Circuit breaker configuration
        """
        descriptor = ServiceDescriptor(
            service_type=service_type,
            factory=factory,
            lifetime=lifetime,
            interfaces=interfaces or [service_type],
            health_check_enabled=health_check_enabled,
            circuit_breaker_config=circuit_breaker_config
        )
        
        # Register under service type
        self._descriptors[service_type] = descriptor
        
        # Register under all interfaces
        if interfaces:
            for interface in interfaces:
                self._descriptors[interface] = descriptor
        
        logger.debug(f"Registered {lifetime} service: {service_type.__name__}")
    
    async def get(self, service_type: Type[T], scope_id: Optional[str] = None) -> T:
        """Get service instance with circuit breaker protection and health checking.
        
        Args:
            service_type: Service type to resolve
            scope_id: Optional scope ID for scoped services
            
        Returns:
            Service instance
            
        Raises:
            KeyError: If service not registered
        """
        # Check for pre-registered singleton first
        if service_type in self._singletons:
            return self._singletons[service_type]
        
        # Get descriptor
        descriptor = self._descriptors.get(service_type)
        if not descriptor:
            raise KeyError(f"Service not registered: {service_type.__name__}")
        
        # Use circuit breaker if configured
        if descriptor._circuit_breaker:
            async def create_service():
                return await self._resolve_service(descriptor, scope_id)
            return await descriptor._circuit_breaker.call(create_service)
        else:
            return await self._resolve_service(descriptor, scope_id)
    
    async def _resolve_service(self, descriptor: ServiceDescriptor, scope_id: Optional[str] = None) -> Any:
        """Resolve service based on lifetime."""
        if descriptor.lifetime == ServiceLifetime.SINGLETON:
            return await self._get_singleton(descriptor)
        elif descriptor.lifetime == ServiceLifetime.SCOPED and scope_id:
            return await self._get_scoped(descriptor, scope_id)
        elif descriptor.lifetime == ServiceLifetime.TRANSIENT:
            return await self._create_instance(descriptor)
        else:
            # Scoped without scope_id falls back to transient
            return await self._create_instance(descriptor)
    
    async def _get_singleton(self, descriptor: ServiceDescriptor) -> Any:
        """Get or create singleton instance."""
        if descriptor._instance is not None:
            return descriptor._instance
        
        async with descriptor._lock:
            # Double-check after acquiring lock
            if descriptor._instance is None:
                descriptor._instance = await self._create_instance(descriptor)
                
                # Perform health check if enabled
                if descriptor.health_check_enabled:
                    await self._health_checker.check_service_health(
                        descriptor.service_type.__name__,
                        descriptor._instance
                    )
                
                logger.debug(f"Created singleton: {descriptor.service_type.__name__}")
            
            return descriptor._instance
    
    async def _get_scoped(self, descriptor: ServiceDescriptor, scope_id: str) -> Any:
        """Get or create scoped instance."""
        if scope_id in descriptor._scoped_instances:
            return descriptor._scoped_instances[scope_id]
        
        async with descriptor._lock:
            if scope_id not in descriptor._scoped_instances:
                instance = await self._create_instance(descriptor)
                descriptor._scoped_instances[scope_id] = instance
                
                # Perform health check if enabled
                if descriptor.health_check_enabled:
                    await self._health_checker.check_service_health(
                        descriptor.service_type.__name__,
                        instance
                    )
                
                logger.debug(f"Created scoped instance: {descriptor.service_type.__name__} [{scope_id}]")
            
            return descriptor._scoped_instances[scope_id]
    
    async def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Create a new instance using the factory."""
        factory = descriptor.factory
        
        if asyncio.iscoroutinefunction(factory):
            instance = await factory()
        else:
            instance = factory()
            if asyncio.iscoroutine(instance):
                instance = await instance
        
        logger.debug(f"Created transient: {descriptor.service_type.__name__}")
        return instance
    
    def get_service_health(self, service_type: Type[T]) -> Optional[ServiceHealth]:
        """Get health status for a specific service."""
        service_name = service_type.__name__
        return self._health_checker.get_service_health(service_name)
    
    def get_all_service_health(self) -> Dict[str, ServiceHealth]:
        """Get health status for all services."""
        return self._health_checker.get_all_health_status()
    
    def get_container_metrics(self) -> Dict[str, Any]:
        """Get container performance metrics."""
        uptime = time.time() - self._start_time
        
        return {
            "uptime_seconds": uptime,
            "registered_services": len(self._descriptors),
            "singleton_instances": len(self._singletons),
            "container_health": "healthy" if uptime > 0 else "starting"
        }
    
    def get_registered_services(self) -> Dict[ServiceKey, ServiceDescriptor]:
        """Get all registered services for introspection."""
        return dict(self._descriptors)
    
    async def close_all(self) -> None:
        """Close all singleton services that have close() methods."""
        # Close pre-registered singletons
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
        
        # Close descriptor-managed singletons
        for descriptor in self._descriptors.values():
            if descriptor._instance and hasattr(descriptor._instance, 'close'):
                try:
                    close_method = getattr(descriptor._instance, 'close')
                    if asyncio.iscoroutinefunction(close_method):
                        await close_method()
                    else:
                        close_method()
                    logger.debug(f"Closed {descriptor.service_type.__name__}")
                except Exception as e:
                    logger.warning(f"Error closing {descriptor.service_type.__name__}: {e}")


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
    
    # Register repositories
    from aurum.data.repositories import (
        CurveRepository,
        MetadataRepository,
        ScenarioRepository,
        PpaRepository,
        DroughtRepository
    )
    
    container.register(
        CurveRepository,
        lambda: CurveRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        MetadataRepository,
        lambda: MetadataRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        ScenarioRepository,
        lambda: ScenarioRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        PpaRepository,
        lambda: PpaRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    container.register(
        DroughtRepository,
        lambda: DroughtRepository(container.settings),
        lifetime=ServiceLifetime.SINGLETON
    )
    
    # Register cache if available
    try:
        from aurum.cache import UnifiedCacheManager
        
        def create_cache():
            """Create cache manager instance."""
            try:
                from aurum.cache.cache import get_unified_cache_manager
                return get_unified_cache_manager()
            except Exception:
                return None
        
        container.register(
            UnifiedCacheManager,
            create_cache,
            lifetime=ServiceLifetime.SINGLETON
        )
    except Exception:
        logger.debug("Cache manager not available")
    
    # Register core services with cache support
    from aurum.services.core import (
        CurveService,
        MetadataService,
        ScenarioService,
        PpaService,
        IsoService,
        DroughtService
    )
    
    async def create_curve_service():
        repo = await container.get(CurveRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return CurveService(repo, cache=cache, cache_ttl=300)
    
    async def create_metadata_service():
        repo = await container.get(MetadataRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return MetadataService(repo, cache=cache, cache_ttl=600)
    
    async def create_scenario_service():
        repo = await container.get(ScenarioRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return ScenarioService(repo, cache=cache, cache_ttl=300)
    
    async def create_ppa_service():
        repo = await container.get(PpaRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return PpaService(repo, cache=cache, cache_ttl=600)
    
    async def create_iso_service():
        repo = await container.get(MetadataRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return IsoService(repo, cache=cache, cache_ttl=300)
    
    async def create_drought_service():
        repo = await container.get(DroughtRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return DroughtService(repo, cache=cache, cache_ttl=1800)
    
    container.register(CurveService, create_curve_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(MetadataService, create_metadata_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(ScenarioService, create_scenario_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(PpaService, create_ppa_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(IsoService, create_iso_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(DroughtService, create_drought_service, lifetime=ServiceLifetime.SINGLETON)
    
    # Register external services with cache support
    from aurum.services.external import EiaService, RenewablesIngestionService
    
    async def create_eia_service():
        repo = await container.get(MetadataRepository)
        await repo.initialize()
        
        # Try to get cache
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        
        return EiaService(repo, cache=cache, cache_ttl=3600)
    
    container.register(EiaService, create_eia_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(RenewablesIngestionService, lambda: RenewablesIngestionService(), lifetime=ServiceLifetime.SINGLETON)
    
    # Register ML services with cache support
    from aurum.services.ml import (
        FeatureStoreService,
        ModelRegistryService,
        RiskEngineService,
        BiddingRLService,
        AutoReforecastService,
        CarbonRECService,
        ESGRiskService,
        AnomalyDetectionService
    )
    
    async def create_feature_store_service():
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        return FeatureStoreService(cache=cache, cache_ttl=3600)
    
    async def create_model_registry_service():
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        return ModelRegistryService(cache=cache, cache_ttl=1800)
    
    async def create_risk_engine_service():
        cache = None
        try:
            cache = await container.get(UnifiedCacheManager)
        except Exception:
            pass
        return RiskEngineService(cache=cache, cache_ttl=900)
    
    container.register(FeatureStoreService, create_feature_store_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(ModelRegistryService, create_model_registry_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(RiskEngineService, create_risk_engine_service, lifetime=ServiceLifetime.SINGLETON)
    container.register(BiddingRLService, lambda: BiddingRLService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(AutoReforecastService, lambda: AutoReforecastService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(CarbonRECService, lambda: CarbonRECService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(ESGRiskService, lambda: ESGRiskService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(AnomalyDetectionService, lambda: AnomalyDetectionService(), lifetime=ServiceLifetime.SINGLETON)
    
    # Register platform services
    from aurum.services.platform import (
        GovernanceService,
        PerformanceMonitoringService,
        RegulatoryTrackerService,
        RiskComplianceService
    )
    
    container.register(GovernanceService, lambda: GovernanceService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(PerformanceMonitoringService, lambda: PerformanceMonitoringService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(RegulatoryTrackerService, lambda: RegulatoryTrackerService(), lifetime=ServiceLifetime.SINGLETON)
    container.register(RiskComplianceService, lambda: RiskComplianceService(), lifetime=ServiceLifetime.SINGLETON)
    
    logger.info("All 20 services registered in DI container")


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
    "ServiceDescriptor",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerState",
    "ServiceHealth",
    "ServiceHealthChecker",
    "get_container",
    "reset_container",
    "get_service",
]

