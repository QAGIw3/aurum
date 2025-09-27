"""Centralized application lifespan and dependency injection manager.

This module provides a unified system for:
- Managing application startup and shutdown lifecycle
- Dependency injection with proper scoping
- Resource management (database connections, cache, etc.)
- Health checks and monitoring
- Test overrides for dependency injection
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import logging
import signal
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypeVar, Callable, Awaitable, Union
from enum import Enum
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends
from pydantic import BaseModel

from ..core.settings import AurumSettings
from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from .cache.consolidated_manager import get_unified_cache_manager
from .rate_limiting.consolidated_policy_engine import get_unified_rate_limiter

T = TypeVar('T')


class LifecycleState(str, Enum):
    """Application lifecycle states."""
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class HealthStatus(str, Enum):
    """Health check statuses."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceHealth:
    """Health information for a service."""
    name: str
    status: HealthStatus
    last_check: datetime
    response_time_ms: float
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ApplicationHealth:
    """Overall application health status."""
    status: HealthStatus
    services: Dict[str, ServiceHealth] = field(default_factory=dict)
    last_check: datetime = field(default_factory=datetime.utcnow)
    uptime_seconds: float = 0.0

    @property
    def is_healthy(self) -> bool:
        """Check if application is healthy."""
        return self.status == HealthStatus.HEALTHY

    @property
    def degraded_services(self) -> List[str]:
        """Get list of degraded services."""
        return [name for name, health in self.services.items()
                if health.status == HealthStatus.DEGRADED]


class LifecycleHook(ABC):
    """Base class for lifecycle hooks."""

    def __init__(self, name: str, priority: int = 100):
        """Initialize lifecycle hook.

        Args:
            name: Hook name for identification
            priority: Execution priority (lower = higher priority)
        """
        self.name = name
        self.priority = priority
        self.logger = get_logger(f"{__name__}.{name}")

    @abstractmethod
    async def startup(self) -> None:
        """Execute startup logic."""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Execute shutdown logic."""
        pass

    async def health_check(self) -> ServiceHealth:
        """Perform health check.

        Returns:
            Service health information
        """
        start_time = datetime.utcnow()
        try:
            # Default implementation - can be overridden
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.HEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time
            )
        except Exception as e:
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message=str(e)
            )


class DatabaseLifecycleHook(LifecycleHook):
    """Database connection lifecycle hook."""

    def __init__(self, settings: AurumSettings):
        """Initialize database hook.

        Args:
            settings: Application settings
        """
        super().__init__("database", priority=10)
        self.settings = settings
        self._connections: Dict[str, Any] = {}

    async def startup(self) -> None:
        """Establish database connections."""
        self.logger.info("Initializing database connections")

        try:
            # Initialize Trino connections
            if hasattr(self.settings, 'trino') and self.settings.trino:
                try:
                    from .database.trino_client import HybridTrinoClientManager
                    trino_manager = HybridTrinoClientManager.get_instance()
                    await trino_manager.initialize()
                    self._connections["trino"] = trino_manager
                    self.logger.info("Trino connections initialized")
                except Exception as e:
                    self.logger.warning("Failed to initialize Trino", error=str(e))

            # Initialize Redis connections
            if hasattr(self.settings, 'redis') and self.settings.redis:
                try:
                    import redis.asyncio as redis
                    redis_client = redis.from_url(self.settings.redis.url)
                    await redis_client.ping()
                    self._connections["redis"] = redis_client
                    self.logger.info("Redis connections initialized")
                except Exception as e:
                    self.logger.warning("Failed to initialize Redis", error=str(e))

        except Exception as e:
            self.logger.error("Database initialization failed", error=str(e))
            raise

    async def shutdown(self) -> None:
        """Close database connections."""
        self.logger.info("Closing database connections")

        for name, connection in self._connections.items():
            try:
                if hasattr(connection, 'close'):
                    if asyncio.iscoroutinefunction(connection.close):
                        await connection.close()
                    else:
                        connection.close()
                self.logger.debug("Closed database connection", connection=name)
            except Exception as e:
                self.logger.warning("Failed to close database connection",
                                  connection=name, error=str(e))

        self._connections.clear()

    async def health_check(self) -> ServiceHealth:
        """Check database health."""
        start_time = datetime.utcnow()
        errors = []

        # Check Trino health
        if "trino" in self._connections:
            try:
                trino_manager = self._connections["trino"]
                # Simple health check - would need actual implementation
                pass
            except Exception as e:
                errors.append(f"Trino: {str(e)}")

        # Check Redis health
        if "redis" in self._connections:
            try:
                redis_client = self._connections["redis"]
                await redis_client.ping()
            except Exception as e:
                errors.append(f"Redis: {str(e)}")

        response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        if errors:
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message="; ".join(errors)
            )

        return ServiceHealth(
            name=self.name,
            status=HealthStatus.HEALTHY,
            last_check=datetime.utcnow(),
            response_time_ms=response_time
        )


class CacheLifecycleHook(LifecycleHook):
    """Cache system lifecycle hook."""

    def __init__(self, settings: AurumSettings):
        """Initialize cache hook.

        Args:
            settings: Application settings
        """
        super().__init__("cache", priority=20)
        self.settings = settings

    async def startup(self) -> None:
        """Initialize cache system."""
        self.logger.info("Initializing cache system")

        try:
            # Initialize unified cache manager
            cache_manager = get_unified_cache_manager()
            # Cache manager initializes itself automatically
            self.logger.info("Cache system initialized")
        except Exception as e:
            self.logger.error("Cache initialization failed", error=str(e))
            raise

    async def shutdown(self) -> None:
        """Shutdown cache system."""
        self.logger.info("Shutting down cache system")

        try:
            cache_manager = get_unified_cache_manager()
            await cache_manager.shutdown()
            self.logger.info("Cache system shutdown complete")
        except Exception as e:
            self.logger.warning("Cache shutdown failed", error=str(e))

    async def health_check(self) -> ServiceHealth:
        """Check cache health."""
        start_time = datetime.utcnow()

        try:
            cache_manager = get_unified_cache_manager()
            health = await cache_manager.get_health()

            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            status = HealthStatus.HEALTHY
            if not health.is_healthy:
                status = HealthStatus.UNHEALTHY
            elif health.error_rate > 0.05:  # 5% error rate threshold
                status = HealthStatus.DEGRADED

            return ServiceHealth(
                name=self.name,
                status=status,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                metadata={
                    "total_entries": health.total_entries,
                    "memory_usage_mb": health.memory_usage_mb,
                    "error_rate": health.error_rate
                }
            )
        except Exception as e:
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message=str(e)
            )


class RateLimiterLifecycleHook(LifecycleHook):
    """Rate limiter lifecycle hook."""

    def __init__(self, settings: AurumSettings):
        """Initialize rate limiter hook.

        Args:
            settings: Application settings
        """
        super().__init__("rate_limiter", priority=15)
        self.settings = settings

    async def startup(self) -> None:
        """Initialize rate limiter."""
        self.logger.info("Initializing rate limiter")

        try:
            # Get Redis URL for rate limiter
            redis_url = None
            if hasattr(self.settings, 'redis') and self.settings.redis:
                redis_url = self.settings.redis.url

            rate_limiter = get_unified_rate_limiter(redis_url)
            self.logger.info("Rate limiter initialized")
        except Exception as e:
            self.logger.error("Rate limiter initialization failed", error=str(e))
            raise

    async def shutdown(self) -> None:
        """Shutdown rate limiter."""
        self.logger.info("Shutting down rate limiter")

        try:
            rate_limiter = get_unified_rate_limiter()
            await rate_limiter.shutdown()
            self.logger.info("Rate limiter shutdown complete")
        except Exception as e:
            self.logger.warning("Rate limiter shutdown failed", error=str(e))

    async def health_check(self) -> ServiceHealth:
        """Check rate limiter health."""
        start_time = datetime.utcnow()

        try:
            rate_limiter = get_unified_rate_limiter()
            stats = await rate_limiter.get_stats()

            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            # Determine status based on error rate and success rate
            status = HealthStatus.HEALTHY
            if stats.error_count > 100:  # Too many errors
                status = HealthStatus.UNHEALTHY
            elif stats.success_rate < 0.95:  # Low success rate
                status = HealthStatus.DEGRADED

            return ServiceHealth(
                name=self.name,
                status=status,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                metadata={
                    "total_requests": stats.total_requests,
                    "success_rate": stats.success_rate,
                    "error_count": stats.error_count,
                    "avg_response_time": stats.avg_response_time
                }
            )
        except Exception as e:
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message=str(e)
            )


class MetricsLifecycleHook(LifecycleHook):
    """Metrics collection lifecycle hook."""

    def __init__(self, settings: AurumSettings):
        """Initialize metrics hook.

        Args:
            settings: Application settings
        """
        super().__init__("metrics", priority=5)
        self.settings = settings

    async def startup(self) -> None:
        """Initialize metrics collection."""
        self.logger.info("Initializing metrics collection")

        try:
            # Metrics client initializes automatically
            # This is mainly for any setup that might be needed
            self.logger.info("Metrics collection initialized")
        except Exception as e:
            self.logger.error("Metrics initialization failed", error=str(e))
            raise

    async def shutdown(self) -> None:
        """Shutdown metrics collection."""
        self.logger.info("Shutting down metrics collection")
        # Metrics client cleanup is handled automatically

    async def health_check(self) -> ServiceHealth:
        """Check metrics health."""
        start_time = datetime.utcnow()

        try:
            metrics_client = get_metrics_client()
            # Simple health check - would need actual implementation
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            return ServiceHealth(
                name=self.name,
                status=HealthStatus.HEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time
            )
        except Exception as e:
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            return ServiceHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message=str(e)
            )


class LifespanManager:
    """Centralized application lifespan manager."""

    def __init__(self, settings: AurumSettings):
        """Initialize lifespan manager.

        Args:
            settings: Application settings
        """
        self.settings = settings
        self.state = LifecycleState.INITIALIZING
        self.start_time: Optional[datetime] = None
        self.hooks: List[LifecycleHook] = []
        self.logger = get_logger(__name__)

        # Dependency injection container
        self._container: Dict[str, Any] = {}
        self._test_overrides: Dict[str, Any] = {}

        # Initialize default hooks
        self._initialize_default_hooks()

    def _initialize_default_hooks(self) -> None:
        """Initialize default lifecycle hooks."""
        self.add_hook(DatabaseLifecycleHook(self.settings))
        self.add_hook(CacheLifecycleHook(self.settings))
        self.add_hook(RateLimiterLifecycleHook(self.settings))
        self.add_hook(MetricsLifecycleHook(self.settings))

    def add_hook(self, hook: LifecycleHook) -> None:
        """Add a lifecycle hook.

        Args:
            hook: Lifecycle hook to add
        """
        self.hooks.append(hook)
        # Sort by priority
        self.hooks.sort(key=lambda h: h.priority)

        self.logger.debug("Added lifecycle hook",
                         hook_name=hook.name,
                         priority=hook.priority)

    def override_dependency(self, dependency_name: str, instance: Any) -> None:
        """Override a dependency for testing.

        Args:
            dependency_name: Name of dependency to override
            instance: Instance to use instead
        """
        self._test_overrides[dependency_name] = instance
        self.logger.debug("Added dependency override",
                         dependency=dependency_name,
                         override_type=type(instance).__name__)

    def get_dependency(self, dependency_name: str) -> Any:
        """Get dependency instance.

        Args:
            dependency_name: Name of dependency

        Returns:
            Dependency instance
        """
        # Check test overrides first
        if dependency_name in self._test_overrides:
            return self._test_overrides[dependency_name]

        # Check container
        if dependency_name in self._container:
            return self._container[dependency_name]

        # Create dependency
        instance = self._create_dependency(dependency_name)
        if instance is not None:
            self._container[dependency_name] = instance

        return instance

    def _create_dependency(self, dependency_name: str) -> Any:
        """Create dependency instance.

        Args:
            dependency_name: Name of dependency to create

        Returns:
            Created dependency instance
        """
        # Map dependency names to creation functions
        dependency_map = {
            "cache_manager": lambda: get_unified_cache_manager(),
            "rate_limiter": lambda: get_unified_rate_limiter(),
            "settings": lambda: self.settings,
        }

        creator = dependency_map.get(dependency_name)
        if creator:
            try:
                return creator()
            except Exception as e:
                self.logger.error("Failed to create dependency",
                                dependency=dependency_name, error=str(e))

        return None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """FastAPI lifespan context manager.

        Args:
            app: FastAPI application instance

        Yields:
            None
        """
        self.state = LifecycleState.STARTING
        self.start_time = datetime.utcnow()

        try:
            # Store lifespan manager in app state
            app.state.lifespan_manager = self

            # Execute startup hooks
            self.logger.info("Starting application lifecycle")

            for hook in self.hooks:
                try:
                    await hook.startup()
                except Exception as e:
                    self.logger.error("Hook startup failed",
                                    hook_name=hook.name, error=str(e))
                    self.state = LifecycleState.ERROR
                    raise

            self.state = LifecycleState.RUNNING
            self.logger.info("Application startup complete")

            yield

        except Exception as e:
            self.state = LifecycleState.ERROR
            self.logger.error("Application startup failed", error=str(e))
            raise

        finally:
            # Execute shutdown hooks
            self.state = LifecycleState.STOPPING
            self.logger.info("Shutting down application")

            for hook in reversed(self.hooks):
                try:
                    await hook.shutdown()
                except Exception as e:
                    self.logger.warning("Hook shutdown failed",
                                      hook_name=hook.name, error=str(e))

            self.state = LifecycleState.STOPPED
            self.logger.info("Application shutdown complete")

    async def health_check(self) -> ApplicationHealth:
        """Perform comprehensive health check.

        Returns:
            Application health status
        """
        start_time = datetime.utcnow()

        service_healths = {}
        healthy_count = 0
        degraded_count = 0
        unhealthy_count = 0

        # Check all services
        for hook in self.hooks:
            try:
                health = await hook.health_check()
                service_healths[hook.name] = health

                if health.status == HealthStatus.HEALTHY:
                    healthy_count += 1
                elif health.status == HealthStatus.DEGRADED:
                    degraded_count += 1
                elif health.status == HealthStatus.UNHEALTHY:
                    unhealthy_count += 1

            except Exception as e:
                # Create failed health check
                service_healths[hook.name] = ServiceHealth(
                    name=hook.name,
                    status=HealthStatus.UNHEALTHY,
                    last_check=datetime.utcnow(),
                    response_time_ms=0.0,
                    error_message=str(e)
                )
                unhealthy_count += 1

        # Determine overall status
        total_services = len(self.hooks)
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY

        # Calculate uptime
        uptime_seconds = 0.0
        if self.start_time:
            uptime_seconds = (datetime.utcnow() - self.start_time).total_seconds()

        return ApplicationHealth(
            status=overall_status,
            services=service_healths,
            last_check=datetime.utcnow(),
            uptime_seconds=uptime_seconds
        )

    async def get_stats(self) -> Dict[str, Any]:
        """Get application statistics.

        Returns:
            Application statistics
        """
        uptime_seconds = 0.0
        if self.start_time:
            uptime_seconds = (datetime.utcnow() - self.start_time).total_seconds()

        return {
            "state": self.state.value,
            "uptime_seconds": uptime_seconds,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "hook_count": len(self.hooks),
            "dependency_overrides": len(self._test_overrides)
        }


# Global lifespan manager instance
_lifespan_manager: Optional[LifespanManager] = None


def get_lifespan_manager(settings: Optional[AurumSettings] = None) -> LifespanManager:
    """Get the global lifespan manager instance.

    Args:
        settings: Optional application settings

    Returns:
        Lifespan manager instance
    """
    global _lifespan_manager

    if _lifespan_manager is None:
        if settings is None:
            from ..core.settings import get_settings
            settings = get_settings()

        _lifespan_manager = LifespanManager(settings)

    return _lifespan_manager


def create_app_with_lifespan(
    app: FastAPI,
    settings: Optional[AurumSettings] = None,
    custom_hooks: Optional[List[LifecycleHook]] = None
) -> FastAPI:
    """Create FastAPI app with centralized lifespan management.

    Args:
        app: FastAPI application instance
        settings: Optional application settings
        custom_hooks: Optional additional lifecycle hooks

    Returns:
        FastAPI app with lifespan management
    """
    lifespan_manager = get_lifespan_manager(settings)

    if custom_hooks:
        for hook in custom_hooks:
            lifespan_manager.add_hook(hook)

    # Set lifespan context
    app.router.lifespan_context = lifespan_manager.lifespan

    return app


# Dependency injection functions for FastAPI
def get_settings_dependency(request: Request) -> AurumSettings:
    """FastAPI dependency for application settings.

    Args:
        request: FastAPI request

    Returns:
        Application settings
    """
    lifespan_manager = getattr(request.app.state, "lifespan_manager", None)
    if lifespan_manager:
        return lifespan_manager.settings

    # Fallback to core settings
    from ..core.settings import get_settings
    return get_settings()


def get_cache_manager_dependency(request: Request):
    """FastAPI dependency for cache manager.

    Args:
        request: FastAPI request

    Returns:
        Cache manager instance
    """
    lifespan_manager = getattr(request.app.state, "lifespan_manager", None)
    if lifespan_manager:
        return lifespan_manager.get_dependency("cache_manager")

    # Fallback to global instance
    return get_unified_cache_manager()


def get_rate_limiter_dependency(request: Request):
    """FastAPI dependency for rate limiter.

    Args:
        request: FastAPI request

    Returns:
        Rate limiter instance
    """
    lifespan_manager = getattr(request.app.state, "lifespan_manager", None)
    if lifespan_manager:
        return lifespan_manager.get_dependency("rate_limiter")

    # Fallback to global instance
    return get_unified_rate_limiter()


# Test utilities
class TestDependencyInjector:
    """Dependency injector for testing."""

    def __init__(self, lifespan_manager: LifespanManager):
        """Initialize test injector.

        Args:
            lifespan_manager: Lifespan manager to inject into
        """
        self.lifespan_manager = lifespan_manager
        self._original_dependencies: Dict[str, Any] = {}

    def override_dependency(self, name: str, instance: Any) -> None:
        """Override a dependency for testing.

        Args:
            name: Dependency name
            instance: Instance to use
        """
        # Store original if not already stored
        if name not in self._original_dependencies:
            self._original_dependencies[name] = self.lifespan_manager._container.get(name)

        self.lifespan_manager.override_dependency(name, instance)

    def restore_dependencies(self) -> None:
        """Restore original dependencies."""
        for name, original_instance in self._original_dependencies.items():
            if original_instance is not None:
                self.lifespan_manager._container[name] = original_instance
            else:
                self.lifespan_manager._container.pop(name, None)

        self.lifespan_manager._test_overrides.clear()
        self._original_dependencies.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.restore_dependencies()


def inject_test_dependencies(
    settings: Optional[AurumSettings] = None
) -> TestDependencyInjector:
    """Create test dependency injector.

    Args:
        settings: Optional settings for test environment

    Returns:
        Test dependency injector
    """
    if settings is None:
        from ..core.settings import get_settings
        settings = get_settings()

    lifespan_manager = LifespanManager(settings)
    return TestDependencyInjector(lifespan_manager)
