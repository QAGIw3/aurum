"""Circuit breaker implementation for search service resilience.

Provides circuit breaker pattern to prevent cascading failures
and graceful degradation when external services are unavailable.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Optional, Dict
from dataclasses import dataclass
from enum import Enum

from aurum.core.settings import get_settings
from aurum.core import AurumSettings


logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, requests rejected
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: float = 60.0  # Seconds before attempting recovery
    success_threshold: int = 3  # Successful calls needed to close circuit
    timeout: float = 30.0  # Request timeout in seconds
    expected_exception: tuple = (Exception,)  # Exceptions that count as failures


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    state: CircuitBreakerState
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    total_requests: int = 0
    total_failures: int = 0
    total_successes: int = 0


class CircuitBreaker:
    """Circuit breaker implementation for resilient service calls."""

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        settings: Optional[AurumSettings] = None
    ):
        """Initialize circuit breaker.

        Args:
            name: Name of the circuit breaker
            config: Circuit breaker configuration
            settings: Application settings
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.settings = settings or get_settings()
        self.state = CircuitBreakerState.CLOSED
        self.stats = CircuitBreakerStats(state=self.state)
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpen: If circuit is open
            Exception: If function execution fails
        """
        async with self._lock:
            # Check if circuit should be opened or closed
            await self._update_state()

            if self.state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpen(f"Circuit breaker '{self.name}' is OPEN")

            self.stats.total_requests += 1

        try:
            # Execute function with timeout
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout
            )

            # Record success
            async with self._lock:
                await self._record_success()

            return result

        except Exception as e:
            # Record failure
            async with self._lock:
                await self._record_failure()

            # Check if this is an expected failure
            if isinstance(e, self.config.expected_exception):
                logger.warning(f"Circuit breaker '{self.name}' recorded failure: {e}")

            raise

    async def _update_state(self):
        """Update circuit breaker state based on time and statistics."""
        now = time.time()

        if self.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if (self.stats.last_failure_time and
                now - self.stats.last_failure_time >= self.config.recovery_timeout):
                self.state = CircuitBreakerState.HALF_OPEN
                self.stats.state = self.state
                logger.info(f"Circuit breaker '{self.name}' transitioning to HALF_OPEN")

        elif self.state == CircuitBreakerState.HALF_OPEN:
            # Stay in half-open until we get enough successes or failures
            if self.stats.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.stats.state = self.state
                self.stats.failure_count = 0
                self.stats.success_count = 0
                logger.info(f"Circuit breaker '{self.name}' transitioning to CLOSED")

            elif self.stats.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.stats.state = self.state
                logger.warning(f"Circuit breaker '{self.name}' transitioning to OPEN")

    async def _record_success(self):
        """Record a successful call."""
        self.stats.success_count += 1
        self.stats.total_successes += 1
        self.stats.last_success_time = time.time()

        if self.state == CircuitBreakerState.HALF_OPEN:
            await self._update_state()

    async def _record_failure(self):
        """Record a failed call."""
        self.stats.failure_count += 1
        self.stats.total_failures += 1
        self.stats.last_failure_time = time.time()

        if (self.state == CircuitBreakerState.CLOSED and
            self.stats.failure_count >= self.config.failure_threshold):
            self.state = CircuitBreakerState.OPEN
            self.stats.state = self.state
            logger.warning(f"Circuit breaker '{self.name}' transitioning to OPEN after {self.stats.failure_count} failures")

        elif self.state == CircuitBreakerState.HALF_OPEN:
            await self._update_state()

    def get_stats(self) -> CircuitBreakerStats:
        """Get current circuit breaker statistics."""
        return self.stats

    def reset(self):
        """Reset circuit breaker to initial state."""
        self.state = CircuitBreakerState.CLOSED
        self.stats = CircuitBreakerStats(state=self.state)
        logger.info(f"Circuit breaker '{self.name}' reset")


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize circuit breaker manager.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    def get_circuit_breaker(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Get or create circuit breaker for a service.

        Args:
            name: Circuit breaker name
            config: Circuit breaker configuration

        Returns:
            Circuit breaker instance
        """
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(name, config, self.settings)

        return self.circuit_breakers[name]

    async def call_with_circuit_breaker(
        self,
        service_name: str,
        func: Callable,
        *args,
        config: Optional[CircuitBreakerConfig] = None,
        **kwargs
    ) -> Any:
        """Execute function with circuit breaker protection.

        Args:
            service_name: Name of the service being called
            func: Function to execute
            *args: Function arguments
            config: Circuit breaker configuration
            **kwargs: Function keyword arguments

        Returns:
            Function result
        """
        circuit_breaker = self.get_circuit_breaker(service_name, config)
        return await circuit_breaker.call(func, *args, **kwargs)

    def get_all_stats(self) -> Dict[str, CircuitBreakerStats]:
        """Get statistics for all circuit breakers.

        Returns:
            Dictionary of circuit breaker name -> stats
        """
        return {
            name: cb.get_stats()
            for name, cb in self.circuit_breakers.items()
        }

    def reset_all(self):
        """Reset all circuit breakers."""
        for cb in self.circuit_breakers.values():
            cb.reset()


class SearchResilienceManager:
    """Manages resilience for search operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize search resilience manager.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.circuit_breaker_manager = CircuitBreakerManager(settings)

        # Create circuit breakers for different search components
        self.elasticsearch_cb = self.circuit_breaker_manager.get_circuit_breaker(
            "elasticsearch",
            CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30.0,
                timeout=10.0
            )
        )

        self.semantic_search_cb = self.circuit_breaker_manager.get_circuit_breaker(
            "semantic_search",
            CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=60.0,
                timeout=15.0
            )
        )

        self.analytics_cb = self.circuit_breaker_manager.get_circuit_breaker(
            "analytics",
            CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=30.0,
                timeout=5.0
            )
        )

    async def execute_elasticsearch_operation(
        self,
        operation: Callable,
        *args,
        fallback: Optional[Callable] = None,
        **kwargs
    ) -> Any:
        """Execute Elasticsearch operation with circuit breaker protection.

        Args:
            operation: Elasticsearch operation function
            *args: Operation arguments
            fallback: Optional fallback function if operation fails
            **kwargs: Operation keyword arguments

        Returns:
            Operation result or fallback result
        """
        try:
            return await self.circuit_breaker_manager.call_with_circuit_breaker(
                "elasticsearch",
                operation,
                *args,
                **kwargs
            )
        except CircuitBreakerOpen:
            logger.warning("Elasticsearch circuit breaker is open, using fallback")
            if fallback:
                try:
                    return await fallback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Fallback also failed: {e}")
                    raise
            else:
                raise
        except Exception as e:
            logger.error(f"Elasticsearch operation failed: {e}")
            raise

    async def execute_semantic_search_operation(
        self,
        operation: Callable,
        *args,
        fallback: Optional[Callable] = None,
        **kwargs
    ) -> Any:
        """Execute semantic search operation with circuit breaker protection.

        Args:
            operation: Semantic search operation function
            *args: Operation arguments
            fallback: Optional fallback function if operation fails
            **kwargs: Operation keyword arguments

        Returns:
            Operation result or fallback result
        """
        try:
            return await self.circuit_breaker_manager.call_with_circuit_breaker(
                "semantic_search",
                operation,
                *args,
                **kwargs
            )
        except CircuitBreakerOpen:
            logger.warning("Semantic search circuit breaker is open, using fallback")
            if fallback:
                try:
                    return await fallback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Fallback also failed: {e}")
                    raise
            else:
                raise
        except Exception as e:
            logger.error(f"Semantic search operation failed: {e}")
            raise

    async def execute_analytics_operation(
        self,
        operation: Callable,
        *args,
        **kwargs
    ) -> Optional[Any]:
        """Execute analytics operation with circuit breaker protection.

        Args:
            operation: Analytics operation function
            *args: Operation arguments
            **kwargs: Operation keyword arguments

        Returns:
            Operation result or None if failed
        """
        try:
            return await self.circuit_breaker_manager.call_with_circuit_breaker(
                "analytics",
                operation,
                *args,
                **kwargs
            )
        except (CircuitBreakerOpen, Exception) as e:
            logger.warning(f"Analytics operation failed: {e}")
            return None

    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all circuit breakers.

        Returns:
            Dictionary with circuit breaker health status
        """
        return {
            "elasticsearch": {
                "state": self.elasticsearch_cb.state.value,
                "stats": self.elasticsearch_cb.get_stats().__dict__
            },
            "semantic_search": {
                "state": self.semantic_search_cb.state.value,
                "stats": self.semantic_search_cb.get_stats().__dict__
            },
            "analytics": {
                "state": self.analytics_cb.state.value,
                "stats": self.analytics_cb.get_stats().__dict__
            }
        }


# Global resilience manager
_resilience_manager: Optional[SearchResilienceManager] = None


def get_search_resilience_manager(settings: Optional[AurumSettings] = None) -> SearchResilienceManager:
    """Get or create global search resilience manager.

    Args:
        settings: Application settings

    Returns:
        Search resilience manager instance
    """
    global _resilience_manager
    if _resilience_manager is None:
        _resilience_manager = SearchResilienceManager(settings)
    return _resilience_manager


def execute_with_circuit_breaker(
    service_name: str,
    operation: Callable,
    *args,
    fallback: Optional[Callable] = None,
    settings: Optional[AurumSettings] = None,
    **kwargs
) -> Any:
    """Execute operation with circuit breaker protection.

    Args:
        service_name: Name of the service
        operation: Operation to execute
        *args: Operation arguments
        fallback: Optional fallback function
        settings: Application settings
        **kwargs: Operation keyword arguments

    Returns:
        Operation result
    """
    manager = get_search_resilience_manager(settings)

    if service_name == "elasticsearch":
        return manager.execute_elasticsearch_operation(operation, *args, fallback=fallback, **kwargs)
    elif service_name == "semantic_search":
        return manager.execute_semantic_search_operation(operation, *args, fallback=fallback, **kwargs)
    elif service_name == "analytics":
        return manager.execute_analytics_operation(operation, *args, **kwargs)
    else:
        # Direct execution without circuit breaker
        return operation(*args, **kwargs)
