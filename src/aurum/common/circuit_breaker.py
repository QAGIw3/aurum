"""Thread-safe circuit breaker utility with comprehensive monitoring and alerting.

Designed to be minimal and reusable across providers and clients while
preserving simple string state semantics for compatibility in tests.
Enhanced with Prometheus metrics and actionable alerts.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Callable

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    success_threshold: int = 3  # Successes needed in HALF_OPEN to close
    alert_callback: Optional[Callable] = None  # Called when circuit opens/closes
    name: str = "default"  # Circuit breaker name for metrics


@dataclass
class CircuitBreaker:
    """Enhanced circuit breaker with monitoring and alerting.

    - CLOSED: operations allowed, failures increment counter
    - OPEN: operations rejected until timeout elapses
    - HALF_OPEN: allow limited trials; success -> CLOSED, failure -> OPEN
    """

    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    success_threshold: int = 3  # Successes needed in HALF_OPEN to close
    alert_callback: Optional[Callable] = None
    name: str = "default"

    _failures: int = field(default=0, init=False)
    _successes: int = field(default=0, init=False)
    _last_failure_time: Optional[float] = field(default=None, init=False)
    _last_success_time: Optional[float] = field(default=None, init=False)
    _state: str = field(default="CLOSED", init=False)
    _state_change_time: Optional[float] = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    # Metrics and monitoring
    _metrics = None
    _logger = None
    _alert_task: Optional[asyncio.Task] = field(default=None, init=False)

    def __post_init__(self):
        """Initialize metrics and logger."""
        self._metrics = get_metrics_client()
        self._logger = get_logger(__name__)
        self._update_metrics()

    def _update_metrics(self):
        """Update Prometheus metrics for this circuit breaker."""
        state_value = 1 if self._state == "CLOSED" else 0
        self._metrics.gauge(f"circuit_breaker_state_{self.name}", state_value)
        self._metrics.gauge(f"circuit_breaker_failures_{self.name}", self._failures)
        self._metrics.gauge(f"circuit_breaker_successes_{self.name}", self._successes)

        # Histogram for state duration
        if self._state_change_time:
            duration = time.time() - self._state_change_time
            self._metrics.histogram(f"circuit_breaker_state_duration_{self.name}", duration)

    async def _trigger_alert(self, old_state: str, new_state: str, reason: str):
        """Trigger an alert for state changes."""
        if self.alert_callback:
            try:
                await self.alert_callback({
                    "type": "circuit_breaker_alert",
                    "circuit_breaker": self.name,
                    "old_state": old_state,
                    "new_state": new_state,
                    "reason": reason,
                    "failures": self._failures,
                    "successes": self._successes,
                    "timestamp": int(time.time() * 1000)
                })
            except Exception as e:
                self._logger.error(f"Failed to send circuit breaker alert: {e}")

        self._logger.warning(
            f"Circuit breaker {self.name} state change: {old_state} -> {new_state} ({reason})"
        )

    def is_open(self) -> bool:
        """Return True if the breaker is OPEN and requests should be short-circuited."""
        with self._lock:
            if self._state == "OPEN":
                if (self._last_failure_time is not None and
                    time.time() - self._last_failure_time > self.recovery_timeout):
                    # Transition to HALF_OPEN after timeout
                    old_state = self._state
                    self._state = "HALF_OPEN"
                    self._state_change_time = time.time()
                    self._update_metrics()

                    # Schedule alert
                    if old_state != self._state:
                        asyncio.create_task(
                            self._trigger_alert(old_state, self._state, "Recovery timeout elapsed")
                        )
                    return False
                return True
            return False

    def record_success(self) -> None:
        """Record a successful call and transition state accordingly."""
        with self._lock:
            old_state = self._state
            self._successes += 1
            self._last_success_time = time.time()

            if self._state == "HALF_OPEN":
                if self._successes >= self.success_threshold:
                    # Transition to CLOSED after sufficient successes
                    self._state = "CLOSED"
                    self._failures = 0
                    self._successes = 0
                    self._state_change_time = time.time()
                    self._update_metrics()

                    # Schedule alert
                    if old_state != self._state:
                        asyncio.create_task(
                            self._trigger_alert(old_state, self._state, f"Success threshold reached: {self._successes}")
                        )
            elif self._state == "CLOSED":
                # Reset failure count on success in CLOSED state
                if self._failures > 0:
                    self._failures = max(0, self._failures - 1)

    def record_failure(self) -> None:
        """Record a failed call and transition state if threshold reached."""
        with self._lock:
            old_state = self._state
            self._failures += 1
            self._last_failure_time = time.time()

            if self._state == "CLOSED" and self._failures >= self.failure_threshold:
                # Transition to OPEN
                self._state = "OPEN"
                self._state_change_time = time.time()
                self._update_metrics()

                # Schedule alert
                if old_state != self._state:
                    asyncio.create_task(
                        self._trigger_alert(old_state, self._state, f"Failure threshold reached: {self._failures}")
                    )
            elif self._state == "HALF_OPEN":
                # Transition back to OPEN on failure in HALF_OPEN
                self._state = "OPEN"
                self._successes = 0
                self._state_change_time = time.time()
                self._update_metrics()

                # Schedule alert
                if old_state != self._state:
                    asyncio.create_task(
                        self._trigger_alert(old_state, self._state, "Failure in half-open state")
                    )

    def get_state(self) -> str:
        """Get current circuit breaker state."""
        with self._lock:
            return self._state

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state,
                "failures": self._failures,
                "successes": self._successes,
                "failure_threshold": self.failure_threshold,
                "success_threshold": self.success_threshold,
                "recovery_timeout": self.recovery_timeout,
                "last_failure_time": self._last_failure_time,
                "last_success_time": self._last_success_time,
                "state_change_time": self._state_change_time,
                "uptime_seconds": time.time() - (self._state_change_time or time.time())
            }

    async def force_open(self) -> None:
        """Manually force the circuit breaker to open."""
        with self._lock:
            old_state = self._state
            if self._state != "OPEN":
                self._state = "OPEN"
                self._state_change_time = time.time()
                self._update_metrics()

                # Schedule alert
                asyncio.create_task(
                    self._trigger_alert(old_state, self._state, "Manual force open")
                )

    async def force_close(self) -> None:
        """Manually force the circuit breaker to close."""
        with self._lock:
            old_state = self._state
            if self._state != "CLOSED":
                self._state = "CLOSED"
                self._failures = 0
                self._successes = 0
                self._state_change_time = time.time()
                self._update_metrics()

                # Schedule alert
                asyncio.create_task(
                    self._trigger_alert(old_state, self._state, "Manual force close")
                )


class CircuitBreakerRegistry:
    """Registry for managing circuit breakers with centralized monitoring."""

    _instance: Optional['CircuitBreakerRegistry'] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self._breakers: Dict[str, CircuitBreaker] = {}
        self._metrics = get_metrics_client()
        self._logger = get_logger(__name__)
        self._initialized = True

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker with the given name."""
        if name not in self._breakers:
            if config is None:
                config = CircuitBreakerConfig(name=name)

            self._breakers[name] = CircuitBreaker(
                failure_threshold=config.failure_threshold,
                recovery_timeout=config.recovery_timeout,
                success_threshold=config.success_threshold,
                alert_callback=config.alert_callback,
                name=name
            )

        return self._breakers[name]

    def get_all_breakers(self) -> Dict[str, CircuitBreaker]:
        """Get all registered circuit breakers."""
        return self._breakers.copy()

    def get_breaker_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {
            name: breaker.get_stats()
            for name, breaker in self._breakers.items()
        }

    def get_open_breakers(self) -> List[str]:
        """Get names of currently open circuit breakers."""
        return [
            name for name, breaker in self._breakers.items()
            if breaker.get_state() == "OPEN"
        ]

    def get_breaker_by_name(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name."""
        return self._breakers.get(name)


# Global registry instance
_registry = CircuitBreakerRegistry()


def get_circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None
) -> CircuitBreaker:
    """Get or create a circuit breaker with the given name."""
    return _registry.get_or_create(name, config)


def get_all_circuit_breakers() -> Dict[str, CircuitBreaker]:
    """Get all circuit breakers."""
    return _registry.get_all_breakers()


def get_circuit_breaker_stats() -> Dict[str, Dict[str, Any]]:
    """Get statistics for all circuit breakers."""
    return _registry.get_breaker_stats()


def get_open_circuit_breakers() -> List[str]:
    """Get names of currently open circuit breakers."""
    return _registry.get_open_breakers()


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "get_circuit_breaker",
    "get_all_circuit_breakers",
    "get_circuit_breaker_stats",
    "get_open_circuit_breakers",
]
