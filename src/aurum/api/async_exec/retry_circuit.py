"""Async retry and circuit breaker utilities."""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Sequence, TypeVar

T = TypeVar("T")


class RetryError(Exception):
    """Raised when retry attempts are exhausted."""


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker blocks execution."""


@dataclass(slots=True)
class RetryPolicy:
    attempts: int = 3
    initial_delay: float = 0.2
    max_delay: float = 5.0
    multiplier: float = 2.0
    jitter: float = 0.2
    retry_for: tuple[type[BaseException], ...] = (Exception,)
    give_up_for: tuple[type[BaseException], ...] = ()

    def iter_delays(self) -> Iterable[float]:
        delay = self.initial_delay
        for _ in range(max(0, self.attempts - 1)):
            jitter = delay * self.jitter if self.jitter else 0.0
            yield max(0.0, min(self.max_delay, random.uniform(delay - jitter, delay + jitter)))
            delay = min(self.max_delay, delay * self.multiplier)


@dataclass(slots=True)
class CircuitBreakerState:
    failure_threshold: int = 5
    recovery_time_s: float = 30.0
    half_open_max_calls: int = 1
    half_open_success_threshold: int = 1


class CircuitBreaker:
    """Simple circuit breaker with open/half-open/closed states."""

    def __init__(self, *, config: CircuitBreakerState | None = None) -> None:
        self._config = config or CircuitBreakerState()
        self._state = "closed"
        self._failures = 0
        self._last_failure_time = 0.0
        self._half_open_in_flight = 0
        self._half_open_success = 0

    def allow(self) -> None:
        now = time.monotonic()
        if self._state == "open":
            if now - self._last_failure_time >= self._config.recovery_time_s:
                self._state = "half-open"
                self._half_open_in_flight = 0
                self._half_open_success = 0
            else:
                raise CircuitBreakerOpenError("circuit breaker is open")
        if self._state == "half-open":
            if self._half_open_in_flight >= self._config.half_open_max_calls:
                raise CircuitBreakerOpenError("circuit breaker half-open limit reached")
            self._half_open_in_flight += 1

    def on_success(self) -> None:
        if self._state == "closed":
            self._failures = 0
            return
        if self._state == "half-open":
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
            self._half_open_success += 1
            if self._half_open_success >= self._config.half_open_success_threshold:
                self._state = "closed"
                self._failures = 0
                self._half_open_success = 0

    def on_failure(self) -> None:
        if self._state == "half-open":
            self._state = "open"
            self._last_failure_time = time.monotonic()
            self._half_open_in_flight = 0
            self._half_open_success = 0
            return
        self._failures += 1
        if self._failures >= self._config.failure_threshold:
            self._state = "open"
            self._last_failure_time = time.monotonic()

    @property
    def state(self) -> str:
        return self._state


def async_retry(
    *,
    policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator adding retry behavior to async callables."""

    retry_policy = policy or RetryPolicy()

    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if circuit_breaker is not None:
                circuit_breaker.allow()
            delays = list(retry_policy.iter_delays())
            attempt = 0
            while True:
                try:
                    result = await fn(*args, **kwargs)
                    if circuit_breaker is not None:
                        circuit_breaker.on_success()
                    return result
                except asyncio.CancelledError:
                    raise
                except retry_policy.give_up_for as exc:  # type: ignore[misc]
                    if circuit_breaker is not None:
                        circuit_breaker.on_failure()
                    raise exc
                except retry_policy.retry_for as exc:  # type: ignore[misc]
                    if circuit_breaker is not None:
                        circuit_breaker.on_failure()
                    if attempt >= retry_policy.attempts - 1:
                        raise RetryError(f"exhausted retries after {attempt + 1} attempts") from exc
                    delay = delays[min(attempt, len(delays) - 1)] if delays else 0.0
                    attempt += 1
                    if delay > 0:
                        await asyncio.sleep(delay)
                except BaseException as exc:
                    if circuit_breaker is not None:
                        circuit_breaker.on_failure()
                    raise exc

        return wrapper

    return decorator


@asynccontextmanager
async def retry_context(
    *,
    policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Awaitable[T]]:
    """Context manager yielding a call helper with retry semantics."""

    retry_policy = policy or RetryPolicy()

    async def call(fn: Callable[..., Awaitable[T]], /, *args: Any, **kwargs: Any) -> T:
        decorated = async_retry(policy=retry_policy, circuit_breaker=circuit_breaker)(fn)
        return await decorated(*args, **kwargs)

    yield call


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitBreakerState",
    "RetryError",
    "RetryPolicy",
    "async_retry",
    "retry_context",
]
