"""Common utilities shared across multiple packages.

This namespace hosts lightweight helpers (e.g., circuit breakers) that do not
introduce heavy dependencies and are safe to use from API, workers, and tools.
"""

from .circuit_breaker import CircuitBreaker

__all__ = ["CircuitBreaker"]
