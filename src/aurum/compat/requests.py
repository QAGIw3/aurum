from __future__ import annotations

"""Enhanced compatibility shim for the optional `requests` dependency with resilience features.

Provides unified timeouts, retries, backoff, and circuit breaker patterns across all
external provider integrations.
"""

import time
import threading
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field

try:  # pragma: no cover - deferred import to keep runtime lightweight
    import requests as _requests  # type: ignore[import]
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry  # type: ignore[import]
except ImportError:  # pragma: no cover - handled by RequestsProxy
    _requests = None
    HTTPAdapter = None  # type: ignore[assignment]
    Retry = None  # type: ignore[assignment]

try:
    from ..common.circuit_breaker import CircuitBreaker
except ImportError:
    CircuitBreaker = None  # type: ignore[assignment]


@dataclass
class ResilienceConfig:
    """Configuration for request resilience patterns."""

    timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 0.5
    max_backoff_seconds: float = 60.0
    retry_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 60.0

    def create_retry_strategy(self) -> Optional[Any]:
        """Create urllib3 Retry strategy for requests adapter."""
        if Retry is None:
            return None

        return Retry(
            total=self.max_retries,
            status_forcelist=self.retry_status_codes,
            backoff_factor=self.backoff_factor,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
        )

    def create_circuit_breaker(self) -> Optional[CircuitBreaker]:
        """Create circuit breaker for request resilience."""
        if CircuitBreaker is None:
            return None

        return CircuitBreaker(
            failure_threshold=self.circuit_breaker_failure_threshold,
            recovery_timeout=self.circuit_breaker_recovery_timeout,
        )


class ResilientSession:
    """Enhanced requests session with resilience features."""

    def __init__(self, config: ResilienceConfig):
        self.config = config
        self.circuit_breaker = config.create_circuit_breaker()
        self._lock = threading.Lock()

        if _requests:
            self.session = _requests.Session()

            # Configure retry strategy
            retry_strategy = config.create_retry_strategy()
            if retry_strategy:
                adapter = HTTPAdapter(max_retries=retry_strategy)
                self.session.mount("http://", adapter)
                self.session.mount("https://", adapter)

            # Set default timeout
            self.session.timeout = config.timeout
        else:
            self.session = None

    def request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[Any]:
        """Execute request with circuit breaker and resilience patterns."""
        if self.session is None:
            raise RuntimeError("requests library not available")

        if self.circuit_breaker and self.circuit_breaker.is_open():
            raise RuntimeError("Circuit breaker is OPEN - requests blocked")

        try:
            # Apply timeout if not specified
            if "timeout" not in kwargs:
                kwargs["timeout"] = self.config.timeout

            response = self.session.request(method, url, **kwargs)

            # Record success with circuit breaker
            if self.circuit_breaker:
                self.circuit_breaker.record_success()

            return response

        except Exception as exc:
            # Record failure with circuit breaker
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()

            raise exc

    def close(self) -> None:
        """Close the underlying session."""
        if self.session:
            self.session.close()


class RequestsProxy:
    """Enhanced proxy exposing a subset of the requests API with resilience features.

    When the real dependency is installed, attributes resolve to the genuine
    implementation enhanced with resilience patterns. Otherwise, attribute access
    raises an informative ModuleNotFoundError unless the attribute has been
    monkeypatched (e.g. in tests).
    """

    __slots__ = {"_module", "_overrides", "_resilience_configs", "_sessions"}

    def __init__(self) -> None:
        object.__setattr__(self, "_module", _requests)
        object.__setattr__(self, "_overrides", {})
        object.__setattr__(self, "_resilience_configs", {})
        object.__setattr__(self, "_sessions", {})

    def configure_resilience(
        self,
        provider_name: str,
        config: Optional[ResilienceConfig] = None
    ) -> None:
        """Configure resilience settings for a provider."""
        if config is None:
            config = ResilienceConfig()

        self._resilience_configs[provider_name] = config

    def get_session(self, provider_name: str) -> ResilientSession:
        """Get or create a resilient session for a provider."""
        with self._lock:
            if provider_name not in self._sessions:
                config = self._resilience_configs.get(provider_name, ResilienceConfig())
                self._sessions[provider_name] = ResilientSession(config)
            return self._sessions[provider_name]

    @property
    def _lock(self) -> threading.Lock:
        """Get or create thread lock for session management."""
        if not hasattr(self, "_lock_attr"):
            object.__setattr__(self, "_lock_attr", threading.Lock())
        return object.__getattribute__(self, "_lock_attr")

    def __getattr__(self, name: str) -> Any:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]

        module = object.__getattribute__(self, "_module")
        if module is None:
            def _missing(*_: Any, **__: Any) -> None:
                raise ModuleNotFoundError(
                    "The 'requests' package is required for this operation. Install it via 'pip install requests'."
                )

            overrides[name] = _missing
            return _missing
        return getattr(module, name)

    def __setattr__(self, name: str, value: Any) -> None:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        overrides[name] = value

    def __delattr__(self, name: str) -> None:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        overrides.pop(name, None)


# Global resilient requests instance
requests = RequestsProxy()

__all__ = ["requests", "RequestsProxy", "ResilienceConfig", "ResilientSession"]
