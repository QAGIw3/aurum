from __future__ import annotations

"""Shared HTTP client utilities with resilience features."""

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Optional

import httpx

LOGGER = logging.getLogger(__name__)


@dataclass
class CircuitBreaker:
    """Minimal circuit breaker implementation for outbound HTTP calls."""

    failure_threshold: int = 5
    recovery_time_seconds: float = 30.0
    _failures: int = 0
    _opened_at: Optional[float] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def allow_request(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return True
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self.recovery_time_seconds:
                # Half-open state: allow a single trial request.
                self._opened_at = None
                self._failures = 0
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()


class HttpClientManager:
    """Singleton manager providing a shared httpx.Client with retries and timeouts."""

    _instance: Optional[HttpClientManager] = None
    _instance_lock = threading.Lock()

    def __init__(
        self,
        *,
        timeout: Optional[httpx.Timeout] = None,
        max_connections: int = 20,
        max_keepalive: int = 10,
        max_retries: int = 2,
    ) -> None:
        self._timeout = timeout or httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)
        self._retries = max(max_retries, 0)
        self._limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive)
        self._client: Optional[httpx.Client] = None
        self._client_lock = threading.Lock()
        self._breaker = CircuitBreaker()

    @classmethod
    def get_instance(cls) -> HttpClientManager:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    transport = httpx.HTTPTransport(retries=self._retries)
                    self._client = httpx.Client(
                        timeout=self._timeout,
                        limits=self._limits,
                        transport=transport,
                    )
        return self._client

    def close(self) -> None:
        with self._client_lock:
            if self._client is not None:
                self._client.close()
                self._client = None

    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        if not self._breaker.allow_request():
            raise httpx.RequestError("HTTP circuit breaker open", request=httpx.Request(method, url))

        client = self._ensure_client()
        try:
            response = client.request(method, url, **kwargs)
            if 500 <= response.status_code < 600:
                self._breaker.record_failure()
            else:
                self._breaker.record_success()
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            raise
        except httpx.RequestError as exc:
            LOGGER.warning("HTTP request failed: %s", exc)
            self._breaker.record_failure()
            raise

    @contextmanager
    def client(self) -> Iterator[httpx.Client]:
        client = self._ensure_client()
        yield client


_http_client_manager = HttpClientManager.get_instance()


def get_http_client() -> httpx.Client:
    """Return the shared httpx.Client instance."""
    return _http_client_manager._ensure_client()


def close_http_client() -> None:
    """Shut down the shared client."""
    _http_client_manager.close()


def request(method: str, url: str, **kwargs) -> httpx.Response:
    """Perform an HTTP request with retries, timeouts, and circuit breaker protection."""
    return _http_client_manager.request(method, url, **kwargs)
