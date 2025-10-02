"""Resilient HTTP client patterns for external API integrations.

This module provides a shared client abstraction with the following features:

- Consistent request/response handling across services
- Retries with exponential backoff and jitter for transient failures
- Circuit breaker integration to prevent cascading failures
- Authentication helpers (API key headers, bearer tokens, OAuth2 client support)
- Optional caching layer for idempotent GET requests
- Structured error hierarchy and telemetry hooks
"""

from __future__ import annotations

import contextlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Tuple
from urllib.parse import urljoin

import httpx
from cachetools import TTLCache
from cachetools.keys import hashkey

from aurum.common.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, get_circuit_breaker
from aurum.observability import tracing
from aurum.observability.metrics import record_external_api_request
from aurum.logging.structured_logger import get_logger


LOGGER = get_logger(__name__)


class ExternalAPIError(Exception):
    """Base error for external API failures."""


class ExternalAPINetworkError(ExternalAPIError):
    """Raised when the request fails due to network issues."""


class ExternalAPIResponseError(ExternalAPIError):
    """Raised when the API responds with a non-success status code."""

    def __init__(self, status_code: int, message: str | None = None, *, payload: Any | None = None) -> None:
        self.status_code = status_code
        self.message = message or f"External API responded with status {status_code}"
        self.payload = payload
        super().__init__(self.message)


class ExternalAPIAuthError(ExternalAPIError):
    """Raised when authentication fails."""


class ExternalAPICircuitOpenError(ExternalAPIError):
    """Raised when the circuit breaker is open."""


class ExternalAPICacheMiss(Exception):
    """Internal signal that cache did not contain the requested response."""


def _default_retryable(status_code: int) -> bool:
    return status_code >= 500 or status_code in {408, 425, 429}


def _default_should_cache(method: str, status_code: int) -> bool:
    return method.upper() == "GET" and status_code == 200


@dataclass(slots=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    jitter_fraction: float = 0.25
    retry_on_status: Callable[[int], bool] = field(default=_default_retryable)

    def compute_delay(self, attempt: int) -> float:
        delay = self.base_delay_seconds * (2 ** (attempt - 1))
        delay = min(delay, self.max_delay_seconds)
        jitter = delay * self.jitter_fraction
        return max(0.0, delay + tracing.random_jitter(-jitter, jitter))


@dataclass(slots=True)
class CacheConfig:
    enabled: bool = False
    ttl_seconds: int = 60
    max_entries: int = 512


@dataclass(slots=True)
class IdempotencyConfig:
    enabled: bool = True
    key_header: str = "X-Idempotency-Key"
    ttl_seconds: int = 86400  # 24 hours
    max_entries: int = 1024


@dataclass(slots=True)
class AuthConfig:
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"
    bearer_token: Optional[str] = None
    oauth_client: Optional[Callable[[], Tuple[str, float]]] = None  # returns token, expires_at


@dataclass(slots=True)
class ClientConfig:
    base_url: str
    timeout_seconds: float = 15.0
    headers: Dict[str, str] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    idempotency: IdempotencyConfig = field(default_factory=IdempotencyConfig)
    circuit_breaker: CircuitBreakerConfig | None = None
    telemetry_namespace: str = "external_api"


def _build_timeout(config: ClientConfig) -> httpx.Timeout:
    return httpx.Timeout(connect=5.0, read=config.timeout_seconds, write=config.timeout_seconds, pool=5.0)


def _load_tracing_tags(config: ClientConfig, path: str) -> Dict[str, Any]:
    return {
        "component": config.telemetry_namespace,
        "external.base_url": config.base_url,
        "external.path": path,
    }


class APIResponseCache:
    def __init__(self, cache_config: CacheConfig) -> None:
        self._cache: TTLCache | None = None
        if cache_config.enabled:
            ttl = max(1, cache_config.ttl_seconds)
            maxsize = max(1, cache_config.max_entries)
            self._cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def get(self, key: Iterable[Any]) -> httpx.Response:
        if self._cache is None:
            raise ExternalAPICacheMiss()
        cached = self._cache.get(hashkey(*key))
        if cached is None:
            raise ExternalAPICacheMiss()
        return cached

    def set(self, key: Iterable[Any], response: httpx.Response) -> None:
        if self._cache is None:
            return
        # Store a copy to avoid consumed response bodies
        stored = httpx.Response(
            status_code=response.status_code,
            headers=response.headers.copy(),
            content=response.content,
            request=response.request,
        )
        self._cache[hashkey(*key)] = stored


class IdempotencyStore:
    def __init__(self, config: IdempotencyConfig) -> None:
        self._cache: TTLCache | None = None
        if config.enabled:
            ttl = max(1, config.ttl_seconds)
            maxsize = max(1, config.max_entries)
            self._cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def get(self, key: str) -> Optional[httpx.Response]:
        if self._cache is None:
            return None
        return self._cache.get(key)

    def set(self, key: str, response: httpx.Response) -> None:
        if self._cache is None:
            return
        # Store a copy to avoid consumed response bodies
        stored = httpx.Response(
            status_code=response.status_code,
            headers=response.headers.copy(),
            content=response.content,
            request=response.request,
        )
        self._cache[key] = stored


class ExternalAPIClient:
    """Unified client for resilient external API access."""

    def __init__(self, config: ClientConfig) -> None:
        if not config.base_url:
            raise ValueError("base_url is required")
        self._config = config
        self._base_url = config.base_url.rstrip("/")
        self._timeout = _build_timeout(config)
        self._client = httpx.Client(
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        breaker_config = config.circuit_breaker or CircuitBreakerConfig(name=f"external:{self._base_url}")
        self._breaker: CircuitBreaker = get_circuit_breaker(breaker_config.name, breaker_config)
        self._cache = APIResponseCache(config.cache)
        self._idempotency_store = IdempotencyStore(config.idempotency)
        self._logger = logging.getLogger(__name__)
        self._token_expiration: float | None = None

    def _resolve_headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {**self._config.headers}
        if extra:
            headers.update(extra)
        if self._config.auth.api_key:
            headers.setdefault(self._config.auth.api_key_header, self._config.auth.api_key)
        token = self._ensure_token()
        if token:
            headers.setdefault("Authorization", f"Bearer {token}")
        return headers

    def _ensure_token(self) -> Optional[str]:
        if self._config.auth.bearer_token:
            return self._config.auth.bearer_token
        oauth_client = self._config.auth.oauth_client
        if oauth_client is None:
            return None
        if self._token_expiration and self._token_expiration - time.time() > 30:
            return self._config.auth.bearer_token
        token, expires_at = oauth_client()
        self._config.auth.bearer_token = token
        self._token_expiration = expires_at
        return token

    def close(self) -> None:
        self._client.close()

    def _cache_key(self, method: str, path: str, params: Optional[Dict[str, Any]]) -> Tuple[str, str, Tuple[Tuple[str, Any], ...]]:
        frozen_params = tuple(sorted((params or {}).items()))
        return (method.upper(), path, frozen_params)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        url = urljoin(self._base_url + "/", path.lstrip("/"))
        request_headers = self._resolve_headers(headers)
        config = self._config
        cache_key = self._cache_key(method, path, params)

        # Circuit breaker gate
        if self._breaker.is_open():
            raise ExternalAPICircuitOpenError(f"Circuit open for {config.base_url}")

        # Handle idempotency for POST requests
        idempotency_key = None
        if method.upper() == "POST" and config.idempotency.enabled:
            idempotency_key = request_headers.get(config.idempotency.key_header)
            if idempotency_key:
                # Check if we already have a response for this idempotency key
                cached_response = self._idempotency_store.get(idempotency_key)
                if cached_response is not None:
                    return cached_response

        # Attempt cache lookup (for GET requests)
        try:
            cached = self._cache.get(cache_key)
        except ExternalAPICacheMiss:
            cached = None
        else:
            return cached

        attempt = 1
        last_exc: Exception | None = None
        while attempt <= max(1, config.retry.max_attempts):
            started = time.perf_counter()
            with tracing.start_span("external.http.request", tags=_load_tracing_tags(config, path)):
                try:
                    response = self._client.request(
                        method=method.upper(),
                        url=url,
                        params=params,
                        json=json,
                        headers=request_headers,
                        timeout=timeout or config.timeout_seconds,
                    )
                except httpx.TransportError as exc:
                    self._breaker.record_failure()
                    last_exc = exc
                    self._record_metric("network_error", extra={"attempt": attempt, "method": method, "url": url})
                    self._logger.warning("External API transport error", exc_info=exc)
                    attempt += 1
                    self._sleep(attempt)
                    continue

                if 200 <= response.status_code < 300:
                    self._breaker.record_success()
                    try:
                        record_external_api_request(path, str(response.status_code), time.perf_counter() - started)
                    except Exception:
                        pass
                    if _default_should_cache(method, response.status_code):
                        self._cache.set(cache_key, response)

                    # Store idempotent POST responses
                    if idempotency_key:
                        self._idempotency_store.set(idempotency_key, response)

                    return response

                if response.status_code in {401, 403}:
                    self._breaker.record_failure()
                    raise ExternalAPIAuthError(f"Authentication failed for {url}: {response.status_code}")

                if config.retry.retry_on_status(response.status_code) and attempt < config.retry.max_attempts:
                    self._breaker.record_failure()
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after and retry_after.isdigit() else config.retry.compute_delay(attempt)
                    else:
                        delay = config.retry.compute_delay(attempt)
                    self._record_metric(
                        "retry",
                        extra={
                            "status": response.status_code,
                            "attempt": attempt,
                            "method": method,
                            "url": url,
                            "delay": delay,
                        },
                    )
                    try:
                        record_external_api_request(path, str(response.status_code), time.perf_counter() - started)
                    except Exception:
                        pass
                    attempt += 1
                    tracing.sleep(delay)
                    continue

                # Non-retryable error
                self._breaker.record_failure()
                payload = self._safe_json(response)
                try:
                    record_external_api_request(path, str(response.status_code), time.perf_counter() - started)
                except Exception:
                    pass
                raise ExternalAPIResponseError(response.status_code, payload=payload)

        # Exceeded attempts
        self._breaker.record_failure()
        if last_exc:
            raise ExternalAPINetworkError(f"Retries exhausted for {url}") from last_exc
        raise ExternalAPIResponseError(response.status_code, payload=self._safe_json(response))

    def _sleep(self, attempt: int) -> None:
        delay = self._config.retry.compute_delay(attempt)
        tracing.sleep(delay)

    def _record_metric(self, event: str, *, extra: Dict[str, Any]) -> None:
        tracing.record_event("external.api", event, extra)

    def _safe_json(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return response.text

    @contextlib.contextmanager
    def scoped_headers(self, **headers: str) -> Iterator["ExternalAPIClient"]:
        original = self._config.headers.copy()
        try:
            self._config.headers.update(headers)
            yield self
        finally:
            self._config.headers = original

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", path, **kwargs)


__all__ = [
    "ExternalAPIClient",
    "ClientConfig",
    "RetryConfig",
    "CacheConfig",
    "IdempotencyConfig",
    "AuthConfig",
    "APIResponseCache",
    "IdempotencyStore",
    "ExternalAPIError",
    "ExternalAPINetworkError",
    "ExternalAPIResponseError",
    "ExternalAPIAuthError",
    "ExternalAPICircuitOpenError",
]
