
"""Shared collector base classes for external provider ingestion."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urljoin

import requests
from requests import Response, Session
from requests.exceptions import RequestException, Timeout

try:
    from ..compat.requests import requests as resilient_requests, ResilienceConfig
except ImportError:
    resilient_requests = None  # type: ignore[assignment]
    ResilienceConfig = None  # type: ignore[assignment]

try:  # Optional instrumentation
    from prometheus_client import Counter, Histogram  # type: ignore
except Exception:  # pragma: no cover - metrics optional
    Counter = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]

try:  # Optional fastavro encoder
    from fastavro import schemaless_writer  # type: ignore
except Exception:  # pragma: no cover - dependency optional
    schemaless_writer = None  # type: ignore[assignment]

try:  # Optional Kafka Avro producer
    from confluent_kafka.avro import AvroProducer  # type: ignore
except Exception:  # pragma: no cover - dependency optional
    AvroProducer = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

if Counter:  # pragma: no cover - simple wrappers around Prometheus
    HTTP_REQUESTS_TOTAL = Counter(
        "aurum_external_http_requests_total",
        "HTTP requests executed by external collectors",
        labelnames=["provider", "method", "status"],
    )
    HTTP_REQUEST_LATENCY = Histogram(
        "aurum_external_http_request_seconds",
        "HTTP request latency",
        labelnames=["provider", "method"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
    )
    RATE_LIMIT_WAIT = Histogram(
        "aurum_external_rate_limit_wait_seconds",
        "Seconds spent waiting for rate limiting",
        labelnames=["provider"],
        buckets=(0.01, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
    )
    RETRY_BACKOFF = Histogram(
        "aurum_external_retry_backoff_seconds",
        "Backoff delay applied before retrying",
        labelnames=["provider", "attempt"],
        buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
    )
    KAFKA_RECORDS_TOTAL = Counter(
        "aurum_external_kafka_records_total",
        "Records emitted to Kafka",
        labelnames=["provider", "topic"],
    )
else:  # pragma: no cover - no metrics backend
    HTTP_REQUESTS_TOTAL = None  # type: ignore[assignment]
    HTTP_REQUEST_LATENCY = None  # type: ignore[assignment]
    RATE_LIMIT_WAIT = None  # type: ignore[assignment]
    RETRY_BACKOFF = None  # type: ignore[assignment]
    KAFKA_RECORDS_TOTAL = None  # type: ignore[assignment]


@dataclass(frozen=True)
class RetryConfig:
    """Retry configuration for HTTP requests."""

    max_attempts: int = 3
    backoff_factor: float = 0.5
    max_backoff_seconds: float = 30.0
    status_forcelist: Sequence[int] = (429, 500, 502, 503, 504)
    retry_on_timeout: bool = True

    def compute_backoff(self, attempt: int) -> float:
        """Compute backoff delay for a given attempt (1-indexed)."""
        delay = self.backoff_factor * (2 ** (attempt - 1))
        return min(delay, self.max_backoff_seconds)


@dataclass(frozen=True)
class RateLimitConfig:
    """Simple token bucket configuration."""

    rate: float
    burst: float = 1.0

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be positive")
        if self.burst <= 0:
            raise ValueError("burst must be positive")


@dataclass(frozen=True)
class CollectorConfig:
    """Static configuration for a collector instance."""

    provider: str
    base_url: str
    kafka_topic: str
    kafka_bootstrap_servers: Optional[str] = None
    schema_registry_url: Optional[str] = None
    key_schema: Optional[Mapping[str, Any]] = None
    value_schema: Optional[Mapping[str, Any]] = None
    default_headers: Mapping[str, str] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: Optional[RateLimitConfig] = None
    http_timeout: float = 30.0
    kafka_config: Mapping[str, Any] = field(default_factory=dict)
    resilience_config: Optional[ResilienceConfig] = None


@dataclass
class CollectorContext:
    """Mutable context scoped to a collector run."""

    run_id: str = field(default_factory=lambda: f"run-{int(time.time() * 1000)}")
    started_at: float = field(default_factory=time.time)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HttpRequest:
    """Declarative HTTP request description."""

    method: str
    path: str
    params: Optional[Mapping[str, Any]] = None
    headers: Optional[Mapping[str, str]] = None
    json: Any = None
    data: Any = None
    timeout: Optional[float] = None

    def resolve_url(self, base_url: str) -> str:
        if self.path.startswith("http://") or self.path.startswith("https://"):
            return self.path
        return urljoin(base_url.rstrip("/") + "/", self.path.lstrip("/"))


@dataclass
class HttpResponse:
    """Normalized HTTP response payload for collectors."""

    status_code: int
    headers: Mapping[str, str]
    content: bytes
    url: str

    def json(self) -> Any:
        if not self.content:
            return None
        return json.loads(self.content.decode("utf-8"))


class CollectorError(Exception):
    """Base collector exception."""


class HttpRequestError(CollectorError):
    """Raised when an HTTP request ultimately fails."""

    def __init__(
        self,
        message: str,
        *,
        request: HttpRequest,
        response: Optional[HttpResponse] = None,
        original: Optional[Exception] = None,
    ) -> None:
        super().__init__(message)
        self.request = request
        self.response = response
        self.original = original


class RetryLimitExceeded(HttpRequestError):
    """Raised when retries are exhausted."""


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._monotonic = monotonic
        self._tokens = config.burst
        self._updated_at = monotonic()
        self._lock = threading.Lock()

    def acquire(self, sleep: Callable[[float], None]) -> float:
        """Acquire a slot, blocking if necessary.

        Returns:
            Total seconds spent waiting before the slot became available.
        """
        waited = 0.0
        while True:
            with self._lock:
                now = self._monotonic()
                elapsed = now - self._updated_at
                if elapsed > 0:
                    refill = elapsed * self.config.rate
                    self._tokens = min(self.config.burst, self._tokens + refill)
                    self._updated_at = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return waited
                shortage = 1.0 - self._tokens
                wait = shortage / self.config.rate
            sleep(wait)
            waited += wait


class CollectorMetrics:
    """In-memory metrics aggregator with optional Prometheus export."""

    def __init__(self, provider: str) -> None:
        self.provider = provider
        self._lock = threading.Lock()
        self.http_requests_total = 0
        self.http_success = 0
        self.http_errors = 0
        self.http_status_breakdown: Dict[str, int] = defaultdict(int)
        self.http_latency_ms: list[float] = []
        self.rate_limit_wait_seconds: list[float] = []
        self.retry_backoff_events: list[Tuple[int, float]] = []
        self.records_emitted_total = 0
        self.records_emitted_by_topic: Dict[str, int] = defaultdict(int)

    def record_http_request(self, method: str, status: int, duration_seconds: float) -> None:
        success = 200 <= status < 400
        with self._lock:
            self.http_requests_total += 1
            if success:
                self.http_success += 1
            else:
                self.http_errors += 1
            self.http_status_breakdown[str(status)] += 1
            self.http_latency_ms.append(duration_seconds * 1000.0)
        if HTTP_REQUESTS_TOTAL:
            HTTP_REQUESTS_TOTAL.labels(
                provider=self.provider,
                method=method,
                status=str(status),
            ).inc()
        if HTTP_REQUEST_LATENCY:
            HTTP_REQUEST_LATENCY.labels(
                provider=self.provider,
                method=method,
            ).observe(duration_seconds)

    def record_rate_limit_wait(self, waited: float) -> None:
        if waited <= 0:
            return
        with self._lock:
            self.rate_limit_wait_seconds.append(waited)
        if RATE_LIMIT_WAIT:
            RATE_LIMIT_WAIT.labels(provider=self.provider).observe(waited)

    def record_backoff(self, attempt: int, delay: float) -> None:
        with self._lock:
            self.retry_backoff_events.append((attempt, delay))
        if RETRY_BACKOFF:
            RETRY_BACKOFF.labels(
                provider=self.provider,
                attempt=str(attempt),
            ).observe(delay)

    def record_kafka_emit(self, topic: str, count: int) -> None:
        with self._lock:
            self.records_emitted_total += count
            self.records_emitted_by_topic[topic] += count
        if KAFKA_RECORDS_TOTAL:
            KAFKA_RECORDS_TOTAL.labels(
                provider=self.provider,
                topic=topic,
            ).inc(count)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "http_requests_total": self.http_requests_total,
                "http_success": self.http_success,
                "http_errors": self.http_errors,
                "http_status_breakdown": dict(self.http_status_breakdown),
                "http_latency_ms": list(self.http_latency_ms),
                "rate_limit_wait_seconds": list(self.rate_limit_wait_seconds),
                "retry_backoff_events": list(self.retry_backoff_events),
                "records_emitted_total": self.records_emitted_total,
                "records_emitted_by_topic": dict(self.records_emitted_by_topic),
            }


class AvroEncoder:
    """Lightweight Avro encoder using fastavro."""

    def __init__(self, schema: Mapping[str, Any]) -> None:
        if schemaless_writer is None:
            raise RuntimeError("fastavro is required for Avro encoding")
        self.schema = schema

    def encode(self, record: Mapping[str, Any]) -> bytes:
        buffer = BytesIO()
        schemaless_writer(buffer, self.schema, record)
        return buffer.getvalue()


class ExternalCollector:
    """Base class providing HTTP + Kafka utilities for collectors."""

    def __init__(
        self,
        config: CollectorConfig,
        *,
        context: Optional[CollectorContext] = None,
        session: Optional[Session] = None,
        metrics: Optional[CollectorMetrics] = None,
        rate_limiter: Optional[RateLimiter] = None,
        producer: Optional[Any] = None,
        encoder: Optional[AvroEncoder] = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.context = context or CollectorContext()
        self.metrics = metrics or CollectorMetrics(config.provider)

        # Use resilient session if resilience config is provided, otherwise fallback to regular session
        if config.resilience_config and resilient_requests:
            self.resilient_session = resilient_requests.get_session(config.provider)
            self.session = None  # Will use resilient session's internal session
        else:
            self.resilient_session = None
            self.session = session or requests.Session()
            self.session.headers.update(config.default_headers)
            self.session.headers.setdefault(
                "User-Agent",
                f"aurum-collector/{config.provider}",
            )

        self._sleep = sleep
        self._monotonic = monotonic
        self.retry_config = config.retry
        self.rate_limiter = rate_limiter or (
            RateLimiter(config.rate_limit, monotonic=monotonic) if config.rate_limit else None
        )
        self.producer = producer
        self.encoder = encoder
        self._closed = False

    def request(self, request: HttpRequest) -> HttpResponse:
        """Execute an HTTP request with retries and metrics instrumentation."""
        if self._closed:
            raise RuntimeError("collector is closed")

        # Use resilient session if available (it handles retries/circuit breaker internally)
        if self.resilient_session:
            waited = self._apply_rate_limit()
            if waited and self.metrics:
                self.metrics.record_rate_limit_wait(waited)

            try:
                start = self._monotonic()
                response = self.resilient_session.request(
                    method=request.method.upper(),
                    url=request.resolve_url(self.config.base_url),
                    params=request.params,
                    headers=self._merge_headers(request.headers),
                    json=request.json,
                    data=request.data,
                )
                duration = self._monotonic() - start

                http_response = self._to_http_response(response)
                self.metrics.record_http_request(
                    request.method.upper(),
                    http_response.status_code,
                    duration,
                )

                return http_response

            except Exception as exc:
                # Resilient session already handled retries and circuit breaker
                raise HttpRequestError(
                    f"HTTP request failed: {exc}",
                    request=request,
                    original=exc,
                ) from exc

        # Fallback to legacy retry logic for backward compatibility
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt < self.retry_config.max_attempts:
            attempt += 1
            waited = self._apply_rate_limit()
            if waited and self.metrics:
                self.metrics.record_rate_limit_wait(waited)
            try:
                start = self._monotonic()
                response = self.session.request(
                    method=request.method.upper(),
                    url=request.resolve_url(self.config.base_url),
                    params=request.params,
                    headers=self._merge_headers(request.headers),
                    json=request.json,
                    data=request.data,
                    timeout=request.timeout or self.config.http_timeout,
                )
                duration = self._monotonic() - start
            except Timeout as exc:
                last_exc = exc
                if not self.retry_config.retry_on_timeout or attempt >= self.retry_config.max_attempts:
                    raise HttpRequestError(
                        f"HTTP request timed out after {attempt} attempts",
                        request=request,
                        original=exc,
                    ) from exc
                delay = self.retry_config.compute_backoff(attempt)
                self.metrics.record_backoff(attempt, delay)
                self._sleep(delay)
                continue
            except RequestException as exc:
                last_exc = exc
                raise HttpRequestError(
                    f"HTTP request failed: {exc}",
                    request=request,
                    original=exc,
                ) from exc

            http_response = self._to_http_response(response)
            self.metrics.record_http_request(
                request.method.upper(),
                http_response.status_code,
                duration,
            )

            if self._should_retry(http_response.status_code) and attempt < self.retry_config.max_attempts:
                delay = self.retry_config.compute_backoff(attempt)
                self.metrics.record_backoff(attempt, delay)
                self._sleep(delay)
                continue

            if 200 <= http_response.status_code < 400:
                return http_response

            raise HttpRequestError(
                f"HTTP request failed with status {http_response.status_code}",
                request=request,
                response=http_response,
                original=last_exc,
            )

        raise RetryLimitExceeded(
            "Retries exhausted",
            request=request,
            original=last_exc,
        )

    def emit_records(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        key_fn: Optional[Callable[[Mapping[str, Any]], Optional[Mapping[str, Any]]]] = None,
        headers_fn: Optional[Callable[[Mapping[str, Any]], Optional[Sequence[Tuple[str, Union[str, bytes]]]]]] = None,
        value_transform: Optional[Callable[[Mapping[str, Any]], Mapping[str, Any]]] = None,
        flush: bool = True,
    ) -> int:
        """Emit a batch of records to Kafka."""
        if self._closed:
            raise RuntimeError("collector is closed")
        producer = self._ensure_producer()
        emitted = 0
        for record in records:
            value_payload = value_transform(record) if value_transform else record
            value = self._encode_value(value_payload)
            key_payload = key_fn(record) if key_fn else None
            headers = self._encode_headers(headers_fn(record) if headers_fn else None)
            try:
                producer.produce(
                    topic=self.config.kafka_topic,
                    value=value,
                    key=key_payload,
                    headers=headers,
                )
            except Exception as exc:  # pragma: no cover - passthrough for producer errors
                logger.exception("Failed to produce record", exc_info=exc)
                raise
            emitted += 1
        if flush and emitted:
            producer.flush()
        if emitted:
            self.metrics.record_kafka_emit(self.config.kafka_topic, emitted)
        return emitted

    def flush(self) -> None:
        if self.producer:
            self.producer.flush()

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self.producer:
                self.producer.flush()
        finally:
            if self.resilient_session:
                self.resilient_session.close()
            elif self.session:
                self.session.close()
            self._closed = True

    def __enter__(self) -> "ExternalCollector":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _apply_rate_limit(self) -> float:
        if not self.rate_limiter:
            return 0.0
        return self.rate_limiter.acquire(self._sleep)

    def _should_retry(self, status_code: int) -> bool:
        return status_code in self.retry_config.status_forcelist

    def _merge_headers(self, headers: Optional[Mapping[str, str]]) -> Dict[str, str]:
        merged = dict(self.config.default_headers)
        if headers:
            merged.update(headers)
        return merged

    def _encode_value(self, value: Mapping[str, Any]) -> Any:
        if self.encoder:
            return self.encoder.encode(value)
        return value

    def _encode_headers(self, headers: Optional[Sequence[Tuple[str, Union[str, bytes]]]]) -> Optional[Sequence[Tuple[str, bytes]]]:
        if not headers:
            return None
        encoded: list[Tuple[str, bytes]] = []
        for key, val in headers:
            if isinstance(val, bytes):
                encoded.append((key, val))
            else:
                encoded.append((key, str(val).encode("utf-8")))
        return encoded

    def _ensure_producer(self) -> Any:
        if self.producer is not None:
            return self.producer
        self.producer = create_avro_producer(self.config)
        return self.producer

    @staticmethod
    def _to_http_response(response: Response) -> HttpResponse:
        return HttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            content=response.content,
            url=response.url,
        )


def create_avro_producer(config: CollectorConfig) -> Any:
    """Create an AvroProducer from collector configuration."""
    if AvroProducer is None:  # pragma: no cover - runtime guard
        raise RuntimeError("confluent-kafka[avro] is required for the default producer")
    if not config.kafka_bootstrap_servers:
        raise ValueError("kafka_bootstrap_servers must be configured")
    if not config.schema_registry_url:
        raise ValueError("schema_registry_url must be configured")
    if not config.value_schema:
        raise ValueError("value_schema must be supplied for Avro encoding")

    kafka_config = {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "schema.registry.url": config.schema_registry_url,
    }
    kafka_config.update(config.kafka_config)
    return AvroProducer(
        kafka_config,
        default_key_schema=config.key_schema,
        default_value_schema=config.value_schema,
    )


__all__ = [
    "AvroEncoder",
    "CollectorConfig",
    "CollectorContext",
    "CollectorError",
    "CollectorMetrics",
    "ExternalCollector",
    "HttpRequest",
    "HttpRequestError",
    "HttpResponse",
    "RateLimitConfig",
    "RateLimiter",
    "RetryConfig",
    "RetryLimitExceeded",
    "create_avro_producer",
]
