"""Base adapter and utilities for cross-ISO ingestion.

This layer builds upon aurum.external.collect to add:
  - Circuit breaker protection
  - Idempotent paging via CheckpointStore
  - Chunking helpers for time-bounded windows
  - Minimal ISO-agnostic metrics bundle
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from ..collect import (
    Checkpoint,
    CheckpointStore,
    CollectorConfig,
    CollectorContext,
    CollectorMetrics,
    ExternalCollector,
    HttpRequest,
    HttpRequestError,
    PostgresCheckpointStore,
    RateLimitConfig,
    RetryConfig,
)


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Circuit breaker tuning parameters."""

    error_threshold: int = 5
    window_seconds: int = 60
    open_seconds: int = 120


class CircuitOpenError(RuntimeError):
    pass


class _CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self._first_error_at: Optional[float] = None
        self._error_count: int = 0
        self._opened_at: Optional[float] = None

    def allow(self, now: float) -> bool:
        if self._opened_at is None:
            return True
        if now - self._opened_at >= self.config.open_seconds:
            # half-open
            self._opened_at = None
            self._error_count = 0
            self._first_error_at = None
            return True
        return False

    def record_success(self) -> None:
        self._first_error_at = None
        self._error_count = 0
        # keep open state as-is (only allow() changes it)

    def record_error(self, now: float) -> None:
        if self._first_error_at is None or (now - self._first_error_at) > self.config.window_seconds:
            self._first_error_at = now
            self._error_count = 1
            return
        self._error_count += 1
        if self._error_count >= self.config.error_threshold:
            self._opened_at = now


@dataclass(frozen=True)
class IsoAdapterConfig:
    provider: str
    base_url: str
    kafka_topic: str
    schema_registry_url: Optional[str] = None
    kafka_bootstrap_servers: Optional[str] = None
    default_headers: Mapping[str, str] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: Optional[RateLimitConfig] = None
    http_timeout: float = 30.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    checkpoint_namespace: str = "aurum:collect:checkpoint"


@dataclass(frozen=True)
class IsoRequestChunk:
    """Time-bounded chunk request descriptor."""

    start: datetime
    end: datetime
    cursor: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)


class IsoIngestMetrics:
    """Small convenience wrapper around CollectorMetrics."""

    def __init__(self, metrics: CollectorMetrics) -> None:
        self._m = metrics

    def http_snapshot(self) -> Mapping[str, Any]:
        return self._m.snapshot()


class IdempotentPager:
    """Manages paging based on last page token and/or last timestamp per series."""

    def __init__(self, store: CheckpointStore, provider: str, series_id: str) -> None:
        self.store = store
        self.provider = provider
        self.series_id = series_id
        self._state: Optional[Checkpoint] = None

    def load(self) -> Optional[Checkpoint]:
        self._state = self.store.get(self.provider, self.series_id)
        return self._state

    def last_cursor(self) -> Optional[str]:
        if not self._state:
            self.load()
        return self._state.last_page if self._state else None

    def last_ts(self) -> Optional[datetime]:
        if not self._state:
            self.load()
        return self._state.last_timestamp if self._state else None

    def commit(self, *, last_page: Optional[str], last_ts: Optional[datetime], metadata: Optional[Mapping[str, Any]] = None) -> None:
        cp = Checkpoint(
            provider=self.provider,
            series_id=self.series_id,
            last_timestamp=last_ts,
            last_page=last_page,
            metadata=dict(metadata or {}),
        )
        self.store.set(cp)


class IsoAdapter:
    """Composable base adapter with circuit breaker and idempotent paging.

    Subclasses must implement:
      - build_request(chunk: IsoRequestChunk) -> HttpRequest
      - parse_page(payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]
        returning (records, next_cursor)
    """

    def __init__(
        self,
        config: IsoAdapterConfig,
        *,
        series_id: str,
        checkpoint_store: Optional[CheckpointStore] = None,
        context: Optional[CollectorContext] = None,
        collector_factory: Optional[Callable[[CollectorConfig, CollectorContext], ExternalCollector]] = None,
    ) -> None:
        self.config = config
        self.collector_config = CollectorConfig(
            provider=config.provider,
            base_url=config.base_url,
            kafka_topic=config.kafka_topic,
            kafka_bootstrap_servers=config.kafka_bootstrap_servers,
            schema_registry_url=config.schema_registry_url,
            default_headers=config.default_headers,
            retry=config.retry,
            rate_limit=config.rate_limit,
            http_timeout=config.http_timeout,
        )
        self.context = context or CollectorContext()
        self.collector = (
            collector_factory(self.collector_config, self.context)
            if collector_factory
            else ExternalCollector(self.collector_config, context=self.context)
        )
        self.series_id = series_id
        self.metrics = IsoIngestMetrics(self.collector.metrics)
        self.circuit = _CircuitBreaker(config.circuit_breaker)
        self.pager = IdempotentPager(
            checkpoint_store or PostgresCheckpointStore(ensure_schema=True),
            provider=config.provider,
            series_id=series_id,
        )

    # --- Hooks to override per adapter ---
    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:  # pragma: no cover - abstract
        raise NotImplementedError

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:  # pragma: no cover - abstract
        raise NotImplementedError

    def value_transform(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        return record

    def key_fn(self, record: Mapping[str, Any]) -> Optional[str]:
        return None

    def headers_fn(self, record: Mapping[str, Any]) -> Optional[Sequence[Tuple[str, str]]]:
        return None

    # --- Core execution ---
    def run_window(self, chunk: IsoRequestChunk, *, flush: bool = True) -> int:
        """Run ingestion for a single request chunk, paging idempotently."""
        emitted = 0
        # short-circuit via circuit breaker
        now = self.collector._monotonic()
        if not self.circuit.allow(now):
            raise CircuitOpenError("circuit is open; refusing to call upstream")

        # resume cursor if present
        cursor = chunk.cursor or self.pager.last_cursor()
        last_ts = self.pager.last_ts() or chunk.start

        while True:
            request = self.build_request(
                IsoRequestChunk(start=last_ts, end=chunk.end, cursor=cursor, params=chunk.params)
            )
            try:
                response = self.collector.request(request)
                records, next_cursor = self.parse_page(response.json() or {})
                if not records:
                    self.circuit.record_success()
                    break
                emitted += self.collector.produce_records(
                    records,
                    value_transform=self.value_transform,
                    key_fn=self.key_fn,
                    headers_fn=self.headers_fn,
                    flush=False,
                )
                # advance state
                max_ts = max((r.get("interval_end") or r.get("interval_start") or last_ts) for r in records)
                if isinstance(max_ts, (int, float)):
                    # ts in micros
                    max_ts = datetime.fromtimestamp(max_ts / 1_000_000.0, tz=timezone.utc)
                last_ts = max_ts or last_ts
                cursor = next_cursor
                # persist checkpoint every page
                self.pager.commit(last_page=cursor, last_ts=last_ts, metadata={"window_end": chunk.end.isoformat()})
                self.circuit.record_success()
                if not next_cursor:
                    break
            except HttpRequestError as exc:  # upstream failure with retry exhaustion
                self.circuit.record_error(self.collector._monotonic())
                raise

        if flush and emitted:
            self.collector.flush()
        return emitted

    # --- Chunking helpers ---
    @staticmethod
    def hourly_chunks(start: datetime, end: datetime, *, step_hours: int = 1) -> Iterator[IsoRequestChunk]:
        cur = start
        while cur < end:
            nxt = min(end, cur + timedelta(hours=step_hours))
            yield IsoRequestChunk(start=cur, end=nxt)
            cur = nxt

    @staticmethod
    def daily_chunks(start: datetime, end: datetime, *, step_days: int = 1) -> Iterator[IsoRequestChunk]:
        cur = start
        while cur < end:
            nxt = min(end, cur + timedelta(days=step_days))
            yield IsoRequestChunk(start=cur, end=nxt)
            cur = nxt
