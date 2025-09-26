from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional, Tuple

import pytest
from unittest.mock import AsyncMock

from aurum.api.database.trino_client import SimpleTrinoClient
from aurum.api.database.config import TrinoConfig
import aurum.observability.metrics as metrics
from aurum.telemetry.context import correlation_context, request_id_context
from aurum.observability.enhanced_tracing import TraceContext, current_traceparent
from aurum.api.exceptions import ServiceUnavailableException


pytestmark = pytest.mark.asyncio


class _MetricValue:
    def __init__(self, metric: "_ScalarMetric") -> None:
        self._metric = metric

    def get(self) -> float:
        return self._metric.value


class _ScalarMetric:
    def __init__(self) -> None:
        self.value = 0.0
        self._wrapped = _MetricValue(self)

    @property
    def _value(self) -> _MetricValue:
        return self._wrapped


class ScalarGauge(_ScalarMetric):
    def set(self, value: float) -> None:
        self.value = float(value)


class ScalarCounter(_ScalarMetric):
    def inc(self, amount: float = 1.0) -> None:
        self.value += float(amount)


class LabeledCounter:
    def __init__(self) -> None:
        self._metrics: Dict[Tuple[Tuple[str, str], ...], ScalarCounter] = {}

    def labels(self, **labels: str) -> ScalarCounter:
        key = tuple(sorted((str(k), str(v)) for k, v in labels.items()))
        metric = self._metrics.get(key)
        if metric is None:
            metric = ScalarCounter()
            self._metrics[key] = metric
        return metric


@pytest.fixture(autouse=True)
def _ensure_metrics(monkeypatch: pytest.MonkeyPatch):
    if metrics.PROMETHEUS_AVAILABLE:
        yield
        return

    gauge_entries = ScalarGauge()
    counter_evictions = ScalarCounter()
    counter_hits = LabeledCounter()
    counter_misses = LabeledCounter()

    monkeypatch.setattr(metrics, "PROMETHEUS_AVAILABLE", True)
    monkeypatch.setattr(metrics, "TRINO_PREPARED_CACHE_ENTRIES", gauge_entries, raising=False)
    monkeypatch.setattr(metrics, "TRINO_PREPARED_CACHE_EVICTIONS", counter_evictions, raising=False)
    monkeypatch.setattr(metrics, "TRINO_HOT_CACHE_HITS", counter_hits, raising=False)
    monkeypatch.setattr(metrics, "TRINO_HOT_CACHE_MISSES", counter_misses, raising=False)

    from aurum.api import database as database_pkg  # lazy import for patching
    trino_module = database_pkg.trino_client

    monkeypatch.setattr(trino_module, "TRINO_PREPARED_CACHE_ENTRIES", gauge_entries, raising=False)
    monkeypatch.setattr(trino_module, "TRINO_PREPARED_CACHE_EVICTIONS", counter_evictions, raising=False)

    async def fake_set_entries(count: int) -> None:
        gauge_entries.set(count)

    async def fake_increment_evictions(amount: int = 1) -> None:
        counter_evictions.inc(amount)

    async def fake_increment_hits(tenant: str) -> None:
        counter_hits.labels(tenant=tenant or "unknown").inc()

    async def fake_increment_misses(tenant: str) -> None:
        counter_misses.labels(tenant=tenant or "unknown").inc()

    async def fake_set_breaker(state: str) -> None:
        return None

    monkeypatch.setattr(metrics, "set_trino_circuit_breaker_state", fake_set_breaker, raising=False)
    monkeypatch.setattr(metrics, "set_trino_prepared_cache_entries", fake_set_entries, raising=False)
    monkeypatch.setattr(metrics, "increment_trino_prepared_cache_evictions", fake_increment_evictions, raising=False)
    monkeypatch.setattr(metrics, "increment_trino_hot_cache_hits", fake_increment_hits, raising=False)
    monkeypatch.setattr(metrics, "increment_trino_hot_cache_misses", fake_increment_misses, raising=False)
    monkeypatch.setattr(trino_module, "set_trino_circuit_breaker_state", fake_set_breaker, raising=False)
    monkeypatch.setattr(trino_module, "set_trino_prepared_cache_entries", fake_set_entries, raising=False)
    monkeypatch.setattr(trino_module, "increment_trino_prepared_cache_evictions", fake_increment_evictions, raising=False)
    monkeypatch.setattr(trino_module, "increment_trino_hot_cache_hits", fake_increment_hits, raising=False)
    monkeypatch.setattr(trino_module, "increment_trino_hot_cache_misses", fake_increment_misses, raising=False)

    yield


class StubSession:
    def __init__(self) -> None:
        self.headers: Dict[str, str] = {}


class StubCursor:
    def __init__(self, connection: "StubConnection") -> None:
        self.connection = connection
        self.description = [("value", None, None, None, None, None, None)]
        self.stats = {"queryId": "20240101_000000_test"}
        self._prepared_objects: Dict[str, object] = {}

    def prepare(self, sql: str) -> object:
        obj = object()
        self._prepared_objects[sql] = obj
        return obj

    def execute_prepared(self, prepared: object, params: Dict[str, Any]) -> None:
        self.connection.last_headers = dict(self.connection.session.headers)

    def execute(self, sql: str, params: Dict[str, Any]) -> None:
        self.connection.last_headers = dict(self.connection.session.headers)

    def fetchall(self):
        return [("ok",)]

    def close(self) -> None:
        pass


class StubConnection:
    def __init__(self) -> None:
        self.session = StubSession()
        self._http_session = self.session
        self.last_headers: Dict[str, str] = {}

    def cursor(self) -> StubCursor:
        return StubCursor(self)

    def rollback(self) -> None:
        pass

    def commit(self) -> None:
        pass


async def test_prepared_cache_metrics_track_size_and_evictions(monkeypatch: pytest.MonkeyPatch) -> None:
    metrics.TRINO_PREPARED_CACHE_ENTRIES.set(0)
    baseline_evictions = metrics.TRINO_PREPARED_CACHE_EVICTIONS._value.get()  # type: ignore[attr-defined]

    client = SimpleTrinoClient(
        TrinoConfig(),
        concurrency_config={
            "trino_prepared_statement_cache_size": 2,
            "trino_prepared_cache_metrics_enabled": True,
            "trino_hot_query_cache_enabled": False,
        },
    )

    conn = StubConnection()
    cursor = conn.cursor()

    client._maybe_prepare_statement(conn, cursor, "SELECT 1")
    assert metrics.TRINO_PREPARED_CACHE_ENTRIES._value.get() == 1  # type: ignore[attr-defined]
    client._maybe_prepare_statement(conn, cursor, "SELECT 2")
    assert metrics.TRINO_PREPARED_CACHE_ENTRIES._value.get() == 2  # type: ignore[attr-defined]
    client._maybe_prepare_statement(conn, cursor, "SELECT 3")
    assert metrics.TRINO_PREPARED_CACHE_ENTRIES._value.get() == 2  # type: ignore[attr-defined]
    assert metrics.TRINO_PREPARED_CACHE_EVICTIONS._value.get() == baseline_evictions + 1  # type: ignore[attr-defined]

    client._invalidate_prepared_statement(conn, "SELECT 2")
    assert metrics.TRINO_PREPARED_CACHE_ENTRIES._value.get() == 1  # type: ignore[attr-defined]


async def test_hot_cache_metrics_record_hits_and_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    tenant = "tenant-hot"
    async def record_hit(tenant_value: str) -> None:
        metrics.TRINO_HOT_CACHE_HITS.labels(tenant=tenant_value)._value.get()  # type: ignore[attr-defined]
        metrics.TRINO_HOT_CACHE_HITS.labels(tenant=tenant_value).inc()  # type: ignore[attr-defined]

    async def record_miss(tenant_value: str) -> None:
        metrics.TRINO_HOT_CACHE_MISSES.labels(tenant=tenant_value)._value.get()  # type: ignore[attr-defined]
        metrics.TRINO_HOT_CACHE_MISSES.labels(tenant=tenant_value).inc()  # type: ignore[attr-defined]

    from aurum.api import database as database_pkg
    trino_module = database_pkg.trino_client

    hit_async = AsyncMock(side_effect=record_hit)
    miss_async = AsyncMock(side_effect=record_miss)
    monkeypatch.setattr(trino_module, "increment_trino_hot_cache_hits", hit_async, raising=False)
    monkeypatch.setattr(trino_module, "increment_trino_hot_cache_misses", miss_async, raising=False)

    client = SimpleTrinoClient(
        TrinoConfig(),
        concurrency_config={
            "trino_hot_query_cache_enabled": True,
            "trino_prepared_cache_metrics_enabled": False,
        },
    )

    first_response = [{"value": "cold"}]
    get_hot_mock = AsyncMock(side_effect=[None, first_response])
    store_hot_mock = AsyncMock()
    clear_hot_mock = AsyncMock()
    monkeypatch.setattr(client, "_get_hot_result", get_hot_mock)
    monkeypatch.setattr(client, "_store_hot_result", store_hot_mock)
    monkeypatch.setattr(client, "_clear_hot_cache", clear_hot_mock)

    mock_run = AsyncMock(return_value=first_response)
    monkeypatch.setattr(client, "_run_with_retries", mock_run)

    result_one = await client.execute_query("SELECT 1", params={}, tenant_id=tenant, cache_ttl=0)
    assert result_one == first_response
    assert mock_run.await_count == 1
    assert miss_async.await_count == 1
    assert hit_async.await_count == 0
    assert store_hot_mock.await_count == 1

    hot_mock = AsyncMock(return_value=[{"value": "should-not-be-used"}])
    monkeypatch.setattr(client, "_run_with_retries", hot_mock)
    result_two = await client.execute_query("SELECT 1", params={}, tenant_id=tenant, cache_ttl=0)
    assert result_two == first_response
    assert hot_mock.await_count == 0
    assert hit_async.await_count == 1
    assert miss_async.await_count == 1
    assert get_hot_mock.await_count == 2


async def test_trace_and_tenant_headers_applied(caplog: pytest.LogCaptureFixture) -> None:
    client = SimpleTrinoClient(
        TrinoConfig(),
        concurrency_config={
            "trino_trace_tagging_enabled": True,
            "trino_hot_query_cache_enabled": False,
            "trino_prepared_cache_metrics_enabled": False,
        },
    )

    conn = StubConnection()
    caplog.set_level(logging.INFO, logger="aurum.structured")

    trace_context = TraceContext.generate_new()
    trace_token = current_traceparent.set(trace_context.to_traceparent())
    try:
        with correlation_context(correlation_id="corr-123", tenant_id="tenant-trace"):
            with request_id_context("req-123"):
                rows = await client._run_query(conn, "SELECT 1", params=None, tenant_label="tenant-trace")
                assert rows == [{"value": "ok"}]
    finally:
        current_traceparent.reset(trace_token)

    applied_headers = conn.last_headers
    assert applied_headers.get("X-Trino-Trace-Token") == trace_context.to_traceparent()
    assert "tenant:tenant-trace" in (applied_headers.get("X-Trino-Client-Tags") or "")
    client_info = json.loads(applied_headers.get("X-Trino-Client-Info", "{}"))
    assert client_info["request_id"] == "req-123"
    assert client_info["tenant"] == "tenant-trace"

    assert conn._http_session.headers == {}

    record = next(
        (
            json.loads(rec.message)
            for rec in caplog.records
            if "trino_query_completed" in rec.message
        ),
        None,
    )
    assert record is not None
    assert record["event"] == "trino_query_completed"
    assert record["query_id"] == "20240101_000000_test"
    assert record["tenant"] == "tenant-trace"
    assert record["traceparent"] == trace_context.to_traceparent()


async def test_execute_query_uses_ttl_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SimpleTrinoClient(TrinoConfig())

    async def fake_run_with_retries(sql, params, tenant, timeout):  # type: ignore[no-untyped-def]
        fake_run_with_retries.calls += 1
        return [{"value": fake_run_with_retries.calls}]

    fake_run_with_retries.calls = 0
    monkeypatch.setattr(client, "_run_with_retries", fake_run_with_retries)

    result_one = await client.execute_query("SELECT 1", params={"foo": "bar"})
    result_two = await client.execute_query("SELECT 1", params={"foo": "bar"})

    assert fake_run_with_retries.calls == 1
    assert result_one == result_two


async def test_execute_query_uses_hot_cache_when_ttl_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SimpleTrinoClient(TrinoConfig())
    client._cache.default_ttl = 0

    async def fake_run_with_retries(sql, params, tenant, timeout):  # type: ignore[no-untyped-def]
        fake_run_with_retries.calls += 1
        return [{"value": fake_run_with_retries.calls}]

    fake_run_with_retries.calls = 0
    monkeypatch.setattr(client, "_run_with_retries", fake_run_with_retries)

    first = await client.execute_query("SELECT 1", params={"foo": "bar"})
    second = await client.execute_query("SELECT 1", params={"foo": "bar"})

    assert fake_run_with_retries.calls == 1
    assert first == second


async def test_circuit_breaker_opens_after_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SimpleTrinoClient(
        TrinoConfig(),
        concurrency_config={
            "trino_circuit_breaker_failure_threshold": 1,
            "trino_circuit_breaker_timeout_seconds": 0.05,
            "trino_circuit_breaker_success_threshold": 1,
            "trino_max_retries": 0,
        },
    )

    async def fail_execute(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "_execute_query_with_timeout", fail_execute)

    with pytest.raises(RuntimeError):
        await client.execute_query("SELECT 1", params={})

    assert client.circuit_breaker.is_open()

    with pytest.raises(ServiceUnavailableException):
        await client.execute_query("SELECT 1", params={})
