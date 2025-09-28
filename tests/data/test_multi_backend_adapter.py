"""Multi-backend adapter tests for latency and fallback across engines.

Covers Trino, ClickHouse, and Timescale adapter fallbacks on errors,
timeouts, and latency thresholds.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pytest
import pytest_asyncio
from unittest.mock import patch

from aurum.core import AurumSettings
from aurum.data import QueryResult
from aurum.data.backend_adapter import MultiBackendAdapter


class _StubBackend:
    def __init__(self, backend_name: str):
        self._name = backend_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def supports_cursor_pagination(self) -> bool:
        return True

    async def close(self) -> None:
        return None


def make_success_backend(backend_name: str, *, columns: Optional[List[str]] = None, rows: Optional[List[tuple]] = None, delay_ms: float = 0.0):
    columns = columns or ["id", "value"]
    rows = rows or [(1, "a"), (2, "b")]

    class _SuccessBackend(_StubBackend):
        async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
            return QueryResult(columns=columns, rows=rows, metadata={"backend": backend_name})

    return _SuccessBackend(backend_name)


def make_error_backend(backend_name: str, message: str = "backend error"):
    class _ErrorBackend(_StubBackend):
        async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
            raise RuntimeError(message)

    return _ErrorBackend(backend_name)


@pytest.mark.asyncio
async def test_fallback_on_error():
    """Falls back to next backend when primary errors."""

    stubs: Dict[str, Any] = {
        "trino": make_error_backend("trino", "boom"),
        "clickhouse": make_success_backend("clickhouse"),
        "timescale": make_success_backend("timescale"),
    }

    def _get_backend(backend_type: str, *_args, **_kwargs):
        return stubs[backend_type]

    with patch("aurum.data.backend_adapter.get_backend", side_effect=_get_backend):
        settings = AurumSettings.from_env()
        adapter = MultiBackendAdapter(settings, backend_order=["trino", "clickhouse", "timescale"])
        result = await adapter.execute_query("SELECT 1")

    assert result["metadata"]["used_backend"] == "clickhouse"
    assert result["metadata"]["fallback_chain"][0]["backend"] == "trino"
    assert result["metadata"]["fallback_chain"][0]["reason"] == "error"


@pytest.mark.asyncio
async def test_fallback_on_latency_threshold():
    """Falls back when primary exceeds latency threshold."""

    stubs: Dict[str, Any] = {
        "trino": make_success_backend("trino", delay_ms=200.0),
        "clickhouse": make_success_backend("clickhouse", delay_ms=0.0),
        "timescale": make_success_backend("timescale", delay_ms=0.0),
    }

    def _get_backend(backend_type: str, *_args, **_kwargs):
        return stubs[backend_type]

    with patch("aurum.data.backend_adapter.get_backend", side_effect=_get_backend):
        settings = AurumSettings.from_env()
        adapter = MultiBackendAdapter(
            settings,
            backend_order=["trino", "clickhouse", "timescale"],
            latency_threshold_ms=50.0,
            timeout_seconds=1.0,
        )
        result = await adapter.execute_query("SELECT 1")

    assert result["metadata"]["used_backend"] == "clickhouse"
    chain = result["metadata"]["fallback_chain"]
    assert chain[0]["backend"] == "trino"
    assert chain[0]["reason"] == "latency_threshold_exceeded"
    assert chain[0]["elapsed_ms"] >= 200.0


@pytest.mark.asyncio
async def test_fallback_on_timeout():
    """Falls back when primary times out."""

    stubs: Dict[str, Any] = {
        "trino": make_success_backend("trino", delay_ms=200.0),
        "clickhouse": make_success_backend("clickhouse", delay_ms=0.0),
    }

    def _get_backend(backend_type: str, *_args, **_kwargs):
        return stubs[backend_type]

    with patch("aurum.data.backend_adapter.get_backend", side_effect=_get_backend):
        settings = AurumSettings.from_env()
        adapter = MultiBackendAdapter(
            settings,
            backend_order=["trino", "clickhouse"],
            timeout_seconds=0.05,
        )
        result = await adapter.execute_query("SELECT 1")

    assert result["metadata"]["used_backend"] == "clickhouse"
    chain = result["metadata"]["fallback_chain"]
    assert chain[0]["backend"] == "trino"
    assert chain[0]["reason"] == "timeout"


@pytest.mark.asyncio
async def test_primary_success_without_fallback():
    """Uses primary backend when it succeeds under latency threshold."""

    stubs: Dict[str, Any] = {
        "trino": make_success_backend("trino", delay_ms=10.0),
        "clickhouse": make_success_backend("clickhouse", delay_ms=0.0),
    }

    def _get_backend(backend_type: str, *_args, **_kwargs):
        return stubs[backend_type]

    with patch("aurum.data.backend_adapter.get_backend", side_effect=_get_backend):
        settings = AurumSettings.from_env()
        adapter = MultiBackendAdapter(
            settings,
            backend_order=["trino", "clickhouse"],
            latency_threshold_ms=100.0,
            timeout_seconds=1.0,
        )
        result = await adapter.execute_query("SELECT 1")

    assert result["metadata"]["used_backend"] == "trino"
    assert "fallback_chain" not in result["metadata"]

