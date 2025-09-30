from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Iterable, List, Sequence

import pytest

from aurum.data import ClickHouseBackend, ConnectionConfig, TimescaleBackend
from aurum.performance.connection_pool import PoolConfig, PoolMetrics


class _DummyQueue:
    def qsize(self) -> int:
        return 0


class _DummyPool:
    def __init__(self, connection: Any):
        self._connection = connection
        self._in_use: set[Any] = set()
        self._pool = _DummyQueue()

    async def acquire(self) -> Any:
        return self._connection

    async def release(self, _connection: Any) -> None:
        return None

    async def close(self) -> None:  # pragma: no cover - not used
        return None


class _DummyClickHouseConnection:
    def __init__(self, rows: Sequence[Sequence[Any]], columns: Sequence[str]):
        self._rows = rows
        self._columns = [(name, "string") for name in columns]
        self.last_kwargs: dict[str, Any] = {}

    def execute(self, query: str, params=None, with_column_types=False, query_id=None):
        self.last_kwargs = {
            "query": query,
            "params": params,
            "with_column_types": with_column_types,
            "query_id": query_id,
        }
        if with_column_types:
            return self._rows, self._columns
        return self._rows


class _DummyTimescaleConnection:
    def __init__(self, rows: Iterable[dict[str, Any]], pid: int = 4242):
        self._rows = [dict(row) for row in rows]
        self._pid = pid

    async def fetch(self, query: str, *params: Any) -> List[dict[str, Any]]:
        self.last_query = query
        self.last_params = params
        return self._rows

    def get_server_pid(self) -> int:
        return self._pid


@pytest.mark.asyncio
async def test_clickhouse_backend_emits_query_id(monkeypatch):
    config = ConnectionConfig(host="localhost", port=9000, database="aurum", username="user", password="pass", ssl=False)
    backend = ClickHouseBackend(config, PoolConfig())
    backend._pool_metrics = PoolMetrics()

    rows = [(1, "curve-x")]
    columns = ["tenant_id", "curve_key"]
    connection = _DummyClickHouseConnection(rows, columns)
    backend._pool = _DummyPool(connection)

    result = await backend.execute_query("SELECT * FROM test")

    assert result.metadata["backend"] == "clickhouse"
    assert result.metadata["query_id"].startswith("ch-")
    assert "pool_metrics" in result.metadata
    assert connection.last_kwargs["with_column_types"] is True
    assert connection.last_kwargs["query_id"] == result.metadata["query_id"]


@pytest.mark.asyncio
async def test_timescale_backend_emits_query_id():
    config = ConnectionConfig(host="localhost", port=5432, database="aurum", username="user", password="pass", ssl=False)
    backend = TimescaleBackend(config, PoolConfig())
    backend._pool_metrics = PoolMetrics()

    rows = [
        {"curve_key": "curve-x", "mid": 100.0},
        {"curve_key": "curve-y", "mid": 88.0},
    ]
    connection = _DummyTimescaleConnection(rows, pid=9999)
    backend._pool = _DummyPool(connection)

    result = await backend.execute_query("SELECT mid FROM market.curve_observation")

    assert result.metadata["backend"] == "timescale"
    assert result.metadata["query_id"].startswith("ts-9999-")
    assert "pool_metrics" in result.metadata
    assert connection.last_query == "SELECT mid FROM market.curve_observation"
