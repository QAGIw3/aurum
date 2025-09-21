
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock

import pytest

from aurum.external.collect.checkpoints import (
    Checkpoint,
    PostgresCheckpointStore,
    RedisCheckpointStore,
)


def test_redis_checkpoint_store_round_trip() -> None:
    client = Mock()
    client.hgetall.return_value = {
        b"last_timestamp": b"2024-01-01T00:00:00+00:00",
        b"last_page": b"42",
        b"metadata": json.dumps({"cursor": "abc"}).encode("utf-8"),
    }
    store = RedisCheckpointStore(client, namespace="aurum:test")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    checkpoint = Checkpoint(
        provider="provider",
        series_id="series",
        last_timestamp=ts,
        last_page="42",
        metadata={"cursor": "abc"},
    )

    store.set(checkpoint)

    client.hset.assert_called_once()
    key_arg = client.hset.call_args.args[0]
    mapping = client.hset.call_args.kwargs["mapping"]
    assert key_arg == "aurum:test:provider:series"
    assert mapping["last_timestamp"] == ts.isoformat()
    assert mapping["last_page"] == "42"
    assert "updated_at" in mapping
    assert json.loads(mapping["metadata"]) == {"cursor": "abc"}

    result = store.get("provider", "series")
    assert result == checkpoint


def test_redis_checkpoint_store_missing_returns_none() -> None:
    client = Mock()
    client.hgetall.return_value = {}
    store = RedisCheckpointStore(client)

    assert store.get("provider", "unknown") is None


class FakeCursor:
    def __init__(self, *, fetchone_value: Optional[Tuple[Any, ...]] = None) -> None:
        self.queries: List[Tuple[str, Tuple[Any, ...]] | Tuple[str, None]] = []
        self.fetchone_value = fetchone_value

    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        self.queries.append((" ".join(query.split()), params))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self.fetchone_value

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConnection:
    def __init__(self, *, fetchone_value: Optional[Tuple[Any, ...]] = None) -> None:
        self.fetchone_value = fetchone_value
        self.cursors: List[FakeCursor] = []
        self.closed = False
        self.commit_calls = 0

    def cursor(self) -> FakeCursor:
        cursor = FakeCursor(fetchone_value=self.fetchone_value)
        self.cursors.append(cursor)
        self.fetchone_value = None
        return cursor

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


class ScriptedConnectionFactory:
    def __init__(self) -> None:
        self._queue: List[Optional[Tuple[Any, ...]]] = []
        self.connections: List[FakeConnection] = []

    def queue_fetchone(self, value: Optional[Tuple[Any, ...]]) -> None:
        self._queue.append(value)

    def __call__(self) -> FakeConnection:
        fetch = self._queue.pop(0) if self._queue else None
        connection = FakeConnection(fetchone_value=fetch)
        self.connections.append(connection)
        return connection


def test_postgres_checkpoint_store_set_and_get() -> None:
    factory = ScriptedConnectionFactory()
    store = PostgresCheckpointStore(connection_factory=factory, ensure_schema=True)

    # Ensure schema creation invoked
    ddl_cursor = factory.connections[0].cursors[0]
    assert "CREATE TABLE IF NOT EXISTS" in ddl_cursor.queries[0][0]
    assert factory.connections[0].commit_calls == 1

    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    checkpoint = Checkpoint(
        provider="provider",
        series_id="series",
        last_timestamp=ts,
        last_page="7",
        metadata={"window": "weekly"},
    )

    store.set(checkpoint)
    insert_cursor = factory.connections[1].cursors[0]
    insert_query, insert_params = insert_cursor.queries[0]
    assert "INSERT INTO" in insert_query
    assert insert_params == (
        "provider",
        "series",
        ts,
        "7",
        json.dumps({"window": "weekly"}),
    )
    assert factory.connections[1].commit_calls == 1

    factory.queue_fetchone((ts, "7", {"window": "weekly"}))
    result = store.get("provider", "series")
    select_cursor = factory.connections[2].cursors[0]
    select_query, select_params = select_cursor.queries[0]
    assert "SELECT" in select_query
    assert select_params == ("provider", "series")
    assert result == checkpoint


def test_postgres_checkpoint_store_get_missing_returns_none() -> None:
    factory = ScriptedConnectionFactory()
    store = PostgresCheckpointStore(connection_factory=factory, ensure_schema=False)

    factory.queue_fetchone(None)
    assert store.get("provider", "missing") is None
