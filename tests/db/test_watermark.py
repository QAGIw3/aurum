from __future__ import annotations

import sys
import types
from datetime import datetime
from typing import Any, List, Optional, Tuple

import pytest

dummy_psycopg = types.SimpleNamespace(connect=lambda *args, **kwargs: None)
sys.modules.setdefault("psycopg", dummy_psycopg)

from aurum.db import get_ingest_watermark, register_ingest_source, update_ingest_watermark


class FakeCursor:
    def __init__(self, results: Optional[List[Tuple[Any, ...]]] = None) -> None:
        self.queries: List[Tuple[str, Tuple[Any, ...]]] = []
        self._results = results or []
        self._result_index = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    def execute(self, query: str, params: Tuple[Any, ...]) -> None:
        self.queries.append((query, params))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        if self._result_index >= len(self._results):
            return None
        result = self._results[self._result_index]
        self._result_index += 1
        return result


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_obj = cursor
        self.commits = 0

    def cursor(self):  # type: ignore[override]
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        pass


@pytest.fixture
def patch_psycopg(monkeypatch: pytest.MonkeyPatch) -> FakeCursor:
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    def fake_connect(*args: Any, **kwargs: Any) -> FakeConnection:
        return conn

    monkeypatch.setattr("psycopg.connect", fake_connect)
    return cursor


def test_register_ingest_source_executes_function(patch_psycopg: FakeCursor) -> None:
    register_ingest_source("vendor_pw", description="PW curves", schedule="daily", target="iceberg")

    assert patch_psycopg.queries == [
        (
            "SELECT public.register_ingest_source(%s, %s, %s, %s)",
            ("vendor_pw", "PW curves", "daily", "iceberg"),
        )
    ]


def test_update_ingest_watermark_executes_function(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    def fake_connect(*args: Any, **kwargs: Any) -> FakeConnection:
        return conn

    monkeypatch.setattr("psycopg.connect", fake_connect)

    watermark = datetime(2024, 1, 1, 0, 0, 0)
    update_ingest_watermark("vendor_pw", "asof", watermark)

    assert cursor.queries == [
        (
            "SELECT public.update_ingest_watermark(%s, %s, %s)",
            ("vendor_pw", "asof", watermark),
        )
    ]
    assert conn.commits == 1


def test_get_ingest_watermark_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = datetime(2024, 1, 1, 0, 0, 0)
    cursor = FakeCursor(results=[(expected,)])
    conn = FakeConnection(cursor)

    def fake_connect(*args: Any, **kwargs: Any) -> FakeConnection:
        return conn

    monkeypatch.setattr("psycopg.connect", fake_connect)

    result = get_ingest_watermark("vendor_pw")
    assert result == expected
    assert cursor.queries == [
        (
            "SELECT public.get_ingest_watermark(%s, %s)",
            ("vendor_pw", "default"),
        )
    ]
