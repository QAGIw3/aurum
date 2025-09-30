from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from aurum.external_contracts.merge import TrinoExternalContractsConsumer, MergeSummary


@dataclass
class _DummyCursor:
    statements: list[str]
    _count_value: int = 3

    def execute(self, statement: str) -> None:
        self.statements.append(statement)
        self._last_statement = statement

    def fetchone(self) -> Optional[tuple[int]]:
        if self._last_statement.startswith("SELECT COUNT(*)"):
            return (self._count_value,)
        return None


class _DummyConn:
    def __init__(self, cursor: _DummyCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _DummyCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class _DummyDbapi:
    def __init__(self) -> None:
        self.cursor = _DummyCursor([])

    def connect(self, **kwargs: Any) -> _DummyConn:  # pragma: no cover - simple passthrough
        return _DummyConn(self.cursor)


def test_merge_catalog_creates_view(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_dbapi = _DummyDbapi()
    monkeypatch.setattr("aurum.external_contracts.merge.dbapi", dummy_dbapi)

    consumer = TrinoExternalContractsConsumer(host="localhost", port=8080)
    summary = consumer.merge_catalog("eia", staging_table="external_stage.temp_catalog")

    assert isinstance(summary, MergeSummary)
    assert summary.records_available == 3
    statements = dummy_dbapi.cursor.statements
    assert any("CREATE OR REPLACE VIEW staging_external_series_catalog" in stmt for stmt in statements)
    assert any("DROP VIEW IF EXISTS staging_external_series_catalog" in stmt for stmt in statements)


def test_merge_resolves_simple_table(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_dbapi = _DummyDbapi()
    monkeypatch.setattr("aurum.external_contracts.merge.dbapi", dummy_dbapi)

    consumer = TrinoExternalContractsConsumer(host="localhost", port=8080, staging_schema="scratch")
    summary = consumer.merge_observations("fred")

    assert summary.staging_table == "iceberg.scratch.timeseries_observation_fred"
