from __future__ import annotations

import types
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

pytest.importorskip("pydantic", reason="pydantic not installed")


class FakeCursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._fetchone_queue: list[dict] = []
        self._fetchall_value: list[dict] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    def execute(self, query: str, params: tuple[Any, ...]) -> None:
        self.queries.append((query.strip(), params))

    def fetchone(self) -> Optional[dict]:
        if not self._fetchone_queue:
            return None
        return self._fetchone_queue.pop(0)

    def fetchall(self) -> list[dict]:
        return self._fetchall_value


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


def _patch_psycopg(monkeypatch: pytest.MonkeyPatch, cursor: FakeCursor) -> None:
    # Patch importlib.import_module used inside PostgresScenarioStore.__init__
    import importlib

    def fake_import_module(name: str):
        if name == "psycopg":
            mod = types.SimpleNamespace()
            def connect(dsn: str, row_factory=None):  # type: ignore[no-redef]
                return FakeConnection(cursor)
            mod.connect = connect
            return mod
        if name == "psycopg.rows":
            rows_mod = types.SimpleNamespace(dict_row=lambda *a, **kw: None)
            return rows_mod
        raise ModuleNotFoundError(name)
    monkeypatch.setattr(importlib, "import_module", fake_import_module)


def test_create_run_inserts_and_returns_record(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.scenario_service import PostgresScenarioStore

    cursor = FakeCursor()
    submitted_at = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    cursor._fetchone_queue = [{"submitted_at": submitted_at}]

    _patch_psycopg(monkeypatch, cursor)
    store = PostgresScenarioStore("postgresql://user:pass@host/db")

    run = store.create_run("scn-1", tenant_id="tenant-1", code_version="abc", seed=42)

    assert run.scenario_id == "scn-1"
    assert run.state == "QUEUED"
    assert run.created_at == submitted_at
    # Validate SQL executed
    assert cursor.queries[0][0].startswith("SET LOCAL app.current_tenant")
    q, params = cursor.queries[1]
    assert "INSERT INTO model_run" in q
    assert params[1] == "scn-1"


def test_update_run_state_running_and_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.scenario_service import PostgresScenarioStore

    cursor = FakeCursor()
    # First SELECT row for run
    cursor._fetchone_queue = [
        {
            "id": "run-1",
            "scenario_id": "scn-1",
            "code_version": "v1",
            "seed": 7,
            "submitted_at": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            "state": "QUEUED",
            "tenant_id": "tenant-1",
        },
        # Second call for SUCCEEDED path returns same row
        {
            "id": "run-1",
            "scenario_id": "scn-1",
            "code_version": "v1",
            "seed": 7,
            "submitted_at": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            "state": "RUNNING",
            "tenant_id": "tenant-1",
        },
    ]

    _patch_psycopg(monkeypatch, cursor)
    store = PostgresScenarioStore("postgresql://user:pass@host/db")

    run1 = store.update_run_state("run-1", state="RUNNING", tenant_id="tenant-1")
    assert run1 is not None and run1.state == "RUNNING"
    # First three queries: SET + SELECT + UPDATE started_at
    assert cursor.queries[0][0].startswith("SET LOCAL app.current_tenant")
    assert "SELECT" in cursor.queries[1][0]
    assert "UPDATE model_run SET state=%s, started_at=COALESCE(started_at, %s) WHERE id=%s" in cursor.queries[2][0]

    run2 = store.update_run_state("run-1", state="SUCCEEDED", tenant_id="tenant-1")
    assert run2 is not None and run2.state == "SUCCEEDED"
    # Next three queries: SET + SELECT + UPDATE completed_at
    assert cursor.queries[3][0].startswith("SET LOCAL app.current_tenant")
    assert "SELECT" in cursor.queries[4][0]
    assert "UPDATE model_run SET state=%s, completed_at=COALESCE(completed_at, %s) WHERE id=%s" in cursor.queries[5][0]
