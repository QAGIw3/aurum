from __future__ import annotations

import types
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

pytest.importorskip("pydantic", reason="pydantic not installed")

from aurum.scenarios import DriverType


class FakeCursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._fetchone_queue: list[dict] = []
        self._fetchall_value: list[dict] = []
        self._fetchall_queue: list[list[dict]] = []

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
        if self._fetchall_queue:
            return self._fetchall_queue.pop(0)
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


def test_list_scenarios_fetches_assumptions(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.scenario_service import PostgresScenarioStore

    cursor = FakeCursor()
    created_at = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    cursor._fetchall_queue = [
        [
            {
                "id": "scn-1",
                "tenant_id": "tenant-1",
                "name": "Demo",
                "description": None,
                "created_at": created_at,
            }
        ],
        [
            {
                "scenario_id": "scn-1",
                "type": "policy",
                "payload": {"policy_name": "Clean"},
                "version": "v1",
            }
        ],
    ]

    _patch_psycopg(monkeypatch, cursor)
    store = PostgresScenarioStore("postgresql://user:pass@host/db")

    scenarios = store.list_scenarios("tenant-1", limit=5, offset=0)
    assert len(scenarios) == 1
    scenario = scenarios[0]
    assert scenario.assumptions[0].driver_type == DriverType.POLICY
    assert cursor.queries[0][0].startswith("SET LOCAL app.current_tenant")
    assert "FROM scenario" in cursor.queries[1][0]


def test_list_runs_filters_by_scenario(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.scenario_service import PostgresScenarioStore

    cursor = FakeCursor()
    submitted_at = datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc)
    cursor._fetchall_queue = [
        [
            {
                "id": "run-1",
                "scenario_id": "scn-1",
                "state": "SUCCEEDED",
                "code_version": "v1",
                "seed": 99,
                "submitted_at": submitted_at,
            }
        ]
    ]

    _patch_psycopg(monkeypatch, cursor)
    store = PostgresScenarioStore("postgresql://user:pass@host/db")

    runs = store.list_runs("tenant-1", scenario_id="scn-1", limit=10, offset=0)
    assert len(runs) == 1
    assert runs[0].scenario_id == "scn-1"


def test_emit_scenario_request_propagates_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    import types
    import sys

    from aurum.api.scenario_service import ScenarioRecord, ScenarioRunRecord, _maybe_emit_scenario_request
    from aurum.scenarios import DriverType, ScenarioAssumption
    from aurum.telemetry.context import set_request_id, reset_request_id

    monkeypatch.setenv("AURUM_SCENARIO_REQUESTS_ENABLED", "1")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://schema")
    monkeypatch.setenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")

    captured: dict[str, list[tuple[str, bytes]]] = {}

    class DummyAvroProducer:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def produce(self, *, topic, value, headers):
            captured["headers"] = headers

        def flush(self, timeout) -> None:  # pragma: no cover - nothing to do
            return None

    fake_avro_module = types.SimpleNamespace(
        load=lambda path: object(),
        AvroProducer=DummyAvroProducer,
    )
    fake_module = types.SimpleNamespace(avro=fake_avro_module)
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_module)
    monkeypatch.setitem(sys.modules, "confluent_kafka.avro", fake_avro_module)

    scenario = ScenarioRecord(
        id="scn-1",
        tenant_id="tenant-1",
        name="demo",
        description=None,
        assumptions=[ScenarioAssumption(driver_type=DriverType.POLICY, payload={"policy_name": "demo"})],
        status="CREATED",
        created_at=datetime.now(timezone.utc),
    )
    run = ScenarioRunRecord(
        run_id="run-1",
        scenario_id="scn-1",
        state="QUEUED",
        created_at=datetime.now(timezone.utc),
    )

    token = set_request_id("req-123")
    try:
        _maybe_emit_scenario_request(scenario, run)
    finally:
        reset_request_id(token)

    headers = dict(captured.get("headers", []))
    assert headers.get("x-request-id") == b"req-123"


def test_update_run_state_cancelled_sets_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    from aurum.api.scenario_service import PostgresScenarioStore

    cursor = FakeCursor()
    cursor._fetchone_queue = [
        {
            "id": "run-1",
            "scenario_id": "scn-1",
            "code_version": "v1",
            "seed": None,
            "submitted_at": datetime(2025, 3, 1, tzinfo=timezone.utc),
            "state": "QUEUED",
            "tenant_id": "tenant-1",
        }
    ]

    _patch_psycopg(monkeypatch, cursor)
    store = PostgresScenarioStore("postgresql://user:pass@host/db")

    run = store.update_run_state("run-1", state="CANCELLED", tenant_id="tenant-1")
    assert run is not None and run.state == "CANCELLED"
    assert any("completed_at" in query[0] for query in cursor.queries if "UPDATE model_run" in query[0])


def test_create_store_prefers_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    monkeypatch.setenv("AURUM_APP_DB_DSN", "postgresql://user:pass@host/db")

    module = importlib.import_module("aurum.api.scenario_service")

    captured: dict[str, str] = {}

    class FakeStore:
        def __init__(self, dsn: str) -> None:
            captured["dsn"] = dsn

    monkeypatch.setattr(module, "PostgresScenarioStore", FakeStore)

    store = module._create_store()

    assert isinstance(store, FakeStore)
    assert captured.get("dsn") == "postgresql://user:pass@host/db"

    monkeypatch.delenv("AURUM_APP_DB_DSN", raising=False)
