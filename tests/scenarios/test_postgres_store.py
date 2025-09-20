from __future__ import annotations

import os
import uuid
from typing import Iterator

import pytest

pytest.importorskip("psycopg", reason="psycopg not installed")

from aurum.api.scenario_service import PostgresScenarioStore
from aurum.scenarios.models import DriverType, ScenarioAssumption


@pytest.fixture()
def postgres_store(monkeypatch: pytest.MonkeyPatch) -> Iterator[PostgresScenarioStore]:
    import psycopg

    dsn = os.getenv("AURUM_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set AURUM_TEST_POSTGRES_DSN to run Postgres-backed scenario store tests")

    schema = f"aurum_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scenario (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    tenant_id UUID NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scenario_driver (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (name, type)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scenario_assumption_value (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
                    driver_id UUID NOT NULL REFERENCES scenario_driver(id) ON DELETE RESTRICT,
                    payload JSONB NOT NULL,
                    version TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (scenario_id, driver_id, version)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS model_run (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    scenario_id UUID NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
                    curve_def_id UUID,
                    code_version TEXT NOT NULL,
                    seed BIGINT,
                    state TEXT NOT NULL,
                    version_hash TEXT NOT NULL,
                    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    error TEXT
                )
                """
            )

    store_dsn = psycopg.conninfo.make_conninfo(dsn, options=f"-csearch_path={schema}")
    monkeypatch.setenv("AURUM_SCENARIO_REQUESTS_ENABLED", "0")
    monkeypatch.setenv("AURUM_SCENARIO_STATUS_ENABLED", "0")

    store = PostgresScenarioStore(store_dsn)
    try:
        yield store
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _policy_assumption(policy_name: str) -> ScenarioAssumption:
    return ScenarioAssumption(driver_type=DriverType.POLICY, payload={"policy_name": policy_name})


def test_postgres_store_create_and_fetch(postgres_store: PostgresScenarioStore) -> None:
    tenant_id = str(uuid.uuid4())
    record = postgres_store.create_scenario(
        tenant_id=tenant_id,
        name="Test Scenario",
        description="Example",
        assumptions=[_policy_assumption("RPS")],
    )

    fetched = postgres_store.get_scenario(record.id, tenant_id=tenant_id)
    assert fetched is not None
    assert fetched.name == "Test Scenario"
    assert fetched.assumptions and fetched.assumptions[0].payload["policy_name"] == "RPS"

    listed = postgres_store.list_scenarios(tenant_id, limit=5, offset=0)
    assert any(item.id == record.id for item in listed)


def test_postgres_store_run_lifecycle(postgres_store: PostgresScenarioStore) -> None:
    tenant_id = str(uuid.uuid4())
    scenario = postgres_store.create_scenario(
        tenant_id=tenant_id,
        name="Lifecycle",
        description=None,
        assumptions=[_policy_assumption("Lifecycle")],
    )

    run = postgres_store.create_run(
        scenario_id=scenario.id,
        tenant_id=tenant_id,
        code_version="v1",
        seed=123,
    )
    assert run.state == "QUEUED"

    updated = postgres_store.update_run_state(run.run_id, state="RUNNING", tenant_id=tenant_id)
    assert updated is not None and updated.state == "RUNNING"

    runs = postgres_store.list_runs(tenant_id, scenario_id=scenario.id, limit=5, offset=0)
    assert runs and runs[0].run_id == run.run_id

    fetched = postgres_store.get_run_for_scenario(scenario.id, run.run_id, tenant_id=tenant_id)
    assert fetched is not None
    assert fetched.state == "RUNNING"
