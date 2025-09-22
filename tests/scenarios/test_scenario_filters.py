from __future__ import annotations

from datetime import datetime, timedelta, timezone

from aurum.api.scenario_service import InMemoryScenarioStore
from aurum.scenarios.models import DriverType, ScenarioAssumption


def _make_assumption(name: str) -> ScenarioAssumption:
    return ScenarioAssumption(driver_type=DriverType.POLICY, payload={"policy_name": name})


def test_list_scenarios_filters_by_name_and_tag():
    store = InMemoryScenarioStore()
    now = datetime.now(timezone.utc)

    record_a = store.create_scenario(
        tenant_id="tenant-1",
        name="Northwind Expansion",
        description=None,
        assumptions=[_make_assumption("northwind")],
    )
    record_a.tags = ["expansion", "north"]
    record_a.created_at = now - timedelta(days=2)
    record_a.updated_at = record_a.created_at

    record_b = store.create_scenario(
        tenant_id="tenant-1",
        name="Southwind Maintenance",
        description=None,
        assumptions=[_make_assumption("southwind")],
    )
    record_b.tags = ["maintenance"]
    record_b.created_at = now - timedelta(days=1)
    record_b.updated_at = record_b.created_at

    results = store.list_scenarios(
        tenant_id="tenant-1",
        name_contains="north",
        tag="expansion",
    )

    assert [scenario.id for scenario in results] == [record_a.id]


def test_list_scenarios_created_window():
    store = InMemoryScenarioStore()
    now = datetime.now(timezone.utc)

    early = store.create_scenario(
        tenant_id="tenant-1",
        name="Scenario Early",
        description=None,
        assumptions=[_make_assumption("early")],
    )
    early.created_at = now - timedelta(days=5)
    early.updated_at = early.created_at

    mid = store.create_scenario(
        tenant_id="tenant-1",
        name="Scenario Mid",
        description=None,
        assumptions=[_make_assumption("mid")],
    )
    mid.created_at = now - timedelta(days=2)
    mid.updated_at = mid.created_at

    late = store.create_scenario(
        tenant_id="tenant-1",
        name="Scenario Late",
        description=None,
        assumptions=[_make_assumption("late")],
    )
    late.created_at = now - timedelta(hours=12)
    late.updated_at = late.created_at

    results = store.list_scenarios(
        tenant_id="tenant-1",
        created_after=now - timedelta(days=3),
        created_before=now - timedelta(hours=1),
    )

    assert [scenario.id for scenario in results] == [mid.id]


def test_list_runs_respects_created_filters():
    store = InMemoryScenarioStore()
    now = datetime.now(timezone.utc)
    scenario = store.create_scenario(
        tenant_id="tenant-1",
        name="Scenario",
        description=None,
        assumptions=[_make_assumption("baseline")],
    )

    run_a = store.create_run(
        scenario.id,
        code_version="v1",
        seed=None,
        tenant_id="tenant-1",
    )
    run_a.created_at = now - timedelta(days=2)
    run_a.queued_at = run_a.created_at

    run_b = store.create_run(
        scenario.id,
        code_version="v1",
        seed=None,
        tenant_id="tenant-1",
    )
    run_b.created_at = now - timedelta(hours=6)
    run_b.queued_at = run_b.created_at

    runs = store.list_runs(
        tenant_id="tenant-1",
        scenario_id=scenario.id,
        created_after=now - timedelta(days=1),
    )

    assert [run.run_id for run in runs] == [run_b.run_id]
