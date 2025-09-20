from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pydantic", reason="pydantic not installed")

from aurum.api.scenario_service import ScenarioRecord
from aurum.scenarios.models import DriverType, ScenarioAssumption
from aurum.scenarios import worker


def test_request_from_message_converts_days():
    payload = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-1",
        "asof_date": 20000,
        "curve_def_ids": ["curve-a", "curve-b"],
        "assumptions": [],
    }
    req = worker.ScenarioRequest.from_message(payload)
    assert req.scenario_id == "scn-1"
    assert req.tenant_id == "tenant-1"
    assert req.asof_date == date(2024, 10, 4)
    assert req.curve_def_ids == ["curve-a", "curve-b"]


def test_build_outputs_uses_assumptions(monkeypatch):
    scenario = ScenarioRecord(
        id="scn-1",
        tenant_id="tenant-1",
        name="Demo",
        description=None,
        assumptions=[
            ScenarioAssumption(driver_type=DriverType.POLICY, payload={"policy_name": "Clean"}),
            ScenarioAssumption(driver_type=DriverType.LOAD_GROWTH, payload={"annual_growth_pct": 2.5}),
        ],
        status="CREATED",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    request = worker.ScenarioRequest(
        scenario_id="scn-1",
        tenant_id="tenant-1",
        assumptions=[],
        asof_date=date(2025, 1, 1),
        curve_def_ids=None,
    )
    monkeypatch.setattr(worker, "_load_base_mid", lambda *args, **kwargs: 60.0)
    fixed_ts = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(worker, "_current_ts", lambda: fixed_ts)

    settings = worker.WorkerSettings(
        base_value=50.0,
        policy_weight=0.1,
        load_weight=0.05,
        tenor_multipliers={"MONTHLY": 1.0, "QUARTER": 1.1, "CALENDAR": 1.2},
    )

    outputs = worker._build_outputs(scenario, request, run_id="run-xyz", settings=settings)
    assert [record["tenor_type"] for record in outputs] == ["MONTHLY", "QUARTER", "CALENDAR"]

    monthly = outputs[0]
    assert monthly["scenario_id"] == "scn-1"
    assert monthly["tenant_id"] == "tenant-1"
    assert monthly["run_id"] == "run-xyz"
    assert monthly["tenor_label"] == "2025-01"
    assert monthly["value"] == pytest.approx(60.625, rel=1e-6)
    assert monthly["computed_ts"] == fixed_ts

    quarter = outputs[1]
    assert quarter["tenor_label"] == "Q1 2025"
    assert quarter["value"] == pytest.approx(66.6875, rel=1e-6)

    calendar = outputs[2]
    assert calendar["tenor_label"] == "Calendar 2025"
    assert calendar["value"] == pytest.approx(72.75, rel=1e-6)
    assert calendar["run_id"] == "run-xyz"

    for record in outputs:
        assert record["band_lower"] < record["value"] < record["band_upper"]
        assert record["_ingest_ts"] == fixed_ts

    components = {item["component"]: item["delta"] for item in monthly["attribution"]}
    assert pytest.approx(components["policy"], rel=1e-6) == 0.5
    assert pytest.approx(components["load_growth"], rel=1e-6) == 0.125


def test_to_avro_payload_converts_types():
    computed_ts = datetime.now(timezone.utc)
    record = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-1",
        "run_id": "run-abc",
        "asof_date": date(2025, 1, 1),
        "curve_key": "curve-a",
        "tenor_type": "MONTHLY",
        "contract_month": date(2025, 1, 1),
        "tenor_label": "2025-01",
        "metric": "mid",
        "value": 55.0,
        "band_lower": 50.0,
        "band_upper": 60.0,
        "attribution": [{"component": "policy", "delta": 0.1}],
        "version_hash": "abcd",
        "computed_ts": computed_ts,
    }
    payload = worker._to_avro_payload(record)
    assert payload["asof_date"] == worker._date_to_days(date(2025, 1, 1))
    assert payload["contract_month"] == worker._date_to_days(date(2025, 1, 1))
    assert isinstance(payload["computed_ts"], int)
    assert payload["attribution"][0]["component"] == "policy"
    assert payload["run_id"] == "run-abc"


def test_worker_settings_from_env(monkeypatch):
    monkeypatch.setenv("AURUM_SCENARIO_POLICY_WEIGHT", "0.2")
    monkeypatch.setenv("AURUM_SCENARIO_LOAD_WEIGHT", "0.1")
    monkeypatch.setenv("AURUM_SCENARIO_BASE_VALUE", "42")
    monkeypatch.setenv("AURUM_SCENARIO_TENOR_WEIGHTS", "MONTHLY:1.0,QUARTER:1.5,CALENDAR:2.0")

    settings = worker.WorkerSettings.from_env()
    assert settings.policy_weight == pytest.approx(0.2)
    assert settings.load_weight == pytest.approx(0.1)
    assert settings.base_value == pytest.approx(42.0)
    assert settings.tenor_multipliers["QUARTER"] == pytest.approx(1.5)
    assert settings.tenor_multipliers["CALENDAR"] == pytest.approx(2.0)


def test_build_outputs_extended_driver_effects(monkeypatch):
    scenario = ScenarioRecord(
        id="scn-2",
        tenant_id="tenant-9",
        name="Extended",
        description=None,
        assumptions=[
            ScenarioAssumption(
                driver_type=DriverType.LOAD_GROWTH,
                payload={"annual_growth_pct": 4.0, "start_year": 2024, "end_year": 2026},
            ),
            ScenarioAssumption(
                driver_type=DriverType.FUEL_CURVE,
                payload={
                    "fuel": "natural_gas",
                    "reference_series": "henry_hub",
                    "basis_points_adjustment": 100,
                },
            ),
            ScenarioAssumption(
                driver_type=DriverType.FLEET_CHANGE,
                payload={
                    "fleet_id": "fleet-1",
                    "change_type": "retire",
                    "effective_date": "2025-12-01",
                    "capacity_mw": 120,
                },
            ),
        ],
        status="CREATED",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    request = worker.ScenarioRequest(
        scenario_id="scn-2",
        tenant_id="tenant-9",
        assumptions=[],
        asof_date=date(2025, 6, 1),
        curve_def_ids=None,
    )

    monkeypatch.setattr(worker, "_load_base_mid", lambda *args, **kwargs: None)
    fixed_ts = datetime(2025, 6, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(worker, "_current_ts", lambda: fixed_ts)

    settings = worker.WorkerSettings(
        base_value=45.0,
        policy_weight=0.1,
        load_weight=0.05,
        tenor_multipliers={"MONTHLY": 1.0},
    )

    outputs = worker._build_outputs(scenario, request, run_id="run-123", settings=settings)
    assert len(outputs) == 1
    record = outputs[0]
    assert record["value"] == pytest.approx(44.4263, rel=1e-6)

    attribution = {item["component"]: item["delta"] for item in record["attribution"]}
    assert attribution["load_growth"] == pytest.approx(0.1333, rel=1e-4)
    assert attribution["fuel_curve"] == pytest.approx(0.6430, rel=1e-4)
    assert attribution["fleet_change"] == pytest.approx(-1.35, rel=1e-6)
