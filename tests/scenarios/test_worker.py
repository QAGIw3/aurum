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
    assert req.asof_date == date(2024, 10, 7)
    assert req.curve_def_ids == ["curve-a", "curve-b"]


def test_build_outputs_uses_assumptions():
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
    outputs = worker._build_outputs(scenario, request)
    assert len(outputs) == 1
    record = outputs[0]
    assert record["scenario_id"] == "scn-1"
    assert record["tenant_id"] == "tenant-1"
    assert record["tenor_label"] == "2025-01"
    assert record["value"] > 50.0
    assert record["band_lower"] < record["value"] < record["band_upper"]
    assert record["computed_ts"].tzinfo is not None


def test_to_avro_payload_converts_types():
    computed_ts = datetime.now(timezone.utc)
    record = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-1",
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
