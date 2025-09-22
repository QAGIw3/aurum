"""Tests for scenario request payload generation."""

from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path('src').resolve()))
os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("confluent_kafka.avro", MagicMock())
sys.modules.setdefault("confluent_kafka.schema_registry", MagicMock())
sys.modules.setdefault("confluent_kafka.schema_registry.avro", MagicMock())

logging_stub = types.ModuleType("aurum.logging")
logging_stub.StructuredLogger = MagicMock()
logging_stub.LogLevel = MagicMock()
logging_stub.create_logger = MagicMock(return_value=MagicMock())
sys.modules.setdefault("aurum.logging", logging_stub)

models_stub = types.ModuleType("aurum.api.models")

class _AurumBaseModel:
    pass

models_stub.AurumBaseModel = _AurumBaseModel
sys.modules.setdefault("aurum.api.models", models_stub)

import aurum.api.scenario_models  # preload to avoid circular imports during tests

from aurum.api.scenario_service import (
    ScenarioRecord,
    ScenarioRunRecord,
    ScenarioStatus,
    ScenarioRunStatus,
    _scenario_request_payload,
)
from aurum.schema_registry.codegen import get_model
from aurum.scenarios import DriverType, ScenarioAssumption


def test_scenario_request_payload_matches_contract() -> None:
    scenario = ScenarioRecord(
        id="scenario-1",
        tenant_id="tenant-1",
        name="Test Scenario",
        description=None,
        assumptions=[
            ScenarioAssumption(
                driver_type=DriverType.POLICY,
                payload={"policy_name": "policy", "description": "desc"},
                version="v1",
            )
        ],
        status=ScenarioStatus.CREATED,
    )
    run = ScenarioRunRecord(run_id="run-1", scenario_id=scenario.id, state=ScenarioRunStatus.QUEUED)

    payload = _scenario_request_payload(scenario, run)

    assert payload["scenario_id"] == scenario.id
    assert payload["tenant_id"] == scenario.tenant_id
    assert isinstance(payload["submitted_ts"], int)

    model = get_model("aurum.scenario.request.v1-value")
    hydrated_payload = payload.copy()
    hydrated_payload["submitted_ts"] = datetime.fromtimestamp(
        payload["submitted_ts"] / 1_000_000,
        tz=timezone.utc,
    )
    model(**hydrated_payload)
