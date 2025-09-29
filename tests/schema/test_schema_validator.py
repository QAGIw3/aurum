from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from aurum.events.streaming import EventEnvelope, SchemaValidationError, SchemaValidator
from aurum.schema_registry import SubjectContracts


@pytest.fixture(scope="module")
def schema_validator() -> SchemaValidator:
    contracts = SubjectContracts(Path("kafka/schemas/contracts.yml"))
    return SchemaValidator(contracts=contracts, enforce=True)


def _valid_alert_payload() -> dict[str, object]:
    return {
        "alert_id": "alert-123",
        "tenant_id": None,
        "category": "DATA_QUALITY",
        "severity": "WARN",
        "source": "scenario-pilot",
        "message": "Scenario pipeline replay detected drift",
        "payload": None,
        "created_ts": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
    }


def test_schema_validator_accepts_valid_payload(schema_validator: SchemaValidator) -> None:
    envelope = EventEnvelope(
        topic="aurum.alert.v1",
        payload=_valid_alert_payload(),
        schema_subject="aurum.alert.v1-value",
    )
    schema_validator.validate(envelope)


def test_schema_validator_rejects_invalid_payload(schema_validator: SchemaValidator) -> None:
    invalid_payload = _valid_alert_payload()
    invalid_payload.pop("message")
    envelope = EventEnvelope(
        topic="aurum.alert.v1",
        payload=invalid_payload,
        schema_subject="aurum.alert.v1-value",
    )
    with pytest.raises(SchemaValidationError):
        schema_validator.validate(envelope)
