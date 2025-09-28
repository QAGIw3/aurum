"""Smoke tests for Avro round-trip using the schema registry manager."""

from __future__ import annotations

import json as json_module
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import pytest

fastavro = pytest.importorskip("fastavro")
from fastavro import parse_schema, schemaless_reader, schemaless_writer  # type: ignore[import]

import sys
import types
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

logging_stub = types.ModuleType("aurum.logging")
logging_stub.StructuredLogger = MagicMock()
logging_stub.LogLevel = MagicMock()
logging_stub.create_logger = MagicMock(return_value=MagicMock())
sys.modules.setdefault("aurum.logging", logging_stub)

from aurum.schema_registry import SchemaRegistryConfig, SchemaRegistryManager


SCHEMA_ROOT = Path("kafka/schemas")


@dataclass
class _FakeResponse:
    """Minimal response object compatible with requests.Response."""

    status_code: int
    _json: Dict[str, Any]
    text: str = ""

    def json(self) -> Dict[str, Any]:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(self.text or str(self._json), response=self)


class _InMemorySchemaRegistry:
    """Simple in-memory registry implementing the bits SchemaRegistryManager uses."""

    def __init__(self) -> None:
        self._next_id = 1
        self._schemas_by_id: Dict[int, Dict[str, Any]] = {}
        self._subjects: Dict[str, Dict[str, Any]] = {}
        self.compatibility: Dict[str, str] = {}

    def register(self, subject: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        schema_id = self._next_id
        self._next_id += 1
        record = {"id": schema_id, "version": 1, "schema": schema}
        self._schemas_by_id[schema_id] = schema
        self._subjects[subject] = record
        return record

    def latest(self, subject: str) -> Dict[str, Any] | None:
        return self._subjects.get(subject)

    def schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        return self._schemas_by_id[schema_id]

    def set_compatibility(self, subject: str, level: str) -> None:
        self.compatibility[subject] = level


class _FakeSession:
    """Stub HTTP session that talks to the in-memory registry."""

    def __init__(self, registry: _InMemorySchemaRegistry) -> None:
        self.registry = registry
        self.verify = True
        self.auth = None

    def post(self, url: str, json: Dict[str, Any] | None = None, timeout: int = 0) -> _FakeResponse:
        if "/subjects/" in url and url.endswith("/versions"):
            subject = url.split("/subjects/")[1].split("/versions")[0]
            payload = json or {}
            schema_payload = payload.get("schema")
            if not schema_payload:
                return _FakeResponse(400, {"message": "schema missing"}, text="schema missing")
            schema_dict = json_module.loads(schema_payload)
            record = self.registry.register(subject, schema_dict)
            return _FakeResponse(200, {"id": record["id"], "version": record["version"]})

        if "/compatibility/subjects/" in url:
            return _FakeResponse(200, {"is_compatible": True})

        raise AssertionError(f"Unexpected POST URL: {url}")

    def put(self, url: str, json: Dict[str, Any] | None = None, timeout: int = 0) -> _FakeResponse:
        if "/config/" in url:
            subject = url.split("/config/")[1]
            compatibility = (json or {}).get("compatibility", "BACKWARD")
            self.registry.set_compatibility(subject, compatibility)
            return _FakeResponse(200, {"compatibility": compatibility})

        raise AssertionError(f"Unexpected PUT URL: {url}")

    def get(self, url: str, timeout: int = 0) -> _FakeResponse:
        if "/subjects/" in url and url.endswith("/versions/latest"):
            subject = url.split("/subjects/")[1].split("/versions")[0]
            latest = self.registry.latest(subject)
            if not latest:
                return _FakeResponse(404, {"message": "subject not found"}, text="not found")
            return _FakeResponse(
                200,
                {
                    "subject": subject,
                    "version": latest["version"],
                    "id": latest["id"],
                    "schema": json_module.dumps(latest["schema"]),
                    "compatibility": self.registry.compatibility.get(subject, "BACKWARD"),
                },
            )

        raise AssertionError(f"Unexpected GET URL: {url}")


def _serialize_avro(schema: Dict[str, Any], record: Dict[str, Any], schema_id: int) -> bytes:
    parsed = parse_schema(schema)
    buffer = BytesIO()
    schemaless_writer(buffer, parsed, record)
    return b"\x00" + schema_id.to_bytes(4, "big") + buffer.getvalue()


def _deserialize_avro(registry: _InMemorySchemaRegistry, payload: bytes) -> Dict[str, Any]:
    assert payload[0] == 0, "magic byte should be zero"
    schema_id = int.from_bytes(payload[1:5], "big")
    schema = registry.schema_by_id(schema_id)
    parsed = parse_schema(schema)
    buffer = BytesIO(payload[5:])
    return schemaless_reader(buffer, parsed)


def test_schema_registry_round_trip_fx_rate() -> None:
    """Ensure producer/consumer round-trip works against SchemaRegistryManager."""

    subject = "aurum.ref.fx.rate.v1-value"
    schema_dict = json_module.loads((SCHEMA_ROOT / "fx.rate.v1.avsc").read_text(encoding="utf-8"))

    registry = _InMemorySchemaRegistry()
    session = _FakeSession(registry)

    config = SchemaRegistryConfig(base_url="http://schema-registry.local")
    manager = SchemaRegistryManager(config)
    manager.session = session

    info = manager.register_subject(subject, schema_dict)
    assert info.schema_id == 1
    assert registry.compatibility[subject] == "BACKWARD"

    def _days_since_epoch(value: date) -> int:
        return (value - date(1970, 1, 1)).days

    def _micros_since_epoch(value: datetime) -> int:
        return int(value.astimezone(timezone.utc).timestamp() * 1_000_000)

    record = {
        "base_currency": "USD",
        "quote_currency": "EUR",
        "rate": 0.91,
        "source": "ECB",
        "as_of_date": _days_since_epoch(date(2024, 1, 2)),
        "ingest_ts": _micros_since_epoch(datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc)),
        "metadata": {"provider": "integration-test"},
    }

    encoded = _serialize_avro(schema_dict, record, info.schema_id)
    assert encoded.startswith(b"\x00"), "payload must include Confluent wire-format magic byte"

    decoded = _deserialize_avro(registry, encoded)

    assert decoded["base_currency"] == record["base_currency"]
    assert decoded["quote_currency"] == record["quote_currency"]
    assert pytest.approx(decoded["rate"]) == record["rate"]
    if isinstance(decoded["as_of_date"], date):
        assert _days_since_epoch(decoded["as_of_date"]) == record["as_of_date"]
    else:
        assert decoded["as_of_date"] == record["as_of_date"]

    if isinstance(decoded["ingest_ts"], datetime):
        assert _micros_since_epoch(decoded["ingest_ts"]) == record["ingest_ts"]
    else:
        assert decoded["ingest_ts"] == record["ingest_ts"]
    assert decoded["metadata"] == record["metadata"]
