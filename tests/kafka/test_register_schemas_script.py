from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.kafka import register_schemas as register_module
from scripts.kafka.register_schemas import (
    SchemaRegistryError,
    create_session,
    iter_subjects,
    load_eia_subjects,
    load_schema,
    load_subject_mapping,
    put_compatibility,
    register_schema,
)


def test_load_subject_mapping(tmp_path: Path) -> None:
    mapping_path = tmp_path / "subjects.json"
    mapping_path.write_text(json.dumps({"foo": "bar.avsc"}), encoding="utf-8")

    mapping = load_subject_mapping(mapping_path)
    assert mapping == {"foo": "bar.avsc"}


def test_load_subject_mapping_invalid_type(tmp_path: Path) -> None:
    mapping_path = tmp_path / "subjects.json"
    mapping_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")

    with pytest.raises(SchemaRegistryError):
        load_subject_mapping(mapping_path)


def test_load_schema(tmp_path: Path) -> None:
    schema_root = tmp_path
    schema_path = schema_root / "example.avsc"
    schema_path.write_text(json.dumps({"type": "record", "name": "Foo", "fields": []}), encoding="utf-8")

    schema = load_schema(schema_root, "example.avsc")
    assert schema["name"] == "Foo"


def test_load_eia_subjects(tmp_path: Path) -> None:
    config_path = tmp_path / "eia.json"
    config_path.write_text(
        json.dumps({"datasets": [{"default_topic": "aurum.ref.eia.test.v1"}]}),
        encoding="utf-8",
    )

    subjects = load_eia_subjects(config_path)
    assert subjects == {"aurum.ref.eia.test.v1-value": "eia.series.v1.avsc"}


def test_iter_subjects_filters() -> None:
    mapping = {"a": "one.avsc", "b": "two.avsc"}
    filtered = iter_subjects(["b"], mapping)
    assert filtered == {"b": "two.avsc"}


def test_iter_subjects_unknown_subject() -> None:
    with pytest.raises(SchemaRegistryError):
        iter_subjects(["missing"], {"a": "file"})


class DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "OK", json_data: dict | None = None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data or {"id": 1, "version": 1}

    def json(self) -> dict:
        return self._json_data


class DummySession:
    def __init__(self) -> None:
        self.put_calls: list[tuple[str, dict]] = []
        self.post_calls: list[tuple[str, dict]] = []
        self.put_response = DummyResponse()
        self.post_response = DummyResponse(json_data={"id": 42})

    def put(self, url: str, json: dict, timeout: int) -> DummyResponse:  # type: ignore[override]
        self.put_calls.append((url, json))
        return self.put_response

    def post(self, url: str, json: dict, timeout: int) -> DummyResponse:  # type: ignore[override]
        self.post_calls.append((url, json))
        return self.post_response


def test_put_compatibility_and_register_schema() -> None:
    session = DummySession()

    put_compatibility("http://registry:8081", "test.subject", "BACKWARD", session=session)
    version = register_schema("http://registry:8081", "test.subject", {"type": "record"}, session=session)

    assert session.put_calls[0][0].endswith("/config/test.subject")
    assert session.post_calls[0][0].endswith("/subjects/test.subject/versions")
    assert version == 42


def test_register_schema_failure_raises() -> None:
    session = DummySession()
    session.post_response = DummyResponse(status_code=500, text="boom")

    with pytest.raises(SchemaRegistryError):
        register_schema("http://registry:8081", "bad.subject", {"type": "record"}, session=session)


def test_create_session_without_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(register_module, "requests", None)

    session = create_session()

    assert isinstance(session, register_module._UrllibSession)
