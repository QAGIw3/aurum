from __future__ import annotations

from pathlib import Path

import pytest

from scripts.kafka import register_schemas as schemas

SCHEMA_ROOT = Path("kafka/schemas")
SUBJECT_FILE = SCHEMA_ROOT / "subjects.json"
EIA_CONFIG = Path("config/eia_ingest_datasets.json")


def test_subject_contracts_match_mapping():
    mapping = schemas.load_subject_mapping(SUBJECT_FILE)
    compatibility = schemas.normalize_compatibility("backward")
    validated = schemas.validate_contracts(mapping, SCHEMA_ROOT, compatibility)

    assert validated, "expected at least one validated subject"
    assert set(validated) == set(mapping)


def test_subject_contracts_include_eia_config():
    mapping = schemas.load_subject_mapping(SUBJECT_FILE)
    mapping.update(schemas.load_eia_subjects(EIA_CONFIG))
    compatibility = schemas.normalize_compatibility("BACKWARD")

    validated = schemas.validate_contracts(mapping, SCHEMA_ROOT, compatibility)
    assert set(mapping) == set(validated)


def test_all_schemas_have_contracts_and_required_fields():
    for schema_path in SCHEMA_ROOT.glob("*.avsc"):
        contract = schemas.find_contract_for_schema(schema_path)
        schema = schemas.load_schema(SCHEMA_ROOT, schema_path.name)
        field_names = {
            field.get("name")
            for field in schema.get("fields", [])
            if isinstance(field, dict) and isinstance(field.get("name"), str)
        }
        missing = contract.required_fields - field_names
        assert not missing, f"{schema_path.name} missing required fields {sorted(missing)}"
        assert contract.subject_patterns, f"{contract.name} missing subject patterns"


def test_subject_names_conform_to_pattern():
    mapping = schemas.load_subject_mapping(SUBJECT_FILE)
    for subject in mapping:
        schemas.validate_subject_name(subject)


@pytest.mark.parametrize(
    "invalid_subject",
    [
        "aurum.CURVE.observation.v1-value",
        "curve.observation.v1",
        "aurum.curve.observation",
    ],
)
def test_invalid_subject_names_raise(invalid_subject: str):
    with pytest.raises(schemas.SchemaRegistryError):
        schemas.validate_subject_name(invalid_subject)
