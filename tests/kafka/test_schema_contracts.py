from __future__ import annotations

from pathlib import Path

import pytest

from scripts.kafka import register_schemas as schemas

SCHEMA_ROOT = Path("kafka/schemas")
SUBJECT_FILE = SCHEMA_ROOT / "subjects.json"
CONTRACT_FILE = SCHEMA_ROOT / "contracts.yml"
EIA_CONFIG = Path("config/eia_ingest_datasets.json")


def test_subject_contracts_match_mapping():
    catalogue = schemas.load_contract_subjects(CONTRACT_FILE)
    validated = schemas.validate_contracts(
        catalogue.subjects,
        SCHEMA_ROOT,
        catalogue.default_compatibility,
        subject_pattern=catalogue.subject_pattern,
    )

    assert validated, "expected at least one validated subject"
    assert set(validated) == set(catalogue.subjects)


def test_subject_contracts_include_eia_config():
    catalogue = schemas.load_contract_subjects(CONTRACT_FILE)
    subjects = dict(catalogue.subjects)
    for subject, schema_file in schemas.load_eia_subjects(EIA_CONFIG).items():
        subjects[subject] = schemas.ContractSubject(
            schema=schema_file,
            topic=schemas.infer_topic_from_subject(subject),
            compatibility=catalogue.default_compatibility,
        )

    validated = schemas.validate_contracts(
        subjects,
        SCHEMA_ROOT,
        catalogue.default_compatibility,
        subject_pattern=catalogue.subject_pattern,
    )
    assert set(subjects) == set(validated)


def test_subject_mapping_json_aligns_with_contracts():
    json_mapping = schemas.load_subject_mapping(SUBJECT_FILE)
    catalogue = schemas.load_contract_subjects(CONTRACT_FILE)

    assert set(json_mapping) == set(catalogue.subjects)
    for subject, schema_file in json_mapping.items():
        contract_entry = catalogue.subjects[subject]
        assert contract_entry.schema == schema_file
        assert schemas.subject_matches_topic(subject, contract_entry.topic)


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
    catalogue = schemas.load_contract_subjects(CONTRACT_FILE)
    for subject in catalogue.subjects:
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
