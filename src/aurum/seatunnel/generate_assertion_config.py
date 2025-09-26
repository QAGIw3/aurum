from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from .assertions import (
    AssertionSeverity,
    AssertionType,
    DataQualityChecker,
    FieldAssertion,
)
from .transforms import DataQualityTransform


class AssertionConfigGenerator:
    """Utility to derive assertion configs and transforms from Avro schemas."""

    def __init__(self, *, default_quality_threshold: float = 0.95) -> None:
        self.default_quality_threshold = default_quality_threshold

    # Helper methods
    def _load_schema(self, schema_path: Path) -> Dict[str, Any]:
        with Path(schema_path).open(encoding="utf-8") as fh:
            return json.load(fh)

    def _build_field_assertions(self, schema: Dict[str, Any]) -> List[FieldAssertion]:
        checker = DataQualityChecker()
        temp_assertion = checker.create_assertion_from_schema(schema, name="_temp")
        return temp_assertion.field_assertions

    def _field_assertion_to_dict(self, assertion: FieldAssertion) -> Dict[str, Any]:
        payload = {
            "field_name": assertion.field_name,
            "assertion_type": assertion.assertion_type.value,
            "severity": assertion.severity.value,
            "required": assertion.required,
            "allow_null": assertion.allow_null,
            "expected_type": assertion.expected_type,
            "min_value": assertion.min_value,
            "max_value": assertion.max_value,
            "allowed_values": assertion.allowed_values,
            "regex_pattern": assertion.regex_pattern,
        }
        if assertion.assertion_type == AssertionType.CUSTOM and assertion.custom_message:
            payload["custom_message"] = assertion.custom_message
        return payload

    def generate_field_presence_checks(self, schema: Dict[str, Any]) -> str:
        clauses: List[str] = []
        for field in schema.get("fields", []):
            name = field["name"]
            clauses.append(
                f"CASE WHEN {name} IS NOT NULL THEN 1 ELSE 0 END as {name}_present"
            )
        select_body = ",\n    ".join(clauses) if clauses else "1 as no_fields"
        return (
            "SELECT\n"
            f"    {select_body}\n"
            "FROM input_table"
        )

    def generate_field_type_checks(self, schema: Dict[str, Any]) -> str:
        clauses: List[str] = []
        type_mapping = {
            "string": "string",
            "int": "integer",
            "long": "integer",
            "float": "real",
            "double": "real",
            "boolean": "boolean",
        }
        for field in schema.get("fields", []):
            name = field["name"]
            field_type = field["type"]
            base_type = self._primary_type(field_type)
            mapped = type_mapping.get(base_type.lower(), "string")
            clauses.append(
                f"CASE WHEN {name} IS NULL OR typeof({name}) = '{mapped}' THEN 1 ELSE 0 END as {name}_type_valid"
            )
        select_body = ",\n    ".join(clauses) if clauses else "1 as no_fields"
        return (
            "SELECT\n"
            f"    {select_body}\n"
            "FROM input_table"
        )

    def _primary_type(self, field_type: Any) -> str:
        if isinstance(field_type, list):
            for candidate in field_type:
                if candidate != "null":
                    return str(candidate)
            return "string"
        return str(field_type)

    def generate_from_schema(self, schema_path: Path, assertion_name: str) -> Dict[str, Any]:
        schema = self._load_schema(schema_path)
        field_assertions = self._build_field_assertions(schema)

        schema_assertion = DataQualityChecker().create_assertion_from_schema(
            schema,
            name=assertion_name,
        )
        schema_assertion.quality_threshold = self.default_quality_threshold

        transform = DataQualityTransform.create_transform(schema_assertion)

        return {
            "assertion_name": assertion_name,
            "schema_name": schema.get("name"),
            "schema_namespace": schema.get("namespace"),
            "field_assertions": [self._field_assertion_to_dict(a) for a in field_assertions],
            "seatunnel_transforms": [transform] if transform else [],
            "schema": schema,
        }
