#!/usr/bin/env python3
"""
Generate SeaTunnel data quality assertion configurations from Avro schemas.

This script analyzes Avro schema files and generates appropriate data quality
assertions for field presence, type checking, and value validation.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.seatunnel.assertions import (
    SchemaAssertion,
    FieldAssertion,
    AssertionType,
    AssertionSeverity
)
from aurum.logging import create_logger


class AssertionConfigGenerator:
    """Generate data quality assertion configurations."""

    def __init__(self):
        """Initialize assertion config generator."""
        self.logger = create_logger(
            source_name="assertion_config_generator",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.seatunnel.config",
            dataset="assertion_config"
        )

    def generate_from_schema(self, schema_path: Path, assertion_name: str) -> Dict[str, Any]:
        """Generate assertion configuration from Avro schema.

        Args:
            schema_path: Path to Avro schema file
            assertion_name: Name for the assertion

        Returns:
            Assertion configuration dictionary
        """
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        # Load schema
        schema = json.loads(schema_path.read_text())

        # Create schema assertion
        schema_assertion = SchemaAssertion(
            name=assertion_name,
            description=f"Data quality assertions for {schema.get('name', assertion_name)}",
            field_assertions=self._create_field_assertions(schema)
        )

        # Generate SeaTunnel transform configuration
        config = {
            "assertion_name": assertion_name,
            "schema_name": schema.get("name", ""),
            "schema_namespace": schema.get("namespace", ""),
            "field_assertions": [self._field_assertion_to_dict(fa) for fa in schema_assertion.field_assertions],
            "seatunnel_transforms": self._generate_seatunnel_transforms(schema_assertion),
            "quality_thresholds": {
                "min_records_expected": 1,
                "max_records_expected": None,
                "quality_threshold": 0.95
            }
        }

        self.logger.log(
            "INFO",
            f"Generated assertion config for {assertion_name}",
            "assertion_config_generated",
            assertion_name=assertion_name,
            field_count=len(schema_assertion.field_assertions)
        )

        return config

    def _create_field_assertions(self, schema: Dict[str, Any]) -> List[FieldAssertion]:
        """Create field assertions from schema.

        Args:
            schema: Avro schema

        Returns:
            List of field assertions
        """
        field_assertions = []

        if "fields" not in schema:
            return field_assertions

        for field in schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            # Field presence assertion
            presence_assertion = FieldAssertion(
                field_name=field_name,
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True,
                allow_null=self._is_nullable_type(field_type),
                severity=AssertionSeverity.HIGH
            )
            field_assertions.append(presence_assertion)

            # Field type assertion
            type_assertion = FieldAssertion(
                field_name=field_name,
                assertion_type=AssertionType.FIELD_TYPE,
                expected_type=self._get_field_type(field_type),
                severity=AssertionSeverity.HIGH
            )
            field_assertions.append(type_assertion)

            # Field value assertions based on type
            value_assertion = self._create_value_assertion(field, field_type)
            if value_assertion:
                field_assertions.append(value_assertion)

            # Field format assertions for string fields
            if self._get_field_type(field_type) == "string":
                format_assertion = self._create_format_assertion(field)
                if format_assertion:
                    field_assertions.append(format_assertion)

        return field_assertions

    def _is_nullable_type(self, field_type: Any) -> bool:
        """Check if field type allows null values.

        Args:
            field_type: Field type definition

        Returns:
            True if type allows null
        """
        if isinstance(field_type, list):
            return "null" in field_type
        return False

    def _get_field_type(self, field_type: Any) -> str:
        """Get the primary type from field type definition.

        Args:
            field_type: Field type definition

        Returns:
            Primary type string
        """
        if isinstance(field_type, list):
            # Remove null from union types
            non_null_types = [t for t in field_type if t != "null"]
            if len(non_null_types) == 1:
                return str(non_null_types[0])
            return "union"

        return str(field_type)

    def _create_value_assertion(self, field: Dict[str, Any], field_type: Any) -> Optional[FieldAssertion]:
        """Create value assertion for field.

        Args:
            field: Field definition
            field_type: Field type

        Returns:
            Field assertion or None
        """
        field_name = field["name"]

        # Check for specific constraints in field definition
        if "constraints" in field:
            constraints = field["constraints"]

            # Numeric range constraints
            if isinstance(field_type, (list, str)) and any(t in ["int", "long", "float", "double"] for t in ([field_type] if isinstance(field_type, str) else field_type)):
                min_val = constraints.get("min")
                max_val = constraints.get("max")

                if min_val is not None or max_val is not None:
                    return FieldAssertion(
                        field_name=field_name,
                        assertion_type=AssertionType.FIELD_VALUE,
                        min_value=min_val,
                        max_value=max_val,
                        severity=AssertionSeverity.MEDIUM
                    )

            # Allowed values constraints
            allowed_values = constraints.get("allowed_values")
            if allowed_values:
                return FieldAssertion(
                    field_name=field_name,
                    assertion_type=AssertionType.FIELD_VALUE,
                    allowed_values=allowed_values,
                    severity=AssertionSeverity.MEDIUM
                )

        return None

    def _create_format_assertion(self, field: Dict[str, Any]) -> Optional[FieldAssertion]:
        """Create format assertion for string field.

        Args:
            field: Field definition

        Returns:
            Field assertion or None
        """
        field_name = field["name"]

        # Check for format constraints
        if "constraints" in field:
            constraints = field["constraints"]
            regex_pattern = constraints.get("regex")

            if regex_pattern:
                return FieldAssertion(
                    field_name=field_name,
                    assertion_type=AssertionType.FIELD_FORMAT,
                    regex_pattern=regex_pattern,
                    severity=AssertionSeverity.MEDIUM
                )

        return None

    def _field_assertion_to_dict(self, field_assertion: FieldAssertion) -> Dict[str, Any]:
        """Convert field assertion to dictionary.

        Args:
            field_assertion: Field assertion

        Returns:
            Dictionary representation
        """
        return {
            "field_name": field_assertion.field_name,
            "assertion_type": field_assertion.assertion_type.value,
            "severity": field_assertion.severity.value,
            "required": field_assertion.required,
            "allow_null": field_assertion.allow_null,
            "expected_type": field_assertion.expected_type,
            "min_value": field_assertion.min_value,
            "max_value": field_assertion.max_value,
            "allowed_values": field_assertion.allowed_values,
            "regex_pattern": field_assertion.regex_pattern,
            "fail_on_error": field_assertion.fail_on_error
        }

    def _generate_seatunnel_transforms(self, schema_assertion: SchemaAssertion) -> List[Dict[str, Any]]:
        """Generate SeaTunnel transform configurations.

        Args:
            schema_assertion: Schema assertion

        Returns:
            List of transform configurations
        """
        transforms = []

        # Field presence checks
        presence_checks = []
        for fa in schema_assertion.field_assertions:
            if fa.assertion_type == AssertionType.FIELD_PRESENCE:
                check = f"CASE WHEN {fa.field_name} IS NOT NULL THEN 1 ELSE 0 END as {fa.field_name}_present"
                presence_checks.append(check)

        if presence_checks:
            transforms.append({
                "plugin_name": "Sql",
                "result_table_name": "field_presence_checked",
                "sql": f"""
                    SELECT
                        *,
                        {', '.join(presence_checks)}
                    FROM input_table
                """
            })

        # Field type checks
        type_checks = []
        for fa in schema_assertion.field_assertions:
            if fa.assertion_type == AssertionType.FIELD_TYPE and fa.expected_type:
                type_checks.append(f"CASE WHEN {fa.field_name} IS NULL OR typeof({fa.field_name}) = '{fa.expected_type}' THEN 1 ELSE 0 END as {fa.field_name}_type_valid")

        if type_checks:
            transforms.append({
                "plugin_name": "Sql",
                "result_table_name": "field_type_checked",
                "sql": f"""
                    SELECT
                        *,
                        {', '.join(type_checks)}
                    FROM field_presence_checked
                """
            })

        # Quality scoring
        quality_calculations = []
        for fa in schema_assertion.field_assertions:
            if fa.assertion_type == AssertionType.FIELD_PRESENCE:
                quality_calculations.append(f"{fa.field_name}_present")
            elif fa.assertion_type == AssertionType.FIELD_TYPE:
                quality_calculations.append(f"{fa.field_name}_type_valid")

        if quality_calculations:
            avg_quality = " + ".join(quality_calculations)
            avg_quality = f"({avg_quality}) / {len(quality_calculations)}.0"

            transforms.append({
                "plugin_name": "Sql",
                "result_table_name": "quality_scored",
                "sql": f"""
                    SELECT
                        *,
                        {avg_quality} as data_quality_score,
                        CASE
                            WHEN {avg_quality} >= 0.95 THEN 'EXCELLENT'
                            WHEN {avg_quality} >= 0.85 THEN 'GOOD'
                            WHEN {avg_quality} >= 0.70 THEN 'FAIR'
                            WHEN {avg_quality} >= 0.50 THEN 'POOR'
                            ELSE 'CRITICAL'
                        END as quality_grade
                    FROM field_type_checked
                """
            })

        return transforms

    def generate_field_presence_checks(self, schema: Dict[str, Any]) -> str:
        """Generate SQL for field presence checks.

        Args:
            schema: Avro schema

        Returns:
            SQL string for field presence checks
        """
        checks = []

        if "fields" not in schema:
            return "*"

        for field in schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            # Check if field allows null
            allow_null = self._is_nullable_type(field_type)

            if allow_null:
                check = f"CASE WHEN {field_name} IS NOT NULL THEN 1 ELSE 0 END as {field_name}_present"
            else:
                check = f"CASE WHEN {field_name} IS NOT NULL THEN 1 ELSE 0 END as {field_name}_required"

            checks.append(check)

        return "*, " + ", ".join(checks) if checks else "*"

    def generate_field_type_checks(self, schema: Dict[str, Any]) -> str:
        """Generate SQL for field type checks.

        Args:
            schema: Avro schema

        Returns:
            SQL string for field type checks
        """
        checks = []

        if "fields" not in schema:
            return "*"

        for field in schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            # Skip null-only types
            if isinstance(field_type, list) and len(field_type) == 1 and field_type[0] == "null":
                continue

            expected_type = self._get_field_type(field_type)

            if expected_type and expected_type != "union":
                # Map Avro types to SQL types
                sql_types = {
                    "string": "string",
                    "int": "integer",
                    "long": "integer",
                    "float": "real",
                    "double": "real",
                    "boolean": "boolean"
                }

                sql_type = sql_types.get(expected_type, "string")
                check = f"CASE WHEN {field_name} IS NULL OR typeof({field_name}) = '{sql_type}' THEN 1 ELSE 0 END as {field_name}_type_valid"
                checks.append(check)

        return "*, " + ", ".join(checks) if checks else "*"


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Generate SeaTunnel data quality assertion configurations"
    )
    parser.add_argument(
        "schema_file",
        type=Path,
        help="Path to Avro schema file"
    )
    parser.add_argument(
        "--assertion-name",
        help="Name for the assertion (defaults to schema name)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (defaults to stdout)"
    )
    parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        default="json",
        help="Output format"
    )

    args = parser.parse_args(argv)

    try:
        generator = AssertionConfigGenerator()

        # Load schema
        if not args.schema_file.exists():
            print(f"Error: Schema file not found: {args.schema_file}")
            return 1

        # Generate assertion name
        assertion_name = args.assertion_name
        if not assertion_name:
            schema = json.loads(args.schema_file.read_text())
            assertion_name = f"{schema.get('namespace', 'unknown')}.{schema.get('name', 'unknown')}"

        # Generate configuration
        config = generator.generate_from_schema(args.schema_file, assertion_name)

        # Format output
        if args.format == "json":
            output_content = json.dumps(config, indent=2)
        else:
            # YAML format would require PyYAML
            print("YAML format not yet implemented")
            return 1

        # Write output
        if args.output:
            args.output.write_text(output_content)
            print(f"Generated assertion configuration: {args.output}")
        else:
            print(output_content)

        return 0

    except Exception as e:
        print(f"Error generating assertion configuration: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
