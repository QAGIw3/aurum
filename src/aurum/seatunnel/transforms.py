"""SeaTunnel transforms for data quality validation."""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .assertions import (
    SchemaAssertion,
    FieldAssertion,
    AssertionType,
    AssertionSeverity,
    DataQualityChecker
)


@dataclass
class TransformConfig:
    """Configuration for data quality transforms."""

    name: str
    transform_type: str
    config: Dict[str, Any] = None

    def __post_init__(self):
        if self.config is None:
            self.config = {}


class FieldPresenceTransform:
    """SeaTunnel transform for checking field presence."""

    @staticmethod
    def create_transform(assertion: FieldAssertion) -> Dict[str, Any]:
        """Create a SeaTunnel transform for field presence checking.

        Args:
            assertion: Field assertion configuration

        Returns:
            SeaTunnel transform configuration
        """
        transform = {
            "plugin_name": "Sql",
            "result_table_name": f"field_presence_check_{assertion.field_name}",
            "sql": f"""
                SELECT
                    *,
                    CASE
                        WHEN {assertion.field_name} IS NOT NULL THEN 1
                        ELSE 0
                    END as {assertion.field_name}_present
                FROM input_table
            """
        }

        return transform


class FieldTypeTransform:
    """SeaTunnel transform for checking field types."""

    @staticmethod
    def create_transform(assertion: FieldAssertion) -> Dict[str, Any]:
        """Create a SeaTunnel transform for field type checking.

        Args:
            assertion: Field assertion configuration

        Returns:
            SeaTunnel transform configuration
        """
        if not assertion.expected_type:
            return {}

        # Map expected types to SQL type checks
        type_checks = {
            "string": f"typeof({assertion.field_name}) = 'string'",
            "int": f"typeof({assertion.field_name}) = 'integer'",
            "long": f"typeof({assertion.field_name}) = 'integer'",
            "float": f"typeof({assertion.field_name}) = 'real'",
            "double": f"typeof({assertion.field_name}) = 'real'",
            "boolean": f"typeof({assertion.field_name}) = 'boolean'"
        }

        type_check = type_checks.get(assertion.expected_type.lower(), "TRUE")

        transform = {
            "plugin_name": "Sql",
            "result_table_name": f"field_type_check_{assertion.field_name}",
            "sql": f"""
                SELECT
                    *,
                    CASE
                        WHEN {assertion.field_name} IS NULL THEN 1
                        WHEN {type_check} THEN 1
                        ELSE 0
                    END as {assertion.field_name}_type_valid
                FROM input_table
            """
        }

        return transform


class DataQualityTransform:
    """SeaTunnel transform for comprehensive data quality checking."""

    @staticmethod
    def create_transform(schema_assertion: SchemaAssertion) -> Dict[str, Any]:
        """Create a SeaTunnel transform for data quality validation.

        Args:
            schema_assertion: Schema assertion configuration

        Returns:
            SeaTunnel transform configuration
        """
        # Build SQL for all field validations
        select_clauses = ["*"]
        where_clauses = []

        for field_assertion in schema_assertion.field_assertions:
            field_name = field_assertion.field_name

            # Add field presence check
            if field_assertion.required:
                select_clauses.append(f"CASE WHEN {field_name} IS NOT NULL THEN 1 ELSE 0 END as {field_name}_present")
            else:
                select_clauses.append(f"CASE WHEN {field_name} IS NULL OR {field_name} IS NOT NULL THEN 1 ELSE 0 END as {field_name}_present")

            # Add type validation if specified
            if field_assertion.expected_type:
                type_checks = {
                    "string": f"typeof({field_name}) = 'string'",
                    "int": f"typeof({field_name}) = 'integer'",
                    "long": f"typeof({field_name}) = 'integer'",
                    "float": f"typeof({field_name}) = 'real'",
                    "double": f"typeof({field_name}) = 'real'",
                    "boolean": f"typeof({field_name}) = 'boolean'"
                }

                type_check = type_checks.get(field_assertion.expected_type.lower(), "TRUE")
                select_clauses.append(f"CASE WHEN {field_name} IS NULL OR {type_check} THEN 1 ELSE 0 END as {field_name}_type_valid")

            # Add value range checks for numeric fields
            if isinstance(field_assertion.min_value, (int, float)):
                select_clauses.append(f"CASE WHEN {field_name} IS NULL OR {field_name} >= {field_assertion.min_value} THEN 1 ELSE 0 END as {field_name}_min_valid")

            if isinstance(field_assertion.max_value, (int, float)):
                select_clauses.append(f"CASE WHEN {field_name} IS NULL OR {field_name} <= {field_assertion.max_value} THEN 1 ELSE 0 END as {field_name}_max_valid")

        # Build complete SQL
        sql = f"""
            SELECT
                {', '.join(select_clauses)}
            FROM input_table
        """

        transform = {
            "plugin_name": "Sql",
            "result_table_name": f"data_quality_check_{schema_assertion.name}",
            "sql": sql
        }

        return transform


class AssertionTransform:
    """Main transform orchestrator for data quality assertions."""

    def __init__(self, data_quality_checker: DataQualityChecker):
        """Initialize assertion transform.

        Args:
            data_quality_checker: Data quality checker instance
        """
        self.data_quality_checker = data_quality_checker

    def create_transforms(self, input_table: str = "input_table") -> List[Dict[str, Any]]:
        """Create SeaTunnel transforms for data quality validation.

        Args:
            input_table: Name of the input table

        Returns:
            List of transform configurations
        """
        transforms = []

        # Add data quality transform
        for assertion_name, assertion in self.data_quality_checker.schema_assertions.items():
            transform = DataQualityTransform.create_transform(assertion)
            if transform:
                transforms.append(transform)

        return transforms

    def validate_and_transform(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate records and return quality results.

        Args:
            records: Records to validate

        Returns:
            Validation results
        """
        return self.data_quality_checker.check_data_quality(records)
