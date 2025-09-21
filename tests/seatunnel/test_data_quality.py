"""Tests for SeaTunnel data quality assertions."""

from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.seatunnel import (
    SchemaAssertion,
    FieldAssertion,
    AssertionResult,
    AssertionType,
    AssertionSeverity,
    DataQualityChecker
)


class TestFieldAssertion:
    def test_field_presence_required_valid(self) -> None:
        """Test field presence assertion with valid required field."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_PRESENCE,
            required=True,
            allow_null=False
        )

        record = {"test_field": "value"}
        result = assertion.validate_field(record)

        assert result.passed is True
        assert result.assertion_name == "FIELD_PRESENCE_test_field"
        assert result.message == "Field test_field presence validation passed"

    def test_field_presence_required_missing(self) -> None:
        """Test field presence assertion with missing required field."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_PRESENCE,
            required=True,
            allow_null=False
        )

        record = {}
        result = assertion.validate_field(record)

        assert result.passed is False
        assert "Required field test_field is missing" in result.message

    def test_field_presence_optional_missing(self) -> None:
        """Test field presence assertion with missing optional field."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_PRESENCE,
            required=False,
            allow_null=True
        )

        record = {}
        result = assertion.validate_field(record)

        assert result.passed is True  # Optional fields can be missing

    def test_field_type_valid(self) -> None:
        """Test field type assertion with valid type."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_TYPE,
            expected_type="string"
        )

        record = {"test_field": "string_value"}
        result = assertion.validate_field(record)

        assert result.passed is True

    def test_field_type_invalid(self) -> None:
        """Test field type assertion with invalid type."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_TYPE,
            expected_type="int"
        )

        record = {"test_field": "not_an_int"}
        result = assertion.validate_field(record)

        assert result.passed is False
        assert "type" in result.message.lower()

    def test_field_value_range_valid(self) -> None:
        """Test field value assertion with valid range."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_VALUE,
            min_value=0,
            max_value=100
        )

        record = {"test_field": 50}
        result = assertion.validate_field(record)

        assert result.passed is True

    def test_field_value_range_invalid(self) -> None:
        """Test field value assertion with invalid range."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_VALUE,
            min_value=0,
            max_value=100
        )

        record = {"test_field": 150}
        result = assertion.validate_field(record)

        assert result.passed is False
        assert "above maximum" in result.message

    def test_field_format_valid(self) -> None:
        """Test field format assertion with valid format."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_FORMAT,
            regex_pattern=r"^[A-Z]{3}\d{3}$"
        )

        record = {"test_field": "ABC123"}
        result = assertion.validate_field(record)

        assert result.passed is True

    def test_field_format_invalid(self) -> None:
        """Test field format assertion with invalid format."""
        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_FORMAT,
            regex_pattern=r"^[A-Z]{3}\d{3}$"
        )

        record = {"test_field": "invalid"}
        result = assertion.validate_field(record)

        assert result.passed is False
        assert "does not match pattern" in result.message

    def test_custom_validation_valid(self) -> None:
        """Test custom validation with valid data."""
        def custom_validator(value, record):
            return value is not None and len(str(value)) > 3

        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.CUSTOM,
            custom_validator=custom_validator,
            custom_message="Field must be non-null and longer than 3 characters"
        )

        record = {"test_field": "valid_value"}
        result = assertion.validate_field(record)

        assert result.passed is True

    def test_custom_validation_invalid(self) -> None:
        """Test custom validation with invalid data."""
        def custom_validator(value, record):
            return value is not None and len(str(value)) > 3

        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.CUSTOM,
            custom_validator=custom_validator,
            custom_message="Field must be non-null and longer than 3 characters"
        )

        record = {"test_field": "ab"}
        result = assertion.validate_field(record)

        assert result.passed is False
        assert "custom validation" in result.message.lower()


class TestSchemaAssertion:
    def test_schema_assertion_creation(self) -> None:
        """Test schema assertion creation."""
        field_assertions = [
            FieldAssertion(
                field_name="id",
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True
            ),
            FieldAssertion(
                field_name="name",
                assertion_type=AssertionType.FIELD_TYPE,
                expected_type="string"
            )
        ]

        assertion = SchemaAssertion(
            name="test_assertion",
            description="Test schema assertion",
            field_assertions=field_assertions
        )

        assert assertion.name == "test_assertion"
        assert assertion.description == "Test schema assertion"
        assert len(assertion.field_assertions) == 2

    def test_validate_single_record(self) -> None:
        """Test validation of a single record."""
        field_assertions = [
            FieldAssertion(
                field_name="id",
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True
            ),
            FieldAssertion(
                field_name="name",
                assertion_type=AssertionType.FIELD_TYPE,
                expected_type="string"
            )
        ]

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=field_assertions
        )

        # Valid record
        valid_record = {"id": "123", "name": "test"}
        results = assertion.validate_record(valid_record)

        assert len(results) == 2
        assert all(r.passed for r in results)

        # Invalid record
        invalid_record = {"id": "123"}  # Missing name field
        results = assertion.validate_record(invalid_record)

        assert len(results) == 2
        assert results[0].passed is True  # id presence
        assert results[1].passed is False  # name presence

    def test_validate_batch(self) -> None:
        """Test batch validation."""
        field_assertions = [
            FieldAssertion(
                field_name="id",
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True
            )
        ]

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=field_assertions
        )

        records = [
            {"id": "1"},
            {"id": "2"},
            {"id": "3"}
        ]

        result = assertion.validate_batch(records)

        assert result["assertion_name"] == "test_assertion"
        assert result["passed"] is True
        assert result["total_assertions"] == 3
        assert result["passed_assertions"] == 3
        assert result["failed_assertions"] == 0

    def test_validate_batch_with_failures(self) -> None:
        """Test batch validation with failures."""
        field_assertions = [
            FieldAssertion(
                field_name="id",
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True
            )
        ]

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=field_assertions
        )

        records = [
            {"id": "1"},
            {},  # Missing id
            {"id": "3"}
        ]

        result = assertion.validate_batch(records)

        assert result["assertion_name"] == "test_assertion"
        assert result["passed"] is False  # One record failed
        assert result["total_assertions"] == 3
        assert result["passed_assertions"] == 2
        assert result["failed_assertions"] == 1

    def test_quality_thresholds(self) -> None:
        """Test quality threshold validation."""
        field_assertions = [
            FieldAssertion(
                field_name="id",
                assertion_type=AssertionType.FIELD_PRESENCE,
                required=True
            )
        ]

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=field_assertions,
            quality_threshold=0.8  # 80% pass rate required
        )

        # Test with 75% pass rate (should fail)
        records = [
            {"id": "1"},
            {},  # Missing id
            {},  # Missing id
            {"id": "4"}  # Valid
        ]

        result = assertion.validate_batch(records)

        assert result["passed"] is False
        assert "Quality score" in result["messages"][0]


class TestDataQualityChecker:
    def test_checker_initialization(self) -> None:
        """Test data quality checker initialization."""
        checker = DataQualityChecker()

        assert len(checker.schema_assertions) == 0

    def test_register_assertion(self) -> None:
        """Test assertion registration."""
        checker = DataQualityChecker()

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )

        checker.register_assertion(assertion)

        assert "test_assertion" in checker.schema_assertions
        assert len(checker.schema_assertions) == 1

    def test_check_data_quality_all_pass(self) -> None:
        """Test data quality check with all passing records."""
        checker = DataQualityChecker()

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )
        checker.register_assertion(assertion)

        records = [
            {"id": "1"},
            {"id": "2"},
            {"id": "3"}
        ]

        result = checker.check_data_quality(records)

        assert result["overall_passed"] is True
        assert result["total_records"] == 3
        assert result["assertions_run"] == 1

    def test_check_data_quality_with_failures(self) -> None:
        """Test data quality check with failing records."""
        checker = DataQualityChecker()

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )
        checker.register_assertion(assertion)

        records = [
            {"id": "1"},
            {},  # Missing id
            {"id": "3"}
        ]

        result = checker.check_data_quality(records)

        assert result["overall_passed"] is False
        assert result["total_records"] == 3
        assert result["assertions_run"] == 1

    def test_create_assertion_from_schema(self) -> None:
        """Test creating assertion from Avro schema."""
        checker = DataQualityChecker()

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "id",
                    "type": "string"
                },
                {
                    "name": "name",
                    "type": ["null", "string"],
                    "default": None
                },
                {
                    "name": "count",
                    "type": "int"
                }
            ]
        }

        assertion = checker.create_assertion_from_schema(schema, "test_schema_assertion")

        assert assertion.name == "test_schema_assertion"
        assert len(assertion.field_assertions) == 6  # 3 fields × 2 assertions each

        # Check field presence assertions
        presence_assertions = [fa for fa in assertion.field_assertions if fa.assertion_type == AssertionType.FIELD_PRESENCE]
        assert len(presence_assertions) == 3

        # Check that name field allows null
        name_assertion = next(fa for fa in presence_assertions if fa.field_name == "name")
        assert name_assertion.allow_null is True

        # Check type assertions
        type_assertions = [fa for fa in assertion.field_assertions if fa.assertion_type == AssertionType.FIELD_TYPE]
        assert len(type_assertions) == 3

        id_type_assertion = next(fa for fa in type_assertions if fa.field_name == "id")
        assert id_type_assertion.expected_type == "string"

    def test_multiple_assertions(self) -> None:
        """Test data quality checker with multiple assertions."""
        checker = DataQualityChecker()

        # Register multiple assertions
        assertion1 = SchemaAssertion(
            name="presence_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )
        checker.register_assertion(assertion1)

        assertion2 = SchemaAssertion(
            name="type_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string"
                )
            ]
        )
        checker.register_assertion(assertion2)

        records = [
            {"id": "1", "name": "test"},
            {"id": "2", "name": "test2"}
        ]

        result = checker.check_data_quality(records)

        assert result["overall_passed"] is True
        assert result["assertions_run"] == 2
        assert result["total_records"] == 2


class TestAssertionConfigGenerator:
    def test_generator_creation(self) -> None:
        """Test assertion config generator creation."""
        from aurum.seatunnel.generate_assertion_config import AssertionConfigGenerator

        generator = AssertionConfigGenerator()
        assert generator is not None

    def test_generate_from_schema(self) -> None:
        """Test generating assertion config from schema."""
        from aurum.seatunnel.generate_assertion_config import AssertionConfigGenerator

        generator = AssertionConfigGenerator()

        # Create test schema
        schema = {
            "type": "record",
            "name": "TestRecord",
            "namespace": "aurum.test",
            "fields": [
                {
                    "name": "id",
                    "type": "string"
                },
                {
                    "name": "name",
                    "type": ["null", "string"],
                    "default": None
                },
                {
                    "name": "count",
                    "type": "int"
                }
            ]
        }

        # Create temporary schema file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_path = Path(f.name)

        try:
            config = generator.generate_from_schema(schema_path, "test_assertion")

            assert config["assertion_name"] == "test_assertion"
            assert config["schema_name"] == "TestRecord"
            assert config["schema_namespace"] == "aurum.test"
            assert len(config["field_assertions"]) > 0
            assert len(config["seatunnel_transforms"]) > 0

        finally:
            schema_path.unlink()

    def test_field_assertion_to_dict(self) -> None:
        """Test converting field assertion to dictionary."""
        from aurum.seatunnel.generate_assertion_config import AssertionConfigGenerator

        generator = AssertionConfigGenerator()

        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_PRESENCE,
            required=True,
            severity=AssertionSeverity.HIGH
        )

        result = generator._field_assertion_to_dict(assertion)

        assert result["field_name"] == "test_field"
        assert result["assertion_type"] == "FIELD_PRESENCE"
        assert result["severity"] == "HIGH"
        assert result["required"] is True

    def test_generate_field_presence_checks(self) -> None:
        """Test generating field presence checks SQL."""
        from aurum.seatunnel.generate_assertion_config import AssertionConfigGenerator

        generator = AssertionConfigGenerator()

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "id",
                    "type": "string"
                },
                {
                    "name": "name",
                    "type": ["null", "string"],
                    "default": None
                }
            ]
        }

        sql = generator.generate_field_presence_checks(schema)

        assert "id_present" in sql
        assert "name_present" in sql
        assert "SELECT" in sql

    def test_generate_field_type_checks(self) -> None:
        """Test generating field type checks SQL."""
        from aurum.seatunnel.generate_assertion_config import AssertionConfigGenerator

        generator = AssertionConfigGenerator()

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {
                    "name": "id",
                    "type": "string"
                },
                {
                    "name": "count",
                    "type": "int"
                }
            ]
        }

        sql = generator.generate_field_type_checks(schema)

        assert "id_type_valid" in sql
        assert "count_type_valid" in sql
        assert "typeof" in sql


class TestSeaTunnelIntegration:
    def test_transform_creation(self) -> None:
        """Test SeaTunnel transform creation."""
        from aurum.seatunnel.transforms import FieldPresenceTransform

        assertion = FieldAssertion(
            field_name="test_field",
            assertion_type=AssertionType.FIELD_PRESENCE,
            required=True
        )

        transform = FieldPresenceTransform.create_transform(assertion)

        assert transform["plugin_name"] == "Sql"
        assert "test_field" in transform["sql"]
        assert "present" in transform["sql"]

    def test_data_quality_transform_creation(self) -> None:
        """Test data quality transform creation."""
        from aurum.seatunnel.transforms import DataQualityTransform

        schema_assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                ),
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string"
                )
            ]
        )

        transform = DataQualityTransform.create_transform(schema_assertion)

        assert transform["plugin_name"] == "Sql"
        assert "data_quality_score" in transform["sql"]
        assert "quality_grade" in transform["sql"]

    def test_assertion_transform_orchestration(self) -> None:
        """Test assertion transform orchestration."""
        from aurum.seatunnel.transforms import AssertionTransform
        from aurum.seatunnel import DataQualityChecker

        checker = DataQualityChecker()

        assertion = SchemaAssertion(
            name="test_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )
        checker.register_assertion(assertion)

        transform = AssertionTransform(checker)

        transforms = transform.create_transforms()

        assert len(transforms) > 0
        assert transforms[0]["plugin_name"] == "Sql"


class TestIntegrationScenarios:
    def test_complete_validation_workflow(self) -> None:
        """Test complete data validation workflow."""
        checker = DataQualityChecker()

        # Create comprehensive assertion
        assertion = SchemaAssertion(
            name="comprehensive_test",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string",
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="age",
                    assertion_type=AssertionType.FIELD_VALUE,
                    min_value=0,
                    max_value=120,
                    severity=AssertionSeverity.MEDIUM
                )
            ],
            quality_threshold=0.9
        )
        checker.register_assertion(assertion)

        # Test with valid records
        valid_records = [
            {"id": "1", "name": "Alice", "age": 25},
            {"id": "2", "name": "Bob", "age": 30},
            {"id": "3", "name": "Charlie", "age": 35}
        ]

        result = checker.check_data_quality(valid_records)

        assert result["overall_passed"] is True
        assert result["total_records"] == 3
        assert result["assertions_run"] == 1

        # Test with invalid records
        invalid_records = [
            {"id": "1", "name": "Alice", "age": 25},  # Valid
            {"name": "Bob", "age": 30},              # Missing id
            {"id": "3", "age": 35}                   # Missing name
        ]

        result = checker.check_data_quality(invalid_records)

        assert result["overall_passed"] is False
        assert result["failed_assertions"] > 0

    def test_schema_driven_assertion_creation(self) -> None:
        """Test creating assertions from Avro schema."""
        checker = DataQualityChecker()

        schema = {
            "type": "record",
            "name": "UserRecord",
            "namespace": "aurum.test",
            "fields": [
                {
                    "name": "user_id",
                    "type": "string"
                },
                {
                    "name": "email",
                    "type": ["null", "string"],
                    "default": None
                },
                {
                    "name": "age",
                    "type": "int"
                },
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "UserStatus",
                        "symbols": ["ACTIVE", "INACTIVE", "SUSPENDED"]
                    }
                }
            ]
        }

        assertion = checker.create_assertion_from_schema(schema, "user_schema_assertion")

        assert assertion.name == "user_schema_assertion"
        assert len(assertion.field_assertions) == 8  # 4 fields × 2 assertions each

        # Check field presence assertions
        presence_assertions = [fa for fa in assertion.field_assertions if fa.assertion_type == AssertionType.FIELD_PRESENCE]
        assert len(presence_assertions) == 4

        # Check that email field allows null
        email_assertion = next(fa for fa in presence_assertions if fa.field_name == "email")
        assert email_assertion.allow_null is True

        # Check type assertions
        type_assertions = [fa for fa in assertion.field_assertions if fa.assertion_type == AssertionType.FIELD_TYPE]
        assert len(type_assertions) == 4

        user_id_assertion = next(fa for fa in type_assertions if fa.field_name == "user_id")
        assert user_id_assertion.expected_type == "string"

    def test_error_handling_and_reporting(self) -> None:
        """Test error handling and reporting in assertions."""
        checker = DataQualityChecker()

        # Create assertion that will fail
        assertion = SchemaAssertion(
            name="failing_assertion",
            field_assertions=[
                FieldAssertion(
                    field_name="nonexistent_field",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.CRITICAL
                )
            ],
            fail_on_error=True
        )
        checker.register_assertion(assertion)

        records = [
            {},  # Missing field
            {"other_field": "value"}
        ]

        result = checker.check_data_quality(records)

        assert result["overall_passed"] is False
        assert result["assertions_run"] == 1
        assert result["failed_assertions"] > 0

        # Check that error samples are collected
        assertion_results = result["assertion_results"]
        assert len(assertion_results) == 1
        assert len(assertion_results[0]["error_samples"]) > 0

    def test_performance_with_large_dataset(self) -> None:
        """Test performance with large dataset."""
        checker = DataQualityChecker()

        assertion = SchemaAssertion(
            name="performance_test",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True
                )
            ]
        )
        checker.register_assertion(assertion)

        # Create large dataset
        large_records = [{"id": str(i)} for i in range(1000)]

        # This should complete in reasonable time
        result = checker.check_data_quality(large_records)

        assert result["overall_passed"] is True
        assert result["total_records"] == 1000
        assert result["assertions_run"] == 1
