"""Data quality assertions for SeaTunnel transformations."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime

from ..logging import StructuredLogger, LogLevel, create_logger


class AssertionSeverity(str, Enum):
    """Severity levels for assertion failures."""
    LOW = "LOW"           # Minor issues, log as warning
    MEDIUM = "MEDIUM"     # Data quality issues, log as warning
    HIGH = "HIGH"         # Data integrity issues, log as error
    CRITICAL = "CRITICAL" # Data corruption, fail processing


class AssertionType(str, Enum):
    """Types of data assertions."""
    FIELD_PRESENCE = "FIELD_PRESENCE"    # Check if field exists and is not null
    FIELD_TYPE = "FIELD_TYPE"           # Check field data type
    FIELD_VALUE = "FIELD_VALUE"         # Check field value constraints
    FIELD_FORMAT = "FIELD_FORMAT"       # Check field format (regex, etc.)
    RECORD_COUNT = "RECORD_COUNT"       # Check record count thresholds
    DATA_FRESHNESS = "DATA_FRESHNESS"   # Check data freshness/timestamps
    CUSTOM = "CUSTOM"                   # Custom assertion logic


@dataclass
class AssertionResult:
    """Result of a data assertion."""

    assertion_name: str
    assertion_type: AssertionType
    severity: AssertionSeverity
    passed: bool
    message: str
    record_count: int = 0
    failed_records: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "assertion_name": self.assertion_name,
            "assertion_type": self.assertion_type.value,
            "severity": self.severity.value,
            "passed": self.passed,
            "message": self.message,
            "record_count": self.record_count,
            "failed_records": self.failed_records[:5],  # Limit for performance
            "metadata": self.metadata,
            "timestamp": self.timestamp
        }


@dataclass
class FieldAssertion:
    """Configuration for a field-level assertion."""

    field_name: str
    assertion_type: AssertionType
    severity: AssertionSeverity = AssertionSeverity.MEDIUM

    # Field presence
    required: bool = True
    allow_null: bool = False

    # Field type
    expected_type: Optional[str] = None
    allow_type_coercion: bool = False

    # Field value constraints
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    allowed_values: Optional[List[Any]] = None
    regex_pattern: Optional[str] = None

    # Custom validation
    custom_validator: Optional[Callable[[Any, Dict[str, Any]], bool]] = None
    custom_message: Optional[str] = None

    # Error handling
    fail_on_error: bool = True
    sample_errors: bool = True
    max_error_samples: int = 10

    def validate_field(self, record: Dict[str, Any], field_path: str = None) -> AssertionResult:
        """Validate a field in a record.

        Args:
            record: Data record to validate
            field_path: Path to the field (for nested fields)

        Returns:
            Assertion result
        """
        if field_path is None:
            field_path = self.field_name

        result = AssertionResult(
            assertion_name=f"{self.assertion_type.value}_{field_path}",
            assertion_type=self.assertion_type,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} validation passed"
        )

        # Get field value
        field_value = self._get_field_value(record, field_path)

        # Check field presence
        if self.assertion_type == AssertionType.FIELD_PRESENCE:
            result = self._validate_field_presence(field_value, field_path)

        # Check field type
        elif self.assertion_type == AssertionType.FIELD_TYPE:
            result = self._validate_field_type(field_value, field_path)

        # Check field value
        elif self.assertion_type == AssertionType.FIELD_VALUE:
            result = self._validate_field_value(field_value, field_path)

        # Check field format
        elif self.assertion_type == AssertionType.FIELD_FORMAT:
            result = self._validate_field_format(field_value, field_path)

        # Custom validation
        elif self.assertion_type == AssertionType.CUSTOM and self.custom_validator:
            result = self._validate_custom(field_value, record, field_path)

        result.record_count = 1
        if not result.passed and self.sample_errors:
            result.failed_records.append(record)

        return result

    def _get_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get field value from record, handling nested fields.

        Args:
            record: Data record
            field_path: Path to field (e.g., "data.value" for nested)

        Returns:
            Field value or None if not found
        """
        if "." not in field_path:
            return record.get(field_path)

        # Handle nested fields
        current = record
        for part in field_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _validate_field_presence(self, field_value: Any, field_path: str) -> AssertionResult:
        """Validate field presence."""
        result = AssertionResult(
            assertion_name=f"FIELD_PRESENCE_{field_path}",
            assertion_type=AssertionType.FIELD_PRESENCE,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} presence validation passed"
        )

        # Check if field exists
        if field_value is None:
            if self.required:
                result.passed = False
                result.message = f"Required field {field_path} is missing"
            elif not self.allow_null:
                result.passed = False
                result.message = f"Field {field_path} is null but nulls are not allowed"
        elif self.allow_null and field_value is None:
            result.passed = False
            result.message = f"Field {field_path} is null but nulls are not allowed"

        return result

    def _validate_field_type(self, field_value: Any, field_path: str) -> AssertionResult:
        """Validate field data type."""
        result = AssertionResult(
            assertion_name=f"FIELD_TYPE_{field_path}",
            assertion_type=AssertionType.FIELD_TYPE,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} type validation passed"
        )

        if field_value is None:
            return result  # Skip type checking for null values

        if not self.expected_type:
            return result  # No type specified

        # Type validation logic
        actual_type = type(field_value).__name__
        expected_type = self.expected_type.lower()

        # Basic type mapping
        type_mapping = {
            "string": str,
            "int": int,
            "integer": int,
            "long": int,
            "float": float,
            "double": float,
            "boolean": bool,
            "bool": bool,
        }

        if expected_type in type_mapping:
            expected_class = type_mapping[expected_type]
            if not isinstance(field_value, expected_class):
                result.passed = False
                result.message = f"Field {field_path} has type {actual_type}, expected {expected_type}"

        return result

    def _validate_field_value(self, field_value: Any, field_path: str) -> AssertionResult:
        """Validate field value constraints."""
        result = AssertionResult(
            assertion_name=f"FIELD_VALUE_{field_path}",
            assertion_type=AssertionType.FIELD_VALUE,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} value validation passed"
        )

        if field_value is None:
            return result  # Skip value checking for null values

        # Check allowed values
        if self.allowed_values and field_value not in self.allowed_values:
            result.passed = False
            result.message = f"Field {field_path} value '{field_value}' not in allowed values: {self.allowed_values}"

        # Check numeric ranges
        if isinstance(field_value, (int, float)):
            if self.min_value is not None and field_value < self.min_value:
                result.passed = False
                result.message = f"Field {field_path} value {field_value} below minimum {self.min_value}"

            if self.max_value is not None and field_value > self.max_value:
                result.passed = False
                result.message = f"Field {field_path} value {field_value} above maximum {self.max_value}"

        return result

    def _validate_field_format(self, field_value: Any, field_path: str) -> AssertionResult:
        """Validate field format using regex."""
        import re

        result = AssertionResult(
            assertion_name=f"FIELD_FORMAT_{field_path}",
            assertion_type=AssertionType.FIELD_FORMAT,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} format validation passed"
        )

        if field_value is None:
            return result  # Skip format checking for null values

        if not self.regex_pattern:
            return result  # No pattern specified

        if not isinstance(field_value, str):
            result.passed = False
            result.message = f"Field {field_path} format validation requires string value"
            return result

        if not re.match(self.regex_pattern, field_value):
            result.passed = False
            result.message = f"Field {field_path} value '{field_value}' does not match pattern '{self.regex_pattern}'"

        return result

    def _validate_custom(self, field_value: Any, record: Dict[str, Any], field_path: str) -> AssertionResult:
        """Validate using custom validator function."""
        result = AssertionResult(
            assertion_name=f"CUSTOM_{field_path}",
            assertion_type=AssertionType.CUSTOM,
            severity=self.severity,
            passed=True,
            message=f"Field {field_path} custom validation passed"
        )

        if not self.custom_validator:
            return result

        try:
            if not self.custom_validator(field_value, record):
                result.passed = False
                result.message = self.custom_message or f"Field {field_path} failed custom validation"
        except Exception as e:
            result.passed = False
            result.message = f"Field {field_path} custom validation error: {e}"

        return result

    def _validate_record_assertion(self, record: Dict[str, Any], assertion_config: Dict[str, Any]) -> AssertionResult:
        """Validate a record-level assertion.

        Args:
            record: Data record to validate
            assertion_config: Assertion configuration

        Returns:
            Assertion result
        """
        assertion_type = assertion_config.get("assertion_type", "unknown")
        assertion_name = assertion_config.get("name", f"record_assertion_{id(assertion_config)}")
        severity = AssertionSeverity(assertion_config.get("severity", "MEDIUM"))

        result = AssertionResult(
            assertion_name=assertion_name,
            assertion_type=AssertionType(assertion_type),
            severity=severity,
            passed=True,
            message=f"Record assertion {assertion_name} passed"
        )

        try:
            # Record count assertion
            if assertion_type == "RECORD_COUNT":
                min_count = assertion_config.get("min_count")
                max_count = assertion_config.get("max_count")

                record_count = 1  # Single record validation
                if min_count is not None and record_count < min_count:
                    result.passed = False
                    result.message = f"Record count {record_count} below minimum {min_count}"
                elif max_count is not None and record_count > max_count:
                    result.passed = False
                    result.message = f"Record count {record_count} above maximum {max_count}"

            # Data freshness assertion
            elif assertion_type == "DATA_FRESHNESS":
                freshness_field = assertion_config.get("field", "timestamp")
                max_age_hours = assertion_config.get("max_age_hours", 24)

                if freshness_field in record:
                    field_value = record[freshness_field]
                    if isinstance(field_value, str):
                        try:
                            import dateutil.parser
                            record_time = dateutil.parser.parse(field_value)
                            age_hours = (datetime.now(record_time.tzinfo) - record_time).total_seconds() / 3600

                            if age_hours > max_age_hours:
                                result.passed = False
                                result.message = f"Record is {age_hours:.1f} hours old, exceeds max age of {max_age_hours} hours"
                        except Exception:
                            result.passed = False
                            result.message = f"Could not parse freshness field '{freshness_field}'"
                    else:
                        result.passed = False
                        result.message = f"Freshness field '{freshness_field}' is not a string"

            # Custom record assertion
            elif assertion_type == "CUSTOM":
                custom_validator = assertion_config.get("validator")
                custom_message = assertion_config.get("message", "Custom record validation failed")

                if custom_validator and callable(custom_validator):
                    try:
                        if not custom_validator(record):
                            result.passed = False
                            result.message = custom_message
                    except Exception as e:
                        result.passed = False
                        result.message = f"Custom record validation error: {e}"
                else:
                    result.passed = False
                    result.message = "Custom validator not provided or not callable"

            else:
                result.passed = False
                result.message = f"Unknown record assertion type: {assertion_type}"

        except Exception as e:
            result.passed = False
            result.message = f"Record assertion error: {e}"

        result.record_count = 1
        return result


@dataclass
class SchemaAssertion:
    """Configuration for schema-level data assertions."""

    name: str
    description: str = ""
    enabled: bool = True

    # Field assertions
    field_assertions: List[FieldAssertion] = field(default_factory=list)

    # Record-level assertions
    record_assertions: List[Dict[str, Any]] = field(default_factory=list)

    # Data quality thresholds
    min_records_expected: Optional[int] = None
    max_records_expected: Optional[int] = None
    quality_threshold: float = 0.95  # 95% pass rate required

    # Error handling
    fail_on_error: bool = False
    continue_on_failure: bool = True
    collect_error_samples: bool = True

    def validate_record(self, record: Dict[str, Any]) -> List[AssertionResult]:
        """Validate a single record.

        Args:
            record: Data record to validate

        Returns:
            List of assertion results
        """
        results = []

        # Validate each field assertion
        for field_assertion in self.field_assertions:
            result = field_assertion.validate_field(record)
            results.append(result)

        # Add record-level assertions
        for record_assertion in self.record_assertions:
            result = self._validate_record_assertion(record, record_assertion)
            results.append(result)

        return results

    def validate_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate a batch of records.

        Args:
            records: List of data records to validate

        Returns:
            Validation summary
        """
        logger = create_logger(
            source_name="schema_assertions",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.data_quality.assertions",
            dataset="data_validation"
        )

        if not self.enabled:
            return {
                "assertion_name": self.name,
                "enabled": False,
                "passed": True,
                "message": "Assertions disabled"
            }

        total_assertions = 0
        passed_assertions = 0
        failed_assertions = 0
        all_results = []

        # Validate each record
        for i, record in enumerate(records):
            record_results = self.validate_record(record)
            all_results.extend(record_results)

            for result in record_results:
                total_assertions += 1
                if result.passed:
                    passed_assertions += 1
                else:
                    failed_assertions += 1

        # Calculate quality score
        quality_score = passed_assertions / total_assertions if total_assertions > 0 else 1.0

        # Check thresholds
        validation_passed = True
        messages = []

        if self.min_records_expected and len(records) < self.min_records_expected:
            validation_passed = False
            messages.append(f"Record count {len(records)} below minimum {self.min_records_expected}")

        if self.max_records_expected and len(records) > self.max_records_expected:
            validation_passed = False
            messages.append(f"Record count {len(records)} above maximum {self.max_records_expected}")

        if quality_score < self.quality_threshold:
            validation_passed = False
            messages.append(f"Quality score {quality_score:.2%} below threshold {self.quality_threshold:.2%}")

        # Log results
        logger.log(
            LogLevel.INFO if validation_passed else LogLevel.ERROR,
            f"Schema assertion {self.name}: {passed_assertions}/{total_assertions} passed ({quality_score:.2%})",
            "schema_assertion_batch",
            assertion_name=self.name,
            total_assertions=total_assertions,
            passed_assertions=passed_assertions,
            failed_assertions=failed_assertions,
            quality_score=quality_score,
            validation_passed=validation_passed,
            record_count=len(records)
        )

        # Collect error samples
        error_samples = []
        if self.collect_error_samples:
            failed_results = [r for r in all_results if not r.passed]
            error_samples = [r.to_dict() for r in failed_results[:10]]  # Limit samples

        return {
            "assertion_name": self.name,
            "description": self.description,
            "enabled": self.enabled,
            "passed": validation_passed,
            "quality_score": quality_score,
            "total_assertions": total_assertions,
            "passed_assertions": passed_assertions,
            "failed_assertions": failed_assertions,
            "record_count": len(records),
            "messages": messages,
            "error_samples": error_samples,
            "timestamp": datetime.now().isoformat()
        }


class AssertionError(Exception):
    """Exception raised when assertions fail."""

    def __init__(self, message: str, assertion_results: List[AssertionResult]):
        super().__init__(message)
        self.assertion_results = assertion_results


class DataQualityChecker:
    """Main data quality checker that orchestrates multiple assertions."""

    def __init__(self):
        """Initialize data quality checker."""
        self.schema_assertions: Dict[str, SchemaAssertion] = {}
        self.logger = create_logger(
            source_name="data_quality_checker",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.data_quality.checks",
            dataset="data_quality"
        )

    def register_assertion(self, assertion: SchemaAssertion) -> None:
        """Register a schema assertion.

        Args:
            assertion: Schema assertion to register
        """
        self.schema_assertions[assertion.name] = assertion

        self.logger.log(
            LogLevel.INFO,
            f"Registered schema assertion: {assertion.name}",
            "assertion_registered",
            assertion_name=assertion.name,
            field_count=len(assertion.field_assertions)
        )

    def check_data_quality(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check data quality against all registered assertions.

        Args:
            records: List of records to check

        Returns:
            Quality check results
        """
        if not records:
            return {
                "total_records": 0,
                "assertions_run": 0,
                "overall_passed": True,
                "message": "No records to check"
            }

        self.logger.log(
            LogLevel.INFO,
            f"Running data quality checks on {len(records)} records",
            "data_quality_check_start",
            record_count=len(records),
            assertion_count=len(self.schema_assertions)
        )

        overall_passed = True
        all_results = []

        # Run each assertion
        for assertion_name, assertion in self.schema_assertions.items():
            try:
                result = assertion.validate_batch(records)
                all_results.append(result)

                if not result["passed"]:
                    overall_passed = False

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error running assertion {assertion_name}: {e}",
                    "assertion_error",
                    assertion_name=assertion_name,
                    error=str(e)
                )

                all_results.append({
                    "assertion_name": assertion_name,
                    "passed": False,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                overall_passed = False

        # Generate summary
        summary = {
            "total_records": len(records),
            "assertions_run": len(all_results),
            "overall_passed": overall_passed,
            "assertion_results": all_results,
            "timestamp": datetime.now().isoformat()
        }

        # Log summary
        self.logger.log(
            LogLevel.INFO if overall_passed else LogLevel.ERROR,
            f"Data quality check complete: {summary['assertions_run']} assertions, {'PASSED' if overall_passed else 'FAILED'}",
            "data_quality_check_complete",
            overall_passed=overall_passed,
            assertions_run=len(all_results)
        )

        return summary

    def create_assertion_from_schema(self, schema: Dict[str, Any], name: str) -> SchemaAssertion:
        """Create a schema assertion from an Avro schema.

        Args:
            schema: Avro schema definition
            name: Assertion name

        Returns:
            Schema assertion configured from schema
        """
        field_assertions = []

        # Create field assertions from schema
        if "fields" in schema:
            for field in schema["fields"]:
                field_name = field["name"]
                field_type = field["type"]

                # Field presence assertion
                presence_assertion = FieldAssertion(
                    field_name=field_name,
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    allow_null=self._is_nullable_type(field_type)
                )
                field_assertions.append(presence_assertion)

                # Field type assertion
                type_assertion = FieldAssertion(
                    field_name=field_name,
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type=self._get_field_type(field_type)
                )
                field_assertions.append(type_assertion)

        return SchemaAssertion(
            name=name,
            description=f"Schema assertion for {schema.get('name', name)}",
            field_assertions=field_assertions
        )

    def _is_nullable_type(self, field_type: Any) -> bool:
        """Check if a field type allows null values.

        Args:
            field_type: Field type definition

        Returns:
            True if type allows null
        """
        if isinstance(field_type, list):
            return "null" in field_type
        return False

    def _get_field_type(self, field_type: Any) -> str:
        """Get the primary type from a field type definition.

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
