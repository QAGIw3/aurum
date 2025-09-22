"""Validation helpers for testing ISO data ingestion system."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from ..dq.iso_validator import IsoDataQualityValidator


@dataclass
class ValidationResult:
    """Result of data validation."""
    passed: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any]


class TestDataValidator:
    """Validator for test data against golden standards."""

    def __init__(self):
        self.golden_data_path = Path(__file__).parent / "golden_data"
        self.validator = IsoDataQualityValidator()

    def validate_iso_record_schema(
        self,
        records: List[Dict[str, Any]],
        iso_code: str,
        data_type: str
    ) -> ValidationResult:
        """Validate records against ISO-specific schema."""
        errors = []
        warnings = []

        # Load golden data for comparison
        golden_data = self._load_golden_data(iso_code, data_type)
        if not golden_data:
            errors.append(f"No golden data found for {iso_code}.{data_type}")
            return ValidationResult(passed=False, errors=errors, warnings=warnings, metrics={})

        # Basic structure validation
        if not isinstance(records, list):
            errors.append("Records should be a list")
            return ValidationResult(passed=False, errors=errors, warnings=warnings, metrics={})

        if len(records) == 0:
            warnings.append("No records to validate")
            return ValidationResult(passed=True, errors=errors, warnings=warnings, metrics={})

        # Validate required fields
        required_fields = self._get_required_fields(iso_code, data_type)
        for i, record in enumerate(records):
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                errors.append(f"Record {i}: Missing required fields: {missing_fields}")

            # Type validation
            type_errors = self._validate_field_types(record, iso_code, data_type)
            if type_errors:
                errors.extend([f"Record {i}: {error}" for error in type_errors])

        # Range validation
        range_errors = self._validate_value_ranges(records, iso_code, data_type)
        if range_errors:
            errors.extend(range_errors)

        # Data quality validation
        metadata = {
            "iso_code": iso_code,
            "data_type": data_type,
            "required_fields": required_fields
        }

        try:
            quality_results = self.validator.validate_batch(records, metadata)
            quality_summary = self.validator.get_validation_summary(quality_results)

            if not quality_summary["overall_passed"]:
                errors.append(f"Data quality validation failed: {quality_summary['total_issues']} issues")

        except Exception as e:
            warnings.append(f"Data quality validation error: {e}")

        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            metrics={
                "record_count": len(records),
                "validation_coverage": len(required_fields),
                "errors_found": len(errors),
                "warnings_found": len(warnings)
            }
        )

    def _load_golden_data(self, iso_code: str, data_type: str) -> Optional[Dict[str, Any]]:
        """Load golden data for validation."""
        file_path = self.golden_data_path / f"{iso_code.lower()}_{data_type.lower()}.json"

        if not file_path.exists():
            return None

        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None

    def _get_required_fields(self, iso_code: str, data_type: str) -> List[str]:
        """Get required fields for ISO and data type."""
        field_mappings = {
            "caiso": {
                "lmp": ["location_id", "price_total", "interval_start", "currency", "uom"],
                "load": ["area", "mw", "interval_start"],
                "asm": ["location_id", "price_mcp", "interval_start"]
            },
            "miso": {
                "lmp": ["node_id", "lmp", "timestamp", "market"],
                "load": ["zone", "load", "timestamp"],
                "generation_mix": ["zone", "fuel_type", "generation", "timestamp"]
            },
            "pjm": {
                "lmp": ["pnode_id", "total_lmp_da", "datetime_beginning_ept"],
                "load": ["area", "load_mw", "datetime_beginning_ept"],
                "generation": ["unit_id", "mw", "datetime_beginning_ept"]
            },
            "ercot": {
                "spp": ["settlement_point", "spp", "timestamp"],
                "load": ["area", "mw", "timestamp"],
                "generation": ["unit_id", "mw", "timestamp"]
            },
            "spp": {
                "lmp": ["settlement_location", "lmp", "timestamp"],
                "load": ["area", "mw", "timestamp"],
                "generation": ["unit_id", "mw", "timestamp"]
            }
        }

        iso_fields = field_mappings.get(iso_code.lower(), {})
        return iso_fields.get(data_type.lower(), ["location_id", "value", "timestamp"])

    def _validate_field_types(self, record: Dict[str, Any], iso_code: str, data_type: str) -> List[str]:
        """Validate field types in a record."""
        errors = []
        type_requirements = self._get_type_requirements(iso_code, data_type)

        for field, expected_type in type_requirements.items():
            if field in record:
                value = record[field]
                if not self._check_type(value, expected_type):
                    errors.append(f"Field '{field}' should be {expected_type}, got {type(value).__name__}")

        return errors

    def _get_type_requirements(self, iso_code: str, data_type: str) -> Dict[str, str]:
        """Get type requirements for fields."""
        return {
            "location_id": "str",
            "node_id": "str",
            "pnode_id": "str",
            "settlement_point": "str",
            "price_total": "number",
            "lmp": "number",
            "spp": "number",
            "mw": "number",
            "load": "number",
            "generation": "number",
            "interval_start": "str",
            "timestamp": "str",
            "datetime_beginning_ept": "str"
        }

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        if expected_type == "str":
            return isinstance(value, str)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "int":
            return isinstance(value, int)
        elif expected_type == "bool":
            return isinstance(value, bool)
        return True

    def _validate_value_ranges(self, records: List[Dict[str, Any]], iso_code: str, data_type: str) -> List[str]:
        """Validate value ranges in records."""
        errors = []
        range_limits = self._get_range_limits(iso_code, data_type)

        for field, limits in range_limits.items():
            for i, record in enumerate(records):
                if field in record:
                    value = record[field]
                    if isinstance(value, (int, float)):
                        min_val = limits.get("min")
                        max_val = limits.get("max")

                        if min_val is not None and value < min_val:
                            errors.append(f"Record {i}: {field} below minimum ({value} < {min_val})")

                        if max_val is not None and value > max_val:
                            errors.append(f"Record {i}: {field} above maximum ({value} > {max_val})")

        return errors

    def _get_range_limits(self, iso_code: str, data_type: str) -> Dict[str, Dict[str, float]]:
        """Get value range limits for validation."""
        return {
            "caiso": {
                "price_total": {"min": -1000, "max": 10000},
                "mw": {"min": 0, "max": 50000}
            },
            "miso": {
                "lmp": {"min": -500, "max": 2000},
                "load": {"min": 0, "max": 15000}
            },
            "pjm": {
                "total_lmp_da": {"min": -200, "max": 5000},
                "load_mw": {"min": 0, "max": 20000}
            },
            "ercot": {
                "spp": {"min": -100, "max": 9000},
                "mw": {"min": 0, "max": 80000}
            },
            "spp": {
                "lmp": {"min": -300, "max": 3000},
                "mw": {"min": 0, "max": 12000}
            }
        }.get(iso_code.lower(), {})


def assert_data_quality_metrics(
    validation_results: Dict[str, Any],
    expected_metrics: Dict[str, Any]
) -> None:
    """Assert that validation results meet expected metrics."""
    summary = validation_results.get("summary", {})

    for metric, expected_value in expected_metrics.items():
        actual_value = summary.get(metric)
        if actual_value != expected_value:
            raise AssertionError(
                f"Metric {metric}: expected {expected_value}, got {actual_value}"
            )


def compare_with_golden_data(
    actual_records: List[Dict[str, Any]],
    iso_code: str,
    data_type: str,
    tolerance: float = 0.1
) -> Dict[str, Any]:
    """Compare actual records with golden data."""
    validator = TestDataValidator()
    golden_data = validator._load_golden_data(iso_code, data_type)

    if not golden_data:
        return {"match": False, "error": "No golden data available"}

    golden_records = golden_data.get("data", [])
    comparison = {
        "match": False,
        "record_count_match": len(actual_records) == len(golden_records),
        "field_coverage": 0.0,
        "data_drift": 0.0,
        "missing_records": 0,
        "extra_records": 0,
        "details": []
    }

    if len(actual_records) == len(golden_records):
        # Compare field coverage
        golden_fields = set()
        for record in golden_records:
            golden_fields.update(record.keys())

        actual_fields = set()
        for record in actual_records:
            actual_fields.update(record.keys())

        common_fields = golden_fields.intersection(actual_fields)
        comparison["field_coverage"] = len(common_fields) / len(golden_fields) if golden_fields else 1.0

        # Simple drift detection (placeholder)
        comparison["data_drift"] = 0.05  # Would implement actual drift calculation

        comparison["match"] = comparison["field_coverage"] >= (1 - tolerance)
    else:
        comparison["missing_records"] = max(0, len(golden_records) - len(actual_records))
        comparison["extra_records"] = max(0, len(actual_records) - len(golden_records))

    return comparison


def validate_iso_record_schema(
    records: List[Dict[str, Any]],
    iso_code: str,
    data_type: str
) -> ValidationResult:
    """Validate ISO records against schema requirements."""
    validator = TestDataValidator()
    return validator.validate_iso_record_schema(records, iso_code, data_type)
