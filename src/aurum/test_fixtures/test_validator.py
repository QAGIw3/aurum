"""Test validator for comparing outputs against golden files."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum

from .golden_file_manager import GoldenFileManager, GoldenFileStatus


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""

    INFO = "INFO"         # Informational issue
    WARNING = "WARNING"   # Potential issue
    ERROR = "ERROR"       # Validation failure
    CRITICAL = "CRITICAL" # Critical validation failure


@dataclass
class ValidationIssue:
    """A validation issue found during testing."""

    test_name: str
    issue_type: str
    severity: ValidationSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "test_name": self.test_name,
            "issue_type": self.issue_type,
            "severity": self.severity.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp
        }


@dataclass
class ValidationResult:
    """Result of a test validation."""

    test_name: str
    passed: bool
    golden_file_status: GoldenFileStatus
    issues: List[ValidationIssue] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def add_issue(self, issue_type: str, message: str, severity: ValidationSeverity = ValidationSeverity.ERROR, **details) -> None:
        """Add a validation issue.

        Args:
            issue_type: Type of issue
            message: Issue description
            severity: Issue severity
            **details: Additional issue details
        """
        issue = ValidationIssue(
            test_name=self.test_name,
            issue_type=issue_type,
            severity=severity,
            message=message,
            details=details
        )
        self.issues.append(issue)

    def get_issues_by_severity(self, severity: ValidationSeverity) -> List[ValidationIssue]:
        """Get issues by severity level.

        Args:
            severity: Severity level to filter by

        Returns:
            List of issues with the specified severity
        """
        return [issue for issue in self.issues if issue.severity == severity]

    def has_critical_issues(self) -> bool:
        """Check if there are any critical issues.

        Returns:
            True if critical issues exist
        """
        return len(self.get_issues_by_severity(ValidationSeverity.CRITICAL)) > 0

    def summary(self) -> str:
        """Get validation summary.

        Returns:
            Human-readable summary
        """
        issue_counts = {}
        for issue in self.issues:
            issue_counts[issue.severity] = issue_counts.get(issue.severity, 0) + 1

        summary = f"Test {self.test_name}: {'PASSED' if self.passed else 'FAILED'}"
        if issue_counts:
            summary += f" ({', '.join(f'{count} {sev.value.lower()}' for sev, count in issue_counts.items())})"

        return summary

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "test_name": self.test_name,
            "passed": self.passed,
            "golden_file_status": self.golden_file_status.value,
            "issues": [issue.to_dict() for issue in self.issues],
            "metrics": self.metrics,
            "execution_time_seconds": self.execution_time_seconds,
            "timestamp": self.timestamp
        }


class TestValidator:
    """Validate test outputs against golden files."""

    def __init__(self, golden_file_manager: GoldenFileManager):
        """Initialize test validator.

        Args:
            golden_file_manager: Golden file manager instance
        """
        self.golden_file_manager = golden_file_manager

    def validate_test_output(
        self,
        test_name: str,
        actual_output: List[Dict[str, Any]],
        expected_golden_name: Optional[str] = None
    ) -> ValidationResult:
        """Validate test output against golden file.

        Args:
            test_name: Name of the test
            actual_output: Actual output from the test
            expected_golden_name: Expected golden file name (defaults to test_name)

        Returns:
            Validation result
        """
        if expected_golden_name is None:
            expected_golden_name = test_name

        result = ValidationResult(
            test_name=test_name,
            passed=True,
            golden_file_status=GoldenFileStatus.MATCH
        )

        # Check if golden file exists
        golden_file = self.golden_file_manager.get_golden_file(expected_golden_name)
        if not golden_file:
            result.passed = False
            result.golden_file_status = GoldenFileStatus.MISSING
            result.add_issue(
                "MISSING_GOLDEN_FILE",
                f"Golden file {expected_golden_name} not found",
                ValidationSeverity.CRITICAL
            )
            return result

        # Compare with golden file
        golden_status = self.golden_file_manager.compare_with_actual(expected_golden_name, actual_output)
        result.golden_file_status = golden_status

        if golden_status != GoldenFileStatus.MATCH:
            result.passed = False
            result.add_issue(
                "OUTPUT_MISMATCH",
                f"Test output does not match golden file {expected_golden_name}",
                ValidationSeverity.CRITICAL,
                expected_records=golden_file.record_count,
                actual_records=len(actual_output)
            )

            # Detailed comparison
            if len(actual_output) != golden_file.record_count:
                result.add_issue(
                    "RECORD_COUNT_MISMATCH",
                    f"Record count mismatch: expected {golden_file.record_count}, got {len(actual_output)}",
                    ValidationSeverity.ERROR
                )

            # Check for schema differences
            if actual_output and golden_file.schema:
                schema_issues = self._validate_schema_compliance(actual_output, golden_file.schema)
                result.issues.extend(schema_issues)

        # Validate data quality metrics
        quality_issues = self._validate_data_quality(actual_output)
        result.issues.extend(quality_issues)

        # Calculate metrics
        result.metrics = self._calculate_metrics(actual_output, result.issues)

        return result

    def _validate_schema_compliance(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate data compliance with schema.

        Args:
            data: Data to validate
            schema: Expected schema

        Returns:
            List of schema compliance issues
        """
        issues = []

        if not data or "fields" not in schema:
            return issues

        expected_fields = {field["name"] for field in schema["fields"]}
        required_fields = {field["name"] for field in schema["fields"] if not _is_nullable_field(field)}

        for i, record in enumerate(data[:5]):  # Check first 5 records
            record_fields = set(record.keys())

            # Check missing required fields
            missing_required = required_fields - record_fields
            if missing_required:
                issues.append(ValidationIssue(
                    test_name="schema_validation",
                    issue_type="MISSING_REQUIRED_FIELDS",
                    severity=ValidationSeverity.CRITICAL,
                    message=f"Record {i} missing required fields: {missing_required}",
                    details={"record_index": i, "missing_fields": list(missing_required)}
                ))

            # Check extra fields
            extra_fields = record_fields - expected_fields
            if extra_fields:
                issues.append(ValidationIssue(
                    test_name="schema_validation",
                    issue_type="EXTRA_FIELDS",
                    severity=ValidationSeverity.WARNING,
                    message=f"Record {i} has extra fields: {extra_fields}",
                    details={"record_index": i, "extra_fields": list(extra_fields)}
                ))

        return issues

    def _validate_data_quality(self, data: List[Dict[str, Any]]) -> List[ValidationIssue]:
        """Validate data quality metrics.

        Args:
            data: Data to validate

        Returns:
            List of data quality issues
        """
        issues = []

        if not data:
            return issues

        # Check for null values
        null_counts = {}
        for record in data:
            for field, value in record.items():
                if value is None:
                    null_counts[field] = null_counts.get(field, 0) + 1

        for field, count in null_counts.items():
            null_rate = count / len(data)
            if null_rate > 0.1:  # More than 10% null values
                issues.append(ValidationIssue(
                    test_name="data_quality",
                    issue_type="HIGH_NULL_RATE",
                    severity=ValidationSeverity.WARNING,
                    message=f"Field {field} has high null rate: {null_rate".2%"}",
                    details={"field": field, "null_count": count, "null_rate": null_rate}
                ))

        # Check for data consistency
        if len(data) > 1:
            # Check for duplicate records
            record_hashes = set()
            duplicates = 0

            for record in data:
                record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
                if record_hash in record_hashes:
                    duplicates += 1
                else:
                    record_hashes.add(record_hash)

            if duplicates > 0:
                issues.append(ValidationIssue(
                    test_name="data_quality",
                    issue_type="DUPLICATE_RECORDS",
                    severity=ValidationSeverity.WARNING,
                    message=f"Found {duplicates} duplicate records",
                    details={"duplicate_count": duplicates}
                ))

        return issues

    def _calculate_metrics(self, data: List[Dict[str, Any]], issues: List[ValidationIssue]) -> Dict[str, Any]:
        """Calculate validation metrics.

        Args:
            data: Data being validated
            issues: Validation issues found

        Returns:
            Dictionary of metrics
        """
        metrics = {
            "record_count": len(data),
            "field_count": len(data[0].keys()) if data else 0,
            "issue_count": len(issues),
            "critical_issues": len([i for i in issues if i.severity == ValidationSeverity.CRITICAL]),
            "error_issues": len([i for i in issues if i.severity == ValidationSeverity.ERROR]),
            "warning_issues": len([i for i in issues if i.severity == ValidationSeverity.WARNING]),
            "info_issues": len([i for i in issues if i.severity == ValidationSeverity.INFO])
        }

        # Data quality score (0-100)
        if metrics["critical_issues"] > 0:
            metrics["quality_score"] = 0
        elif metrics["error_issues"] > 0:
            metrics["quality_score"] = 50
        elif metrics["warning_issues"] > 0:
            metrics["quality_score"] = 75
        else:
            metrics["quality_score"] = 100

        return metrics

    def validate_batch(
        self,
        test_results: Dict[str, List[Dict[str, Any]]]
    ) -> List[ValidationResult]:
        """Validate a batch of test results.

        Args:
            test_results: Dictionary mapping test names to outputs

        Returns:
            List of validation results
        """
        results = []

        for test_name, actual_output in test_results.items():
            result = self.validate_test_output(test_name, actual_output)
            results.append(result)

        return results

    def generate_validation_report(self, results: List[ValidationResult]) -> str:
        """Generate a validation report from results.

        Args:
            results: Validation results

        Returns:
            Report as string
        """
        report_lines = [
            "# Test Validation Report",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Summary",
            f"- Tests validated: {len(results)}",
            f"- Passed: {sum(1 for r in results if r.passed)}",
            f"- Failed: {sum(1 for r in results if not r.passed)}",
            f"- Overall success rate: {sum(1 for r in results if r.passed) / len(results) * 100".1f"}%",
            ""
        ]

        # Critical issues
        critical_issues = []
        for result in results:
            for issue in result.issues:
                if issue.severity == ValidationSeverity.CRITICAL:
                    critical_issues.append(issue)

        if critical_issues:
            report_lines.extend([
                "## Critical Issues",
                ""
            ])
            for issue in critical_issues:
                report_lines.append(f"- {issue.test_name}: {issue.message}")
            report_lines.append("")

        # Test results
        report_lines.extend([
            "## Test Results",
            "",
            "| Test | Status | Golden File | Issues | Quality Score |",
            "|------|--------|-------------|--------|---------------|"
        ])

        for result in results:
            status = "✅ PASSED" if result.passed else "❌ FAILED"
            golden_status = result.golden_file_status.value
            issue_count = len(result.issues)
            quality_score = result.metrics.get("quality_score", 0)

            report_lines.append(
                f"| {result.test_name} | {status} | {golden_status} | {issue_count} | {quality_score} |"
            )

        report_lines.append("")

        # Metrics
        if results:
            total_records = sum(r.metrics.get("record_count", 0) for r in results)
            avg_quality = sum(r.metrics.get("quality_score", 0) for r in results) / len(results)

            report_lines.extend([
                "## Metrics",
                "",
                f"- Total records processed: {total_records}",
                f"- Average quality score: {avg_quality".1f"}",
                f"- Critical issues: {sum(1 for r in results if r.has_critical_issues())}",
                ""
            ])

        return "\n".join(report_lines)


def _is_nullable_field(field_def: Dict[str, Any]) -> bool:
    """Check if a field allows null values.

    Args:
        field_def: Field definition

    Returns:
        True if field allows null
    """
    field_type = field_def.get("type", [])
    if isinstance(field_type, list):
        return "null" in field_type
    return False
