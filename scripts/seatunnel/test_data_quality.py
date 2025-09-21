#!/usr/bin/env python3
"""
Test script for SeaTunnel data quality assertions.

This script tests the data quality assertion system with sample data
and validates that assertions work correctly in various scenarios.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.seatunnel import (
    SchemaAssertion,
    FieldAssertion,
    AssertionType,
    AssertionSeverity,
    DataQualityChecker
)
from aurum.logging import create_logger


def create_test_data() -> List[dict]:
    """Create test data for validation.

    Returns:
        List of test records
    """
    return [
        # Valid records
        {"id": "1", "name": "Alice", "age": 25, "email": "alice@example.com"},
        {"id": "2", "name": "Bob", "age": 30, "email": "bob@example.com"},
        {"id": "3", "name": "Charlie", "age": 35, "email": "charlie@example.com"},

        # Records with issues
        {"id": "4", "name": "", "age": 25, "email": "invalid-email"},  # Empty name
        {"id": "5", "name": "David", "age": -5, "email": "david@example.com"},  # Negative age
        {"id": "6", "name": "Eve", "age": 25},  # Missing email
        {"name": "Frank", "age": 25, "email": "frank@example.com"},  # Missing id
        {"id": "8", "name": "Grace", "age": "not_a_number", "email": "grace@example.com"},  # Wrong age type
    ]


def create_sample_assertions() -> List[SchemaAssertion]:
    """Create sample data quality assertions.

    Returns:
        List of sample assertions
    """
    return [
        SchemaAssertion(
            name="basic_field_presence",
            description="Basic field presence validation",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="email",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=False,
                    allow_null=True,
                    severity=AssertionSeverity.MEDIUM
                )
            ]
        ),

        SchemaAssertion(
            name="data_type_validation",
            description="Data type validation for all fields",
            field_assertions=[
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string",
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
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="int",
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="email",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string",
                    severity=AssertionSeverity.MEDIUM
                )
            ]
        ),

        SchemaAssertion(
            name="value_constraints",
            description="Value range and format constraints",
            field_assertions=[
                FieldAssertion(
                    field_name="age",
                    assertion_type=AssertionType.FIELD_VALUE,
                    min_value=0,
                    max_value=150,
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="email",
                    assertion_type=AssertionType.FIELD_FORMAT,
                    regex_pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                    severity=AssertionSeverity.MEDIUM
                ),
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.CUSTOM,
                    custom_validator=lambda value, record: len(str(value).strip()) > 0,
                    custom_message="Name cannot be empty",
                    severity=AssertionSeverity.HIGH
                )
            ]
        ),

        SchemaAssertion(
            name="comprehensive_quality",
            description="Comprehensive data quality validation",
            field_assertions=[
                # Field presence
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.CRITICAL
                ),
                FieldAssertion(
                    field_name="name",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.HIGH
                ),
                FieldAssertion(
                    field_name="age",
                    assertion_type=AssertionType.FIELD_PRESENCE,
                    required=True,
                    severity=AssertionSeverity.HIGH
                ),

                # Field types
                FieldAssertion(
                    field_name="id",
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="string",
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
                    assertion_type=AssertionType.FIELD_TYPE,
                    expected_type="int",
                    severity=AssertionSeverity.HIGH
                ),

                # Value constraints
                FieldAssertion(
                    field_name="age",
                    assertion_type=AssertionType.FIELD_VALUE,
                    min_value=0,
                    max_value=150,
                    severity=AssertionSeverity.HIGH
                ),

                # Format validation
                FieldAssertion(
                    field_name="email",
                    assertion_type=AssertionType.FIELD_FORMAT,
                    regex_pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                    severity=AssertionSeverity.MEDIUM
                )
            ],
            quality_threshold=0.95  # 95% pass rate required
        )
    ]


def test_single_assertion(assertion: SchemaAssertion, test_data: List[dict]) -> dict:
    """Test a single assertion against test data.

    Args:
        assertion: Assertion to test
        test_data: Test data records

    Returns:
        Test results
    """
    print(f"\n🧪 Testing assertion: {assertion.name}")
    print(f"   Description: {assertion.description}")
    print(f"   Field assertions: {len(assertion.field_assertions)}")

    result = assertion.validate_batch(test_data)

    print(f"   Records tested: {result['record_count']}")
    print(f"   Total assertions: {result['total_assertions']}")
    print(f"   Passed: {result['passed_assertions']}")
    print(f"   Failed: {result['failed_assertions']}")
    print(f"   Quality score: {result['quality_score']".2%"}")
    print(f"   Overall result: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")

    if not result['passed']:
        print(f"   Issues: {len(result['messages'])}")
        for message in result['messages']:
            print(f"     - {message}")

    return result


def test_data_quality_checker(test_data: List[dict]) -> dict:
    """Test the data quality checker with all assertions.

    Args:
        test_data: Test data records

    Returns:
        Overall test results
    """
    print("\n🚀 Testing Data Quality Checker")
    print(f"   Test records: {len(test_data)}")

    checker = DataQualityChecker()

    # Register all assertions
    assertions = create_sample_assertions()
    for assertion in assertions:
        checker.register_assertion(assertion)

    print(f"   Registered assertions: {len(assertions)}")

    # Run quality check
    result = checker.check_data_quality(test_data)

    print(f"   Overall result: {'✅ PASSED' if result['overall_passed'] else '❌ FAILED'}")
    print(f"   Assertions run: {result['assertions_run']}")
    print(f"   Records processed: {result['total_records']}")

    # Show individual assertion results
    if 'assertion_results' in result:
        for assertion_result in result['assertion_results']:
            assertion_name = assertion_result.get('assertion_name', 'Unknown')
            passed = assertion_result.get('passed', False)
            quality_score = assertion_result.get('quality_score', 0)
            print(f"   - {assertion_name}: {'✅' if passed else '❌'} ({quality_score".2%"})")

    return result


def generate_test_report(results: List[dict]) -> str:
    """Generate a test report from results.

    Args:
        results: Test results from individual assertions

    Returns:
        Report as string
    """
    report_lines = [
        "# Data Quality Assertion Test Report",
        f"Generated: {str(json.dumps({'timestamp': str('2024-01-01')}))}",
        "",
        "## Summary",
        f"- Assertions tested: {len(results)}",
        f"- Overall pass rate: {sum(1 for r in results if r['passed']) / len(results) * 100".1f"}%",
        ""
    ]

    # Individual results
    report_lines.extend([
        "## Assertion Results",
        "",
        "| Assertion | Passed | Quality Score | Issues |",
        "|-----------|--------|---------------|---------|"
    ])

    for result in results:
        assertion_name = result.get('assertion_name', 'Unknown')
        passed = result.get('passed', False)
        quality_score = result.get('quality_score', 0)
        issues = len(result.get('messages', []))

        report_lines.append(
            f"| {assertion_name} | {'✅' if passed else '❌'} | {quality_score".2%"} | {issues} |"
        )

    report_lines.append("")

    # Detailed issues
    issues_found = []
    for result in results:
        if not result.get('passed') and 'messages' in result:
            for message in result['messages']:
                issues_found.append(f"- {result['assertion_name']}: {message}")

    if issues_found:
        report_lines.extend([
            "## Issues Found",
            ""
        ] + issues_found + [""])

    return "\n".join(report_lines)


def main(argv: Optional[List[str]] = None) -> int:
    """Main test function.

    Args:
        argv: Command line arguments

    Returns:
        Exit code (0 for success)
    """
    print("🚀 Starting Data Quality Assertion Tests")
    print("=" * 50)

    # Create test data
    test_data = create_test_data()

    # Create and test individual assertions
    assertions = create_sample_assertions()
    individual_results = []

    for assertion in assertions:
        result = test_single_assertion(assertion, test_data)
        individual_results.append(result)

    # Test comprehensive checker
    checker_result = test_data_quality_checker(test_data)

    # Generate report
    report = generate_test_report(individual_results)

    # Save report
    report_path = REPO_ROOT / "data_quality_test_report.md"
    report_path.write_text(report)
    print(f"\n📋 Test report saved to: {report_path}")

    # Print summary
    print("\n" + "=" * 50)
    print("🏁 Test Summary:")

    passed_count = sum(1 for r in individual_results if r['passed'])
    total_count = len(individual_results)

    print(f"  Individual assertions: {passed_count}/{total_count} passed")
    print(f"  Overall quality check: {'✅ PASSED' if checker_result['overall_passed'] else '❌ FAILED'}")
    print(f"  Report generated: {report_path}")

    # Exit code based on results
    if passed_count == total_count and checker_result['overall_passed']:
        print("  🎉 All tests passed!")
        return 0
    else:
        print("  ⚠️ Some tests failed - check report for details")
        return 1


if __name__ == "__main__":
    sys.exit(main())
