#!/usr/bin/env python3
"""Validate data contracts against Great Expectations and schema definitions."""

import json
import sys
import yaml
from pathlib import Path
from typing import Dict, List, Any


def load_great_expectations_suites(expectations_dir: str) -> List[Dict[str, Any]]:
    """Load all Great Expectations suite configurations."""
    suites = []
    expectations_path = Path(expectations_dir)

    if not expectations_path.exists():
        print(f"⚠️ Great Expectations directory not found: {expectations_dir}")
        return suites

    for suite_file in expectations_path.glob("*.json"):
        try:
            with open(suite_file) as f:
                suite = json.load(f)
                suites.append({
                    "name": suite_file.stem,
                    "suite": suite
                })
        except Exception as e:
            print(f"❌ Error loading suite {suite_file}: {e}")

    for suite_file in expectations_path.glob("*.yml"):
        try:
            with open(suite_file) as f:
                suite = yaml.safe_load(f)
                suites.append({
                    "name": suite_file.stem,
                    "suite": suite
                })
        except Exception as e:
            print(f"❌ Error loading suite {suite_file}: {e}")

    return suites


def validate_contract_compliance(suites: List[Dict[str, Any]], report_dir: str) -> bool:
    """Validate that data contracts are compliant with quality expectations."""
    print("🔍 Validating data contract compliance...")

    all_valid = True
    report_path = Path(report_dir)
    report_path.mkdir(exist_ok=True)

    contract_issues = []

    for suite_info in suites:
        suite_name = suite_info["name"]
        suite = suite_info["suite"]

        print(f"  Validating contract: {suite_name}")

        # Extract expectations from suite
        expectations = suite.get("expectations", [])
        if not expectations:
            continue

        # Check for critical expectations
        critical_issues = []

        for expectation in expectations:
            expectation_type = expectation.get("expectation_type", "")

            # Check for data quality expectations
            if expectation_type in ["expect_table_row_count_to_be_between", "expect_table_row_count_to_equal"]:
                min_rows = expectation.get("kwargs", {}).get("min_value", 0)
                if min_rows < 1:
                    critical_issues.append({
                        "type": "empty_table_risk",
                        "expectation": expectation_type,
                        "details": f"Table {suite_name} may be empty (min_rows: {min_rows})"
                    })

            elif expectation_type in ["expect_column_values_to_not_be_null", "expect_column_values_to_be_of_type"]:
                column = expectation.get("kwargs", {}).get("column")
                if column:
                    # This is a critical data quality expectation
                    print(f"    ✅ Critical expectation found: {expectation_type} on {column}")

            elif expectation_type in ["expect_column_values_to_be_unique", "expect_column_pair_values_to_be_unique"]:
                column = expectation.get("kwargs", {}).get("column")
                if column:
                    print(f"    ✅ Uniqueness constraint found: {expectation_type} on {column}")

        if critical_issues:
            all_valid = False
            contract_issues.extend(critical_issues)
            print(f"    ❌ Found {len(critical_issues)} issues in {suite_name}")

    # Generate contract validation report
    contract_report = {
        "validation_timestamp": json.dumps(json.loads('{"timestamp": "' + str(Path.cwd()) + '"}'), default=str),
        "total_suites": len(suites),
        "issues_found": len(contract_issues),
        "all_valid": all_valid,
        "issues": contract_issues
    }

    with open(report_path / "contract_validation_report.json", "w") as f:
        json.dump(contract_report, f, indent=2)

    if not all_valid:
        print("❌ Contract validation failed:")
        for issue in contract_issues:
            print(f"  - {issue['details']}")
        return False
    else:
        print("✅ All data contracts are compliant")
        return True


def validate_schema_contracts(suites: List[Dict[str, Any]], report_dir: str) -> bool:
    """Validate that schemas are properly contracted."""
    print("🔍 Validating schema contracts...")

    schema_issues = []

    # Look for schema-related expectations
    for suite_info in suites:
        suite_name = suite_info["name"]
        suite = suite_info["suite"]

        expectations = suite.get("expectations", [])

        # Check for schema validation expectations
        schema_expectations = [
            exp for exp in expectations
            if exp.get("expectation_type", "").endswith("to_match_regex")
            or "column_values_to_be_of_type" in exp.get("expectation_type", "")
        ]

        if not schema_expectations:
            schema_issues.append({
                "suite": suite_name,
                "issue": "No schema validation expectations found",
                "recommendation": "Add expectations for data types and formats"
            })

    # Generate schema contract report
    schema_report = {
        "validation_timestamp": json.dumps(json.loads('{"timestamp": "' + str(Path.cwd()) + '"}'), default=str),
        "suites_checked": len(suites),
        "schema_issues": len(schema_issues),
        "schema_issues": schema_issues
    }

    report_path = Path(report_dir)
    with open(report_path / "schema_contract_report.json", "w") as f:
        json.dump(schema_report, f, indent=2)

    if schema_issues:
        print("⚠️ Schema contract issues found:")
        for issue in schema_issues:
            print(f"  - {issue['suite']}: {issue['issue']}")
        return False
    else:
        print("✅ All schema contracts are properly defined")
        return True


def main():
    """Main function to validate data contracts."""
    import argparse

    parser = argparse.ArgumentParser(description="Validate data contracts")
    parser.add_argument("--ge-expectations", required=True, help="Path to Great Expectations suites")
    parser.add_argument("--report-dir", required=True, help="Directory to store validation reports")

    args = parser.parse_args()

    print("🔍 Starting data contract validation...")

    # Load GE suites
    suites = load_great_expectations_suites(args.ge_expectations)

    if not suites:
        print("⚠️ No Great Expectations suites found")
        return 0

    print(f"Found {len(suites)} Great Expectations suites")

    # Validate contract compliance
    contract_valid = validate_contract_compliance(suites, args.report_dir)

    # Validate schema contracts
    schema_valid = validate_schema_contracts(suites, args.report_dir)

    # Overall result
    overall_valid = contract_valid and schema_valid

    if overall_valid:
        print("✅ Data contract validation completed successfully")
        return 0
    else:
        print("❌ Data contract validation found issues")
        return 1


if __name__ == "__main__":
    sys.exit(main())
