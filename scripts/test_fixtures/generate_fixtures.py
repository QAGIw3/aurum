#!/usr/bin/env python3
"""
Generate test fixtures and golden files for data ingestion validation.

This script generates comprehensive test fixtures for all data sources
including synthetic data, expected outputs, and validation metadata.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.test_fixtures import FixtureGenerator, FixtureConfig
from aurum.logging import create_logger


def generate_fixtures(output_dir: Path, data_sources: List[str]) -> None:
    """Generate test fixtures for specified data sources.

    Args:
        output_dir: Output directory for fixtures
        data_sources: List of data sources to generate fixtures for
    """
    print(f"🚀 Generating test fixtures in {output_dir}")

    # Create fixture configuration
    config = FixtureConfig(
        output_dir=output_dir,
        generate_golden_files=True,
        include_null_values=True,
        include_invalid_values=True,
        data_sources=data_sources
    )

    # Create fixture generator
    generator = FixtureGenerator(config)

    # Generate all fixtures
    summary = generator.generate_all_fixtures()

    print("✅ Fixture generation complete!")
    print(f"   Total data sources: {summary['total_fixtures']}")
    print(f"   Total test cases: {summary['total_test_cases']}")
    print(f"   Total records: {summary['total_records']}")

    # Print details by source
    print("\n📊 Fixtures by data source:")
    for source, source_summary in summary['fixtures_by_source'].items():
        print(f"   {source}: {source_summary['test_cases']} test cases, {source_summary['total_records']} records")

    return summary


def validate_fixtures(output_dir: Path) -> bool:
    """Validate generated fixtures.

    Args:
        output_dir: Directory containing fixtures

    Returns:
        True if validation passes
    """
    print(f"🔍 Validating fixtures in {output_dir}")

    if not output_dir.exists():
        print("❌ Output directory does not exist")
        return False

    # Check directory structure
    required_dirs = ["test_data", "golden_files", "schemas"]
    for dir_name in required_dirs:
        dir_path = output_dir / dir_name
        if not dir_path.exists():
            print(f"❌ Missing directory: {dir_name}")
            return False
        print(f"✅ Found directory: {dir_name}")

    # Check for test data files
    test_data_files = list((output_dir / "test_data").rglob("*.json"))
    if not test_data_files:
        print("❌ No test data files found")
        return False

    print(f"✅ Found {len(test_data_files)} test data files")

    # Check for golden files
    golden_files = list((output_dir / "golden_files").rglob("*.json"))
    if not golden_files:
        print("❌ No golden files found")
        return False

    print(f"✅ Found {len(golden_files)} golden files")

    # Check for schema files
    schema_files = list((output_dir / "schemas").rglob("*.avsc"))
    if not schema_files:
        print("⚠️ No schema files found (optional)")

    print(f"✅ Found {len(schema_files)} schema files")

    # Validate fixture data integrity
    print("🔍 Validating fixture data integrity...")

    validation_passed = True

    for test_file in test_data_files:
        if test_file.name.startswith("input"):
            try:
                with open(test_file) as f:
                    data = json.load(f)

                if not isinstance(data, list):
                    print(f"❌ Invalid data format in {test_file}")
                    validation_passed = False
                    continue

                # Check for required fields in first record
                if data:
                    first_record = data[0]
                    required_fields = ["source", "ingested_at"]
                    for field in required_fields:
                        if field not in first_record:
                            print(f"❌ Missing required field '{field}' in {test_file}")
                            validation_passed = False

                print(f"✅ Validated {test_file.name} ({len(data)} records)")

            except Exception as e:
                print(f"❌ Error reading {test_file}: {e}")
                validation_passed = False

    return validation_passed


def generate_fixture_report(output_dir: Path, summary: dict) -> None:
    """Generate a comprehensive fixture report.

    Args:
        output_dir: Directory containing fixtures
        summary: Generation summary
    """
    report_path = output_dir / "fixture_report.json"

    report = {
        "summary": summary,
        "validation_status": "PASSED",
        "file_counts": {
            "test_data_files": len(list((output_dir / "test_data").rglob("*.json"))),
            "golden_files": len(list((output_dir / "golden_files").rglob("*.json"))),
            "schema_files": len(list((output_dir / "schemas").rglob("*.avsc"))),
            "metadata_files": len(list((output_dir / "test_data").rglob("metadata.json")))
        },
        "data_sources": list(summary.get("fixtures_by_source", {}).keys()),
        "generated_at": str(datetime.now().isoformat())
    }

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"📋 Generated fixture report: {report_path}")


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.

    Args:
        argv: Command line arguments

    Returns:
        Exit code (0 for success)
    """
    parser = argparse.ArgumentParser(
        description="Generate test fixtures and golden files for data ingestion validation"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "test_fixtures",
        help="Output directory for fixtures"
    )
    parser.add_argument(
        "--data-source",
        action="append",
        help="Specific data source to generate fixtures for (can be used multiple times)"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing fixtures, don't generate new ones"
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate report for existing fixtures"
    )

    args = parser.parse_args(argv)

    try:
        # Determine data sources
        if args.data_source:
            data_sources = args.data_source
        else:
            data_sources = ["eia", "fred", "cpi", "noaa", "iso"]

        print("🔧 Test Fixtures Generator")
        print(f"   Data sources: {', '.join(data_sources)}")
        print(f"   Output directory: {args.output_dir}")

        # Generate fixtures
        if not args.validate_only and not args.report_only:
            summary = generate_fixtures(args.output_dir, data_sources)
            print("✅ Fixtures generated successfully")

        # Validate fixtures
        if not args.report_only:
            if validate_fixtures(args.output_dir):
                print("✅ Fixture validation passed")
            else:
                print("❌ Fixture validation failed")
                return 1

        # Generate report
        if args.report_only:
            # Load existing summary
            report_path = args.output_dir / "fixture_report.json"
            if report_path.exists():
                with open(report_path) as f:
                    summary = json.load(f)
                print("📋 Using existing fixture report")
            else:
                print("❌ No existing fixture report found")
                return 1
        elif not args.validate_only:
            # Generate new report
            summary = summary  # From generation step

        generate_fixture_report(args.output_dir, summary)

        print("🎉 Test fixtures workflow completed successfully!")
        return 0

    except Exception as e:
        print(f"❌ Error in test fixtures generation: {e}")
        return 1


if __name__ == "__main__":
    from datetime import datetime
    sys.exit(main())
