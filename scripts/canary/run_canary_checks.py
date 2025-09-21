#!/usr/bin/env python3
"""
Run canary checks manually for testing and debugging.

This script allows manual execution of canary checks outside of
the Airflow DAG for testing and debugging purposes.
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

from aurum.canary import (
    CanaryManager,
    CanaryConfig,
    APIHealthChecker,
    CanaryRunner,
    CanaryAlertManager
)


def create_test_canaries() -> List[CanaryConfig]:
    """Create test canary configurations.

    Returns:
        List of test canary configurations
    """
    return [
        CanaryConfig(
            name="test_eia_canary",
            source="eia",
            dataset="electricity_prices",
            api_endpoint="https://api.eia.gov/v2/electricity/retail-sales/data/",
            api_params={
                "api_key": "DEMO_KEY",
                "frequency": "monthly",
                "data[0]": "price",
                "facets[stateid][]": "CA",
                "start": "2023-01",
                "end": "2023-01",
                "length": "1"
            },
            expected_response_format="json",
            expected_fields=["response", "data"],
            timeout_seconds=30,
            description="Test EIA electricity prices canary"
        ),

        CanaryConfig(
            name="test_fred_canary",
            source="fred",
            dataset="unemployment",
            api_endpoint="https://api.stlouisfed.org/fred/series/observations",
            api_params={
                "series_id": "UNRATE",
                "api_key": "demo",
                "file_type": "json",
                "observation_start": "2024-01-01",
                "observation_end": "2024-01-01",
                "limit": "1"
            },
            expected_response_format="json",
            expected_fields=["realtime_start", "realtime_end", "observations"],
            timeout_seconds=30,
            description="Test FRED unemployment canary"
        ),

        CanaryConfig(
            name="test_noaa_canary",
            source="noaa",
            dataset="weather",
            api_endpoint="https://www.ncei.noaa.gov/access/services/data/v1",
            api_params={
                "dataset": "daily-summaries",
                "stations": "USW00094728",
                "startDate": "2024-01-01",
                "endDate": "2024-01-01",
                "format": "json",
                "limit": "1"
            },
            expected_response_format="json",
            expected_fields=["results"],
            timeout_seconds=45,
            description="Test NOAA weather canary"
        )
    ]


def run_canary_check(canary_name: str, verbose: bool = False) -> bool:
    """Run a single canary check.

    Args:
        canary_name: Name of the canary to run
        verbose: Enable verbose output

    Returns:
        True if successful
    """
    try:
        # Initialize components
        canary_manager = CanaryManager()
        api_checker = APIHealthChecker()

        # Create test canary if it doesn't exist
        if not canary_manager.get_canary(canary_name):
            test_canaries = create_test_canaries()
            for config in test_canaries:
                if config.name == canary_name:
                    canary_manager.register_canary(config)
                    break
            else:
                print(f"Test canary '{canary_name}' not found")
                return False

        # Create runner and run check
        runner = CanaryRunner(canary_manager, api_checker)
        result = runner.run_canary(canary_name)

        # Print results
        print(f"\n🐦 Canary Check: {canary_name}")
        print(f"   Status: {result.status.value}")
        print(f"   Execution Time: {result.execution_time_seconds:.2f}s")
        print(f"   Records Processed: {result.records_processed}")

        if result.errors:
            print(f"   Errors: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"     - {error}")

        if result.warnings:
            print(f"   Warnings: {len(result.warnings)}")
            for warning in result.warnings[:3]:  # Show first 3 warnings
                print(f"     - {warning}")

        if verbose:
            print(f"   API Health Results: {len(result.api_health_results)}")
            for api_result in result.api_health_results:
                print(f"     - {api_result.check_name}: {api_result.status.value}")

        return result.is_success()

    except Exception as e:
        print(f"Error running canary check: {e}")
        return False


def run_all_canary_checks(verbose: bool = False) -> Dict[str, bool]:
    """Run all canary checks.

    Args:
        verbose: Enable verbose output

    Returns:
        Dictionary mapping canary names to success status
    """
    results = {}

    print("🐦 Running all canary checks...")

    test_canaries = create_test_canaries()
    for config in test_canaries:
        print(f"\n--- Running {config.name} ---")
        success = run_canary_check(config.name, verbose)
        results[config.name] = success

    # Print summary
    successful = sum(1 for success in results.values() if success)
    total = len(results)

    print("""
🏁 Canary Check Summary:""")
    print(f"   Total: {total}")
    print(f"   Successful: {successful}")
    print(f"   Failed: {total - successful}")
    print(f"   Success Rate: {successful/total*100:.1f}%")

    return results


def generate_canary_report() -> None:
    """Generate a comprehensive canary monitoring report."""
    try:
        canary_manager = CanaryManager()

        # Add test canaries
        test_canaries = create_test_canaries()
        for config in test_canaries:
            canary_manager.register_canary(config)

        # Run checks
        api_checker = APIHealthChecker()
        runner = CanaryRunner(canary_manager, api_checker)

        results = runner.run_all_canaries()

        # Generate report
        report = {
            "timestamp": str(datetime.now()),
            "total_canaries": len(results),
            "successful_canaries": len([r for r in results.values() if r.is_success()]),
            "failed_canaries": len([r for r in results.values() if r.is_failure()]),
            "canaries_with_warnings": len([r for r in results.values() if r.has_warnings()]),
            "canary_details": {
                name: result.to_dict() for name, result in results.items()
            }
        }

        # Save report
        report_path = REPO_ROOT / "canary_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"📋 Canary report saved to {report_path}")

        # Print summary
        print("""
📊 Canary Report Summary:""")
        for key, value in report.items():
            if key != "canary_details":
                print(f"   {key}: {value}")

        return report

    except Exception as e:
        print(f"Error generating canary report: {e}")
        return None


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Run canary checks manually"
    )
    parser.add_argument(
        "canary",
        nargs="?",
        help="Specific canary to run (run all if not specified)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate comprehensive report"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available test canaries"
    )

    args = parser.parse_args(argv)

    try:
        if args.list:
            print("Available test canaries:")
            test_canaries = create_test_canaries()
            for config in test_canaries:
                print(f"  - {config.name}: {config.description}")
            return 0

        if args.report:
            report = generate_canary_report()
            return 0 if report else 1

        if args.canary:
            success = run_canary_check(args.canary, args.verbose)
            return 0 if success else 1
        else:
            results = run_all_canary_checks(args.verbose)
            failed_count = sum(1 for success in results.values() if not success)
            return 0 if failed_count == 0 else 1

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    from datetime import datetime
    sys.exit(main())
