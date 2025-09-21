#!/usr/bin/env python3
"""Command-line interface for cost profiler system."""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from aurum.cost_profiler import (
    CostProfiler,
    ProfilerConfig,
    PerformanceAnalyzer,
    CostEstimator,
    ProfileReporter,
    ReportConfig
)
from aurum.logging import create_logger, LogLevel


def load_profiles_from_directory(directory_path: Path) -> Dict[str, Any]:
    """Load dataset profiles from directory.

    Args:
        directory_path: Path to directory containing profile files

    Returns:
        Dictionary of profiles by dataset name
    """
    profiles = {}

    if not directory_path.exists():
        print(f"Directory {directory_path} does not exist")
        return profiles

    # Look for JSON files containing profiles
    json_files = list(directory_path.glob("*.json"))

    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                profile_data = json.load(f)

            dataset_name = profile_data.get("dataset_name", file_path.stem)

            # Create a simplified profile object (for CLI purposes)
            profiles[dataset_name] = profile_data

        except Exception as e:
            print(f"Error loading profile from {file_path}: {e}")

    return profiles


def analyze_profiles(profiles: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze profiles and generate report.

    Args:
        profiles: Dictionary of profile data

    Returns:
        Analysis report
    """
    # Initialize components
    profiler_config = ProfilerConfig(
        enable_memory_monitoring=False,
        enable_cpu_monitoring=False
    )

    profiler = CostProfiler(profiler_config)
    analyzer = PerformanceAnalyzer()
    cost_estimator = CostEstimator()
    reporter = ProfileReporter()

    # Convert loaded profile data to proper profile objects
    # This is a simplified conversion for CLI purposes
    analysis_results = {}

    for dataset_name, profile_data in profiles.items():
        print(f"Analyzing {dataset_name}...")

        # Create a basic profile object
        profile = type('Profile', (), {
            'dataset_name': dataset_name,
            'data_source': profile_data.get('data_source', 'unknown'),
            'metrics': [],
            'get_average_metrics': lambda: None,
            'get_performance_summary': lambda: profile_data
        })()

        # Analyze this profile
        analysis_result = analyzer.analyze_dataset_profile(profile)
        analysis_results[dataset_name] = analysis_result

    # Generate comprehensive report
    report = reporter.generate_profile_report(
        {},  # Empty profiles dict for CLI
        analysis_results,
        cost_estimator
    )

    return report.to_dict()


def generate_cost_report(profiles: Dict[str, Any], time_period: str = "monthly") -> Dict[str, Any]:
    """Generate cost report from profiles.

    Args:
        profiles: Dictionary of profile data
        time_period: Time period for cost estimation

    Returns:
        Cost report
    """
    cost_estimator = CostEstimator()

    # Generate cost report
    cost_data = cost_estimator.generate_cost_report({}, time_period)

    return cost_data


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Data Ingestion Cost Profiler CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze profiles from directory
  python cost_profiler_cli.py analyze /path/to/profiles

  # Generate cost report
  python cost_profiler_cli.py cost /path/to/profiles --period monthly

  # Generate performance report in JSON format
  python cost_profiler_cli.py report /path/to/profiles --format json --output report.json

  # Compare two profile directories
  python cost_profiler_cli.py compare /path/to/profiles1 /path/to/profiles2
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze performance profiles')
    analyze_parser.add_argument('profiles_dir', help='Directory containing profile files')

    # Cost command
    cost_parser = subparsers.add_parser('cost', help='Generate cost report')
    cost_parser.add_argument('profiles_dir', help='Directory containing profile files')
    cost_parser.add_argument('--period', choices=['daily', 'monthly', 'yearly'],
                           default='monthly', help='Time period for cost estimation')

    # Report command
    report_parser = subparsers.add_parser('report', help='Generate comprehensive report')
    report_parser.add_argument('profiles_dir', help='Directory containing profile files')
    report_parser.add_argument('--format', choices=['json', 'csv', 'markdown'],
                             default='json', help='Output format')
    report_parser.add_argument('--output', '-o', help='Output file path')
    report_parser.add_argument('--include-costs', action='store_true', default=True,
                             help='Include cost estimates')
    report_parser.add_argument('--include-analysis', action='store_true', default=True,
                             help='Include performance analysis')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare two profile directories')
    compare_parser.add_argument('profiles_dir1', help='First profiles directory')
    compare_parser.add_argument('profiles_dir2', help='Second profiles directory')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == 'analyze':
            profiles_dir = Path(args.profiles_dir)
            profiles = load_profiles_from_directory(profiles_dir)

            if not profiles:
                print(f"No profiles found in {profiles_dir}")
                return 1

            print(f"Loaded {len(profiles)} profiles")
            analysis = analyze_profiles(profiles)

            # Print summary
            print("\n=== Analysis Summary ===")
            issues = analysis.get("analysis_results", {}).get("performance_issues", [])
            print(f"Total issues found: {len(issues)}")

            for issue in issues:
                print(f"- {issue.get('dataset_name', 'unknown')}: {issue.get('issue_type', 'unknown')} ({issue.get('severity', 'unknown')})")

        elif args.command == 'cost':
            profiles_dir = Path(args.profiles_dir)
            profiles = load_profiles_from_directory(profiles_dir)

            if not profiles:
                print(f"No profiles found in {profiles_dir}")
                return 1

            print(f"Generating cost report for {len(profiles)} profiles")
            cost_data = generate_cost_report(profiles, args.period)

            # Print cost summary
            print(f"\n=== Cost Report ({args.period}) ===")
            summary = cost_data.get("summary", {})
            print(f"Total cost: ${summary.get('total_costs_usd', 0)".2f"}")
            print(f"Datasets: {summary.get('total_datasets', 0)}")
            print(f"Average cost per dataset: ${summary.get('average_cost_per_dataset_usd', 0)".2f"}")

            # Print breakdown by data source
            breakdown = cost_data.get("data_source_breakdown", {})
            if breakdown:
                print("\nBy Data Source:")
                for ds, data in breakdown.items():
                    print(f"  {ds}: ${data.get('total_costs_usd', 0)".2f"}")

        elif args.command == 'report':
            profiles_dir = Path(args.profiles_dir)
            profiles = load_profiles_from_directory(profiles_dir)

            if not profiles:
                print(f"No profiles found in {profiles_dir}")
                return 1

            print(f"Generating report for {len(profiles)} profiles")

            # Configure reporter
            config = ReportConfig(
                include_costs=args.include_costs,
                include_performance_analysis=args.include_analysis,
                output_format=args.format,
                cost_time_period=args.period
            )

            reporter = ProfileReporter(config)
            cost_estimator = CostEstimator()
            analysis_results = {}  # Would need proper analysis setup

            report = reporter.generate_profile_report(
                {},  # Empty for CLI
                analysis_results,
                cost_estimator
            )

            # Generate output
            if args.format == 'json':
                output_content = report.to_json()
            elif args.format == 'csv':
                output_content = report.to_csv()
            elif args.format == 'markdown':
                output_content = report.to_markdown()

            # Output to file or stdout
            if args.output:
                output_path = Path(args.output)
                with open(output_path, 'w') as f:
                    f.write(output_content)
                print(f"Report saved to {output_path}")
            else:
                print(output_content)

        elif args.command == 'compare':
            profiles_dir1 = Path(args.profiles_dir1)
            profiles_dir2 = Path(args.profiles_dir2)

            profiles1 = load_profiles_from_directory(profiles_dir1)
            profiles2 = load_profiles_from_directory(profiles_dir2)

            print(f"Comparing profiles:")
            print(f"  Directory 1 ({profiles_dir1}): {len(profiles1)} profiles")
            print(f"  Directory 2 ({profiles_dir2}): {len(profiles2)} profiles")

            # Generate reports for comparison
            analysis1 = analyze_profiles(profiles1)
            analysis2 = analyze_profiles(profiles2)

            # Compare issues
            issues1 = analysis1.get("analysis_results", {}).get("performance_issues", [])
            issues2 = analysis2.get("analysis_results", {}).get("performance_issues", [])

            print("
=== Comparison Results ===")
            print(f"Issues in directory 1: {len(issues1)}")
            print(f"Issues in directory 2: {len(issues2)}")

            if len(issues1) != len(issues2):
                improvement = (len(issues1) - len(issues2)) / max(len(issues1), 1) * 100
                print(f"Change: {improvement"+.1f"}%")

            # Show common issues
            if issues1 and issues2:
                print("
Common issue types:")
                issue_types1 = set(issue.get('issue_type') for issue in issues1)
                issue_types2 = set(issue.get('issue_type') for issue in issues2)
                common_types = issue_types1 & issue_types2

                for issue_type in common_types:
                    count1 = len([i for i in issues1 if i.get('issue_type') == issue_type])
                    count2 = len([i for i in issues2 if i.get('issue_type') == issue_type])
                    print(f"  {issue_type}: {count1} -> {count2}")

        else:
            parser.print_help()
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
