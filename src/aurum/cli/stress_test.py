"""CLI commands for stress testing and scenario analysis."""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from typing import Dict, List, Optional

from ..scenarios.stress_testing import get_stress_test_engine, StressTestConfig


def add_stress_test_subparser(subparsers):
    """Add stress test subcommands to the CLI."""

    stress_test_parser = subparsers.add_parser(
        "stress-test",
        help="Run stress test scenarios"
    )
    stress_test_parser.set_defaults(func=handle_stress_test)

    # Subcommands
    stress_subparsers = stress_test_parser.add_subparsers(
        dest="stress_command",
        help="Stress test commands"
    )

    # List templates
    list_parser = stress_subparsers.add_parser(
        "list",
        help="List available stress test templates"
    )
    list_parser.add_argument(
        "--category",
        choices=["policy", "outage", "weather"],
        help="Filter by category"
    )
    list_parser.set_defaults(func=handle_list_templates)

    # Run single test
    run_parser = stress_subparsers.add_parser(
        "run",
        help="Run a single stress test"
    )
    run_parser.add_argument(
        "template_id",
        help="Template ID to run"
    )
    run_parser.add_argument(
        "--severity",
        choices=["low", "medium", "high", "extreme"],
        default="medium",
        help="Severity level"
    )
    run_parser.add_argument(
        "--duration",
        type=int,
        help="Duration in hours"
    )
    run_parser.add_argument(
        "--impact-multiplier",
        type=float,
        default=1.0,
        help="Impact multiplier"
    )
    run_parser.set_defaults(func=handle_run_test)

    # Run batch tests
    batch_parser = stress_subparsers.add_parser(
        "batch",
        help="Run multiple stress tests"
    )
    batch_parser.add_argument(
        "category",
        choices=["policy", "outage", "weather", "all"],
        help="Category of tests to run"
    )
    batch_parser.set_defaults(func=handle_batch_tests)

    # Generate report
    report_parser = stress_subparsers.add_parser(
        "report",
        help="Generate stress test report"
    )
    report_parser.set_defaults(func=handle_generate_report)

    # Interactive mode
    interactive_parser = stress_subparsers.add_parser(
        "interactive",
        help="Run interactive stress test session"
    )
    interactive_parser.set_defaults(func=handle_interactive)


async def handle_stress_test(args: argparse.Namespace) -> None:
    """Main handler for stress test commands."""
    engine = get_stress_test_engine()

    if hasattr(args, 'func'):
        await args.func(args, engine)
    else:
        print("No stress test command specified. Use --help for available commands.")


async def handle_list_templates(args: argparse.Namespace, engine) -> None:
    """List available stress test templates."""
    category = getattr(args, 'category', None)
    templates = engine.list_templates(category)

    if not templates:
        print("No templates found.")
        return

    print(f"Available Templates ({len(templates)}):")
    print("-" * 80)

    for template in templates:
        print(f"ID: {template.template_id}")
        print(f"Name: {template.name}")
        print(f"Category: {template.category}")
        print(f"Description: {template.description}")
        print(f"Probability: {template.default_config.probability}")
        print(f"Severity: {template.default_config.severity}")
        print(f"Duration: {template.default_config.duration_hours}h")
        print()


async def handle_run_test(args: argparse.Namespace, engine) -> None:
    """Run a single stress test."""
    template_id = args.template_id

    template = engine.get_template(template_id)
    if not template:
        print(f"Template not found: {template_id}")
        return

    # Build configuration
    config = template.default_config.copy()
    config.severity = args.severity
    config.impact_multiplier = args.impact_multiplier

    if args.duration:
        config.duration_hours = args.duration

    print(f"Running stress test: {template.name}")
    print(f"Template: {template_id}")
    print(f"Severity: {config.severity}")
    print(f"Duration: {config.duration_hours} hours")
    print(f"Impact Multiplier: {config.impact_multiplier}")
    print("Please wait...")

    impact = await engine.run_stress_test(template_id, config)

    if impact:
        print("\n" + "="*60)
        print("STRESS TEST RESULTS")
        print("="*60)
        print(f"Scenario ID: {impact.scenario_id}")
        print(f"Portfolio Impact: {impact.portfolio_impact}")
        print(f"Affected Curves: {len(impact.affected_curves)}")
        print(f"Confidence Level: {impact.confidence_level}")
        print()
        print("Risk Metrics:")
        print(f"  VaR 95%: {impact.risk_metrics.get('var_95', 0)}")
        print(f"  VaR 99%: {impact.risk_metrics.get('var_99', 0)}")
        print(f"  Max Drawdown: {impact.risk_metrics.get('max_drawdown', 0)}")
        print()
        print("Price Impacts:")
        for curve_key, multiplier in impact.price_impact.items():
            print(f"  {curve_key}: {multiplier".2%"}")
        print()
        print("Volume Impacts:")
        for curve_key, multiplier in impact.volume_impact.items():
            print(f"  {curve_key}: {multiplier".2%"}")
    else:
        print("Stress test failed")


async def handle_batch_tests(args: argparse.Namespace, engine) -> None:
    """Run multiple stress tests."""
    category = args.category

    templates = engine.list_templates()
    if category != "all":
        templates = [t for t in templates if t.category == category]

    if not templates:
        print(f"No templates found for category: {category}")
        return

    print(f"Running {len(templates)} stress tests for category: {category}")
    print("Please wait...")

    results = await engine.run_multiple_scenarios([t.template_id for t in templates])

    successful = sum(1 for impact in results.values() if impact)
    failed = len(results) - successful

    print(f"\nCompleted: {successful} successful, {failed} failed")

    if successful > 0:
        report = await engine.generate_stress_test_report(results)
        print("
Summary:")
        print(f"Total Portfolio Impact: {report['total_portfolio_impact']}")
        print(f"VaR 95%: {report['aggregated_risk_metrics']['var_95']}")
        print(f"VaR 99%: {report['aggregated_risk_metrics']['var_99']}")
        print("
Most Affected Curves:")
        for curve, count in report['most_affected_curves'][:3]:
            print(f"  {curve}: affected by {count} scenarios")


async def handle_generate_report(args: argparse.Namespace, engine) -> None:
    """Generate comprehensive stress test report."""
    print("Generating comprehensive stress test report...")
    print("This will run all available stress test scenarios.")
    print("Please wait...")

    templates = engine.list_templates()
    results = await engine.run_multiple_scenarios([t.template_id for t in templates])

    report = await engine.generate_stress_test_report(results)

    print("\n" + "="*80)
    print("COMPREHENSIVE STRESS TEST REPORT")
    print("="*80)
    print(f"Report Date: {report['report_date']}")
    print(f"Scenarios Analyzed: {report['scenarios_analyzed']}")
    print()
    print("AGGREGATED RISK METRICS:")
    print(f"  Total Portfolio Impact: {report['total_portfolio_impact']}")
    print(f"  VaR 95%: {report['aggregated_risk_metrics']['var_95']}")
    print(f"  VaR 99%: {report['aggregated_risk_metrics']['var_99']}")
    print(f"  Max Drawdown: {report['aggregated_risk_metrics']['max_drawdown']}")
    print()
    print("MOST AFFECTED CURVES:")
    for curve, count in report['most_affected_curves']:
        print(f"  {curve}: {count} scenarios")
    print()
    print("SCENARIO DETAILS:")
    for template_id, details in report['scenario_details'].items():
        print(f"  {template_id}:")
        print(f"    Impact: {details['impact']}")
        print(f"    Affected Curves: {details['affected_curves']}")
        print(f"    Confidence: {details['confidence_level']}")
    print()

    # Save report to file
    report_filename = f"stress_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    print(f"Report saved to: {report_filename}")


async def handle_interactive(args: argparse.Namespace, engine) -> None:
    """Run interactive stress test session."""
    from ..scenarios.stress_testing import StressTestCLI

    cli = StressTestCLI(engine)
    await cli.run_interactive_session()


def main():
    """Main entry point for stress test CLI."""
    parser = argparse.ArgumentParser(description="Aurum Stress Testing CLI")
    parser.add_argument("--config", help="Configuration file path")

    args = parser.parse_args()

    # Run the CLI
    asyncio.run(handle_stress_test(args))


if __name__ == "__main__":
    main()
