#!/usr/bin/env python3
"""Validate current configuration against data source quotas."""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class ValidationIssue:
    """A validation issue found during quota checking."""

    source: str
    issue_type: str  # "quota_violation", "missing_config", "invalid_value"
    severity: str  # "error", "warning", "info"
    message: str
    current_value: Any
    recommended_value: Any
    impact: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source": self.source,
            "issue_type": self.issue_type,
            "severity": self.severity,
            "message": self.message,
            "current_value": self.current_value,
            "recommended_value": self.recommended_value,
            "impact": self.impact
        }


class QuotaValidator:
    """Validate configuration against data source quotas."""

    def __init__(self, quotas_file: Path, env_file: Optional[Path] = None):
        """Initialize validator.

        Args:
            quotas_file: Path to quotas configuration
            env_file: Path to environment file (optional)
        """
        self.quotas_file = quotas_file
        self.env_file = env_file
        self.quotas_data = self._load_quotas()
        self.env_vars = self._load_env_vars()
        self.issues: List[ValidationIssue] = []

    def _load_quotas(self) -> Dict[str, Any]:
        """Load quotas configuration."""
        with open(self.quotas_file, 'r') as f:
            return json.load(f)

    def _load_env_vars(self) -> Dict[str, str]:
        """Load environment variables."""
        env_vars = dict(os.environ)

        if self.env_file and self.env_file.exists():
            load_dotenv(self.env_file)
            # Reload with .env file values
            env_vars.update(dict(os.environ))

        return env_vars

    def validate_all_sources(self) -> List[ValidationIssue]:
        """Validate all data sources."""
        self.issues = []

        sources = list(self.quotas_data["data_sources"].keys())

        for source in sources:
            self.validate_source(source)

        return self.issues

    def validate_source(self, source: str) -> List[ValidationIssue]:
        """Validate a specific data source.

        Args:
            source: Data source name

        Returns:
            List of validation issues
        """
        if source not in self.quotas_data["data_sources"]:
            self.issues.append(ValidationIssue(
                source=source,
                issue_type="missing_config",
                severity="error",
                message=f"Data source '{source}' not found in quotas configuration",
                current_value=None,
                recommended_value="Add to config/data_source_quotas.json",
                impact="Cannot validate quotas for this source"
            ))
            return self.issues

        source_config = self.quotas_data["data_sources"][source]
        source_issues = []

        # Validate environment variables
        env_prefix = f"AURUM_{source.upper()}"
        env_vars = {k: v for k, v in self.env_vars.items() if k.startswith(env_prefix)}

        # Check for missing critical variables
        critical_vars = [
            f"{env_prefix}_BATCH_SIZE",
            f"{env_prefix}_MAX_CONCURRENT_JOBS",
            f"{env_prefix}_RETRY_BACKOFF_SECONDS",
            f"{env_prefix}_RATE_LIMIT_SLEEP_MS"
        ]

        for var in critical_vars:
            if var not in env_vars:
                self.issues.append(ValidationIssue(
                    source=source,
                    issue_type="missing_config",
                    severity="warning",
                    message=f"Missing environment variable: {var}",
                    current_value=None,
                    recommended_value=source_config["optimal_settings"].get(
                        var.split('_')[-1].lower(), 'N/A'
                    ),
                    impact="Using default values, may not be optimal"
                ))

        # Validate values against quotas
        if env_vars:
            self._validate_env_values(source, env_vars, source_config)

        return self.issues

    def _validate_env_values(self, source: str, env_vars: Dict[str, str], source_config: Dict[str, Any]):
        """Validate environment variable values against quotas.

        Args:
            source: Data source name
            env_vars: Environment variables
            source_config: Source configuration
        """
        quotas = source_config["quotas"]
        optimal = source_config["optimal_settings"]

        # Validate batch size
        batch_size_var = f"AURUM_{source.upper()}_BATCH_SIZE"
        if batch_size_var in env_vars:
            batch_size = int(env_vars[batch_size_var])
            if batch_size > quotas.get("requests_per_minute", 1000):
                self.issues.append(ValidationIssue(
                    source=source,
                    issue_type="quota_violation",
                    severity="warning",
                    message=f"Batch size ({batch_size}) may exceed rate limits",
                    current_value=batch_size,
                    recommended_value=optimal["batch_size"],
                    impact="May cause rate limiting or API failures"
                ))

        # Validate concurrent jobs
        concurrent_var = f"AURUM_{source.upper()}_MAX_CONCURRENT_JOBS"
        if concurrent_var in env_vars:
            concurrent_jobs = int(env_vars[concurrent_var])
            if concurrent_jobs > quotas.get("concurrent_connections", 10):
                self.issues.append(ValidationIssue(
                    source=source,
                    issue_type="quota_violation",
                    severity="warning",
                    message=f"Max concurrent jobs ({concurrent_jobs}) exceeds connection limits",
                    current_value=concurrent_jobs,
                    recommended_value=optimal["max_concurrent_jobs"],
                    impact="May cause connection failures"
                ))

        # Validate retry backoff
        backoff_var = f"AURUM_{source.upper()}_RETRY_BACKOFF_SECONDS"
        if backoff_var in env_vars:
            backoff = int(env_vars[backoff_var])
            if backoff < 1:
                self.issues.append(ValidationIssue(
                    source=source,
                    issue_type="invalid_value",
                    severity="warning",
                    message=f"Retry backoff ({backoff}s) is too aggressive",
                    current_value=backoff,
                    recommended_value=optimal["retry_backoff_seconds"],
                    impact="May cause rapid retry loops"
                ))

        # Validate rate limit sleep
        sleep_var = f"AURUM_{source.upper()}_RATE_LIMIT_SLEEP_MS"
        if sleep_var in env_vars:
            sleep_ms = int(env_vars[sleep_var])
            if sleep_ms < 50:
                self.issues.append(ValidationIssue(
                    source=source,
                    issue_type="invalid_value",
                    severity="info",
                    message=f"Rate limit sleep ({sleep_ms}ms) may be too short",
                    current_value=sleep_ms,
                    recommended_value=optimal["rate_limit_sleep_ms"],
                    impact="May not provide sufficient rate limiting"
                ))

    def generate_report(self, output_format: str = "text") -> str:
        """Generate validation report.

        Args:
            output_format: Output format (text, json, markdown)

        Returns:
            Report string
        """
        if output_format == "json":
            return self._generate_json_report()
        elif output_format == "markdown":
            return self._generate_markdown_report()
        else:
            return self._generate_text_report()

    def _generate_text_report(self) -> str:
        """Generate text report."""
        report = []
        report.append("Quota Validation Report")
        report.append("=" * 50)
        report.append(f"Generated: {self._get_timestamp()}")
        report.append(f"Sources validated: {len(self.quotas_data['data_sources'])}")
        report.append("")

        if not self.issues:
            report.append("✅ No issues found. All configurations are valid.")
            return "\n".join(report)

        # Group issues by severity
        errors = [i for i in self.issues if i.severity == "error"]
        warnings = [i for i in self.issues if i.severity == "warning"]
        infos = [i for i in self.issues if i.severity == "info"]

        if errors:
            report.append(f"❌ Errors ({len(errors)}):")
            for issue in errors:
                report.append(f"  - {issue.source}: {issue.message}")

        if warnings:
            report.append(f"⚠️  Warnings ({len(warnings)}):")
            for issue in warnings:
                report.append(f"  - {issue.source}: {issue.message}")

        if infos:
            report.append(f"ℹ️  Info ({len(infos)}):")
            for issue in infos:
                report.append(f"  - {issue.source}: {issue.message}")

        report.append("")
        report.append("Recommendations:")
        report.append("- Fix all error issues before deployment")
        report.append("- Review warning issues and consider adjustments")
        report.append("- Info issues are suggestions for optimization")

        return "\n".join(report)

    def _generate_json_report(self) -> str:
        """Generate JSON report."""
        report_data = {
            "generated_at": self._get_timestamp(),
            "summary": {
                "total_issues": len(self.issues),
                "errors": len([i for i in self.issues if i.severity == "error"]),
                "warnings": len([i for i in self.issues if i.severity == "warning"]),
                "info": len([i for i in self.issues if i.severity == "info"])
            },
            "issues": [issue.to_dict() for issue in self.issues]
        }

        return json.dumps(report_data, indent=2)

    def _generate_markdown_report(self) -> str:
        """Generate Markdown report."""
        report = []
        report.append("# Quota Validation Report")
        report.append("")
        report.append(f"Generated: {self._get_timestamp()}")
        report.append("")

        if not self.issues:
            report.append("✅ **No issues found.** All configurations are valid.")
            return "\n".join(report)

        # Summary
        errors = len([i for i in self.issues if i.severity == "error"])
        warnings = len([i for i in self.issues if i.severity == "warning"])
        infos = len([i for i in self.issues if i.severity == "info"])

        report.append("## Summary")
        report.append(f"- Total issues: {len(self.issues)}")
        report.append(f"- Errors: {errors}")
        report.append(f"- Warnings: {warnings}")
        report.append(f"- Info: {infos}")
        report.append("")

        # Issues by severity
        for severity, title, emoji in [("error", "Errors", "❌"), ("warning", "Warnings", "⚠️"), ("info", "Info", "ℹ️")]:
            severity_issues = [i for i in self.issues if i.severity == severity]
            if severity_issues:
                report.append(f"## {title}")
                report.append("")
                report.append("| Source | Issue | Current | Recommended | Impact |")
                report.append("|--------|--------|---------|-------------|---------|")

                for issue in severity_issues:
                    report.append(f"| {issue.source} | {issue.message} | {issue.current_value} | {issue.recommended_value} | {issue.impact} |")

                report.append("")

        report.append("## Recommendations")
        report.append("")
        report.append("- **Fix all error issues** before deployment")
        report.append("- **Review warning issues** and consider adjustments")
        report.append("- **Info issues** are suggestions for optimization")
        report.append("")
        report.append("Run this validation regularly to ensure compliance with API quotas.")

        return "\n".join(report)

    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Validate configuration against data source quotas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all sources against current environment
  python validate_quotas.py

  # Validate specific sources
  python validate_quotas.py --sources eia fred

  # Use specific environment file
  python validate_quotas.py --env-file .env.production

  # Generate JSON report
  python validate_quotas.py --format json --output validation_report.json

  # Generate Markdown report
  python validate_quotas.py --format markdown --output validation_report.md
        """
    )

    parser.add_argument(
        "--quotas-file",
        default="config/data_source_quotas.json",
        help="Path to quotas configuration file"
    )

    parser.add_argument(
        "--env-file",
        help="Path to environment file to validate"
    )

    parser.add_argument(
        "--sources",
        nargs="*",
        help="Specific sources to validate (default: all)"
    )

    parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format"
    )

    parser.add_argument(
        "--output", "-o",
        help="Output file path"
    )

    args = parser.parse_args()

    # Initialize validator
    quotas_file = Path(args.quotas_file)
    if not quotas_file.exists():
        print(f"Error: Quotas file not found: {quotas_file}")
        return 1

    env_file = Path(args.env_file) if args.env_file else None

    validator = QuotaValidator(quotas_file, env_file)

    # Validate sources
    if args.sources:
        for source in args.sources:
            validator.validate_source(source)
    else:
        validator.validate_all_sources()

    # Generate report
    report = validator.generate_report(args.format)

    # Output report
    if args.output:
        output_file = Path(args.output)
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"Report saved to: {output_file}")
    else:
        print(report)

    # Exit with appropriate code
    error_count = len([i for i in validator.issues if i.severity == "error"])
    if error_count > 0:
        print(f"\n❌ Found {error_count} errors. Please fix before deployment.")
        return 1
    elif len(validator.issues) > 0:
        print(f"\n⚠️  Found {len(validator.issues)} issues. Review warnings.")
        return 0
    else:
        print("\n✅ Validation passed. No issues found.")
        return 0


if __name__ == "__main__":
    exit(main())
