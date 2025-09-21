#!/usr/bin/env python3
"""Dry-run renderer for SeaTunnel job templates with diff output and validation."""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from aurum.seatunnel.renderer import render_template, RendererError, _collect_placeholders

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = REPO_ROOT / "seatunnel" / "jobs" / "templates"
OUTPUT_DIR = REPO_ROOT / "seatunnel" / "jobs" / "dry_run_output"
DIFF_DIR = REPO_ROOT / "seatunnel" / "jobs" / "diffs"
HASH_FILE = REPO_ROOT / "seatunnel" / "jobs" / "template_hashes.json"

# Default sample values for common environment variables
DEFAULT_VALUES = {
    # Core infrastructure
    "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "AURUM_SCHEMA_REGISTRY_URL": "http://localhost:8081",

    # TimescaleDB (optional, for sink jobs)
    "AURUM_TIMESCALE_JDBC_URL": "jdbc:postgresql://localhost:5432/aurum",
    "AURUM_TIMESCALE_USER": "aurum",
    "AURUM_TIMESCALE_PASSWORD": "aurum",

    # Common API keys and endpoints (will be empty strings for dry-run)
    "EIA_API_KEY": "",
    "FRED_API_KEY": "",
    "NOAA_GHCND_TOKEN": "",
    "PJM_API_KEY": "",

    # Date ranges
    "EIA_START": "2024-01-01",
    "EIA_END": "2024-12-31",
    "NOAA_GHCND_START_DATE": "2024-01-01",
    "NOAA_GHCND_END_DATE": "2024-01-02",
    "FRED_START_DATE": "2024-01-01",
    "FRED_END_DATE": "2024-12-31",

    # Topics
    "EIA_TOPIC": "aurum.ref.eia.test.v1",
    "FRED_TOPIC": "aurum.ref.fred.test.v1",
    "CPI_TOPIC": "aurum.ref.cpi.test.v1",
    "NOAA_GHCND_TOPIC": "aurum.ref.noaa.weather.v1",

    # ISO-specific
    "PJM_ENDPOINT": "https://api.pjm.com/api/v1/test",
    "MISO_ENDPOINT": "https://api.misoenergy.org/test",
    "CAISO_ENDPOINT": "https://api.caiso.com/test",
    "AESO_ENDPOINT": "https://api.aeso.ca/test",
    "NYISO_URL": "https://api.nyiso.com/test",
    "ISONE_URL": "https://api.isone.com/test",
    "SPP_ENDPOINT": "https://api.spp.org/test",
    "ERCOT_INPUT_JSON": "/tmp/ercot_test.json",

    # Schema registry subjects
    "EIA_SUBJECT": "aurum.ref.eia.test.v1-value",
    "FRED_SUBJECT": "aurum.ref.fred.test.v1-value",
    "CPI_SUBJECT": "aurum.ref.cpi.test.v1-value",
    "NOAA_GHCND_SUBJECT": "aurum.ref.noaa.weather.v1-value",
}

# Schema content for rendering
SCHEMA_CONTENT = {
    "ISO_LMP_SCHEMA": '{"type":"record","name":"IsoLmpRecord","namespace":"aurum.iso","fields":[{"name":"iso_code","type":"string"}]}',
    "ISO_LOAD_SCHEMA": '{"type":"record","name":"IsoLoadRecord","namespace":"aurum.iso","fields":[{"name":"iso_code","type":"string"}]}',
    "ISO_GENMIX_SCHEMA": '{"type":"record","name":"IsoGenmixRecord","namespace":"aurum.iso","fields":[{"name":"iso_code","type":"string"}]}',
    "EIA_SERIES_SCHEMA": '{"type":"record","name":"EiaSeriesRecord","namespace":"aurum.eia","fields":[{"name":"series_id","type":"string"}]}',
    "FRED_SERIES_SCHEMA": '{"type":"record","name":"FredSeriesRecord","namespace":"aurum.fred","fields":[{"name":"series_id","type":"string"}]}',
    "CPI_SCHEMA": '{"type":"record","name":"CpiSeriesRecord","namespace":"aurum.cpi","fields":[{"name":"series_id","type":"string"}]}',
    "NOAA_GHCND_SCHEMA": '{"type":"record","name":"NoaaWeatherRecord","namespace":"aurum.noaa","fields":[{"name":"station","type":"string"}]}',
}

class DryRunError(RuntimeError):
    """Raised when dry-run rendering fails."""

class DryRunRenderer:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.template_hashes: Dict[str, str] = {}
        self._load_hashes()

    def _load_hashes(self) -> None:
        """Load previous template hashes for diff comparison."""
        if HASH_FILE.exists():
            try:
                with HASH_FILE.open(encoding="utf-8") as f:
                    self.template_hashes = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                if self.verbose:
                    print(f"Warning: Could not load hash file: {e}")

    def _save_hashes(self) -> None:
        """Save current template hashes for future diff comparison."""
        HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
        with HASH_FILE.open("w", encoding="utf-8") as f:
            json.dump(self.template_hashes, f, indent=2)

    def _get_template_hash(self, template_path: Path) -> str:
        """Compute hash of template content for change detection."""
        content = template_path.read_text(encoding="utf-8")
        return hashlib.sha256(content.encode()).hexdigest()

    def _get_sample_env(self, template_name: str) -> Dict[str, str]:
        """Get sample environment variables for the given template."""
        env = DEFAULT_VALUES.copy()

        # Add schema content
        env.update(SCHEMA_CONTENT)

        # Template-specific defaults
        if "eia" in template_name:
            env.update({
                "EIA_SERIES_PATH": "test/dataset",
                "EIA_SERIES_ID": "test_series",
                "EIA_FREQUENCY": "DAILY",
                "EIA_UNITS": "USD",
                "EIA_SERIES_ID_EXPR": "'test_series'",
                "EIA_AREA_EXPR": "CAST(NULL AS STRING)",
                "EIA_SECTOR_EXPR": "CAST(NULL AS STRING)",
                "EIA_DESCRIPTION_EXPR": "'Test EIA series'",
                "EIA_SOURCE_EXPR": "'EIA'",
                "EIA_DATASET_EXPR": "'test'",
                "EIA_FILTER_EXPR": "TRUE",
                "EIA_PERIOD_START_EXPR": "period",
                "EIA_PERIOD_END_EXPR": "period",
                "EIA_CANONICAL_VALUE_EXPR": "value",
                "EIA_CANONICAL_UNIT_EXPR": "'USD'",
                "EIA_CANONICAL_CURRENCY_EXPR": "'USD'",
            })
        elif "fred" in template_name:
            env.update({
                "FRED_SERIES_ID": "test_series",
                "FRED_FREQUENCY": "MONTHLY",
                "FRED_SEASONAL_ADJ": "SA",
            })
        elif "cpi" in template_name:
            env.update({
                "CPI_SERIES_ID": "test_series",
                "CPI_FREQUENCY": "MONTHLY",
                "CPI_SEASONAL_ADJ": "SA",
                "CPI_AREA": "US",
                "CPI_UNITS": "Index",
                "CPI_SOURCE": "FRED",
            })
        elif "noaa" in template_name:
            env.update({
                "NOAA_GHCND_DATASET": "GHCND",
                "NOAA_GHCND_LIMIT": "1000",
                "NOAA_GHCND_OFFSET": "1",
                "NOAA_GHCND_TIMEOUT": "30000",
                "NOAA_GHCND_STATION_LIMIT": "1000",
                "NOAA_GHCND_UNIT_CODE": "unknown",
            })

        return env

    def _validate_seatunnel_config(self, content: str) -> List[str]:
        """Validate SeaTunnel configuration syntax and common issues."""
        issues = []

        lines = content.splitlines()
        in_block = False
        block_stack = []

        for i, line in enumerate(lines, 1):
            line = line.strip()

            # Check for basic syntax issues
            if line.startswith("env {") or line.startswith("source {") or line.startswith("transform {") or line.startswith("sink {"):
                if in_block:
                    issues.append(f"Line {i}: Nested block without proper closure")
                in_block = True
                block_stack.append(line.split("{")[0])

            elif line.endswith("}"):
                if not in_block:
                    issues.append(f"Line {i}: Closing brace without matching opening block")
                elif not block_stack:
                    issues.append(f"Line {i}: Closing brace without matching opening block")
                else:
                    block_stack.pop()
                    if not block_stack:
                        in_block = False

            # Check for common configuration issues
            if "=" in line and not line.endswith("{") and not line.endswith("}"):
                parts = line.split("=", 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()

                    # Check for unquoted string values that might need quotes
                    if value and not (value.startswith('"') and value.endswith('"')) and not (value.startswith("'") and value.endswith("'")):
                        if any(c in value for c in " \t\n\r"):
                            issues.append(f"Line {i}: Value '{value}' contains whitespace and should be quoted")

                    # Check for empty values
                    if not value:
                        issues.append(f"Line {i}: Empty value for key '{key}'")

        # Check for unclosed blocks
        if block_stack:
            issues.append(f"Unclosed blocks: {', '.join(block_stack)}")

        return issues

    def _get_required_vars(self, template_name: str) -> List[str]:
        """Get required environment variables for the template."""
        # This is a simplified mapping - in a real implementation,
        # this would parse the run_job.sh script to extract required vars
        required_vars_map = {
            "eia_series_to_kafka": [
                "EIA_API_KEY", "EIA_SERIES_PATH", "EIA_SERIES_ID", "EIA_FREQUENCY", "EIA_TOPIC",
                "EIA_API_BASE_URL", "EIA_CONVERSION_FACTOR_EXPR", "EIA_DIRECTION", "EIA_LIMIT",
                "EIA_METADATA_EXPR", "EIA_OFFSET", "EIA_PARAM_OVERRIDES", "EIA_SEASONAL_ADJUSTMENT", "EIA_SORT"
            ],
            "fred_series_to_kafka": ["FRED_API_KEY", "FRED_SERIES_ID", "FRED_FREQUENCY", "FRED_SEASONAL_ADJ", "FRED_TOPIC"],
            "cpi_series_to_kafka": ["FRED_API_KEY", "CPI_SERIES_ID", "CPI_FREQUENCY", "CPI_SEASONAL_ADJ", "CPI_TOPIC"],
            "noaa_ghcnd_to_kafka": ["NOAA_GHCND_TOKEN", "NOAA_GHCND_START_DATE", "NOAA_GHCND_END_DATE", "NOAA_GHCND_TOPIC"],
        }

        return required_vars_map.get(template_name, [])

    def _render_template(self, template_path: Path, env: Dict[str, str], required_vars: List[str]) -> str:
        """Render a single template with the given environment."""
        # Merge custom env with system environment, with custom env taking precedence
        merged_env = dict(os.environ)
        merged_env.update(env)

        # Get all placeholders from the template
        template_content = template_path.read_text(encoding="utf-8")
        placeholders = _collect_placeholders(template_content)

        # Ensure all placeholders (not just required vars) are in the environment
        for placeholder in placeholders:
            if placeholder not in merged_env:
                # Set realistic defaults for dry-run rendering
                if placeholder == "EIA_API_KEY":
                    merged_env[placeholder] = "dry_run_api_key"
                elif placeholder.startswith("EIA_"):
                    merged_env[placeholder] = "dry_run_default"
                elif placeholder.startswith("FRED_"):
                    if placeholder == "FRED_API_KEY":
                        merged_env[placeholder] = "dry_run_fred_key"
                    else:
                        merged_env[placeholder] = "dry_run_default"
                elif placeholder.startswith("CPI_"):
                    merged_env[placeholder] = "dry_run_default"
                elif placeholder.startswith("NOAA_"):
                    if placeholder == "NOAA_GHCND_TOKEN":
                        merged_env[placeholder] = "dry_run_token"
                    else:
                        merged_env[placeholder] = "dry_run_default"
                else:
                    merged_env[placeholder] = "dry_run_default"

        try:
            output_path = OUTPUT_DIR / f"{template_path.stem}.conf"
            render_template(
                job=template_path.stem,
                template_path=template_path,
                output_path=output_path,
                required_vars=[],  # Don't enforce required vars for dry-run
                env=merged_env
            )
            return output_path.read_text(encoding="utf-8")
        except RendererError as e:
            raise DryRunError(f"Failed to render {template_path.name}: {e}")

    def render_template_dry_run(self, template_path: Path) -> Dict[str, Any]:
        """Perform dry-run rendering of a single template."""
        template_name = template_path.stem
        env = self._get_sample_env(template_name)
        required_vars = self._get_required_vars(template_name)

        # Compute template hash for change detection
        current_hash = self._get_template_hash(template_path)
        previous_hash = self.template_hashes.get(template_name)

        result = {
            "template": template_name,
            "path": str(template_path),
            "hash": current_hash,
            "changed": current_hash != previous_hash,
            "required_vars": required_vars,
            "placeholders": sorted(_collect_placeholders(template_path.read_text())),
        }

        try:
            rendered_content = self._render_template(template_path, env, required_vars)
            result["rendered"] = True
            result["content"] = rendered_content
            result["content_length"] = len(rendered_content)
            result["line_count"] = len(rendered_content.splitlines())

            # Validate rendered configuration
            validation_issues = self._validate_seatunnel_config(rendered_content)
            result["validation_issues"] = validation_issues
            result["valid"] = len(validation_issues) == 0

            # Generate diff if template changed
            if result["changed"] and previous_hash:
                try:
                    # Try to read previous rendered content
                    prev_output_path = OUTPUT_DIR / f"{template_name}_previous.conf"
                    if prev_output_path.exists():
                        prev_content = prev_output_path.read_text(encoding="utf-8")
                        diff = list(difflib.unified_diff(
                            prev_content.splitlines(keepends=True),
                            rendered_content.splitlines(keepends=True),
                            fromfile=f"{template_name}_previous.conf",
                            tofile=f"{template_name}.conf",
                            lineterm="",
                        ))
                        result["diff"] = "".join(diff)
                        result["diff_available"] = True

                        # Save current as previous for next run
                        prev_output_path.write_text(rendered_content, encoding="utf-8")
                    else:
                        result["diff_available"] = False
                except Exception as e:
                    result["diff_error"] = str(e)
                    result["diff_available"] = False

        except DryRunError as e:
            result["rendered"] = False
            result["error"] = str(e)
            result["content"] = None
            result["validation_issues"] = []
            result["valid"] = False

        # Update hash tracking
        self.template_hashes[template_name] = current_hash

        return result

    def render_all_templates(self) -> List[Dict[str, Any]]:
        """Render all SeaTunnel templates and return results."""
        if not TEMPLATE_DIR.exists():
            raise DryRunError(f"Template directory not found: {TEMPLATE_DIR}")

        results = []
        for template_path in sorted(TEMPLATE_DIR.glob("*.conf.tmpl")):
            if self.verbose:
                print(f"Processing {template_path.name}...")

            result = self.render_template_dry_run(template_path)
            results.append(result)

        return results

    def generate_report(self, results: List[Dict[str, Any]]) -> str:
        """Generate a comprehensive report of the dry-run results."""
        report_lines = [
            "# SeaTunnel Template Dry-Run Report",
            f"Generated: {len(results)} templates processed",
            "",
        ]

        # Summary statistics
        successful = sum(1 for r in results if r.get("rendered", False))
        failed = len(results) - successful
        changed = sum(1 for r in results if r.get("changed", False))
        valid = sum(1 for r in results if r.get("valid", False))
        with_validation_issues = sum(1 for r in results if r.get("rendered", False) and len(r.get("validation_issues", [])) > 0)

        report_lines.extend([
            "## Summary",
            f"- Successful renders: {successful}",
            f"- Failed renders: {failed}",
            f"- Templates changed: {changed}",
            f"- Valid configurations: {valid}",
            f"- Configurations with validation issues: {with_validation_issues}",
            "",
        ])

        # Failed templates
        if failed > 0:
            report_lines.extend([
                "## Failed Templates",
            ])
            for result in results:
                if not result.get("rendered", False):
                    report_lines.append(f"- **{result['template']}**: {result.get('error', 'Unknown error')}")
            report_lines.append("")

        # Changed templates
        if changed > 0:
            report_lines.extend([
                "## Changed Templates",
            ])
            for result in results:
                if result.get("changed", False):
                    report_lines.append(f"- **{result['template']}**")
                    placeholders = result.get("placeholders", [])
                    if placeholders:
                        report_lines.append(f"  - Placeholders: {', '.join(placeholders)}")
                    if result.get("diff_available", False):
                        report_lines.append("  - Diff available (check diff directory)")
            report_lines.append("")

        # Template details
        report_lines.extend([
            "## Template Details",
        ])
        for result in results:
            status = "✅" if result.get("rendered", False) else "❌"
            report_lines.append(f"### {result['template']} {status}")

            if result.get("rendered", False):
                report_lines.append(f"- Placeholders: {len(result.get('placeholders', []))}")
                report_lines.append(f"- Rendered lines: {result.get('line_count', 0)}")
                report_lines.append(f"- Content length: {result.get('content_length', 0)}")
                report_lines.append(f"- Valid: {'✅' if result.get('valid', False) else '❌'}")

                validation_issues = result.get("validation_issues", [])
                if validation_issues:
                    report_lines.append(f"- Validation issues: {len(validation_issues)}")
                    for issue in validation_issues[:3]:  # Show first 3 issues
                        report_lines.append(f"  - {issue}")
                    if len(validation_issues) > 3:
                        report_lines.append(f"  - ... and {len(validation_issues) - 3} more")

                if result.get("diff_available", False):
                    report_lines.append("- Diff available (check rendered files)")
            else:
                report_lines.append(f"- Error: {result.get('error', 'Unknown error')}")

            report_lines.append("")

        return "\n".join(report_lines)

def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run renderer for SeaTunnel templates")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--report-only", action="store_true", help="Generate report without rendering")
    parser.add_argument("--template", type=str, help="Process only this template")

    args = parser.parse_args()

    try:
        renderer = DryRunRenderer(verbose=args.verbose)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        if args.template:
            # Process single template
            template_path = TEMPLATE_DIR / f"{args.template}.conf.tmpl"
            if not template_path.exists():
                print(f"Template not found: {template_path}", file=sys.stderr)
                return 1

            result = renderer.render_template_dry_run(template_path)
            print(json.dumps(result, indent=2))
        else:
            # Process all templates
            results = renderer.render_all_templates()

            if not args.report_only:
                print(f"Processed {len(results)} templates")

            # Generate and save report
            report = renderer.generate_report(results)
            report_path = OUTPUT_DIR / "dry_run_report.md"
            report_path.write_text(report, encoding="utf-8")

            if args.report_only:
                print(report)
            else:
                print(f"Report saved to: {report_path}")

        # Save hashes for future diff comparison
        renderer._save_hashes()

    except DryRunError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
