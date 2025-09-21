#!/usr/bin/env python3
"""
CI/CD script to register Avro schemas with Schema Registry.

This script is designed to be run as part of the CI/CD pipeline to:
- Register new schemas with Schema Registry
- Validate schema compatibility
- Set appropriate compatibility modes
- Generate schema documentation
- Verify schema references
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.schema_registry import (
    SchemaRegistryManager,
    SchemaRegistryConfig,
    SchemaCompatibilityMode,
    CompatibilityChecker,
    SchemaRegistryError,
    SubjectRegistrationError,
    SchemaCompatibilityError
)
from aurum.logging import StructuredLogger, LogLevel, create_logger


class SchemaRegistrationError(Exception):
    """Error during schema registration."""
    pass


class SchemaRegistrationScript:
    """CI/CD script for schema registration."""

    def __init__(self, config: SchemaRegistryConfig):
        """Initialize registration script.

        Args:
            config: Schema Registry configuration
        """
        self.registry_manager = SchemaRegistryManager(config)
        self.compatibility_checker = CompatibilityChecker()

        self.logger = create_logger(
            source_name="schema_registration_ci",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.schema_registry.ci",
            dataset="schema_registration"
        )

    def register_schemas_from_directory(
        self,
        schema_dir: Path,
        compatibility_mode: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Register all schemas from a directory.

        Args:
            schema_dir: Directory containing Avro schema files
            compatibility_mode: Default compatibility mode
            dry_run: If True, only validate without registering

        Returns:
            Registration results

        Raises:
            SchemaRegistrationError: If registration fails
        """
        if not schema_dir.exists():
            raise SchemaRegistrationError(f"Schema directory not found: {schema_dir}")

        results = {
            "total_schemas": 0,
            "registered_schemas": 0,
            "failed_registrations": 0,
            "compatibility_issues": 0,
            "schema_details": [],
            "errors": []
        }

        # Find all .avsc files
        schema_files = list(schema_dir.rglob("*.avsc"))
        results["total_schemas"] = len(schema_files)

        self.logger.log(
            LogLevel.INFO,
            f"Found {len(schema_files)} schema files in {schema_dir}",
            "schema_discovery",
            schema_dir=str(schema_dir),
            schema_count=len(schema_files)
        )

        for schema_file in schema_files:
            try:
                result = self.register_single_schema(
                    schema_file,
                    compatibility_mode,
                    dry_run
                )
                results["schema_details"].append(result)

                if result["registered"]:
                    results["registered_schemas"] += 1
                elif result["failed"]:
                    results["failed_registrations"] += 1

                if result["compatibility_issues"]:
                    results["compatibility_issues"] += 1

            except Exception as e:
                results["errors"].append({
                    "schema_file": str(schema_file),
                    "error": str(e)
                })
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failed to process schema {schema_file}: {e}",
                    "schema_processing_error",
                    schema_file=str(schema_file),
                    error=str(e)
                )

        # Log summary
        self.logger.log(
            LogLevel.INFO,
            f"Schema registration complete: {results['registered_schemas']}/{results['total_schemas']} registered",
            "schema_registration_complete",
            **results
        )

        return results

    def register_single_schema(
        self,
        schema_file: Path,
        compatibility_mode: SchemaCompatibilityMode,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Register a single schema file.

        Args:
            schema_file: Path to schema file
            compatibility_mode: Compatibility mode
            dry_run: If True, only validate without registering

        Returns:
            Registration result details
        """
        result = {
            "schema_file": str(schema_file),
            "subject": "",
            "registered": False,
            "failed": False,
            "compatibility_issues": False,
            "schema_id": None,
            "version": None,
            "errors": [],
            "warnings": []
        }

        try:
            # Load schema
            schema = json.loads(schema_file.read_text())
            subject = self._extract_subject_from_schema(schema, schema_file)

            result["subject"] = subject

            # Validate schema
            validation_errors = self._validate_schema_file(schema, schema_file)
            if validation_errors:
                result["errors"].extend(validation_errors)
                result["failed"] = True
                return result

            # Check compatibility
            compatibility_result = self.registry_manager.check_compatibility(subject, schema)

            if not compatibility_result.is_compatible:
                result["compatibility_issues"] = True
                result["errors"].append(f"Schema not compatible: {compatibility_result.summary()}")
                result["warnings"].extend(compatibility_result.messages)

                if self.registry_manager.config.fail_on_incompatible:
                    result["failed"] = True
                    return result

            # Register schema
            if not dry_run:
                try:
                    schema_info = self.registry_manager.register_subject(
                        subject,
                        schema,
                        compatibility_mode
                    )

                    result["registered"] = True
                    result["schema_id"] = schema_info.schema_id
                    result["version"] = schema_info.version

                    self.logger.log(
                        LogLevel.INFO,
                        f"Successfully registered schema {subject} version {schema_info.version}",
                        "schema_registered",
                        subject=subject,
                        schema_id=schema_info.schema_id,
                        version=schema_info.version
                    )

                except (SubjectRegistrationError, SchemaCompatibilityError) as e:
                    result["errors"].append(str(e))
                    result["failed"] = True

                    self.logger.log(
                        LogLevel.ERROR,
                        f"Failed to register schema {subject}: {e}",
                        "schema_registration_failed",
                        subject=subject,
                        error=str(e)
                    )
            else:
                result["warnings"].append("Dry run mode - schema not actually registered")

        except json.JSONDecodeError as e:
            result["errors"].append(f"Invalid JSON in schema file: {e}")
            result["failed"] = True
        except Exception as e:
            result["errors"].append(f"Unexpected error: {e}")
            result["failed"] = True

        return result

    def _extract_subject_from_schema(self, schema: Dict[str, Any], schema_file: Path) -> str:
        """Extract subject name from schema.

        Args:
            schema: Avro schema
            schema_file: Path to schema file

        Returns:
            Schema subject name
        """
        # Try to get subject from schema metadata
        namespace = schema.get("namespace", "")
        name = schema.get("name", "")

        if namespace and name:
            subject = f"{namespace}.{name}"
        else:
            # Fallback to filename
            subject = schema_file.stem

        # Add prefix/suffix from config
        config = self.registry_manager.config
        if config.subject_prefix and not subject.startswith(config.subject_prefix):
            subject = f"{config.subject_prefix}.{subject}"
        if config.subject_suffix and not subject.endswith(config.subject_suffix):
            subject = f"{subject}.{config.subject_suffix}"

        return subject

    def _validate_schema_file(self, schema: Dict[str, Any], schema_file: Path) -> List[str]:
        """Validate schema file.

        Args:
            schema: Avro schema
            schema_file: Path to schema file

        Returns:
            List of validation errors
        """
        errors = []

        # Check required fields
        if schema.get("type") != "record":
            errors.append("Schema must be of type 'record'")

        if "name" not in schema:
            errors.append("Schema must have a 'name' field")

        if "fields" not in schema:
            errors.append("Schema must have a 'fields' array")

        # Validate fields
        fields = schema.get("fields", [])
        if not isinstance(fields, list):
            errors.append("'fields' must be an array")
        else:
            for i, field in enumerate(fields):
                if not isinstance(field, dict):
                    errors.append(f"Field {i} must be an object")
                    continue

                if "name" not in field:
                    errors.append(f"Field {i} missing 'name'")
                if "type" not in field:
                    errors.append(f"Field {i} missing 'type'")

        return errors

    def generate_schema_report(self, results: Dict[str, Any], output_file: Optional[Path] = None) -> str:
        """Generate a schema registration report.

        Args:
            results: Registration results
            output_file: Optional output file path

        Returns:
            Report as string
        """
        report_lines = [
            "# Schema Registration Report",
            f"Generated: {str(datetime.now())}",
            "",
            "## Summary",
            f"- Total schemas: {results['total_schemas']}",
            f"- Registered: {results['registered_schemas']}",
            f"- Failed: {results['failed_registrations']}",
            f"- Compatibility issues: {results['compatibility_issues']}",
            ""
        ]

        if results["errors"]:
            report_lines.extend([
                "## Errors",
                ""
            ])
            for error in results["errors"]:
                report_lines.append(f"- {error['schema_file']}: {error['error']}")
            report_lines.append("")

        if results["schema_details"]:
            report_lines.extend([
                "## Schema Details",
                "",
                "| Schema File | Subject | Status | Schema ID | Version | Issues |",
                "|-------------|---------|--------|-----------|---------|---------|"
            ])

            for detail in results["schema_details"]:
                status = "✅" if detail["registered"] else "❌" if detail["failed"] else "⚠️"
                issues = "Yes" if detail["compatibility_issues"] else "No"

                report_lines.append(
                    f"| {Path(detail['schema_file']).name} | {detail['subject']} | {status} | "
                    f"{detail['schema_id'] or 'N/A'} | {detail['version'] or 'N/A'} | {issues} |"
                )

            report_lines.append("")

        report = "\n".join(report_lines)

        if output_file:
            output_file.write_text(report)
            print(f"Report saved to {output_file}")

        return report

    def verify_registry_health(self) -> Dict[str, Any]:
        """Verify Schema Registry health.

        Returns:
            Health check results
        """
        health = {
            "registry_status": {},
            "connection_ok": False,
            "subjects_accessible": False,
            "compatibility_enforced": False
        }

        try:
            # Check registry status
            status = self.registry_manager.get_registry_status()
            health["registry_status"] = status

            if "error" not in status:
                health["connection_ok"] = True
                health["subjects_accessible"] = True

                # Check if we have any subjects
                if status.get("total_subjects", 0) > 0:
                    health["subjects_accessible"] = True

            # Check compatibility enforcement
            health["compatibility_enforced"] = self.registry_manager.config.enforce_compatibility

        except Exception as e:
            health["registry_status"]["error"] = str(e)

        return health


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Register Avro schemas with Schema Registry"
    )

    parser.add_argument(
        "schema_dir",
        type=Path,
        help="Directory containing Avro schema files (.avsc)"
    )

    parser.add_argument(
        "--registry-url",
        default="http://localhost:8081",
        help="Schema Registry base URL"
    )

    parser.add_argument(
        "--compatibility",
        choices=["NONE", "BACKWARD", "FORWARD", "FULL", "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE"],
        default="BACKWARD",
        help="Schema compatibility mode"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate schemas without registering"
    )

    parser.add_argument(
        "--report",
        type=Path,
        help="Generate report file"
    )

    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Fail if any schema registration fails"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )

    args = parser.parse_args(argv)

    try:
        # Create configuration
        config = SchemaRegistryConfig(
            base_url=args.registry_url,
            default_compatibility_mode=SchemaCompatibilityMode(args.compatibility),
            enforce_compatibility=not args.dry_run,
            fail_on_incompatible=args.fail_on_error
        )

        # Create registration script
        script = SchemaRegistrationScript(config)

        # Verify registry health
        if args.verbose:
            health = script.verify_registry_health()
            print("Schema Registry Health:")
            print(f"  Connection: {'✅' if health['connection_ok'] else '❌'}")
            print(f"  Subjects Accessible: {'✅' if health['subjects_accessible'] else '❌'}")
            print(f"  Compatibility Enforced: {'✅' if health['compatibility_enforced'] else '❌'}")

        # Register schemas
        results = script.register_schemas_from_directory(
            args.schema_dir,
            SchemaCompatibilityMode(args.compatibility),
            args.dry_run
        )

        # Generate report
        report = script.generate_schema_report(results, args.report)
        if not args.report:
            print("\n" + report)

        # Check for failures
        if args.fail_on_error and (results["failed_registrations"] > 0 or results["errors"]):
            print(f"\n❌ Schema registration failed: {results['failed_registrations']} failures")
            return 1

        if results["registered_schemas"] > 0:
            print(f"\n✅ Successfully registered {results['registered_schemas']} schemas")
            return 0
        else:
            print(f"\n⚠️  No schemas were registered (dry run or no valid schemas)")
            return 0

    except SchemaRegistrationError as e:
        print(f"❌ Schema registration error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    from datetime import datetime
    sys.exit(main())
