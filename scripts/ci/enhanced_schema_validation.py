#!/usr/bin/env python3
"""Enhanced schema validation for CI/CD pipeline with policy enforcement."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))

from aurum.schema_registry.policy_manager import (
    SchemaRegistryPolicyManager,
    SchemaRegistryPolicyConfig,
    SubjectPolicy,
    SchemaCompatibilityMode,
    PolicyEnforcementLevel,
    SchemaEvolutionPolicy
)
from aurum.schema_registry.registry_manager import (
    SchemaRegistryManager,
    SchemaRegistryConfig
)


class EnhancedSchemaValidator:
    """Enhanced schema validator with comprehensive policy enforcement."""

    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        self.schema_registry_url = schema_registry_url
        self.schemas_dir = Path("kafka/schemas")

        # Initialize components
        self.policy_manager: Optional[SchemaRegistryPolicyManager] = None
        self.registry_manager: Optional[SchemaRegistryManager] = None

    async def initialize(self):
        """Initialize validators and managers."""
        # Setup policy configuration
        policy_config = SchemaRegistryPolicyConfig(
            subject_policies=[
                # Default policy for all subjects
                SubjectPolicy(
                    subject_pattern="*",
                    compatibility_mode=SchemaCompatibilityMode.BACKWARD,
                    evolution_policy=SchemaEvolutionPolicy.BACKWARD_COMPATIBLE,
                    enforcement_level=PolicyEnforcementLevel.FAIL,
                    allow_field_addition=True,
                    allow_field_removal=False,
                    require_documentation=True,
                    allowed_namespaces=["aurum"],
                    blocked_field_names=["password", "secret", "key", "token", "credential"]
                ),
                # Specific policy for reference data
                SubjectPolicy(
                    subject_pattern="aurum.ref.*",
                    compatibility_mode=SchemaCompatibilityMode.BACKWARD,
                    evolution_policy=SchemaEvolutionPolicy.BACKWARD_COMPATIBLE,
                    enforcement_level=PolicyEnforcementLevel.FAIL,
                    allow_field_addition=True,
                    allow_field_removal=False,
                    max_field_count=50,
                    field_name_pattern=r'^[a-z][a-z0-9_]*$'
                ),
                # Specific policy for scenario data
                SubjectPolicy(
                    subject_pattern="aurum.scenario.*",
                    compatibility_mode=SchemaCompatibilityMode.FULL,
                    evolution_policy=SchemaEvolutionPolicy.FULL_COMPATIBLE,
                    enforcement_level=PolicyEnforcementLevel.WARN,
                    allow_field_addition=True,
                    allow_field_removal=False,
                    require_documentation=True
                )
            ],
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD,
            default_enforcement_level=PolicyEnforcementLevel.FAIL,
            ci_validation_enabled=True,
            require_ci_approval=False,
            enable_performance_checks=True,
            max_schema_size_bytes=1024 * 1024,  # 1MB
            test_compatibility_modes=[
                SchemaCompatibilityMode.BACKWARD,
                SchemaCompatibilityMode.FORWARD,
                SchemaCompatibilityMode.FULL
            ]
        )

        # Initialize policy manager
        self.policy_manager = SchemaRegistryPolicyManager(policy_config)

        # Initialize registry manager
        registry_config = SchemaRegistryConfig(
            base_url=self.schema_registry_url,
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD,
            enforce_compatibility=True,
            fail_on_incompatible=True,
            validate_schema=True
        )

        self.registry_manager = SchemaRegistryManager(registry_config)

        print("✅ Enhanced schema validation initialized")

    async def validate_all_schemas(self) -> Dict[str, any]:
        """Run comprehensive validation on all schemas."""
        print("🔍 Starting comprehensive schema validation...")

        results = {
            'validation_timestamp': datetime.now().isoformat(),
            'total_schemas': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'policy_violations': 0,
            'schema_registry_errors': 0,
            'schema_details': [],
            'summary': {}
        }

        # Get all schema files
        schema_files = list(self.schemas_dir.glob("*.avsc")) + list(self.schemas_dir.glob("*.json"))
        results['total_schemas'] = len(schema_files)

        if not schema_files:
            print("❌ No schema files found")
            results['summary'] = {'status': 'FAILED', 'reason': 'No schema files found'}
            return results

        print(f"📋 Found {len(schema_files)} schema files to validate")

        # Validate each schema
        for schema_file in schema_files:
            print(f"\n🔎 Validating {schema_file.name}...")

            schema_result = await self.validate_single_schema(schema_file)
            results['schema_details'].append(schema_result)

            if schema_result['overall_status'] == 'PASSED':
                results['passed_validations'] += 1
            else:
                results['failed_validations'] += 1

                # Count specific failure types
                if any(test['test_type'] == 'policy_validation' and not test['passed']
                      for test in schema_result.get('validation_results', [])):
                    results['policy_violations'] += 1

                if any(test['test_type'] == 'registry_compatibility' and not test['passed']
                      for test in schema_result.get('validation_results', [])):
                    results['schema_registry_errors'] += 1

        # Generate summary
        results['summary'] = self._generate_validation_summary(results)

        return results

    async def validate_single_schema(self, schema_file: Path) -> Dict[str, any]:
        """Validate a single schema file comprehensively."""
        schema_result = {
            'file_name': schema_file.name,
            'file_path': str(schema_file),
            'overall_status': 'PASSED',
            'validation_results': [],
            'policy_violations': [],
            'recommendations': [],
            'metadata': {}
        }

        try:
            # Load schema
            with open(schema_file, 'r') as f:
                schema = json.load(f)

            schema_result['metadata'] = {
                'schema_name': schema.get('name', 'unknown'),
                'schema_namespace': schema.get('namespace', ''),
                'schema_type': schema.get('type', 'unknown'),
                'field_count': len(schema.get('fields', [])) if schema.get('type') == 'record' else 0
            }

            # Run comprehensive tests
            validation_results = await self.policy_manager.run_comprehensive_compatibility_tests(
                schema_file, self.registry_manager
            )

            schema_result['validation_results'] = [
                {
                    'test_type': result.test_type,
                    'passed': result.passed,
                    'messages': result.messages,
                    'details': result.details,
                    'execution_time_seconds': result.execution_time_seconds
                }
                for result in validation_results
            ]

            # Check overall status
            if not all(result.passed for result in validation_results):
                schema_result['overall_status'] = 'FAILED'

                # Collect policy violations
                policy_violations = [
                    msg for result in validation_results
                    if not result.passed
                    for msg in result.messages
                ]
                schema_result['policy_violations'] = policy_violations

            # Generate recommendations
            schema_result['recommendations'] = self._generate_schema_recommendations(
                schema, validation_results
            )

            status_icon = "✅" if schema_result['overall_status'] == 'PASSED' else "❌"
            print(f"  {status_icon} {schema_file.name} - {schema_result['overall_status']}")

            if policy_violations:
                print(f"    Policy violations: {len(policy_violations)}")

        except Exception as e:
            schema_result['overall_status'] = 'ERROR'
            schema_result['validation_results'].append({
                'test_type': 'general',
                'passed': False,
                'messages': [f"Validation failed: {e}"],
                'details': {},
                'execution_time_seconds': 0.0
            })
            print(f"  ❌ {schema_file.name} - ERROR: {e}")

        return schema_result

    def _generate_schema_recommendations(self, schema: Dict, validation_results: List) -> List[str]:
        """Generate recommendations for schema improvement."""
        recommendations = []

        # Check for missing documentation
        if schema.get('type') == 'record' and 'fields' in schema:
            undocumented_fields = [
                field['name'] for field in schema['fields']
                if 'doc' not in field or not field['doc']
            ]
            if undocumented_fields:
                recommendations.append(f"Add documentation for fields: {', '.join(undocumented_fields[:5])}")

        # Performance recommendations
        field_count = len(schema.get('fields', []))
        if field_count > 50:
            recommendations.append("Consider splitting large record into multiple related schemas")

        # Naming convention recommendations
        schema_name = schema.get('name', '')
        if schema_name and not schema_name.replace('_', '').isalnum():
            recommendations.append("Use alphanumeric characters and underscores only in schema names")

        # Check validation results for additional recommendations
        for result in validation_results:
            if not result.passed and result.messages:
                for message in result.messages:
                    if "performance" in message.lower():
                        recommendations.append(f"Performance issue: {message}")
                    elif "security" in message.lower():
                        recommendations.append(f"Security concern: {message}")

        return recommendations

    def _generate_validation_summary(self, results: Dict) -> Dict[str, any]:
        """Generate summary of validation results."""
        total_schemas = results['total_schemas']
        passed = results['passed_validations']
        failed = results['failed_validations']

        if total_schemas == 0:
            return {
                'status': 'FAILED',
                'reason': 'No schema files found',
                'pass_rate': 0.0
            }

        pass_rate = passed / total_schemas

        if pass_rate == 1.0:
            status = 'PASSED'
            message = f"All {total_schemas} schemas passed validation"
        elif pass_rate >= 0.9:
            status = 'PASSED_WITH_WARNINGS'
            message = f"{passed}/{total_schemas} schemas passed validation"
        else:
            status = 'FAILED'
            message = f"Only {passed}/{total_schemas} schemas passed validation"

        return {
            'status': status,
            'message': message,
            'pass_rate': pass_rate,
            'total_schemas': total_schemas,
            'passed_schemas': passed,
            'failed_schemas': failed,
            'policy_violations': results['policy_violations'],
            'schema_registry_errors': results['schema_registry_errors']
        }

    async def generate_detailed_report(self, results: Dict) -> str:
        """Generate a detailed validation report."""
        report_lines = [
            "# Schema Validation Report",
            f"Generated: {results['validation_timestamp']}",
            "",
            "## Summary",
            f"- **Status**: {results['summary']['status']}",
            f"- **Total Schemas**: {results['total_schemas']}",
            f"- **Passed**: {results['passed_validations']}",
            f"- **Failed**: {results['failed_validations']}",
            f"- **Pass Rate**: {results['summary']['pass_rate']".2%"}",
            f"- **Policy Violations**: {results['policy_violations']}",
            f"- **Registry Errors**: {results['schema_registry_errors']}",
            ""
        ]

        if results['schema_details']:
            report_lines.extend([
                "## Schema Details",
                "| File | Status | Policy Violations | Recommendations |",
                "|------|--------|-------------------|-----------------|"
            ])

            for detail in results['schema_details']:
                status_icon = "✅" if detail['overall_status'] == 'PASSED' else "❌"
                violations = len(detail.get('policy_violations', []))
                recommendations = len(detail.get('recommendations', []))

                report_lines.append(
                    f"| {detail['file_name']} | {status_icon} {detail['overall_status']} | {violations} | {recommendations} |"
                )

            report_lines.append("")

        # Add details for failed schemas
        failed_schemas = [d for d in results['schema_details'] if d['overall_status'] != 'PASSED']

        if failed_schemas:
            report_lines.extend([
                "## Failed Schemas Details",
                ""
            ])

            for detail in failed_schemas:
                report_lines.extend([
                    f"### {detail['file_name']}",
                    f"**Status**: {detail['overall_status']}",
                    ""
                ])

                if detail.get('policy_violations'):
                    report_lines.extend([
                        "**Policy Violations**:",
                        "```"
                    ])
                    for violation in detail['policy_violations']:
                        report_lines.append(f"- {violation}")
                    report_lines.extend(["```", ""])

                if detail.get('recommendations'):
                    report_lines.extend([
                        "**Recommendations**:",
                        "```"
                    ])
                    for rec in detail['recommendations']:
                        report_lines.append(f"- {rec}")
                    report_lines.extend(["```", ""])

        return "\n".join(report_lines)


async def main():
    """Main validation function."""
    import argparse

    parser = argparse.ArgumentParser(description='Enhanced schema validation for CI/CD')
    parser.add_argument('--schema-registry-url', default='http://localhost:8081',
                       help='Schema Registry URL')
    parser.add_argument('--schemas-dir', default='kafka/schemas',
                       help='Directory containing schema files')
    parser.add_argument('--output-format', choices=['text', 'json', 'markdown'],
                       default='text', help='Output format')
    parser.add_argument('--fail-on-warnings', action='store_true',
                       help='Fail on policy warnings')

    args = parser.parse_args()

    # Initialize validator
    validator = EnhancedSchemaValidator(args.schema_registry_url)
    await validator.initialize()

    # Run validation
    print("🚀 Starting enhanced schema validation...")
    results = await validator.validate_all_schemas()

    # Generate report
    if args.output_format == 'json':
        print(json.dumps(results, indent=2))
    elif args.output_format == 'markdown':
        report = await validator.generate_detailed_report(results)
        print(report)
    else:
        # Text format
        summary = results['summary']
        print("
📊 Validation Summary:"        print(f"  Status: {summary['status']}")
        print(f"  Total Schemas: {summary['total_schemas']}")
        print(f"  Passed: {summary['passed_schemas']}")
        print(f"  Failed: {summary['failed_schemas']}")
        print(f"  Pass Rate: {summary['pass_rate']".2%"}")
        print(f"  Policy Violations: {summary['policy_violations']}")
        print(f"  Registry Errors: {summary['schema_registry_errors']}")

        # Show details for failed schemas
        failed_schemas = [d for d in results['schema_details'] if d['overall_status'] != 'PASSED']
        if failed_schemas:
            print("
❌ Failed Schemas:"            for schema in failed_schemas:
                print(f"  • {schema['file_name']}: {schema['overall_status']}")
                if schema.get('policy_violations'):
                    for violation in schema['policy_violations'][:3]:  # Show first 3 violations
                        print(f"    - {violation}")

    # Exit with appropriate code
    if results['summary']['status'] == 'FAILED' or (args.fail_on_warnings and results['summary']['status'] == 'PASSED_WITH_WARNINGS'):
        sys.exit(1)
    else:
        print("\n✅ All validations passed!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
