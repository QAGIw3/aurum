"""Schema Registry policy management and compatibility testing framework."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any
import subprocess
import tempfile
import os

from .registry_manager import SchemaRegistryManager, SchemaRegistryConfig, SchemaCompatibilityMode, SchemaInfo
from ..logging import StructuredLogger, LogLevel, create_logger

logger = logging.getLogger(__name__)


class PolicyEnforcementLevel(str, Enum):
    """Policy enforcement levels."""
    WARN = "warn"      # Log warnings but allow operation
    FAIL = "fail"      # Block operation with error
    DISABLED = "disabled"  # No enforcement


class SchemaEvolutionPolicy(str, Enum):
    """Schema evolution policies."""
    BACKWARD_COMPATIBLE = "backward_compatible"
    FORWARD_COMPATIBLE = "forward_compatible"
    FULL_COMPATIBLE = "full_compatible"
    NONE = "none"


@dataclass
class SubjectPolicy:
    """Policy configuration for a schema subject."""

    subject_pattern: str
    compatibility_mode: SchemaCompatibilityMode
    evolution_policy: SchemaEvolutionPolicy
    enforcement_level: PolicyEnforcementLevel = PolicyEnforcementLevel.FAIL

    # Field-level policies
    allow_field_addition: bool = True
    allow_field_removal: bool = False
    allow_type_narrowing: bool = False
    allow_type_widening: bool = True

    # Naming conventions
    field_name_pattern: Optional[str] = None
    require_documentation: bool = True
    max_field_count: Optional[int] = None

    # Performance policies
    max_record_size_bytes: Optional[int] = None
    max_array_length: Optional[int] = None

    # Security policies
    allowed_namespaces: List[str] = field(default_factory=list)
    blocked_field_names: List[str] = field(default_factory=list)


@dataclass
class CompatibilityTestResult:
    """Result of a schema compatibility test."""

    subject: str
    test_type: str  # "syntax", "compatibility", "policy", "evolution"
    passed: bool
    messages: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0


@dataclass
class SchemaRegistryPolicyConfig:
    """Configuration for schema registry policies."""

    # Policy definitions
    subject_policies: List[SubjectPolicy] = field(default_factory=list)

    # Global settings
    default_compatibility_mode: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD
    default_enforcement_level: PolicyEnforcementLevel = PolicyEnforcementLevel.FAIL

    # CI/CD integration
    ci_validation_enabled: bool = True
    require_ci_approval: bool = False
    auto_merge_enabled: bool = False

    # Testing configuration
    test_compatibility_modes: List[SchemaCompatibilityMode] = field(default_factory=lambda: [
        SchemaCompatibilityMode.BACKWARD,
        SchemaCompatibilityMode.FORWARD,
        SchemaCompatibilityMode.FULL
    ])

    # Performance settings
    enable_performance_checks: bool = True
    max_schema_size_bytes: int = 1024 * 1024  # 1MB

    # Evolution policies
    allow_breaking_changes: bool = False
    require_migration_strategy: bool = True


class SchemaRegistryPolicyManager:
    """Manager for schema registry policies and compatibility testing."""

    def __init__(self, config: SchemaRegistryPolicyConfig):
        self.config = config
        self.logger = create_logger(
            source_name="schema_registry_policy",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.schema_registry.policy",
            dataset="schema_policy"
        )

        # Policy cache
        self._policy_cache: Dict[str, SubjectPolicy] = {}

        # Test results tracking
        self._test_results: List[CompatibilityTestResult] = []

        # Initialize policy cache
        self._build_policy_cache()

        logger.info("Schema Registry Policy Manager initialized")

    def _build_policy_cache(self):
        """Build cache of subject policies for fast lookup."""
        for policy in self.config.subject_policies:
            # Cache the policy by pattern
            self._policy_cache[policy.subject_pattern] = policy

        logger.info(f"Cached {len(self._policy_cache)} subject policies")

    def get_policy_for_subject(self, subject: str) -> Optional[SubjectPolicy]:
        """Get the policy that applies to a subject."""
        # Check for exact pattern matches first
        for pattern, policy in self._policy_cache.items():
            if pattern == subject:
                return policy

        # Check for pattern matches
        for pattern, policy in self._policy_cache.items():
            if self._matches_pattern(subject, pattern):
                return policy

        # Return default policy
        return SubjectPolicy(
            subject_pattern=subject,
            compatibility_mode=self.config.default_compatibility_mode,
            evolution_policy=SchemaEvolutionPolicy.BACKWARD_COMPATIBLE,
            enforcement_level=self.config.default_enforcement_level
        )

    def _matches_pattern(self, subject: str, pattern: str) -> bool:
        """Check if subject matches a pattern."""
        # Simple pattern matching - could be enhanced with regex
        if pattern.endswith("*"):
            return subject.startswith(pattern[:-1])
        elif pattern.startswith("*"):
            return subject.endswith(pattern[1:])
        else:
            return subject == pattern

    async def validate_schema_registration(
        self,
        subject: str,
        schema: Dict[str, Any],
        registry_manager: Optional[SchemaRegistryManager] = None
    ) -> CompatibilityTestResult:
        """Validate schema registration against policies."""
        start_time = time.time()
        result = CompatibilityTestResult(
            subject=subject,
            test_type="policy_validation",
            passed=True
        )

        try:
            # Get policy for subject
            policy = self.get_policy_for_subject(subject)
            if not policy:
                result.passed = False
                result.messages.append(f"No policy found for subject: {subject}")
                return result

            # Validate against policy
            validation_results = []

            # 1. Field-level validation
            validation_results.append(await self._validate_field_policies(schema, policy))

            # 2. Naming convention validation
            validation_results.append(self._validate_naming_conventions(schema, policy))

            # 3. Performance validation
            if self.config.enable_performance_checks:
                validation_results.append(self._validate_performance_constraints(schema, policy))

            # 4. Security validation
            validation_results.append(self._validate_security_constraints(schema, policy))

            # 5. Schema Registry compatibility (if manager provided)
            if registry_manager:
                compatibility_result = await self._validate_registry_compatibility(
                    subject, schema, policy, registry_manager
                )
                validation_results.append(compatibility_result)

            # Aggregate results
            for validation_result in validation_results:
                if not validation_result.passed:
                    result.passed = False
                result.messages.extend(validation_result.messages)
                result.details.update(validation_result.details)

            result.execution_time_seconds = time.time() - start_time

            if result.passed:
                result.messages.append("All policy validations passed")
            else:
                result.messages.insert(0, f"Policy violations for subject: {subject}")

        except Exception as e:
            result.passed = False
            result.messages.append(f"Policy validation failed: {e}")
            logger.error(f"Policy validation error for {subject}: {e}")

        self._test_results.append(result)
        return result

    async def _validate_field_policies(self, schema: Dict[str, Any], policy: SubjectPolicy) -> CompatibilityTestResult:
        """Validate field-level policies."""
        result = CompatibilityTestResult(
            subject=schema.get('name', 'unknown'),
            test_type="field_policy",
            passed=True
        )

        if schema.get('type') != 'record' or 'fields' not in schema:
            return result

        fields = schema['fields']

        # Check field count
        if policy.max_field_count and len(fields) > policy.max_field_count:
            result.passed = False
            result.messages.append(f"Field count {len(fields)} exceeds maximum {policy.max_field_count}")

        # Validate each field
        for field in fields:
            field_name = field.get('name', '')
            field_type = field.get('type')

            # Check naming convention
            if policy.field_name_pattern:
                if not re.match(policy.field_name_pattern, field_name):
                    result.passed = False
                    result.messages.append(f"Field name '{field_name}' violates naming pattern: {policy.field_name_pattern}")

            # Check blocked field names
            if field_name in policy.blocked_field_names:
                result.passed = False
                result.messages.append(f"Field name '{field_name}' is blocked by policy")

            # Check documentation requirement
            if policy.require_documentation and 'doc' not in field:
                result.messages.append(f"Field '{field_name}' missing documentation")

        result.details['field_count'] = len(fields)
        result.details['validated_fields'] = len(fields)

        return result

    def _validate_naming_conventions(self, schema: Dict[str, Any], policy: SubjectPolicy) -> CompatibilityTestResult:
        """Validate naming conventions."""
        result = CompatibilityTestResult(
            subject=schema.get('name', 'unknown'),
            test_type="naming_convention",
            passed=True
        )

        schema_name = schema.get('name', '')
        namespace = schema.get('namespace', '')

        # Check allowed namespaces
        if policy.allowed_namespaces and namespace not in policy.allowed_namespaces:
            result.passed = False
            result.messages.append(f"Namespace '{namespace}' not in allowed namespaces: {policy.allowed_namespaces}")

        # Check schema name format (if we have a pattern)
        if policy.field_name_pattern and not re.match(policy.field_name_pattern.replace('field', 'schema'), schema_name):
            result.passed = False
            result.messages.append(f"Schema name '{schema_name}' violates naming convention")

        return result

    def _validate_performance_constraints(self, schema: Dict[str, Any], policy: SubjectPolicy) -> CompatibilityTestResult:
        """Validate performance constraints."""
        result = CompatibilityTestResult(
            subject=schema.get('name', 'unknown'),
            test_type="performance",
            passed=True
        )

        # Check schema size
        schema_json = json.dumps(schema)
        schema_size = len(schema_json.encode('utf-8'))

        if schema_size > self.config.max_schema_size_bytes:
            result.passed = False
            result.messages.append(f"Schema size {schema_size} bytes exceeds limit {self.config.max_schema_size_bytes}")

        # Check array field limits
        if schema.get('type') == 'record' and 'fields' in schema:
            for field in schema['fields']:
                field_type = field.get('type')
                if isinstance(field_type, dict) and field_type.get('type') == 'array':
                    if policy.max_array_length:
                        result.messages.append(f"Array field '{field['name']}' should be limited to {policy.max_array_length} elements")

        result.details['schema_size_bytes'] = schema_size
        result.details['estimated_record_size'] = self._estimate_record_size(schema)

        return result

    def _estimate_record_size(self, schema: Dict[str, Any]) -> int:
        """Estimate the average size of a record based on the schema."""
        if schema.get('type') != 'record':
            return 100  # Default estimate

        total_size = 0
        fields = schema.get('fields', [])

        for field in fields:
            field_type = field.get('type')
            field_name = field.get('name', '')

            # Type-based size estimation
            if field_type == 'string':
                total_size += 50  # Average string length
            elif field_type in ['int', 'long']:
                total_size += 8
            elif field_type in ['float', 'double']:
                total_size += 8
            elif field_type == 'boolean':
                total_size += 1
            elif isinstance(field_type, dict):
                if field_type.get('type') == 'array':
                    total_size += 100  # Average array size
                elif field_type.get('type') == 'record':
                    total_size += self._estimate_record_size(field_type)
            elif isinstance(field_type, list):
                # Union type - take the largest
                sizes = [self._estimate_record_size({'type': t}) if isinstance(t, str) else 100 for t in field_type]
                total_size += max(sizes)

        return max(total_size, 100)  # Minimum size

    def _validate_security_constraints(self, schema: Dict[str, Any], policy: SubjectPolicy) -> CompatibilityTestResult:
        """Validate security constraints."""
        result = CompatibilityTestResult(
            subject=schema.get('name', 'unknown'),
            test_type="security",
            passed=True
        )

        # Check for sensitive field patterns
        sensitive_patterns = [
            r'password', r'secret', r'key', r'token', r'credential',
            r'ssn', r'social.*security', r'credit.*card'
        ]

        if schema.get('type') == 'record' and 'fields' in schema:
            for field in schema['fields']:
                field_name = field.get('name', '').lower()

                for pattern in sensitive_patterns:
                    if re.search(pattern, field_name):
                        result.messages.append(f"Potentially sensitive field detected: '{field['name']}'")

        return result

    async def _validate_registry_compatibility(
        self,
        subject: str,
        schema: Dict[str, Any],
        policy: SubjectPolicy,
        registry_manager: SchemaRegistryManager
    ) -> CompatibilityTestResult:
        """Validate compatibility with Schema Registry."""
        result = CompatibilityTestResult(
            subject=subject,
            test_type="registry_compatibility",
            passed=True
        )

        try:
            # Check if subject already exists
            existing_schema = registry_manager.get_latest_schema(subject)

            if existing_schema:
                # Test compatibility
                compatibility_result = registry_manager.check_compatibility(
                    subject, schema, existing_schema.schema
                )

                if not compatibility_result.is_compatible:
                    severity = "ERROR" if policy.enforcement_level == PolicyEnforcementLevel.FAIL else "WARN"
                    result.passed = policy.enforcement_level != PolicyEnforcementLevel.FAIL
                    result.messages.append(
                        f"Schema incompatible with existing version: {compatibility_result.messages}"
                    )

                result.details['existing_version'] = existing_schema.version
                result.details['compatibility_mode'] = compatibility_result.mode.value
            else:
                result.messages.append("No existing schema - will be registered as new version")

        except Exception as e:
            result.passed = False
            result.messages.append(f"Registry compatibility check failed: {e}")

        return result

    async def run_comprehensive_compatibility_tests(
        self,
        schema_file: Path,
        registry_manager: Optional[SchemaRegistryManager] = None
    ) -> List[CompatibilityTestResult]:
        """Run comprehensive compatibility tests on a schema file."""
        results = []

        try:
            # Load schema
            with open(schema_file, 'r') as f:
                schema = json.load(f)

            # Extract subject name
            subject = f"{schema_file.stem}-value"

            # 1. Syntax validation
            syntax_result = await self._validate_schema_syntax(schema_file, schema)
            results.append(syntax_result)

            # 2. Policy validation
            policy_result = await self.validate_schema_registration(subject, schema, registry_manager)
            results.append(policy_result)

            # 3. Evolution testing (if existing schema)
            if registry_manager:
                evolution_result = await self._test_schema_evolution(schema_file, registry_manager)
                results.append(evolution_result)

            # 4. Performance testing
            perf_result = await self._test_schema_performance(schema_file, schema)
            results.append(perf_result)

        except Exception as e:
            results.append(CompatibilityTestResult(
                subject=schema_file.stem,
                test_type="general",
                passed=False,
                messages=[f"Test execution failed: {e}"]
            ))

        return results

    async def _validate_schema_syntax(self, schema_file: Path, schema: Dict[str, Any]) -> CompatibilityTestResult:
        """Validate schema syntax."""
        result = CompatibilityTestResult(
            subject=schema_file.stem,
            test_type="syntax",
            passed=True
        )

        try:
            # Basic JSON validation (already done by json.load)
            # Additional Avro-specific validation could go here
            schema_json = json.dumps(schema)

            # Check schema size
            if len(schema_json) > self.config.max_schema_size_bytes:
                result.passed = False
                result.messages.append(f"Schema size {len(schema_json)} bytes exceeds limit")

        except Exception as e:
            result.passed = False
            result.messages.append(f"Syntax validation failed: {e}")

        return result

    async def _test_schema_evolution(self, schema_file: Path, registry_manager: SchemaRegistryManager) -> CompatibilityTestResult:
        """Test schema evolution scenarios."""
        result = CompatibilityTestResult(
            subject=schema_file.stem,
            test_type="evolution",
            passed=True
        )

        subject = f"{schema_file.stem}-value"

        try:
            # Get existing schema
            existing_schema = registry_manager.get_latest_schema(subject)
            if not existing_schema:
                result.messages.append("No existing schema to test evolution against")
                return result

            # Test different compatibility modes
            for mode in self.config.test_compatibility_modes:
                try:
                    # Temporarily change compatibility mode
                    old_mode = registry_manager._get_compatibility_mode(subject)
                    registry_manager._set_compatibility_mode(subject, mode)

                    # Test compatibility
                    compatibility_result = registry_manager.check_compatibility(
                        subject, json.load(open(schema_file, 'r')), existing_schema.schema
                    )

                    if not compatibility_result.is_compatible:
                        severity = "FAIL" if mode == SchemaCompatibilityMode.BACKWARD else "WARN"
                        result.messages.append(f"Not {mode.value} compatible: {compatibility_result.messages}")

                    # Restore original mode
                    registry_manager._set_compatibility_mode(subject, old_mode)

                except Exception as e:
                    result.messages.append(f"Evolution test for {mode.value} failed: {e}")

        except Exception as e:
            result.passed = False
            result.messages.append(f"Evolution testing failed: {e}")

        return result

    async def _test_schema_performance(self, schema_file: Path, schema: Dict[str, Any]) -> CompatibilityTestResult:
        """Test schema performance characteristics."""
        result = CompatibilityTestResult(
            subject=schema_file.stem,
            test_type="performance",
            passed=True
        )

        try:
            # Test serialization/deserialization performance
            import avro.schema
            import avro.io
            import io

            # Parse schema
            avro_schema = avro.schema.parse(json.dumps(schema))

            # Generate test data
            test_records = self._generate_test_data(schema, 100)  # 100 test records

            start_time = time.time()
            serialized_size = 0

            # Test serialization performance
            for record in test_records:
                buffer = io.BytesIO()
                encoder = avro.io.DatumWriter(avro_schema)
                writer = avro.io.DataFileWriter(buffer, encoder, avro_schema)
                writer.append(record)
                writer.close()
                serialized_size += len(buffer.getvalue())

            serialization_time = time.time() - start_time

            # Performance metrics
            avg_record_size = serialized_size / len(test_records)
            records_per_second = len(test_records) / serialization_time

            result.details.update({
                'serialization_time_seconds': serialization_time,
                'avg_record_size_bytes': avg_record_size,
                'records_per_second': records_per_second,
                'test_records_count': len(test_records)
            })

            # Check performance thresholds
            if records_per_second < 1000:  # Less than 1000 records/second
                result.messages.append(f"Low serialization performance: {records_per_second:.0f} records/sec")

            if avg_record_size > 1024:  # Larger than 1KB
                result.messages.append(f"Large average record size: {avg_record_size:.0f} bytes")

        except Exception as e:
            result.passed = False
            result.messages.append(f"Performance testing failed: {e}")

        return result

    def _generate_test_data(self, schema: Dict[str, Any], count: int) -> List[Dict[str, Any]]:
        """Generate test data for performance testing."""
        if schema.get('type') != 'record' or 'fields' not in schema:
            return []

        fields = schema['fields']
        test_records = []

        for i in range(count):
            record = {}
            for field in fields:
                field_name = field['name']
                field_type = field['type']

                # Generate appropriate test data based on type
                if field_type == 'string':
                    record[field_name] = f"test_value_{i}"
                elif field_type in ['int', 'long']:
                    record[field_name] = i
                elif field_type in ['float', 'double']:
                    record[field_name] = float(i) + 0.5
                elif field_type == 'boolean':
                    record[field_name] = i % 2 == 0
                elif isinstance(field_type, list):
                    # Union type - use first option
                    if field_type[0] == 'null':
                        record[field_name] = None
                    else:
                        record[field_name] = f"test_{field_type[0]}_{i}"
                else:
                    record[field_name] = f"test_{field_type}_{i}"

            test_records.append(record)

        return test_records

    async def generate_policy_report(self) -> Dict[str, Any]:
        """Generate comprehensive policy compliance report."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'policy_config': {
                'subject_policies_count': len(self.config.subject_policies),
                'default_compatibility': self.config.default_compatibility_mode.value,
                'enforcement_level': self.config.default_enforcement_level.value
            },
            'test_results': [
                {
                    'subject': result.subject,
                    'test_type': result.test_type,
                    'passed': result.passed,
                    'message_count': len(result.messages),
                    'execution_time_seconds': result.execution_time_seconds
                }
                for result in self._test_results
            ],
            'summary': {
                'total_tests': len(self._test_results),
                'passed_tests': sum(1 for r in self._test_results if r.passed),
                'failed_tests': sum(1 for r in self._test_results if not r.passed),
                'pass_rate': len(self._test_results) > 0 and
                           sum(1 for r in self._test_results if r.passed) / len(self._test_results)
            }
        }

        return report

    async def register_schemas_with_policies(
        self,
        schema_files: List[Path],
        registry_manager: SchemaRegistryManager,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Register schemas with policy enforcement."""
        results = {
            'processed_files': len(schema_files),
            'successful_registrations': 0,
            'failed_registrations': 0,
            'policy_violations': 0,
            'details': []
        }

        for schema_file in schema_files:
            try:
                # Load and validate schema
                with open(schema_file, 'r') as f:
                    schema = json.load(f)

                subject = f"{schema_file.stem}-value"

                # Run comprehensive validation
                validation_results = await self.run_comprehensive_compatibility_tests(
                    schema_file, registry_manager
                )

                # Check if all validations passed
                all_passed = all(result.passed for result in validation_results)

                if all_passed:
                    if not dry_run:
                        # Register the schema
                        schema_info = registry_manager.register_subject(subject, schema)
                        results['successful_registrations'] += 1

                        logger.info(f"Successfully registered {subject} version {schema_info.version}")
                    else:
                        logger.info(f"Dry run: Would register {subject}")

                    results['details'].append({
                        'file': str(schema_file),
                        'subject': subject,
                        'status': 'success',
                        'validation_results': [
                            {
                                'test_type': r.test_type,
                                'passed': r.passed,
                                'messages': r.messages
                            }
                            for r in validation_results
                        ]
                    })

                else:
                    results['failed_registrations'] += 1
                    results['policy_violations'] += sum(
                        1 for r in validation_results if not r.passed
                    )

                    logger.error(f"Failed to register {subject}: Policy violations detected")

                    results['details'].append({
                        'file': str(schema_file),
                        'subject': subject,
                        'status': 'failed',
                        'violations': [
                            {
                                'test_type': r.test_type,
                                'passed': r.passed,
                                'messages': r.messages
                            }
                            for r in validation_results if not r.passed
                        ]
                    })

            except Exception as e:
                results['failed_registrations'] += 1
                logger.error(f"Error processing {schema_file}: {e}")

                results['details'].append({
                    'file': str(schema_file),
                    'subject': f"{schema_file.stem}-value",
                    'status': 'error',
                    'error': str(e)
                })

        return results


# Global policy manager
_global_policy_manager: Optional[SchemaRegistryPolicyManager] = None


async def get_schema_registry_policy_manager() -> SchemaRegistryPolicyManager:
    """Get or create the global schema registry policy manager."""
    global _global_policy_manager

    if _global_policy_manager is None:
        config = SchemaRegistryPolicyConfig()
        _global_policy_manager = SchemaRegistryPolicyManager(config)

    return _global_policy_manager


async def initialize_schema_registry_policies():
    """Initialize the schema registry policy system."""
    manager = await get_schema_registry_policy_manager()
    return manager
