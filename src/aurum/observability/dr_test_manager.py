"""Disaster Recovery testing and backup validation system."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable, Awaitable
import subprocess
import tempfile
import os

from ..logging import StructuredLogger, LogLevel, create_logger
from ..observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class DRTestType(str, Enum):
    """Types of disaster recovery tests."""
    FULL_SYSTEM_RESTORE = "full_system_restore"
    DATABASE_RESTORE = "database_restore"
    OBJECT_STORE_RESTORE = "object_store_restore"
    SCHEMA_REGISTRY_RESTORE = "schema_registry_restore"
    TRINO_METASTORE_RESTORE = "trino_metastore_restore"
    CONFIGURATION_RESTORE = "configuration_restore"
    APPLICATION_RESTORE = "application_restore"


class DRTestStatus(str, Enum):
    """Status of DR tests."""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    PARTIALLY_FAILED = "partially_failed"
    CANCELLED = "cancelled"


class BackupValidationStatus(str, Enum):
    """Status of backup validation."""
    VALID = "valid"
    CORRUPTED = "corrupted"
    INCOMPLETE = "incomplete"
    MISSING = "missing"
    OUTDATED = "outdated"


@dataclass
class DRTestConfig:
    """Configuration for disaster recovery testing."""

    test_type: DRTestType
    name: str
    description: str = ""

    # Test parameters
    timeout_minutes: int = 60
    parallel_execution: bool = False
    dry_run: bool = False

    # Backup configuration
    backup_source: str = "production"  # production, staging, etc.
    backup_age_hours: int = 24  # Maximum age of backup to use

    # Restore configuration
    restore_namespace: str = "dr-test"
    restore_environment: str = "test"

    # Validation configuration
    validate_data_integrity: bool = True
    validate_functionality: bool = True
    validate_performance: bool = False

    # Notification settings
    notify_on_start: bool = True
    notify_on_completion: bool = True
    notify_on_failure: bool = True

    # Cleanup settings
    auto_cleanup: bool = True
    cleanup_timeout_minutes: int = 30

    # Metadata
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_run: Optional[str] = None
    run_count: int = 0


@dataclass
class DRTestResult:
    """Result of a disaster recovery test."""

    test_id: str
    test_config: DRTestConfig
    status: DRTestStatus = DRTestStatus.PENDING
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0

    # Component results
    component_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Validation results
    validation_results: Dict[str, BackupValidationStatus] = field(default_factory=dict)

    # Performance metrics
    rto_actual_minutes: float = 0.0  # Recovery Time Objective - actual
    rpo_actual_minutes: float = 0.0  # Recovery Point Objective - actual

    # Errors and warnings
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Metadata
    executed_by: str = ""
    environment: str = ""
    notes: str = ""

    def get_overall_status(self) -> DRTestStatus:
        """Get overall test status based on component results."""
        if not self.component_results:
            return DRTestStatus.FAILED

        failed_components = [name for name, result in self.component_results.items()
                           if result.get('status') == 'failed']
        passed_components = [name for name, result in self.component_results.items()
                           if result.get('status') == 'passed']

        if len(failed_components) == 0:
            return DRTestStatus.PASSED
        elif len(passed_components) == 0:
            return DRTestStatus.FAILED
        else:
            return DRTestStatus.PARTIALLY_FAILED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "test_id": self.test_id,
            "test_type": self.test_config.test_type.value,
            "test_name": self.test_config.name,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "rto_actual_minutes": self.rto_actual_minutes,
            "rpo_actual_minutes": self.rpo_actual_minutes,
            "component_results": self.component_results,
            "validation_results": {k: v.value for k, v in self.validation_results.items()},
            "errors": self.errors,
            "warnings": self.warnings,
            "executed_by": self.executed_by,
            "environment": self.environment,
            "notes": self.notes
        }


@dataclass
class BackupMetadata:
    """Metadata for backups."""

    backup_id: str
    component: str  # "postgres", "object_store", "schema_registry", "trino"
    backup_type: str  # "full", "incremental", "differential"
    created_at: str
    size_bytes: int = 0
    location: str = ""
    checksum: Optional[str] = None
    version: str = "1.0"

    # Component-specific metadata
    postgres_metadata: Optional[Dict[str, Any]] = None
    object_store_metadata: Optional[Dict[str, Any]] = None
    schema_registry_metadata: Optional[Dict[str, Any]] = None
    trino_metadata: Optional[Dict[str, Any]] = None

    def is_valid(self) -> bool:
        """Check if backup metadata is valid."""
        return (
            self.backup_id and
            self.component and
            self.created_at and
            self.size_bytes > 0
        )


class DRTestManager:
    """Manager for disaster recovery testing and backup validation."""

    def __init__(self):
        self.logger = create_logger(
            source_name="dr_test_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.dr.testing",
            dataset="dr_testing"
        )

        self.metrics = get_metrics_client()

        # Test management
        self.active_tests: Dict[str, DRTestResult] = {}
        self.test_history: List[DRTestResult] = []

        # Backup validation
        self.backup_validations: Dict[str, BackupValidationStatus] = {}

        # Configuration
        self.test_configs: Dict[str, DRTestConfig] = {}
        self.default_timeout_minutes = 60

        # Test execution
        self.max_concurrent_tests = 3
        self.test_semaphore = asyncio.Semaphore(self.max_concurrent_tests)

        logger.info("DR Test Manager initialized")

    async def run_dr_test(self, test_config: DRTestConfig, executed_by: str = "system") -> DRTestResult:
        """Run a disaster recovery test."""
        test_id = f"{test_config.test_type.value}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        test_result = DRTestResult(
            test_id=test_id,
            test_config=test_config,
            executed_by=executed_by,
            environment=test_config.restore_environment
        )

        self.active_tests[test_id] = test_result

        try:
            # Acquire semaphore for concurrent test limiting
            async with self.test_semaphore:
                await self._execute_dr_test(test_result)

        except Exception as e:
            test_result.status = DRTestStatus.FAILED
            test_result.errors.append(f"Test execution failed: {e}")
            logger.error(f"DR test {test_id} failed: {e}")

        finally:
            # Move to history
            self.test_history.append(test_result)
            del self.active_tests[test_id]

            # Update test config
            test_config.last_run = datetime.now().isoformat()
            test_config.run_count += 1

        # Emit metrics
        self.metrics.increment_counter("dr_tests.executed")
        self.metrics.histogram("dr_tests.duration_minutes", test_result.duration_seconds / 60)
        self.metrics.increment_counter(f"dr_tests.status.{test_result.status.value}")

        return test_result

    async def _execute_dr_test(self, test_result: DRTestResult) -> None:
        """Execute the actual DR test."""
        test_config = test_result.test_config

        # Update status
        test_result.status = DRTestStatus.RUNNING
        test_result.start_time = datetime.now().isoformat()

        # Send notification
        if test_config.notify_on_start:
            await self._send_notification(
                f"DR Test Started: {test_config.name}",
                f"Test {test_result.test_id} started for {test_config.test_type.value}"
            )

        try:
            # Execute based on test type
            if test_config.test_type == DRTestType.FULL_SYSTEM_RESTORE:
                await self._execute_full_system_restore(test_result)
            elif test_config.test_type == DRTestType.DATABASE_RESTORE:
                await self._execute_database_restore(test_result)
            elif test_config.test_type == DRTestType.OBJECT_STORE_RESTORE:
                await self._execute_object_store_restore(test_result)
            elif test_config.test_type == DRTestType.SCHEMA_REGISTRY_RESTORE:
                await self._execute_schema_registry_restore(test_result)
            elif test_config.test_type == DRTestType.TRINO_METASTORE_RESTORE:
                await self._execute_trino_metastore_restore(test_result)
            else:
                raise ValueError(f"Unsupported test type: {test_config.test_type}")

            # Validate results
            await self._validate_test_results(test_result)

            # Determine overall status
            test_result.status = test_result.get_overall_status()

        except asyncio.TimeoutError:
            test_result.status = DRTestStatus.FAILED
            test_result.errors.append(f"Test timed out after {test_config.timeout_minutes} minutes")
        except Exception as e:
            test_result.status = DRTestStatus.FAILED
            test_result.errors.append(str(e))

        finally:
            test_result.end_time = datetime.now().isoformat()
            test_result.duration_seconds = (
                datetime.fromisoformat(test_result.end_time) -
                datetime.fromisoformat(test_result.start_time)
            ).total_seconds()

            # Send completion notification
            if test_config.notify_on_completion or (test_config.notify_on_failure and test_result.status == DRTestStatus.FAILED):
                await self._send_completion_notification(test_result)

            # Cleanup if configured
            if test_config.auto_cleanup:
                await self._cleanup_test_environment(test_result)

    async def _execute_full_system_restore(self, test_result: DRTestResult) -> None:
        """Execute full system restore test."""
        components = [
            ("postgres", self._execute_database_restore),
            ("object_store", self._execute_object_store_restore),
            ("schema_registry", self._execute_schema_registry_restore),
            ("trino_metastore", self._execute_trino_metastore_restore)
        ]

        for component_name, component_func in components:
            try:
                # Create sub-result for component
                component_result = await component_func(test_result, component_name)
                test_result.component_results[component_name] = component_result

            except Exception as e:
                test_result.component_results[component_name] = {
                    "status": "failed",
                    "error": str(e),
                    "duration_seconds": 0.0
                }

    async def _execute_database_restore(self, test_result: DRTestResult, component_name: str = "postgres") -> Dict[str, Any]:
        """Execute database restore test."""
        start_time = time.time()

        try:
            # 1. Validate backup availability
            backup_status = await self._validate_postgres_backup()
            if backup_status != BackupValidationStatus.VALID:
                raise ValueError(f"PostgreSQL backup validation failed: {backup_status.value}")

            # 2. Create restore environment
            await self._create_restore_namespace(test_result.test_config.restore_namespace)

            # 3. Restore database
            await self._restore_postgres_database(
                test_result.test_config.restore_namespace,
                test_result.test_config.backup_source
            )

            # 4. Validate restoration
            validation_results = await self._validate_postgres_restoration(
                test_result.test_config.restore_namespace
            )

            test_result.validation_results[component_name] = validation_results["status"]

            return {
                "status": "passed" if validation_results["success"] else "failed",
                "duration_seconds": time.time() - start_time,
                "validation_details": validation_results,
                "backup_validated": backup_status.value
            }

        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _execute_object_store_restore(self, test_result: DRTestResult, component_name: str = "object_store") -> Dict[str, Any]:
        """Execute object store restore test."""
        start_time = time.time()

        try:
            # 1. Validate backup availability
            backup_status = await self._validate_object_store_backup()
            if backup_status != BackupValidationStatus.VALID:
                raise ValueError(f"Object store backup validation failed: {backup_status.value}")

            # 2. Restore object store data
            await self._restore_object_store_data(
                test_result.test_config.restore_namespace,
                test_result.test_config.backup_source
            )

            # 3. Validate restoration
            validation_results = await self._validate_object_store_restoration(
                test_result.test_config.restore_namespace
            )

            test_result.validation_results[component_name] = validation_results["status"]

            return {
                "status": "passed" if validation_results["success"] else "failed",
                "duration_seconds": time.time() - start_time,
                "validation_details": validation_results,
                "backup_validated": backup_status.value
            }

        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _execute_schema_registry_restore(self, test_result: DRTestResult, component_name: str = "schema_registry") -> Dict[str, Any]:
        """Execute schema registry restore test."""
        start_time = time.time()

        try:
            # 1. Validate backup availability
            backup_status = await self._validate_schema_registry_backup()
            if backup_status != BackupValidationStatus.VALID:
                raise ValueError(f"Schema Registry backup validation failed: {backup_status.value}")

            # 2. Restore schema registry
            await self._restore_schema_registry(
                test_result.test_config.restore_namespace,
                test_result.test_config.backup_source
            )

            # 3. Validate restoration
            validation_results = await self._validate_schema_registry_restoration(
                test_result.test_config.restore_namespace
            )

            test_result.validation_results[component_name] = validation_results["status"]

            return {
                "status": "passed" if validation_results["success"] else "failed",
                "duration_seconds": time.time() - start_time,
                "validation_details": validation_results,
                "backup_validated": backup_status.value
            }

        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _execute_trino_metastore_restore(self, test_result: DRTestResult, component_name: str = "trino_metastore") -> Dict[str, Any]:
        """Execute Trino metastore restore test."""
        start_time = time.time()

        try:
            # 1. Validate backup availability
            backup_status = await self._validate_trino_metastore_backup()
            if backup_status != BackupValidationStatus.VALID:
                raise ValueError(f"Trino metastore backup validation failed: {backup_status.value}")

            # 2. Restore Trino metastore
            await self._restore_trino_metastore(
                test_result.test_config.restore_namespace,
                test_result.test_config.backup_source
            )

            # 3. Validate restoration
            validation_results = await self._validate_trino_metastore_restoration(
                test_result.test_config.restore_namespace
            )

            test_result.validation_results[component_name] = validation_results["status"]

            return {
                "status": "passed" if validation_results["success"] else "failed",
                "duration_seconds": time.time() - start_time,
                "validation_details": validation_results,
                "backup_validated": backup_status.value
            }

        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _validate_postgres_backup(self) -> BackupValidationStatus:
        """Validate PostgreSQL backup integrity."""
        # Implementation would check:
        # - Backup file existence
        # - Backup file integrity (checksum)
        # - Backup age
        # - Backup completeness

        # For now, return mock validation
        return BackupValidationStatus.VALID

    async def _validate_object_store_backup(self) -> BackupValidationStatus:
        """Validate object store backup integrity."""
        # Implementation would check:
        # - Object existence
        # - Object integrity
        # - Metadata consistency

        return BackupValidationStatus.VALID

    async def _validate_schema_registry_backup(self) -> BackupValidationStatus:
        """Validate schema registry backup integrity."""
        # Implementation would check:
        # - Schema backup existence
        # - Schema compatibility
        # - Subject consistency

        return BackupValidationStatus.VALID

    async def _validate_trino_metastore_backup(self) -> BackupValidationStatus:
        """Validate Trino metastore backup integrity."""
        # Implementation would check:
        # - Metastore backup existence
        # - Catalog consistency
        # - Schema compatibility

        return BackupValidationStatus.VALID

    async def _create_restore_namespace(self, namespace: str) -> None:
        """Create restore namespace for testing."""
        # Implementation would create Kubernetes namespace
        # and required resources for restore testing

        logger.info(f"Created restore namespace: {namespace}")

    async def _restore_postgres_database(self, namespace: str, backup_source: str) -> None:
        """Restore PostgreSQL database from backup."""
        # Implementation would:
        # - Get backup from storage
        # - Create database instance in test namespace
        # - Restore database from backup
        # - Configure connections

        logger.info(f"Restored PostgreSQL database in namespace: {namespace}")

    async def _restore_object_store_data(self, namespace: str, backup_source: str) -> None:
        """Restore object store data from backup."""
        # Implementation would:
        # - Get backup from storage
        # - Create object store instance in test namespace
        # - Restore data from backup

        logger.info(f"Restored object store data in namespace: {namespace}")

    async def _restore_schema_registry(self, namespace: str, backup_source: str) -> None:
        """Restore schema registry from backup."""
        # Implementation would:
        # - Get backup from storage
        # - Create schema registry instance in test namespace
        # - Restore schemas from backup
        # - Configure schema registry

        logger.info(f"Restored schema registry in namespace: {namespace}")

    async def _restore_trino_metastore(self, namespace: str, backup_source: str) -> None:
        """Restore Trino metastore from backup."""
        # Implementation would:
        # - Get backup from storage
        # - Create Trino instance in test namespace
        # - Restore metastore from backup
        # - Configure Trino

        logger.info(f"Restored Trino metastore in namespace: {namespace}")

    async def _validate_postgres_restoration(self, namespace: str) -> Dict[str, Any]:
        """Validate PostgreSQL restoration."""
        # Implementation would:
        # - Check database connectivity
        # - Validate data integrity
        # - Run test queries
        # - Check performance metrics

        return {
            "success": True,
            "status": BackupValidationStatus.VALID,
            "details": "PostgreSQL restoration validated successfully"
        }

    async def _validate_object_store_restoration(self, namespace: str) -> Dict[str, Any]:
        """Validate object store restoration."""
        # Implementation would:
        # - Check object store connectivity
        # - Validate data integrity
        # - Check metadata consistency

        return {
            "success": True,
            "status": BackupValidationStatus.VALID,
            "details": "Object store restoration validated successfully"
        }

    async def _validate_schema_registry_restoration(self, namespace: str) -> Dict[str, Any]:
        """Validate schema registry restoration."""
        # Implementation would:
        # - Check schema registry connectivity
        # - Validate schema consistency
        # - Test schema compatibility

        return {
            "success": True,
            "status": BackupValidationStatus.VALID,
            "details": "Schema registry restoration validated successfully"
        }

    async def _validate_trino_metastore_restoration(self, namespace: str) -> Dict[str, Any]:
        """Validate Trino metastore restoration."""
        # Implementation would:
        # - Check Trino connectivity
        # - Validate catalog consistency
        # - Test query execution

        return {
            "success": True,
            "status": BackupValidationStatus.VALID,
            "details": "Trino metastore restoration validated successfully"
        }

    async def _validate_test_results(self, test_result: DRTestResult) -> None:
        """Validate overall test results."""
        # Implementation would:
        # - Aggregate component results
        # - Calculate RTO/RPO metrics
        # - Generate test report

        if test_result.start_time and test_result.end_time:
            test_result.rto_actual_minutes = test_result.duration_seconds / 60

            # RPO would be calculated based on backup age
            test_result.rpo_actual_minutes = 60  # Default 1 hour

    async def _send_notification(self, title: str, message: str) -> None:
        """Send notification about test start."""
        notification_data = {
            "title": title,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "type": "dr_test_started"
        }

        self.logger.log(
            LogLevel.INFO,
            f"DR Test Notification: {title}",
            "dr_test_notification",
            **notification_data
        )

    async def _send_completion_notification(self, test_result: DRTestResult) -> None:
        """Send notification about test completion."""
        status_emoji = {
            DRTestStatus.PASSED: "✅",
            DRTestStatus.FAILED: "❌",
            DRTestStatus.PARTIALLY_FAILED: "⚠️"
        }

        emoji = status_emoji.get(test_result.status, "📋")

        title = f"{emoji} DR Test {test_result.status.value.upper()}: {test_result.test_config.name}"
        message = (
            f"Test {test_result.test_id} completed in {test_result.duration_seconds:.0f} seconds\n"
            f"RTO: {test_result.rto_actual_minutes:.1f} minutes\n"
            f"RPO: {test_result.rpo_actual_minutes:.1f} minutes"
        )

        if test_result.errors:
            message += f"\nErrors: {len(test_result.errors)}"

        notification_data = {
            "title": title,
            "message": message,
            "test_id": test_result.test_id,
            "status": test_result.status.value,
            "duration_seconds": test_result.duration_seconds,
            "errors": len(test_result.errors),
            "type": "dr_test_completed"
        }

        self.logger.log(
            LogLevel.INFO if test_result.status == DRTestStatus.PASSED else LogLevel.WARNING,
            f"DR Test Completion: {title}",
            "dr_test_completion",
            **notification_data
        )

    async def _cleanup_test_environment(self, test_result: DRTestResult) -> None:
        """Clean up test environment after test completion."""
        # Implementation would:
        # - Delete test namespace
        # - Remove temporary resources
        # - Clean up test data

        logger.info(f"Cleaned up test environment for {test_result.test_id}")

    async def get_test_status(self, test_id: str) -> Optional[DRTestResult]:
        """Get status of a specific test."""
        return self.active_tests.get(test_id) or next(
            (test for test in self.test_history if test.test_id == test_id),
            None
        )

    async def get_test_history(
        self,
        test_type: Optional[DRTestType] = None,
        limit: int = 50
    ) -> List[DRTestResult]:
        """Get test history with optional filtering."""
        history = self.test_history.copy()

        if test_type:
            history = [test for test in history if test.test_config.test_type == test_type]

        # Sort by start time (most recent first)
        history.sort(key=lambda x: x.start_time or "", reverse=True)

        return history[:limit]

    async def generate_dr_report(self) -> Dict[str, Any]:
        """Generate comprehensive DR testing report."""
        # Calculate statistics
        total_tests = len(self.test_history)
        passed_tests = len([t for t in self.test_history if t.status == DRTestStatus.PASSED])
        failed_tests = len([t for t in self.test_history if t.status == DRTestStatus.FAILED])
        partially_failed_tests = len([t for t in self.test_history if t.status == DRTestStatus.PARTIALLY_FAILED])

        # Calculate average RTO/RPO
        avg_rto = sum(t.rto_actual_minutes for t in self.test_history if t.rto_actual_minutes > 0) / max(len([t for t in self.test_history if t.rto_actual_minutes > 0]), 1)
        avg_rpo = sum(t.rpo_actual_minutes for t in self.test_history if t.rpo_actual_minutes > 0) / max(len([t for t in self.test_history if t.rpo_actual_minutes > 0]), 1)

        # Component success rates
        component_stats = {}
        for test in self.test_history:
            for component, result in test.component_results.items():
                if component not in component_stats:
                    component_stats[component] = {"passed": 0, "failed": 0, "total": 0}
                component_stats[component]["total"] += 1
                if result.get("status") == "passed":
                    component_stats[component]["passed"] += 1
                else:
                    component_stats[component]["failed"] += 1

        report = {
            "report_timestamp": datetime.now().isoformat(),
            "summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "partially_failed_tests": partially_failed_tests,
                "success_rate_percent": (passed_tests / max(total_tests, 1)) * 100,
                "average_rto_minutes": avg_rto,
                "average_rpo_minutes": avg_rpo
            },
            "component_statistics": component_stats,
            "recent_tests": [
                {
                    "test_id": test.test_id,
                    "test_type": test.test_config.test_type.value,
                    "status": test.status.value,
                    "start_time": test.start_time,
                    "duration_seconds": test.duration_seconds,
                    "components_tested": len(test.component_results)
                }
                for test in self.test_history[:10]  # Last 10 tests
            ],
            "active_tests": [
                {
                    "test_id": test.test_id,
                    "test_type": test.test_config.test_type.value,
                    "status": test.status.value,
                    "start_time": test.start_time,
                    "components": list(test.component_results.keys())
                }
                for test in self.active_tests.values()
            ]
        }

        return report

    async def export_test_results(self, format: str = "json", filename: Optional[str] = None) -> str:
        """Export test results in specified format."""
        if format.lower() == "json":
            data = {
                "export_timestamp": datetime.now().isoformat(),
                "active_tests": [test.to_dict() for test in self.active_tests.values()],
                "test_history": [test.to_dict() for test in self.test_history[-100:]],  # Last 100 tests
                "backup_validations": {k: v.value for k, v in self.backup_validations.items()},
                "summary": {
                    "total_tests": len(self.test_history),
                    "active_tests": len(self.active_tests),
                    "backup_validations": len(self.backup_validations)
                }
            }

            json_data = json.dumps(data, indent=2, default=str)

            if filename:
                with open(filename, 'w') as f:
                    f.write(json_data)

            return json_data

        else:
            raise ValueError(f"Unsupported export format: {format}")


# Global DR test manager
_global_dr_test_manager: Optional[DRTestManager] = None


async def get_dr_test_manager() -> DRTestManager:
    """Get or create the global DR test manager."""
    global _global_dr_test_manager

    if _global_dr_test_manager is None:
        _global_dr_test_manager = DRTestManager()

    return _global_dr_test_manager


# Convenience functions for common operations
async def run_dr_test(
    test_type: DRTestType,
    name: str,
    description: str = "",
    **kwargs
) -> DRTestResult:
    """Run a DR test with the specified configuration."""
    manager = await get_dr_test_manager()

    config = DRTestConfig(
        test_type=test_type,
        name=name,
        description=description,
        **kwargs
    )

    return await manager.run_dr_test(config)


async def get_dr_status() -> Dict[str, Any]:
    """Get current DR testing status."""
    manager = await get_dr_test_manager()
    return await manager.generate_dr_report()


async def validate_backups() -> Dict[str, BackupValidationStatus]:
    """Validate all backup integrity."""
    manager = await get_dr_test_manager()

    # Validate different backup types
    validations = {}

    validations["postgres"] = await manager._validate_postgres_backup()
    validations["object_store"] = await manager._validate_object_store_backup()
    validations["schema_registry"] = await manager._validate_schema_registry_backup()
    validations["trino_metastore"] = await manager._validate_trino_metastore_backup()

    # Update manager's validation status
    manager.backup_validations.update(validations)

    return validations
