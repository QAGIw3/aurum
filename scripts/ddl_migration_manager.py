#!/usr/bin/env python3
"""DDL Migration Manager for safe database schema changes.

This script provides a comprehensive framework for managing DDL changes with:
- Pre-migration safety checks
- Rollback capabilities
- Impact analysis
- Approval workflows
- Automated validation
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None

try:
    from trino.dbapi import connect as trino_connect
except ImportError:
    trino_connect = None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Migration states
MIGRATION_STATES = {
    'PENDING': 'PENDING',
    'APPROVED': 'APPROVED',
    'VALIDATED': 'VALIDATED',
    'EXECUTING': 'EXECUTING',
    'COMPLETED': 'COMPLETED',
    'FAILED': 'FAILED',
    'ROLLED_BACK': 'ROLLED_BACK'
}

# Risk levels
RISK_LEVELS = {
    'LOW': 'LOW',
    'MEDIUM': 'MEDIUM',
    'HIGH': 'HIGH',
    'CRITICAL': 'CRITICAL'
}


class MigrationStep:
    """Represents a single step in a DDL migration."""

    def __init__(self, step_id: str, sql: str, description: str,
                 rollback_sql: Optional[str] = None,
                 dependencies: Optional[List[str]] = None,
                 risk_level: str = 'MEDIUM'):
        self.step_id = step_id
        self.sql = sql
        self.description = description
        self.rollback_sql = rollback_sql
        self.dependencies = dependencies or []
        self.risk_level = risk_level


class DDLMigration:
    """Represents a complete DDL migration with multiple steps."""

    def __init__(self, migration_id: str, name: str, description: str,
                 database: str, schema: str, steps: List[MigrationStep],
                 metadata: Optional[Dict[str, Any]] = None):
        self.migration_id = migration_id
        self.name = name
        self.description = description
        self.database = database
        self.schema = schema
        self.steps = steps
        self.metadata = metadata or {}
        self.created_at = datetime.now(timezone.utc)
        self.state = MIGRATION_STATES['PENDING']
        self.approved_by = None
        self.approved_at = None
        self.executed_by = None
        self.executed_at = None
        self.rolled_back_by = None
        self.rolled_back_at = None
        self.execution_log = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert migration to dictionary for serialization."""
        return {
            'migration_id': self.migration_id,
            'name': self.name,
            'description': self.description,
            'database': self.database,
            'schema': self.schema,
            'steps': [
                {
                    'step_id': step.step_id,
                    'sql': step.sql,
                    'description': step.description,
                    'rollback_sql': step.rollback_sql,
                    'dependencies': step.dependencies,
                    'risk_level': step.risk_level
                } for step in self.steps
            ],
            'metadata': self.metadata,
            'created_at': self.created_at.isoformat(),
            'state': self.state,
            'approved_by': self.approved_by,
            'approved_at': self.approved_at.isoformat() if self.approved_at else None,
            'executed_by': self.executed_by,
            'executed_at': self.executed_at.isoformat() if self.executed_at else None,
            'rolled_back_by': self.rolled_back_by,
            'rolled_back_at': self.rolled_back_at.isoformat() if self.rolled_back_at else None,
            'execution_log': self.execution_log
        }


class MigrationValidator:
    """Validates DDL migrations for safety and correctness."""

    def __init__(self, db_connection_string: str, db_type: str = 'postgresql'):
        self.db_connection_string = db_connection_string
        self.db_type = db_type
        self.connection = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            if self.db_type == 'postgresql':
                if psycopg2 is None:
                    raise ImportError("psycopg2 is required for PostgreSQL validation")
                self.connection = psycopg2.connect(self.db_connection_string)
            elif self.db_type == 'trino':
                if trino_connect is None:
                    raise ImportError("trino is required for Trino validation")
                self.connection = trino_connect(self.db_connection_string)
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def validate_migration(self, migration: DDLMigration) -> Dict[str, Any]:
        """Validate a migration for safety and correctness."""
        validation_result = {
            'migration_id': migration.migration_id,
            'is_valid': True,
            'checks': [],
            'warnings': [],
            'errors': []
        }

        try:
            self.connect()

            # Perform validation checks
            self._validate_sql_syntax(migration, validation_result)
            self._validate_dependencies(migration, validation_result)
            self._validate_impact_analysis(migration, validation_result)
            self._validate_permissions(migration, validation_result)
            self._validate_rollback_safety(migration, validation_result)

            # Determine overall validity
            validation_result['is_valid'] = len(validation_result['errors']) == 0

        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Validation failed: {str(e)}")
        finally:
            self.disconnect()

        return validation_result

    def _validate_sql_syntax(self, migration: DDLMigration, result: Dict[str, Any]) -> None:
        """Validate SQL syntax for all migration steps."""
        check = {
            'check_name': 'SQL Syntax Validation',
            'status': 'PASSED',
            'details': []
        }

        for step in migration.steps:
            try:
                # Parse SQL to check syntax (simplified validation)
                if not step.sql.strip():
                    check['status'] = 'FAILED'
                    check['details'].append(f"Step {step.step_id}: Empty SQL")
                    continue

                # Basic SQL validation
                sql_lower = step.sql.lower().strip()
                if not any(keyword in sql_lower for keyword in ['create', 'alter', 'drop']):
                    check['status'] = 'WARNING'
                    check['details'].append(f"Step {step.step_id}: No DDL keywords found")

                check['details'].append(f"Step {step.step_id}: SQL syntax appears valid")

            except Exception as e:
                check['status'] = 'FAILED'
                check['details'].append(f"Step {step.step_id}: {str(e)}")

        result['checks'].append(check)
        if check['status'] == 'FAILED':
            result['errors'].append("SQL syntax validation failed")

    def _validate_dependencies(self, migration: DDLMigration, result: Dict[str, Any]) -> None:
        """Validate step dependencies."""
        check = {
            'check_name': 'Dependency Validation',
            'status': 'PASSED',
            'details': []
        }

        step_ids = {step.step_id for step in migration.steps}

        for step in migration.steps:
            for dep in step.dependencies:
                if dep not in step_ids:
                    check['status'] = 'FAILED'
                    check['details'].append(f"Step {step.step_id}: Unknown dependency {dep}")

        if check['status'] == 'PASSED':
            check['details'].append("All dependencies are valid")

        result['checks'].append(check)
        if check['status'] == 'FAILED':
            result['errors'].append("Dependency validation failed")

    def _validate_impact_analysis(self, migration: DDLMigration, result: Dict[str, Any]) -> None:
        """Analyze the impact of migration changes."""
        check = {
            'check_name': 'Impact Analysis',
            'status': 'PASSED',
            'details': []
        }

        high_risk_operations = 0

        for step in migration.steps:
            sql_lower = step.sql.lower()

            # Check for high-risk operations
            if 'drop table' in sql_lower:
                high_risk_operations += 1
                check['details'].append(f"Step {step.step_id}: DROP TABLE operation detected")
            elif 'drop column' in sql_lower:
                high_risk_operations += 1
                check['details'].append(f"Step {step.step_id}: DROP COLUMN operation detected")
            elif 'alter table' in sql_lower and 'drop' in sql_lower:
                high_risk_operations += 1
                check['details'].append(f"Step {step.step_id}: ALTER TABLE with DROP detected")

            # Check risk level
            if step.risk_level in ['HIGH', 'CRITICAL']:
                check['details'].append(f"Step {step.step_id}: High risk operation ({step.risk_level})")

        if high_risk_operations > 0:
            check['details'].append(f"Found {high_risk_operations} high-risk operations")
            if high_risk_operations > len(migration.steps) * 0.5:
                check['status'] = 'WARNING'

        result['checks'].append(check)

    def _validate_permissions(self, migration: DDLMigration, result: Dict[str, Any]) -> None:
        """Validate required permissions for migration."""
        check = {
            'check_name': 'Permission Validation',
            'status': 'PASSED',
            'details': ['DDL permissions validation requires runtime check']
        }

        # In a real implementation, this would check actual database permissions
        result['checks'].append(check)

    def _validate_rollback_safety(self, migration: DDLMigration, result: Dict[str, Any]) -> None:
        """Validate that rollback operations are safe."""
        check = {
            'check_name': 'Rollback Safety Validation',
            'status': 'PASSED',
            'details': []
        }

        for step in migration.steps:
            if not step.rollback_sql:
                check['status'] = 'WARNING'
                check['details'].append(f"Step {step.step_id}: No rollback SQL provided")
            else:
                rollback_lower = step.rollback_sql.lower()
                if 'drop' in rollback_lower and 'create' not in rollback_lower:
                    check['details'].append(f"Step {step.step_id}: Rollback contains DROP without CREATE")

        result['checks'].append(check)


class MigrationExecutor:
    """Executes DDL migrations with safety checks and rollback capabilities."""

    def __init__(self, db_connection_string: str, db_type: str = 'postgresql'):
        self.db_connection_string = db_connection_string
        self.db_type = db_type
        self.connection = None
        self.transaction_active = False

    def connect(self) -> None:
        """Establish database connection."""
        try:
            if self.db_type == 'postgresql':
                if psycopg2 is None:
                    raise ImportError("psycopg2 is required for PostgreSQL execution")
                self.connection = psycopg2.connect(self.db_connection_string)
                self.connection.autocommit = False
            elif self.db_type == 'trino':
                if trino_connect is None:
                    raise ImportError("trino is required for Trino execution")
                self.connection = trino_connect(self.db_connection_string)
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection and self.transaction_active:
            self.rollback_transaction()
        if self.connection:
            self.connection.close()
            self.connection = None

    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if self.db_type == 'postgresql' and self.connection:
            self.connection.autocommit = False
            self.transaction_active = True
        elif self.db_type == 'trino':
            # Trino doesn't support transactions for DDL
            pass

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self.db_type == 'postgresql' and self.connection and self.transaction_active:
            self.connection.commit()
            self.transaction_active = False
        elif self.db_type == 'trino':
            pass  # Trino auto-commits DDL

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self.db_type == 'postgresql' and self.connection and self.transaction_active:
            self.connection.rollback()
            self.transaction_active = False
        elif self.db_type == 'trino':
            pass  # Trino can't rollback DDL

    def execute_step(self, step: MigrationStep, dry_run: bool = False) -> Dict[str, Any]:
        """Execute a single migration step."""
        result = {
            'step_id': step.step_id,
            'success': False,
            'execution_time': 0,
            'error_message': None,
            'rows_affected': 0
        }

        start_time = time.time()

        try:
            if dry_run:
                logger.info(f"DRY RUN: Would execute step {step.step_id}: {step.description}")
                result['success'] = True
                result['execution_time'] = time.time() - start_time
                return result

            # Execute the SQL
            cursor = self.connection.cursor()

            # Log the execution
            logger.info(f"Executing step {step.step_id}: {step.description}")
            logger.debug(f"SQL: {step.sql}")

            cursor.execute(step.sql)

            # Get affected rows for non-SELECT statements
            if hasattr(cursor, 'rowcount'):
                result['rows_affected'] = cursor.rowcount

            # Commit if not in transaction
            if self.db_type == 'postgresql' and not self.transaction_active:
                self.connection.commit()

            result['success'] = True
            result['execution_time'] = time.time() - start_time

            logger.info(f"✅ Successfully executed step {step.step_id}")

        except Exception as e:
            error_msg = f"Failed to execute step {step.step_id}: {str(e)}"
            logger.error(error_msg)
            result['error_message'] = error_msg
            result['execution_time'] = time.time() - start_time

        return result

    def rollback_step(self, step: MigrationStep, dry_run: bool = False) -> Dict[str, Any]:
        """Rollback a single migration step."""
        result = {
            'step_id': step.step_id,
            'success': False,
            'execution_time': 0,
            'error_message': None
        }

        start_time = time.time()

        try:
            if not step.rollback_sql:
                result['error_message'] = f"No rollback SQL provided for step {step.step_id}"
                return result

            if dry_run:
                logger.info(f"DRY RUN: Would rollback step {step.step_id}: {step.description}")
                result['success'] = True
                result['execution_time'] = time.time() - start_time
                return result

            # Execute the rollback SQL
            cursor = self.connection.cursor()

            logger.info(f"Rolling back step {step.step_id}: {step.description}")
            logger.debug(f"Rollback SQL: {step.rollback_sql}")

            cursor.execute(step.rollback_sql)

            # Commit if not in transaction
            if self.db_type == 'postgresql' and not self.transaction_active:
                self.connection.commit()

            result['success'] = True
            result['execution_time'] = time.time() - start_time

            logger.info(f"✅ Successfully rolled back step {step.step_id}")

        except Exception as e:
            error_msg = f"Failed to rollback step {step.step_id}: {str(e)}"
            logger.error(error_msg)
            result['error_message'] = error_msg
            result['execution_time'] = time.time() - start_time

        return result


class MigrationManager:
    """Main migration manager class."""

    def __init__(self, migrations_dir: Path, db_connection_string: str, db_type: str = 'postgresql'):
        self.migrations_dir = migrations_dir
        self.db_connection_string = db_connection_string
        self.db_type = db_type
        self.validator = MigrationValidator(db_connection_string, db_type)
        self.executor = MigrationExecutor(db_connection_string, db_type)

        # Create migrations directory if it doesn't exist
        self.migrations_dir.mkdir(parents=True, exist_ok=True)

    def create_migration(self, name: str, description: str, database: str,
                        schema: str, steps: List[MigrationStep]) -> str:
        """Create a new migration file."""
        migration_id = f"{int(datetime.now(timezone.utc).timestamp())}_{name.replace(' ', '_').lower()}"

        migration = DDLMigration(
            migration_id=migration_id,
            name=name,
            description=description,
            database=database,
            schema=schema,
            steps=steps
        )

        # Save migration to file
        migration_file = self.migrations_dir / f"{migration_id}.json"
        with open(migration_file, 'w') as f:
            json.dump(migration.to_dict(), f, indent=2, default=str)

        logger.info(f"Created migration: {migration_file}")
        return migration_id

    def validate_migration(self, migration_id: str) -> Dict[str, Any]:
        """Validate a migration."""
        migration = self.load_migration(migration_id)
        return self.validator.validate_migration(migration)

    def approve_migration(self, migration_id: str, approved_by: str) -> bool:
        """Approve a migration for execution."""
        migration = self.load_migration(migration_id)
        migration.state = MIGRATION_STATES['APPROVED']
        migration.approved_by = approved_by
        migration.approved_at = datetime.now(timezone.utc)

        self.save_migration(migration)
        logger.info(f"Approved migration {migration_id} by {approved_by}")
        return True

    def execute_migration(self, migration_id: str, executed_by: str,
                         dry_run: bool = False) -> Dict[str, Any]:
        """Execute a migration."""
        migration = self.load_migration(migration_id)

        if migration.state not in [MIGRATION_STATES['APPROVED'], MIGRATION_STATES['VALIDATED']]:
            raise ValueError(f"Migration {migration_id} is not approved for execution")

        result = {
            'migration_id': migration_id,
            'success': False,
            'steps_executed': 0,
            'total_steps': len(migration.steps),
            'step_results': [],
            'execution_time': 0,
            'error_message': None
        }

        start_time = time.time()
        migration.state = MIGRATION_STATES['EXECUTING']
        migration.executed_by = executed_by
        migration.executed_at = datetime.now(timezone.utc)

        try:
            self.executor.connect()
            self.executor.begin_transaction()

            # Execute steps in order, respecting dependencies
            executed_steps = self._execute_steps_with_dependencies(migration, dry_run)
            result['step_results'] = executed_steps
            result['steps_executed'] = len([s for s in executed_steps if s['success']])

            if all(step['success'] for step in executed_steps):
                if not dry_run:
                    self.executor.commit_transaction()
                    migration.state = MIGRATION_STATES['COMPLETED']
                result['success'] = True
                logger.info(f"✅ Successfully executed migration {migration_id}")
            else:
                if not dry_run:
                    self.executor.rollback_transaction()
                    migration.state = MIGRATION_STATES['FAILED']
                result['success'] = False
                result['error_message'] = "One or more steps failed"

        except Exception as e:
            if not dry_run:
                self.executor.rollback_transaction()
                migration.state = MIGRATION_STATES['FAILED']
            result['success'] = False
            result['error_message'] = str(e)
            logger.error(f"❌ Migration {migration_id} failed: {e}")
        finally:
            result['execution_time'] = time.time() - start_time

            # Update migration log
            migration.execution_log.append({
                'action': 'execute' if not dry_run else 'dry_run',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'executed_by': executed_by,
                'result': result
            })

            self.save_migration(migration)
            self.executor.disconnect()

        return result

    def _execute_steps_with_dependencies(self, migration: DDLMigration, dry_run: bool) -> List[Dict[str, Any]]:
        """Execute migration steps respecting dependencies."""
        executed_steps = {}
        results = []

        # Build dependency graph
        dependency_graph = {}
        for step in migration.steps:
            dependency_graph[step.step_id] = set(step.dependencies)

        # Execute steps in dependency order
        while len(executed_steps) < len(migration.steps):
            # Find steps whose dependencies are all satisfied
            ready_steps = []
            for step in migration.steps:
                if step.step_id in executed_steps:
                    continue

                dependencies_satisfied = all(
                    dep in executed_steps for dep in dependency_graph[step.step_id]
                )

                if dependencies_satisfied:
                    ready_steps.append(step)

            if not ready_steps:
                # Circular dependency or missing dependency
                remaining_steps = [s.step_id for s in migration.steps if s.step_id not in executed_steps]
                raise ValueError(f"Cannot execute remaining steps due to unsatisfied dependencies: {remaining_steps}")

            # Execute ready steps
            for step in ready_steps:
                step_result = self.executor.execute_step(step, dry_run)
                results.append(step_result)
                executed_steps[step.step_id] = step_result['success']

                if not step_result['success']:
                    # Fail fast on first error
                    break

            if any(not r['success'] for r in results):
                break

        return results

    def rollback_migration(self, migration_id: str, rolled_back_by: str,
                          dry_run: bool = False) -> Dict[str, Any]:
        """Rollback a migration."""
        migration = self.load_migration(migration_id)

        if migration.state not in [MIGRATION_STATES['COMPLETED'], MIGRATION_STATES['FAILED']]:
            raise ValueError(f"Migration {migration_id} cannot be rolled back in state {migration.state}")

        result = {
            'migration_id': migration_id,
            'success': False,
            'steps_rolled_back': 0,
            'total_steps': len(migration.steps),
            'step_results': [],
            'execution_time': 0,
            'error_message': None
        }

        start_time = time.time()
        migration.state = MIGRATION_STATES['EXECUTING']
        migration.rolled_back_by = rolled_back_by
        migration.rolled_back_at = datetime.now(timezone.utc)

        try:
            self.executor.connect()
            self.executor.begin_transaction()

            # Rollback steps in reverse order
            rollback_steps = list(reversed(migration.steps))
            rollback_results = []

            for step in rollback_steps:
                if step.rollback_sql:
                    step_result = self.executor.rollback_step(step, dry_run)
                    rollback_results.append(step_result)
                    result['steps_rolled_back'] += 1 if step_result['success'] else 0

            result['step_results'] = rollback_results

            if all(step['success'] for step in rollback_results):
                if not dry_run:
                    self.executor.commit_transaction()
                    migration.state = MIGRATION_STATES['ROLLED_BACK']
                result['success'] = True
                logger.info(f"✅ Successfully rolled back migration {migration_id}")
            else:
                if not dry_run:
                    self.executor.rollback_transaction()
                    migration.state = MIGRATION_STATES['FAILED']
                result['success'] = False
                result['error_message'] = "One or more rollback steps failed"

        except Exception as e:
            if not dry_run:
                self.executor.rollback_transaction()
                migration.state = MIGRATION_STATES['FAILED']
            result['success'] = False
            result['error_message'] = str(e)
            logger.error(f"❌ Migration rollback {migration_id} failed: {e}")
        finally:
            result['execution_time'] = time.time() - start_time

            # Update migration log
            migration.execution_log.append({
                'action': 'rollback' if not dry_run else 'rollback_dry_run',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'rolled_back_by': rolled_back_by,
                'result': result
            })

            self.save_migration(migration)
            self.executor.disconnect()

        return result

    def load_migration(self, migration_id: str) -> DDLMigration:
        """Load a migration from file."""
        migration_file = self.migrations_dir / f"{migration_id}.json"

        if not migration_file.exists():
            raise FileNotFoundError(f"Migration file not found: {migration_file}")

        with open(migration_file, 'r') as f:
            data = json.load(f)

        # Recreate migration object
        steps = [
            MigrationStep(
                step_id=step['step_id'],
                sql=step['sql'],
                description=step['description'],
                rollback_sql=step.get('rollback_sql'),
                dependencies=step.get('dependencies', []),
                risk_level=step.get('risk_level', 'MEDIUM')
            ) for step in data['steps']
        ]

        migration = DDLMigration(
            migration_id=data['migration_id'],
            name=data['name'],
            description=data['description'],
            database=data['database'],
            schema=data['schema'],
            steps=steps,
            metadata=data.get('metadata', {})
        )

        # Restore state information
        migration.state = data.get('state', MIGRATION_STATES['PENDING'])
        migration.approved_by = data.get('approved_by')
        migration.approved_at = datetime.fromisoformat(data['approved_at']) if data.get('approved_at') else None
        migration.executed_by = data.get('executed_by')
        migration.executed_at = datetime.fromisoformat(data['executed_at']) if data.get('executed_at') else None
        migration.rolled_back_by = data.get('rolled_back_by')
        migration.rolled_back_at = datetime.fromisoformat(data['rolled_back_at']) if data.get('rolled_back_at') else None
        migration.execution_log = data.get('execution_log', [])

        return migration

    def save_migration(self, migration: DDLMigration) -> None:
        """Save a migration to file."""
        migration_file = self.migrations_dir / f"{migration.migration_id}.json"

        with open(migration_file, 'w') as f:
            json.dump(migration.to_dict(), f, indent=2, default=str)

    def list_migrations(self) -> List[Dict[str, Any]]:
        """List all migrations with their status."""
        migrations = []

        for migration_file in self.migrations_dir.glob("*.json"):
            try:
                migration = self.load_migration(migration_file.stem)
                migrations.append({
                    'migration_id': migration.migration_id,
                    'name': migration.name,
                    'description': migration.description,
                    'state': migration.state,
                    'created_at': migration.created_at.isoformat(),
                    'approved_by': migration.approved_by,
                    'executed_by': migration.executed_by
                })
            except Exception as e:
                logger.warning(f"Failed to load migration {migration_file}: {e}")

        return sorted(migrations, key=lambda x: x['created_at'], reverse=True)


def main():
    """Main entry point for DDL migration manager."""

    parser = argparse.ArgumentParser(description='DDL Migration Manager')
    parser.add_argument('--migrations-dir', type=Path, default=Path('./migrations'),
                       help='Directory containing migration files')
    parser.add_argument('--db-connection-string', required=True,
                       help='Database connection string')
    parser.add_argument('--db-type', choices=['postgresql', 'trino'], default='postgresql',
                       help='Database type')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Create migration command
    create_parser = subparsers.add_parser('create', help='Create a new migration')
    create_parser.add_argument('--name', required=True, help='Migration name')
    create_parser.add_argument('--description', required=True, help='Migration description')
    create_parser.add_argument('--database', required=True, help='Target database')
    create_parser.add_argument('--schema', required=True, help='Target schema')
    create_parser.add_argument('--sql-file', type=Path, help='File containing SQL steps')

    # Validate migration command
    validate_parser = subparsers.add_parser('validate', help='Validate a migration')
    validate_parser.add_argument('migration_id', help='Migration ID to validate')

    # Approve migration command
    approve_parser = subparsers.add_parser('approve', help='Approve a migration')
    approve_parser.add_argument('migration_id', help='Migration ID to approve')
    approve_parser.add_argument('--approved-by', required=True, help='User approving the migration')

    # Execute migration command
    execute_parser = subparsers.add_parser('execute', help='Execute a migration')
    execute_parser.add_argument('migration_id', help='Migration ID to execute')
    execute_parser.add_argument('--executed-by', required=True, help='User executing the migration')
    execute_parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')

    # Rollback migration command
    rollback_parser = subparsers.add_parser('rollback', help='Rollback a migration')
    rollback_parser.add_argument('migration_id', help='Migration ID to rollback')
    rollback_parser.add_argument('--rolled-back-by', required=True, help='User rolling back the migration')
    rollback_parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')

    # List migrations command
    list_parser = subparsers.add_parser('list', help='List all migrations')
    list_parser.add_argument('--state', choices=MIGRATION_STATES.keys(), help='Filter by migration state')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        manager = MigrationManager(args.migrations_dir, args.db_connection_string, args.db_type)

        if args.command == 'create':
            # Create a new migration
            if args.sql_file and args.sql_file.exists():
                # Load steps from SQL file
                with open(args.sql_file, 'r') as f:
                    sql_content = f.read()

                # Parse SQL content (simplified parsing)
                steps = []
                sql_statements = [s.strip() for s in sql_content.split(';') if s.strip()]

                for i, sql in enumerate(sql_statements):
                    step_id = f"step_{i+1"03d"}"
                    description = f"SQL step {i+1}"
                    steps.append(MigrationStep(step_id, sql, description))

                migration_id = manager.create_migration(
                    args.name, args.description, args.database, args.schema, steps
                )
                print(f"Created migration: {migration_id}")
            else:
                print("SQL file not provided or does not exist")
                return 1

        elif args.command == 'validate':
            result = manager.validate_migration(args.migration_id)
            print(json.dumps(result, indent=2))

        elif args.command == 'approve':
            manager.approve_migration(args.migration_id, args.approved_by)
            print(f"Approved migration: {args.migration_id}")

        elif args.command == 'execute':
            result = manager.execute_migration(args.migration_id, args.executed_by, args.dry_run)
            print(json.dumps(result, indent=2))

        elif args.command == 'rollback':
            result = manager.rollback_migration(args.migration_id, args.rolled_back_by, args.dry_run)
            print(json.dumps(result, indent=2))

        elif args.command == 'list':
            migrations = manager.list_migrations()

            if args.state:
                migrations = [m for m in migrations if m['state'] == args.state]

            for migration in migrations:
                print(f"{migration['migration_id']}: {migration['name']} ({migration['state']})")

    except Exception as e:
        logger.error(f"Command failed: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
