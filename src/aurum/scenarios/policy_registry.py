"""Policy driver registry with versioning and parameter validation."""

from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

import asyncpg
from pydantic import BaseModel, Field, field_validator, root_validator

from ..telemetry.context import get_correlation_id, get_tenant_id, get_user_id, log_structured
from .models import DriverType


class PolicyDriverParameter(BaseModel):
    """Definition of a policy driver parameter."""

    name: str = Field(..., description="Parameter name")
    parameter_type: str = Field(..., description="Parameter type (string, number, boolean, array, object)")
    required: bool = Field(default=False, description="Whether parameter is required")
    default_value: Optional[Any] = Field(None, description="Default value for parameter")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules")
    description: Optional[str] = Field(None, description="Parameter description")
    order_index: int = Field(default=0, description="Display order")

    @field_validator("parameter_type")
    @classmethod
    def validate_parameter_type(cls, v: str) -> str:
        """Validate parameter type."""
        valid_types = ["string", "number", "boolean", "array", "object"]
        if v not in valid_types:
            raise ValueError(f"Parameter type must be one of: {valid_types}")
        return v

    @field_validator("name")
    @classmethod
    def validate_parameter_name(cls, v: str) -> str:
        """Validate parameter name format."""
        if not v or not v.strip():
            raise ValueError("Parameter name cannot be empty")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Parameter name must be a valid identifier")
        return v.strip()


class PolicyDriverVersion(BaseModel):
    """Version information for a policy driver."""

    version: str = Field(..., description="Semantic version (e.g., '1.0.0')")
    parameter_schema: Dict[str, Any] = Field(default_factory=dict, description="Parameter schema")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules")
    implementation_path: str = Field(..., description="Path to driver implementation")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate semantic version format."""
        if not re.match(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?$", v):
            raise ValueError("Version must be in semantic version format (e.g., '1.0.0')")
        return v

    @root_validator(pre=True, skip_on_failure=True)
    @classmethod
    def validate_parameter_schema(cls, values) -> dict:
        """Validate parameter schema consistency."""
        schema = values.get("parameter_schema")
        validation_rules = values.get("validation_rules")

        # Ensure schema and validation rules are consistent
        if schema and validation_rules:
            schema_params = set(schema.keys())
            validation_params = set(validation_rules.keys())

            if schema_params != validation_params:
                raise ValueError("Parameter schema and validation rules must have matching parameters")

        return values


class PolicyDriverTestCase(BaseModel):
    """Test case for policy driver validation."""

    name: str = Field(..., description="Test case name")
    description: Optional[str] = Field(None, description="Test case description")
    input_payload: Dict[str, Any] = Field(..., description="Input parameters for test")
    expected_output: Optional[Dict[str, Any]] = Field(None, description="Expected output (for validation)")
    is_valid: bool = Field(default=True, description="Whether this is a valid test case")

    @field_validator("name")
    @classmethod
    def validate_test_case_name(cls, v: str) -> str:
        """Validate test case name."""
        if not v or not v.strip():
            raise ValueError("Test case name cannot be empty")
        if len(v) > 100:
            raise ValueError("Test case name cannot exceed 100 characters")
        return v.strip()


class PolicyDriver(BaseModel):
    """Complete policy driver definition."""

    id: Optional[str] = Field(None, description="Driver ID")
    name: str = Field(..., description="Driver name")
    driver_type: DriverType = Field(..., description="Driver type")
    description: Optional[str] = Field(None, description="Driver description")
    version: str = Field(default="1.0.0", description="Current version")
    status: str = Field(default="active", description="Driver status")
    parameter_schema: Dict[str, Any] = Field(default_factory=dict, description="Parameter schema")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules")
    implementation_path: Optional[str] = Field(None, description="Path to driver implementation")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    parameters: List[PolicyDriverParameter] = Field(default_factory=list, description="Parameter definitions")
    versions: List[PolicyDriverVersion] = Field(default_factory=list, description="Version history")
    test_cases: List[PolicyDriverTestCase] = Field(default_factory=list, description="Test cases")

    @field_validator("name")
    @classmethod
    def validate_driver_name(cls, v: str) -> str:
        """Validate driver name format."""
        if not v or not v.strip():
            raise ValueError("Driver name cannot be empty")
        if len(v) > 100:
            raise ValueError("Driver name cannot exceed 100 characters")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.-]*$", v):
            raise ValueError("Driver name must be a valid identifier")
        return v.strip()

    @root_validator(pre=True, skip_on_failure=True)
    @classmethod
    def validate_driver_consistency(cls, values) -> dict:
        """Validate driver configuration consistency."""
        if values.get("driver_type") != DriverType.POLICY:
            raise ValueError("Only policy drivers are supported in the registry")

        # Ensure parameter definitions match schema
        schema_params = set(values.get("parameter_schema", {}).keys())
        defined_params = {p.name for p in values.get("parameters", [])}

        if schema_params != defined_params:
            raise ValueError("Parameter schema must match parameter definitions")

        # Ensure versions are valid
        versions = values.get("versions", [])
        version = values.get("version")
        parameter_schema = values.get("parameter_schema")

        for v in versions:
            if v.version == version:
                # Current version should match main definition
                if v.parameter_schema != parameter_schema:
                    raise ValueError("Current version schema must match main parameter schema")

        return values


class PolicyDriverRegistry:
    """Registry for managing policy drivers with versioning and validation."""

    def __init__(self, postgres_dsn: str):
        self.postgres_dsn = postgres_dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        """Initialize database connection pool."""
        self.pool = await asyncpg.create_pool(
            self.postgres_dsn,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        log_structured(
            "info",
            "policy_driver_registry_initialized",
            pool_size_min=2,
            pool_size_max=10,
            dsn=self.postgres_dsn.replace("//", "//[REDACTED]@"),
        )

    async def close(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            log_structured("info", "policy_driver_registry_closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool."""
        if not self.pool:
            raise RuntimeError("PolicyDriverRegistry not initialized")

        async with self.pool.acquire() as conn:
            yield conn

    # === DRIVER MANAGEMENT ===

    async def register_driver(self, driver: PolicyDriver, created_by: str) -> PolicyDriver:
        """Register a new policy driver."""
        driver_id = str(uuid4())
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            # Insert main driver record
            await conn.execute("""
                INSERT INTO scenario_driver (
                    id, name, type, description, version, status,
                    parameter_schema, validation_rules, implementation_path,
                    metadata, created_by, updated_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
                driver_id,
                driver.name,
                driver.driver_type.value,
                driver.description,
                driver.version,
                driver.status,
                json.dumps(driver.parameter_schema),
                json.dumps(driver.validation_rules),
                driver.implementation_path,
                json.dumps(driver.metadata),
                created_by,
                created_by,
            )

            # Insert parameter definitions
            for param in driver.parameters:
                await conn.execute("""
                    INSERT INTO policy_driver_parameter (
                        driver_id, parameter_name, parameter_type, required,
                        default_value, validation_rules, description, order_index
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                    driver_id,
                    param.name,
                    param.parameter_type,
                    param.required,
                    json.dumps(param.default_value) if param.default_value is not None else None,
                    json.dumps(param.validation_rules),
                    param.description,
                    param.order_index,
                )

            # Insert version history
            for version in driver.versions:
                await conn.execute("""
                    INSERT INTO policy_driver_version (
                        driver_id, version, parameter_schema, validation_rules,
                        implementation_path, metadata, created_by
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                    driver_id,
                    version.version,
                    json.dumps(version.parameter_schema),
                    json.dumps(version.validation_rules),
                    version.implementation_path,
                    json.dumps(version.metadata),
                    created_by,
                )

            # Insert test cases
            for test_case in driver.test_cases:
                await conn.execute("""
                    INSERT INTO policy_driver_test_case (
                        driver_id, name, description, input_payload,
                        expected_output, is_valid, created_by
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                    driver_id,
                    test_case.name,
                    test_case.description,
                    json.dumps(test_case.input_payload),
                    json.dumps(test_case.expected_output) if test_case.expected_output else None,
                    test_case.is_valid,
                    created_by,
                )

        log_structured(
            "info",
            "policy_driver_registered",
            driver_id=driver_id,
            tenant_id=tenant_id,
            driver_name=driver.name,
            version=driver.version,
            created_by=created_by,
        )

        # Return driver with assigned ID
        driver.id = driver_id
        return driver

    async def get_driver(self, driver_id: str) -> Optional[PolicyDriver]:
        """Get policy driver by ID."""
        async with self.get_connection() as conn:
            # Get main driver record
            driver_row = await conn.fetchrow("""
                SELECT id, name, type, description, version, status,
                       parameter_schema, validation_rules, implementation_path,
                       metadata, created_by, created_at, updated_by, updated_at
                FROM scenario_driver
                WHERE id = $1 AND type = 'policy'
            """, driver_id)

            if not driver_row:
                return None

            # Get parameter definitions
            param_rows = await conn.fetch("""
                SELECT parameter_name, parameter_type, required, default_value,
                       validation_rules, description, order_index
                FROM policy_driver_parameter
                WHERE driver_id = $1
                ORDER BY order_index, parameter_name
            """, driver_id)

            # Get version history
            version_rows = await conn.fetch("""
                SELECT version, parameter_schema, validation_rules,
                       implementation_path, metadata, created_by, created_at
                FROM policy_driver_version
                WHERE driver_id = $1
                ORDER BY created_at DESC
            """, driver_id)

            # Get test cases
            test_case_rows = await conn.fetch("""
                SELECT name, description, input_payload, expected_output,
                       is_valid, created_by, created_at
                FROM policy_driver_test_case
                WHERE driver_id = $1
                ORDER BY created_at DESC
            """, driver_id)

        # Build parameter list
        parameters = []
        for row in param_rows:
            parameters.append(PolicyDriverParameter(
                name=row["parameter_name"],
                parameter_type=row["parameter_type"],
                required=row["required"],
                default_value=json.loads(row["default_value"]) if row["default_value"] else None,
                validation_rules=json.loads(row["validation_rules"]),
                description=row["description"],
                order_index=row["order_index"],
            ))

        # Build version list
        versions = []
        for row in version_rows:
            versions.append(PolicyDriverVersion(
                version=row["version"],
                parameter_schema=json.loads(row["parameter_schema"]),
                validation_rules=json.loads(row["validation_rules"]),
                implementation_path=row["implementation_path"],
                metadata=json.loads(row["metadata"]),
            ))

        # Build test case list
        test_cases = []
        for row in test_case_rows:
            test_cases.append(PolicyDriverTestCase(
                name=row["name"],
                description=row["description"],
                input_payload=json.loads(row["input_payload"]),
                expected_output=json.loads(row["expected_output"]) if row["expected_output"] else None,
                is_valid=row["is_valid"],
            ))

        return PolicyDriver(
            id=driver_row["id"],
            name=driver_row["name"],
            driver_type=DriverType(driver_row["type"]),
            description=driver_row["description"],
            version=driver_row["version"],
            status=driver_row["status"],
            parameter_schema=json.loads(driver_row["parameter_schema"]),
            validation_rules=json.loads(driver_row["validation_rules"]),
            implementation_path=driver_row["implementation_path"],
            metadata=json.loads(driver_row["metadata"]),
            parameters=parameters,
            versions=versions,
            test_cases=test_cases,
        )

    async def list_drivers(
        self,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[PolicyDriver], int]:
        """List policy drivers with optional filtering."""
        conditions = ["type = 'policy'"]
        params = []
        param_index = 1

        if status:
            conditions.append(f"status = ${param_index}")
            params.append(status)
            param_index += 1

        where_clause = " AND ".join(conditions)

        async with self.get_connection() as conn:
            # Get total count
            count_row = await conn.fetchrow(f"""
                SELECT COUNT(*) as total
                FROM scenario_driver
                WHERE {where_clause}
            """, *params)

            # Get paginated results
            driver_rows = await conn.fetch(f"""
                SELECT id, name, description, version, status,
                       parameter_schema, validation_rules, implementation_path,
                       metadata, created_by, created_at, updated_by, updated_at
                FROM scenario_driver
                WHERE {where_clause}
                ORDER BY updated_at DESC
                LIMIT ${param_index} OFFSET ${param_index + 1}
            """, *params, limit, offset)

        drivers = []
        for row in driver_rows:
            # Get simplified driver info (without full parameter/version details for performance)
            drivers.append(PolicyDriver(
                id=row["id"],
                name=row["name"],
                driver_type=DriverType.POLICY,
                description=row["description"],
                version=row["version"],
                status=row["status"],
                parameter_schema=json.loads(row["parameter_schema"]),
                validation_rules=json.loads(row["validation_rules"]),
                implementation_path=row["implementation_path"],
                metadata=json.loads(row["metadata"]),
            ))

        total = count_row["total"]
        return drivers, total

    async def update_driver(
        self,
        driver_id: str,
        updates: Dict[str, Any],
        updated_by: str
    ) -> Optional[PolicyDriver]:
        """Update policy driver."""
        update_fields = []
        params = [driver_id]
        param_index = 2

        for field, value in updates.items():
            if field == "name":
                update_fields.append(f"name = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "description":
                update_fields.append(f"description = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "status":
                update_fields.append(f"status = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "parameter_schema":
                update_fields.append(f"parameter_schema = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1
            elif field == "validation_rules":
                update_fields.append(f"validation_rules = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1
            elif field == "implementation_path":
                update_fields.append(f"implementation_path = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "metadata":
                update_fields.append(f"metadata = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1

        if not update_fields:
            return await self.get_driver(driver_id)

        params.append(updated_by)  # updated_by
        params.append(datetime.utcnow())  # updated_at

        async with self.get_connection() as conn:
            await conn.execute(f"""
                UPDATE scenario_driver
                SET {", ".join(update_fields)}, updated_by = ${param_index}, updated_at = ${param_index + 1}
                WHERE id = $1
            """, *params)

        log_structured(
            "info",
            "policy_driver_updated",
            driver_id=driver_id,
            updated_by=updated_by,
            updated_fields=list(updates.keys()),
        )

        return await self.get_driver(driver_id)

    async def create_driver_version(
        self,
        driver_id: str,
        version: PolicyDriverVersion,
        created_by: str
    ) -> PolicyDriverVersion:
        """Create a new version of a policy driver."""
        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO policy_driver_version (
                    driver_id, version, parameter_schema, validation_rules,
                    implementation_path, metadata, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
                driver_id,
                version.version,
                json.dumps(version.parameter_schema),
                json.dumps(version.validation_rules),
                version.implementation_path,
                json.dumps(version.metadata),
                created_by,
            )

        log_structured(
            "info",
            "policy_driver_version_created",
            driver_id=driver_id,
            version=version.version,
            created_by=created_by,
        )

        return version

    # === VALIDATION ===

    async def validate_driver_parameters(
        self,
        driver_id: str,
        parameter_payload: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate parameter payload against driver specification."""
        async with self.get_connection() as conn:
            # Use database function for validation
            result = await conn.fetchval("""
                SELECT validate_driver_parameter_compatibility($1, $2)
            """, driver_id, json.dumps(parameter_payload))

        if result:
            return True, None
        else:
            return False, "Parameter validation failed"

    async def run_driver_test_cases(self, driver_id: str) -> Dict[str, Any]:
        """Run test cases for a policy driver."""
        async with self.get_connection() as conn:
            test_case_rows = await conn.fetch("""
                SELECT name, description, input_payload, expected_output, is_valid
                FROM policy_driver_test_case
                WHERE driver_id = $1
                ORDER BY created_at
            """, driver_id)

        results = {
            "total_tests": len(test_case_rows),
            "passed_tests": 0,
            "failed_tests": 0,
            "test_results": []
        }

        for row in test_case_rows:
            # Validate input payload against driver
            is_valid, error = await self.validate_driver_parameters(
                driver_id,
                json.loads(row["input_payload"])
            )

            expected_valid = row["is_valid"]

            test_result = {
                "name": row["name"],
                "description": row["description"],
                "input_valid": is_valid,
                "expected_valid": expected_valid,
                "passed": is_valid == expected_valid,
                "error": error if not is_valid else None
            }

            results["test_results"].append(test_result)

            if test_result["passed"]:
                results["passed_tests"] += 1
            else:
                results["failed_tests"] += 1

        return results

    # === UTILITY METHODS ===

    async def get_driver_version_history(self, driver_id: str) -> List[Dict[str, Any]]:
        """Get version history for a driver."""
        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT version, parameter_schema, validation_rules,
                       implementation_path, metadata, created_by, created_at
                FROM policy_driver_version
                WHERE driver_id = $1
                ORDER BY created_at DESC
            """, driver_id)

        return [
            {
                "version": row["version"],
                "parameter_schema": json.loads(row["parameter_schema"]),
                "validation_rules": json.loads(row["validation_rules"]),
                "implementation_path": row["implementation_path"],
                "metadata": json.loads(row["metadata"]),
                "created_by": row["created_by"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    async def get_popular_drivers(self, limit: int = 10) -> List[PolicyDriver]:
        """Get most frequently used policy drivers."""
        # This would require tracking usage statistics
        # For now, return all active drivers
        drivers, _ = await self.list_drivers(status="active", limit=limit, offset=0)
        return drivers

    async def search_drivers(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[PolicyDriver], int]:
        """Search policy drivers by name or description."""
        async with self.get_connection() as conn:
            # Get total count
            count_row = await conn.fetchrow("""
                SELECT COUNT(*) as total
                FROM scenario_driver
                WHERE type = 'policy'
                AND (name ILIKE $1 OR description ILIKE $1)
            """, f"%{query}%")

            # Get paginated results
            driver_rows = await conn.fetch("""
                SELECT id, name, description, version, status,
                       parameter_schema, validation_rules, implementation_path,
                       metadata, created_by, created_at, updated_by, updated_at
                FROM scenario_driver
                WHERE type = 'policy'
                AND (name ILIKE $1 OR description ILIKE $1)
                ORDER BY updated_at DESC
                LIMIT $2 OFFSET $3
            """, f"%{query}%", limit, offset)

        drivers = []
        for row in driver_rows:
            drivers.append(PolicyDriver(
                id=row["id"],
                name=row["name"],
                driver_type=DriverType.POLICY,
                description=row["description"],
                version=row["version"],
                status=row["status"],
                parameter_schema=json.loads(row["parameter_schema"]),
                validation_rules=json.loads(row["validation_rules"]),
                implementation_path=row["implementation_path"],
                metadata=json.loads(row["metadata"]),
            ))

        total = count_row["total"]
        return drivers, total


# Global registry instance
_policy_driver_registry: Optional[PolicyDriverRegistry] = None


def get_policy_driver_registry() -> PolicyDriverRegistry:
    """Get the global policy driver registry instance."""
    if _policy_driver_registry is None:
        raise RuntimeError("PolicyDriverRegistry not initialized")
    return _policy_driver_registry


async def initialize_policy_driver_registry(postgres_dsn: str) -> PolicyDriverRegistry:
    """Initialize the global policy driver registry."""
    global _policy_driver_registry
    if _policy_driver_registry is None:
        _policy_driver_registry = PolicyDriverRegistry(postgres_dsn)
        await _policy_driver_registry.initialize()
    return _policy_driver_registry


async def close_policy_driver_registry() -> None:
    """Close the global policy driver registry."""
    global _policy_driver_registry
    if _policy_driver_registry is not None:
        await _policy_driver_registry.close()
        _policy_driver_registry = None
