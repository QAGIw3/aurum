"""Scenario persistence layer (Postgres/Timescale) with RLS.

Responsibilities:
- Manage scenario and run metadata in Postgres (`scenario`, `scenario_run`)
- Persist fine‑grained outputs/metrics when configured (`scenario_output`)
- Enforce tenant scoping via RLS by setting `app.current_tenant`

Notes:
- See DDL in `postgres/ddl/app.sql` and the RLS migration in
  `db/migrations/versions/*tenant_rls*`.
- The API `/v1/scenarios/*` reads outputs either from Iceberg via Trino
  or from Postgres depending on deployment; this store focuses on
  operational metadata and feature flags.
- All methods assume `aurum.telemetry.context` has tenant/user context.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

import asyncpg
from pydantic import BaseModel

from ..api.scenario_models import (
    ScenarioData,
    ScenarioRunData,
    ScenarioStatus,
    ScenarioRunStatus,
    ScenarioRunPriority,
    ScenarioOutputPoint,
)
from ..telemetry.context import get_correlation_id, get_tenant_id, get_user_id, log_structured


class ScenarioStore:
    """Database persistence layer for scenarios and related data."""

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
            "scenario_store_initialized",
            pool_size_min=2,
            pool_size_max=10,
            dsn=self.postgres_dsn.replace("//", "//[REDACTED]@"),
        )

    async def close(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            log_structured("info", "scenario_store_closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool."""
        if not self.pool:
            raise RuntimeError("ScenarioStore not initialized")

        async with self.pool.acquire() as conn:
            # Set tenant context for RLS policies
            tenant_id = get_tenant_id()
            if tenant_id:
                await conn.execute("SET LOCAL app.current_tenant = $1", tenant_id)
            yield conn

    # === SCENARIO OPERATIONS ===

    async def create_scenario(self, scenario_data: Dict[str, Any]) -> ScenarioData:
        """Create a new scenario."""
        scenario_id = str(uuid4())
        tenant_id = get_tenant_id()
        user_id = get_user_id()

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario (
                    id, tenant_id, name, description, status, assumptions,
                    parameters, tags, version, created_by, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
                scenario_id,
                tenant_id,
                scenario_data["name"],
                scenario_data.get("description"),
                scenario_data.get("status", "created"),
                json.dumps(scenario_data.get("assumptions", [])),
                json.dumps(scenario_data.get("parameters", {})),
                json.dumps(scenario_data.get("tags", [])),
                scenario_data.get("version", 1),
                user_id,
                datetime.utcnow(),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "scenario_created",
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            user_id=user_id,
            scenario_name=scenario_data["name"],
        )

        return await self.get_scenario(scenario_id)

    async def get_scenario(self, scenario_id: str) -> Optional[ScenarioData]:
        """Get scenario by ID with schema v2 features."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, tenant_id, name, description, status, assumptions,
                       parameters, tags, version, created_by, created_at, updated_at,
                       schema_version, curve_families, constraints, provenance_enabled
                FROM scenario
                WHERE id = $1 AND tenant_id = $2
            """, scenario_id, tenant_id)

        if not row:
            return None

        # Get associated curve families
        curve_families = await self.list_scenario_curve_families(scenario_id)

        # Get constraints
        constraints = await self.list_scenario_constraints(scenario_id)

        return ScenarioData(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            description=row["description"],
            status=ScenarioStatus(row["status"]),
            assumptions=json.loads(row["assumptions"]),
            parameters=json.loads(row["parameters"]),
            tags=json.loads(row["tags"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            created_by=row["created_by"],
            version=row["version"],
            metadata={
                "schema_version": row["schema_version"] or 2,
                "curve_families": [cf["curve_family_name"] for cf in curve_families],
                "constraints_count": len(constraints),
                "provenance_enabled": row["provenance_enabled"] or False,
            }
        )

    async def list_scenarios(
        self,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[ScenarioData], int]:
        """List scenarios with optional filtering."""
        current_tenant_id = get_tenant_id() if tenant_id is None else tenant_id

        conditions = ["tenant_id = $1"]
        params = [current_tenant_id]

        if status:
            conditions.append("status = $2")
            params.append(status)

        where_clause = " AND ".join(conditions)

        async with self.get_connection() as conn:
            # Get total count
            count_row = await conn.fetchrow(f"""
                SELECT COUNT(*) as total
                FROM scenario
                WHERE {where_clause}
            """, *params)

            # Get paginated results
            rows = await conn.fetch(f"""
                SELECT id, tenant_id, name, description, status, assumptions,
                       parameters, tags, version, created_by, created_at, updated_at
                FROM scenario
                WHERE {where_clause}
                ORDER BY updated_at DESC
                LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """, *params, limit, offset)

        scenarios = []
        for row in rows:
            scenarios.append(ScenarioData(
                id=row["id"],
                tenant_id=row["tenant_id"],
                name=row["name"],
                description=row["description"],
                status=ScenarioStatus(row["status"]),
                assumptions=json.loads(row["assumptions"]),
                parameters=json.loads(row["parameters"]),
                tags=json.loads(row["tags"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                created_by=row["created_by"],
                version=row["version"],
            ))

        total = count_row["total"]
        return scenarios, total

    async def update_scenario(self, scenario_id: str, updates: Dict[str, Any]) -> Optional[ScenarioData]:
        """Update scenario fields."""
        tenant_id = get_tenant_id()
        user_id = get_user_id()

        update_fields = []
        params = [tenant_id, scenario_id]
        param_index = 3

        for field, value in updates.items():
            if field == "assumptions":
                update_fields.append(f"assumptions = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1
            elif field == "parameters":
                update_fields.append(f"parameters = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1
            elif field == "tags":
                update_fields.append(f"tags = ${param_index}")
                params.append(json.dumps(value))
                param_index += 1
            elif field in ["name", "description", "status"]:
                update_fields.append(f"{field} = ${param_index}")
                params.append(value)
                param_index += 1

        if not update_fields:
            return await self.get_scenario(scenario_id)

        params.append(datetime.utcnow())  # updated_at
        params.append(user_id)  # updated_by (not in schema yet)

        async with self.get_connection() as conn:
            await conn.execute(f"""
                UPDATE scenario
                SET {", ".join(update_fields)}, updated_at = ${param_index}
                WHERE id = $2 AND tenant_id = $1
            """, *params)

        log_structured(
            "info",
            "scenario_updated",
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            user_id=user_id,
            updated_fields=list(updates.keys()),
        )

        return await self.get_scenario(scenario_id)

    async def delete_scenario(self, scenario_id: str) -> bool:
        """Delete scenario by ID."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM scenario
                WHERE id = $1 AND tenant_id = $2
            """, scenario_id, tenant_id)

        deleted = result != "DELETE 0"
        if deleted:
            log_structured(
                "info",
                "scenario_deleted",
                scenario_id=scenario_id,
                tenant_id=tenant_id,
            )

        return deleted

    # === SCENARIO RUN OPERATIONS ===

    async def create_run(self, scenario_id: str, run_data: Dict[str, Any]) -> ScenarioRunData:
        """Create a new scenario run."""
        run_id = str(uuid4())
        tenant_id = get_tenant_id()
        user_id = get_user_id()

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_run (
                    id, scenario_id, tenant_id, status, priority, parameters,
                    environment, created_at, queued_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                run_id,
                scenario_id,
                tenant_id,
                run_data.get("status", "queued"),
                run_data.get("priority", "normal"),
                json.dumps(run_data.get("parameters", {})),
                json.dumps(run_data.get("environment", {})),
                datetime.utcnow(),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "scenario_run_created",
            run_id=run_id,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            user_id=user_id,
        )

        return await self.get_run(scenario_id, run_id)

    async def get_run(self, scenario_id: str, run_id: str) -> Optional[ScenarioRunData]:
        """Get scenario run by ID."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT sr.id, sr.scenario_id, sr.tenant_id, sr.status, sr.priority,
                       sr.started_at, sr.completed_at, sr.duration_seconds,
                       sr.error_message, sr.retry_count, sr.max_retries,
                       sr.progress_percent, sr.parameters, sr.environment,
                       sr.created_at, sr.queued_at, sr.cancelled_at
                FROM scenario_run sr
                JOIN scenario s ON sr.scenario_id = s.id
                WHERE sr.id = $1 AND sr.tenant_id = $2 AND s.tenant_id = $2
            """, run_id, tenant_id)

        if not row:
            return None

        return ScenarioRunData(
            id=row["id"],
            scenario_id=row["scenario_id"],
            status=ScenarioRunStatus(row["status"]),
            priority=ScenarioRunPriority(row["priority"]),
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            duration_seconds=row["duration_seconds"],
            error_message=row["error_message"],
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
            progress_percent=row["progress_percent"],
            parameters=json.loads(row["parameters"]),
            environment=json.loads(row["environment"]),
            created_at=row["created_at"],
            queued_at=row["queued_at"],
            cancelled_at=row["cancelled_at"],
        )

    async def list_runs(
        self,
        scenario_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[ScenarioRunData], int]:
        """List runs for a scenario."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            # Get total count
            count_row = await conn.fetchrow("""
                SELECT COUNT(*) as total
                FROM scenario_run sr
                JOIN scenario s ON sr.scenario_id = s.id
                WHERE s.id = $1 AND s.tenant_id = $2
            """, scenario_id, tenant_id)

            # Get paginated results
            rows = await conn.fetch("""
                SELECT sr.id, sr.scenario_id, sr.tenant_id, sr.status, sr.priority,
                       sr.started_at, sr.completed_at, sr.duration_seconds,
                       sr.error_message, sr.retry_count, sr.max_retries,
                       sr.progress_percent, sr.parameters, sr.environment,
                       sr.created_at, sr.queued_at, sr.cancelled_at
                FROM scenario_run sr
                JOIN scenario s ON sr.scenario_id = s.id
                WHERE s.id = $1 AND s.tenant_id = $2
                ORDER BY sr.created_at DESC
                LIMIT $3 OFFSET $4
            """, scenario_id, tenant_id, limit, offset)

        runs = []
        for row in rows:
            runs.append(ScenarioRunData(
                id=row["id"],
                scenario_id=row["scenario_id"],
                status=ScenarioRunStatus(row["status"]),
                priority=ScenarioRunPriority(row["priority"]),
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                duration_seconds=row["duration_seconds"],
                error_message=row["error_message"],
                retry_count=row["retry_count"],
                max_retries=row["max_retries"],
                progress_percent=row["progress_percent"],
                parameters=json.loads(row["parameters"]),
                environment=json.loads(row["environment"]),
                created_at=row["created_at"],
                queued_at=row["queued_at"],
                cancelled_at=row["cancelled_at"],
            ))

        total = count_row["total"]
        return runs, total

    async def update_run_state(
        self,
        run_id: str,
        state_update: Dict[str, Any]
    ) -> Optional[ScenarioRunData]:
        """Update scenario run state."""
        tenant_id = get_tenant_id()

        update_fields = []
        params = [tenant_id, run_id]
        param_index = 3

        for field, value in state_update.items():
            if field == "status":
                update_fields.append(f"status = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "started_at" and value:
                update_fields.append(f"started_at = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "completed_at" and value:
                update_fields.append(f"completed_at = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "error_message":
                update_fields.append(f"error_message = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "progress_percent":
                update_fields.append(f"progress_percent = ${param_index}")
                params.append(value)
                param_index += 1
            elif field == "retry_count":
                update_fields.append(f"retry_count = ${param_index}")
                params.append(value)
                param_index += 1

        if not update_fields:
            return await self.get_run("", run_id)  # scenario_id not needed here

        async with self.get_connection() as conn:
            await conn.execute(f"""
                UPDATE scenario_run
                SET {", ".join(update_fields)}
                WHERE id = $2 AND tenant_id = $1
            """, *params)

        log_structured(
            "info",
            "scenario_run_updated",
            run_id=run_id,
            tenant_id=tenant_id,
            state_changes=list(state_update.keys()),
        )

        return await self.get_run("", run_id)  # scenario_id not needed here

    async def cancel_run(self, run_id: str) -> Optional[ScenarioRunData]:
        """Cancel a scenario run."""
        return await self.update_run_state(run_id, {
            "status": "cancelled",
            "cancelled_at": datetime.utcnow(),
        })

    # === SCENARIO OUTPUT OPERATIONS ===

    async def create_output(
        self,
        scenario_run_id: str,
        output_data: Dict[str, Any]
    ) -> None:
        """Create scenario output data point."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_output (
                    scenario_run_id, tenant_id, timestamp, metric_name,
                    value, unit, tags, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
                scenario_run_id,
                tenant_id,
                output_data["timestamp"],
                output_data["metric_name"],
                output_data["value"],
                output_data["unit"],
                json.dumps(output_data.get("tags", {})),
                datetime.utcnow(),
            )

    async def get_outputs(
        self,
        scenario_run_id: str,
        limit: int = 100,
        offset: int = 0,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        metric_name: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Tuple[List[ScenarioOutputPoint], int]:
        """Get scenario outputs with filtering."""
        tenant_id = get_tenant_id()

        conditions = ["so.scenario_run_id = $1", "so.tenant_id = $2"]
        params = [scenario_run_id, tenant_id]
        param_index = 3

        if start_time:
            conditions.append(f"so.timestamp >= ${param_index}")
            params.append(start_time)
            param_index += 1

        if end_time:
            conditions.append(f"so.timestamp <= ${param_index}")
            params.append(end_time)
            param_index += 1

        if metric_name:
            conditions.append(f"so.metric_name = ${param_index}")
            params.append(metric_name)
            param_index += 1

        if min_value is not None:
            conditions.append(f"so.value >= ${param_index}")
            params.append(min_value)
            param_index += 1

        if max_value is not None:
            conditions.append(f"so.value <= ${param_index}")
            params.append(max_value)
            param_index += 1

        if tags:
            for key, value in tags.items():
                conditions.append(f"so.tags->>${param_index} = ${param_index + 1}")
                params.extend([key, value])
                param_index += 2

        where_clause = " AND ".join(conditions)

        async with self.get_connection() as conn:
            # Get total count
            count_row = await conn.fetchrow(f"""
                SELECT COUNT(*) as total
                FROM scenario_output so
                WHERE {where_clause}
            """, *params)

            # Get paginated results
            rows = await conn.fetch(f"""
                SELECT so.id, so.scenario_run_id, so.tenant_id, so.timestamp,
                       so.metric_name, so.value, so.unit, so.tags, so.created_at
                FROM scenario_output so
                WHERE {where_clause}
                ORDER BY so.timestamp ASC
                LIMIT ${param_index} OFFSET ${param_index + 1}
            """, *params, limit, offset)

        outputs = []
        for row in rows:
            outputs.append(ScenarioOutputPoint(
                timestamp=row["timestamp"],
                metric_name=row["metric_name"],
                value=row["value"],
                unit=row["unit"],
                tags=json.loads(row["tags"]),
            ))

        total = count_row["total"]
        return outputs, total

    # === FEATURE FLAG OPERATIONS ===

    async def get_feature_flag(
        self,
        feature_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get feature flag for current tenant."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT feature_name, enabled, configuration, created_at, updated_at
                FROM scenario_feature_flag
                WHERE tenant_id = $1 AND feature_name = $2
            """, tenant_id, feature_name)

        if not row:
            return None

        return {
            "feature_name": row["feature_name"],
            "enabled": row["enabled"],
            "configuration": json.loads(row["configuration"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def set_feature_flag(
        self,
        feature_name: str,
        enabled: bool,
        configuration: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Set feature flag for current tenant."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_feature_flag (tenant_id, feature_name, enabled, configuration, updated_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (tenant_id, feature_name) DO UPDATE
                SET enabled = EXCLUDED.enabled,
                    configuration = EXCLUDED.configuration,
                    updated_at = EXCLUDED.updated_at
            """,
                tenant_id,
                feature_name,
                enabled,
                json.dumps(configuration or {}),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "feature_flag_updated",
            tenant_id=tenant_id,
            feature_name=feature_name,
            enabled=enabled,
        )

        return await self.get_feature_flag(feature_name) or {}

    async def list_feature_flags(
        self,
        tenant_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all feature flags for a tenant."""
        current_tenant_id = get_tenant_id() if tenant_id is None else tenant_id

        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT feature_name, enabled, configuration, created_at, updated_at
                FROM scenario_feature_flag
                WHERE tenant_id = $1
                ORDER BY feature_name
            """, current_tenant_id)

        return [{
            "feature_name": row["feature_name"],
            "enabled": row["enabled"],
            "configuration": json.loads(row["configuration"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        } for row in rows]

    # === RATE LIMIT OVERRIDE OPERATIONS ===

    async def get_rate_limit_override(
        self,
        path_prefix: str,
        tenant_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get rate limit override for a path prefix."""
        current_tenant_id = get_tenant_id() if tenant_id is None else tenant_id

        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT path_prefix, requests_per_second, burst_capacity, daily_cap, enabled, created_at, updated_at
                FROM rate_limit_override
                WHERE tenant_id = $1 AND path_prefix = $2
            """, current_tenant_id, path_prefix)

        if not row:
            return None

        return {
            "path_prefix": row["path_prefix"],
            "requests_per_second": row["requests_per_second"],
            "burst_capacity": row["burst_capacity"],
            "daily_cap": row["daily_cap"],
            "enabled": row["enabled"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def set_rate_limit_override(
        self,
        path_prefix: str,
        requests_per_second: int,
        burst_capacity: int,
        daily_cap: int = 100000,
        enabled: bool = True,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Set rate limit override for a path prefix."""
        current_tenant_id = get_tenant_id() if tenant_id is None else tenant_id

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO rate_limit_override (
                    tenant_id, path_prefix, requests_per_second, burst_capacity, daily_cap, enabled, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (tenant_id, path_prefix) DO UPDATE
                SET requests_per_second = EXCLUDED.requests_per_second,
                    burst_capacity = EXCLUDED.burst_capacity,
                    daily_cap = EXCLUDED.daily_cap,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
            """,
                current_tenant_id,
                path_prefix,
                requests_per_second,
                burst_capacity,
                daily_cap,
                enabled,
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "rate_limit_override_updated",
            tenant_id=current_tenant_id,
            path_prefix=path_prefix,
            requests_per_second=requests_per_second,
            burst_capacity=burst_capacity,
            enabled=enabled,
        )

        return await self.get_rate_limit_override(path_prefix, current_tenant_id) or {}

    async def list_rate_limit_overrides(
        self,
        tenant_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all rate limit overrides for a tenant."""
        current_tenant_id = get_tenant_id() if tenant_id is None else tenant_id

        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT path_prefix, requests_per_second, burst_capacity, daily_cap, enabled, created_at, updated_at
                FROM rate_limit_override
                WHERE tenant_id = $1
                ORDER BY path_prefix
            """, current_tenant_id)

        return [{
            "path_prefix": row["path_prefix"],
            "requests_per_second": row["requests_per_second"],
            "burst_capacity": row["burst_capacity"],
            "daily_cap": row["daily_cap"],
            "enabled": row["enabled"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        } for row in rows]

    # === SCHEMA V2 OPERATIONS ===

    # Curve Family Operations

    async def create_curve_family(self, family_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new curve family."""
        family_id = str(uuid4())
        user_id = get_user_id()

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO curve_family (
                    id, name, description, family_type, curve_keys,
                    default_parameters, validation_rules, is_active,
                    created_by, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
                family_id,
                family_data["name"],
                family_data.get("description"),
                family_data["family_type"],
                json.dumps(family_data.get("curve_keys", [])),
                json.dumps(family_data.get("default_parameters", {})),
                json.dumps(family_data.get("validation_rules", {})),
                family_data.get("is_active", True),
                user_id,
                datetime.utcnow(),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "curve_family_created",
            family_id=family_id,
            user_id=user_id,
            family_name=family_data["name"],
        )

        return await self.get_curve_family(family_id)

    async def get_curve_family(self, family_id: str) -> Optional[Dict[str, Any]]:
        """Get curve family by ID."""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, name, description, family_type, curve_keys,
                       default_parameters, validation_rules, is_active,
                       created_by, created_at, updated_by, updated_at
                FROM curve_family
                WHERE id = $1
            """, family_id)

        if not row:
            return None

        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "family_type": row["family_type"],
            "curve_keys": json.loads(row["curve_keys"]),
            "default_parameters": json.loads(row["default_parameters"]),
            "validation_rules": json.loads(row["validation_rules"]),
            "is_active": row["is_active"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_by": row["updated_by"],
            "updated_at": row["updated_at"],
        }

    async def list_curve_families(
        self,
        family_type: Optional[str] = None,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List curve families with optional filtering."""
        query = """
            SELECT id, name, description, family_type, curve_keys,
                   default_parameters, validation_rules, is_active,
                   created_by, created_at, updated_by, updated_at
            FROM curve_family
            WHERE 1=1
        """
        params = []

        if family_type:
            query += " AND family_type = $1"
            params.append(family_type)

        if active_only:
            query += " AND is_active = TRUE"

        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1) + " OFFSET $" + str(len(params) + 2)
        params.extend([limit, offset])

        async with self.get_connection() as conn:
            rows = await conn.fetch(query, *params)

        return [{
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "family_type": row["family_type"],
            "curve_keys": json.loads(row["curve_keys"]),
            "default_parameters": json.loads(row["default_parameters"]),
            "validation_rules": json.loads(row["validation_rules"]),
            "is_active": row["is_active"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_by": row["updated_by"],
            "updated_at": row["updated_at"],
        } for row in rows]

    # Scenario Constraint Operations

    async def create_scenario_constraint(
        self,
        scenario_id: str,
        constraint_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a constraint for a scenario."""
        constraint_id = str(uuid4())
        user_id = get_user_id()
        tenant_id = get_tenant_id()

        # Verify scenario exists and belongs to tenant
        scenario = await self.get_scenario(scenario_id)
        if not scenario or str(scenario.tenant_id) != tenant_id:
            raise ValueError(f"Scenario {scenario_id} not found or not accessible")

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_constraint (
                    id, scenario_id, constraint_type, constraint_key, constraint_value,
                    operator, severity, is_enforced, violation_message,
                    created_by, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
                constraint_id,
                scenario_id,
                constraint_data["constraint_type"],
                constraint_data["constraint_key"],
                json.dumps(constraint_data["constraint_value"]),
                constraint_data["operator"],
                constraint_data.get("severity", "warning"),
                constraint_data.get("is_enforced", False),
                constraint_data.get("violation_message"),
                user_id,
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "scenario_constraint_created",
            constraint_id=constraint_id,
            scenario_id=scenario_id,
            user_id=user_id,
        )

        return await self.get_scenario_constraint(constraint_id)

    async def get_scenario_constraint(self, constraint_id: str) -> Optional[Dict[str, Any]]:
        """Get scenario constraint by ID."""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, scenario_id, constraint_type, constraint_key, constraint_value,
                       operator, severity, is_enforced, violation_message,
                       created_by, created_at
                FROM scenario_constraint
                WHERE id = $1
            """, constraint_id)

        if not row:
            return None

        return {
            "id": row["id"],
            "scenario_id": row["scenario_id"],
            "constraint_type": row["constraint_type"],
            "constraint_key": row["constraint_key"],
            "constraint_value": json.loads(row["constraint_value"]),
            "operator": row["operator"],
            "severity": row["severity"],
            "is_enforced": row["is_enforced"],
            "violation_message": row["violation_message"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    async def list_scenario_constraints(self, scenario_id: str) -> List[Dict[str, Any]]:
        """List constraints for a scenario."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, scenario_id, constraint_type, constraint_key, constraint_value,
                       operator, severity, is_enforced, violation_message,
                       created_by, created_at
                FROM scenario_constraint
                WHERE scenario_id = $1
                ORDER BY created_at DESC
            """, scenario_id)

        return [{
            "id": row["id"],
            "scenario_id": row["scenario_id"],
            "constraint_type": row["constraint_type"],
            "constraint_key": row["constraint_key"],
            "constraint_value": json.loads(row["constraint_value"]),
            "operator": row["operator"],
            "severity": row["severity"],
            "is_enforced": row["is_enforced"],
            "violation_message": row["violation_message"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        } for row in rows]

    # Scenario Provenance Operations

    async def create_scenario_provenance(
        self,
        scenario_id: str,
        provenance_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create provenance record for a scenario."""
        provenance_id = str(uuid4())
        tenant_id = get_tenant_id()

        # Verify scenario exists and belongs to tenant
        scenario = await self.get_scenario(scenario_id)
        if not scenario or str(scenario.tenant_id) != tenant_id:
            raise ValueError(f"Scenario {scenario_id} not found or not accessible")

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_provenance (
                    id, scenario_id, data_source, source_version, data_timestamp,
                    transformation_hash, input_parameters, quality_metrics,
                    lineage_metadata, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                provenance_id,
                scenario_id,
                provenance_data["data_source"],
                provenance_data.get("source_version"),
                provenance_data["data_timestamp"],
                provenance_data["transformation_hash"],
                json.dumps(provenance_data.get("input_parameters", {})),
                json.dumps(provenance_data.get("quality_metrics", {})),
                json.dumps(provenance_data.get("lineage_metadata", {})),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "scenario_provenance_created",
            provenance_id=provenance_id,
            scenario_id=scenario_id,
        )

        return await self.get_scenario_provenance(provenance_id)

    async def get_scenario_provenance(self, provenance_id: str) -> Optional[Dict[str, Any]]:
        """Get scenario provenance by ID."""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, scenario_id, data_source, source_version, data_timestamp,
                       transformation_hash, input_parameters, quality_metrics,
                       lineage_metadata, created_at
                FROM scenario_provenance
                WHERE id = $1
            """, provenance_id)

        if not row:
            return None

        return {
            "id": row["id"],
            "scenario_id": row["scenario_id"],
            "data_source": row["data_source"],
            "source_version": row["source_version"],
            "data_timestamp": row["data_timestamp"],
            "transformation_hash": row["transformation_hash"],
            "input_parameters": json.loads(row["input_parameters"]),
            "quality_metrics": json.loads(row["quality_metrics"]),
            "lineage_metadata": json.loads(row["lineage_metadata"]),
            "created_at": row["created_at"],
        }

    async def list_scenario_provenance(self, scenario_id: str) -> List[Dict[str, Any]]:
        """List provenance records for a scenario."""
        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, scenario_id, data_source, source_version, data_timestamp,
                       transformation_hash, input_parameters, quality_metrics,
                       lineage_metadata, created_at
                FROM scenario_provenance
                WHERE scenario_id = $1
                ORDER BY created_at DESC
            """, scenario_id)

        return [{
            "id": row["id"],
            "scenario_id": row["scenario_id"],
            "data_source": row["data_source"],
            "source_version": row["source_version"],
            "data_timestamp": row["data_timestamp"],
            "transformation_hash": row["transformation_hash"],
            "input_parameters": json.loads(row["input_parameters"]),
            "quality_metrics": json.loads(row["quality_metrics"]),
            "lineage_metadata": json.loads(row["lineage_metadata"]),
            "created_at": row["created_at"],
        } for row in rows]

    # Scenario Curve Family Association Operations

    async def associate_scenario_curve_family(
        self,
        scenario_id: str,
        curve_family_id: str,
        weight: float = 1.0,
        is_primary: bool = False,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Associate a curve family with a scenario."""
        association_id = str(uuid4())
        tenant_id = get_tenant_id()

        # Verify scenario exists and belongs to tenant
        scenario = await self.get_scenario(scenario_id)
        if not scenario or str(scenario.tenant_id) != tenant_id:
            raise ValueError(f"Scenario {scenario_id} not found or not accessible")

        # Verify curve family exists
        curve_family = await self.get_curve_family(curve_family_id)
        if not curve_family:
            raise ValueError(f"Curve family {curve_family_id} not found")

        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO scenario_curve_family (
                    scenario_id, curve_family_id, weight, is_primary, parameters, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
                scenario_id,
                curve_family_id,
                weight,
                is_primary,
                json.dumps(parameters or {}),
                datetime.utcnow(),
            )

        log_structured(
            "info",
            "scenario_curve_family_associated",
            scenario_id=scenario_id,
            curve_family_id=curve_family_id,
            weight=weight,
            is_primary=is_primary,
        )

        return await self.get_scenario_curve_family_association(scenario_id, curve_family_id)

    async def get_scenario_curve_family_association(
        self,
        scenario_id: str,
        curve_family_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get scenario-curve family association."""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT scenario_id, curve_family_id, weight, is_primary, parameters, created_at
                FROM scenario_curve_family
                WHERE scenario_id = $1 AND curve_family_id = $2
            """, scenario_id, curve_family_id)

        if not row:
            return None

        return {
            "scenario_id": row["scenario_id"],
            "curve_family_id": row["curve_family_id"],
            "weight": row["weight"],
            "is_primary": row["is_primary"],
            "parameters": json.loads(row["parameters"]),
            "created_at": row["created_at"],
        }

    async def list_scenario_curve_families(self, scenario_id: str) -> List[Dict[str, Any]]:
        """List curve families associated with a scenario."""
        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT scf.scenario_id, scf.curve_family_id, scf.weight, scf.is_primary,
                       scf.parameters, scf.created_at, cf.name, cf.family_type
                FROM scenario_curve_family scf
                JOIN curve_family cf ON scf.curve_family_id = cf.id
                WHERE scf.scenario_id = $1
                ORDER BY scf.is_primary DESC, scf.weight DESC
            """, scenario_id)

        return [{
            "scenario_id": row["scenario_id"],
            "curve_family_id": row["curve_family_id"],
            "curve_family_name": row["name"],
            "family_type": row["family_type"],
            "weight": row["weight"],
            "is_primary": row["is_primary"],
            "parameters": json.loads(row["parameters"]),
            "created_at": row["created_at"],
        } for row in rows]


# Global scenario store instance
_scenario_store: Optional[ScenarioStore] = None


def get_scenario_store() -> ScenarioStore:
    """Get the global scenario store instance."""
    if _scenario_store is None:
        raise RuntimeError("ScenarioStore not initialized")
    return _scenario_store


async def initialize_scenario_store(postgres_dsn: str) -> ScenarioStore:
    """Initialize the global scenario store."""
    global _scenario_store
    if _scenario_store is None:
        _scenario_store = ScenarioStore(postgres_dsn)
        await _scenario_store.initialize()
    return _scenario_store


async def close_scenario_store() -> None:
    """Close the global scenario store."""
    global _scenario_store
    if _scenario_store is not None:
        await _scenario_store.close()
        _scenario_store = None
