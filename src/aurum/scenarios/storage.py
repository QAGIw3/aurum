"""Database persistence layer for scenario management."""

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
        """Get scenario by ID."""
        tenant_id = get_tenant_id()

        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, tenant_id, name, description, status, assumptions,
                       parameters, tags, version, created_by, created_at, updated_at
                FROM scenario
                WHERE id = $1 AND tenant_id = $2
            """, scenario_id, tenant_id)

        if not row:
            return None

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
