"""Persistence layer for auto-reforecast triggers and jobs using TimescaleDB."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from aurum.core import AurumSettings
from aurum.data.dao.timescale import TimescaleDAO
from aurum.api.services.auto_reforecast_shim import (
    ForecastTrigger,
    TriggerCondition,
    TriggerEvent,
    ReforcastJob,
    ForecastRequest,
)


class AutoReforecastRepository:
    """Timescale-backed persistence for auto-reforecast triggers."""

    def __init__(self, dao: TimescaleDAO):
        self._dao = dao

    async def list_triggers(self, tenant_id: UUID) -> List[ForecastTrigger]:
        query = """
        SELECT
            trigger_id,
            name,
            description,
            conditions,
            forecast_config,
            cooldown_minutes,
            priority,
            enabled,
            last_triggered,
            trigger_count
        FROM auto_reforecast_trigger
        WHERE tenant_id = %(tenant_id)s
        ORDER BY created_at DESC
        """
        rows = await self._dao.execute_query(query, {"tenant_id": tenant_id})
        return [self._deserialize_trigger(row) for row in rows]

    async def get_trigger(self, tenant_id: UUID, trigger_id: UUID) -> Optional[ForecastTrigger]:
        query = """
        SELECT
            trigger_id,
            name,
            description,
            conditions,
            forecast_config,
            cooldown_minutes,
            priority,
            enabled,
            last_triggered,
            trigger_count
        FROM auto_reforecast_trigger
        WHERE tenant_id = %(tenant_id)s
          AND trigger_id = %(trigger_id)s
        LIMIT 1
        """
        params = {"tenant_id": tenant_id, "trigger_id": trigger_id}
        rows = await self._dao.execute_query(query, params)
        if not rows:
            return None
        return self._deserialize_trigger(rows[0])

    async def create_trigger(
        self,
        tenant_id: UUID,
        trigger: ForecastTrigger,
    ) -> ForecastTrigger:
        query = """
        INSERT INTO auto_reforecast_trigger (
            trigger_id,
            tenant_id,
            name,
            description,
            conditions,
            forecast_config,
            cooldown_minutes,
            priority,
            enabled,
            last_triggered,
            trigger_count
        ) VALUES (
            %(trigger_id)s,
            %(tenant_id)s,
            %(name)s,
            %(description)s,
            %(conditions)s,
            %(forecast_config)s,
            %(cooldown_minutes)s,
            %(priority)s,
            %(enabled)s,
            %(last_triggered)s,
            %(trigger_count)s
        )
        RETURNING trigger_id,
                  name,
                  description,
                  conditions,
                  forecast_config,
                  cooldown_minutes,
                  priority,
                  enabled,
                  last_triggered,
                  trigger_count
        """
        params = self._serialize_trigger(tenant_id, trigger)
        rows = await self._dao.execute_query(query, params)
        return self._deserialize_trigger(rows[0])

    async def update_trigger(
        self,
        tenant_id: UUID,
        trigger: ForecastTrigger,
    ) -> ForecastTrigger:
        query = """
        UPDATE auto_reforecast_trigger
        SET
            name = %(name)s,
            description = %(description)s,
            conditions = %(conditions)s,
            forecast_config = %(forecast_config)s,
            cooldown_minutes = %(cooldown_minutes)s,
            priority = %(priority)s,
            enabled = %(enabled)s,
            last_triggered = %(last_triggered)s,
            trigger_count = %(trigger_count)s
        WHERE tenant_id = %(tenant_id)s
          AND trigger_id = %(trigger_id)s
        RETURNING trigger_id,
                  name,
                  description,
                  conditions,
                  forecast_config,
                  cooldown_minutes,
                  priority,
                  enabled,
                  last_triggered,
                  trigger_count
        """
        params = self._serialize_trigger(tenant_id, trigger)
        rows = await self._dao.execute_query(query, params)
        if not rows:
            raise ValueError(f"Trigger {trigger.trigger_id} not found")
        return self._deserialize_trigger(rows[0])

    async def delete_trigger(self, tenant_id: UUID, trigger_id: UUID) -> bool:
        query = """
        DELETE FROM auto_reforecast_trigger
        WHERE tenant_id = %(tenant_id)s
          AND trigger_id = %(trigger_id)s
        RETURNING trigger_id
        """
        params = {"tenant_id": tenant_id, "trigger_id": trigger_id}
        rows = await self._dao.execute_query(query, params)
        return bool(rows)

    def _serialize_trigger(
        self,
        tenant_id: UUID,
        trigger: ForecastTrigger,
    ) -> Dict[str, Any]:
        return {
            "trigger_id": UUID(trigger.trigger_id) if isinstance(trigger.trigger_id, str) else trigger.trigger_id,
            "tenant_id": tenant_id,
            "name": trigger.name,
            "description": trigger.description,
            "conditions": json.dumps([cond.model_dump() for cond in trigger.conditions]),
            "forecast_config": json.dumps(trigger.forecast_config.model_dump()),
            "cooldown_minutes": trigger.cooldown_minutes,
            "priority": trigger.priority,
            "enabled": trigger.enabled,
            "last_triggered": trigger.last_triggered,
            "trigger_count": trigger.trigger_count,
        }

    def _deserialize_trigger(self, row: Dict[str, Any]) -> ForecastTrigger:
        raw_conditions = row["conditions"]
        conditions = json.loads(raw_conditions) if isinstance(raw_conditions, str) else raw_conditions
        forecast_data = row["forecast_config"]
        if isinstance(forecast_data, str):
            forecast_data = json.loads(forecast_data)

        trigger = ForecastTrigger(
            trigger_id=str(row["trigger_id"]),
            name=row["name"],
            description=row["description"],
            conditions=[TriggerCondition(**cond) for cond in conditions],
            forecast_config=ForecastRequest(**forecast_data),
            priority=float(row["priority"]),
            cooldown_minutes=row["cooldown_minutes"],
            enabled=row["enabled"],
            last_triggered=row.get("last_triggered"),
        )
        trigger.trigger_count = row.get("trigger_count", 0) or 0
        return trigger


class AutoReforecastJobRepository:
    """Timescale-backed persistence for reforecast jobs."""

    def __init__(self, dao: TimescaleDAO):
        self._dao = dao

    async def enqueue_job(
        self,
        tenant_id: UUID,
        job: ReforcastJob,
    ) -> ReforcastJob:
        query = """
        INSERT INTO auto_reforecast_job (
            job_id,
            tenant_id,
            trigger_id,
            event_id,
            status,
            priority,
            scheduled_for,
            started_at,
            completed_at,
            attempts,
            max_attempts,
            error_message,
            trigger_event,
            forecast_config,
            metrics,
            created_at
        ) VALUES (
            %(job_id)s,
            %(tenant_id)s,
            %(trigger_id)s,
            %(event_id)s,
            %(status)s,
            %(priority)s,
            %(scheduled_for)s,
            %(started_at)s,
            %(completed_at)s,
            %(attempts)s,
            %(max_attempts)s,
            %(error_message)s,
            %(trigger_event)s,
            %(forecast_config)s,
            %(metrics)s,
            %(created_at)s
        )
        RETURNING job_id,
                  trigger_id,
                  event_id,
                  status,
                  priority,
                  scheduled_for,
                  started_at,
                  completed_at,
                  attempts,
                  max_attempts,
                  error_message,
                  trigger_event,
                  forecast_config,
                  metrics,
                  created_at
        """
        params = self._serialize_job(tenant_id, job)
        rows = await self._dao.execute_query(query, params)
        return self._deserialize_job(rows[0])

    async def update_status(
        self,
        tenant_id: UUID,
        job_id: UUID,
        *,
        status: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        attempts: Optional[int] = None,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Optional[ReforcastJob]:
        query = """
        UPDATE auto_reforecast_job
        SET
            status = COALESCE(%(status)s, status),
            started_at = COALESCE(%(started_at)s, started_at),
            completed_at = COALESCE(%(completed_at)s, completed_at),
            attempts = COALESCE(%(attempts)s, attempts),
            error_message = COALESCE(%(error_message)s, error_message),
            metrics = COALESCE(%(metrics)s, metrics)
        WHERE tenant_id = %(tenant_id)s
          AND job_id = %(job_id)s
        RETURNING job_id,
                  trigger_id,
                  event_id,
                  status,
                  priority,
                  scheduled_for,
                  started_at,
                  completed_at,
                  attempts,
                  max_attempts,
                  error_message,
                  trigger_event,
                  forecast_config,
                  metrics,
                  created_at
        """
        params = {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "attempts": attempts,
            "error_message": error_message,
            "metrics": json.dumps(metrics) if metrics is not None else None,
        }
        rows = await self._dao.execute_query(query, params)
        if not rows:
            return None
        return self._deserialize_job(rows[0])

    async def list_jobs(
        self,
        tenant_id: UUID,
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ReforcastJob]:
        query = """
        SELECT
            job_id,
            trigger_id,
            event_id,
            status,
            priority,
            scheduled_for,
            started_at,
            completed_at,
            attempts,
            max_attempts,
            error_message,
            trigger_event,
            forecast_config,
            metrics,
            created_at
        FROM auto_reforecast_job
        WHERE tenant_id = %(tenant_id)s
          AND (%(status)s IS NULL OR status = %(status)s)
        ORDER BY scheduled_for DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """
        params = {
            "tenant_id": tenant_id,
            "status": status,
            "limit": limit,
            "offset": offset,
        }
        rows = await self._dao.execute_query(query, params)
        return [self._deserialize_job(row) for row in rows]

    def _serialize_job(
        self,
        tenant_id: UUID,
        job: ReforcastJob,
    ) -> Dict[str, Any]:
        trigger_event = job.trigger_event.model_dump()
        forecast_config = job.forecast_request.model_dump()
        return {
            "job_id": UUID(job.job_id) if isinstance(job.job_id, str) else job.job_id,
            "tenant_id": tenant_id,
            "trigger_id": UUID(job.trigger_event.trigger_id) if isinstance(job.trigger_event.trigger_id, str) else job.trigger_event.trigger_id,
            "event_id": UUID(job.trigger_event.event_id) if isinstance(job.trigger_event.event_id, str) else job.trigger_event.event_id,
            "status": job.status,
            "priority": job.priority,
            "scheduled_for": job.scheduled_for,
            "started_at": None,
            "completed_at": None,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "error_message": job.error_message,
            "trigger_event": json.dumps(trigger_event),
            "forecast_config": json.dumps(forecast_config),
            "metrics": json.dumps({}),
            "created_at": job.created_at,
        }

    def _deserialize_job(self, row: Dict[str, Any]) -> ReforcastJob:
        trigger_event_data = row["trigger_event"]
        if isinstance(trigger_event_data, str):
            trigger_event_data = json.loads(trigger_event_data)
        forecast_config_data = row["forecast_config"]
        if isinstance(forecast_config_data, str):
            forecast_config_data = json.loads(forecast_config_data)

        trigger_event = TriggerEvent(**trigger_event_data)
        job = ReforcastJob(
            job_id=str(row["job_id"]),
            trigger_event=trigger_event,
            forecast_request=ForecastRequest(**forecast_config_data),
            priority=float(row["priority"]),
            created_at=row["created_at"],
            scheduled_for=row["scheduled_for"],
        )
        job.status = row["status"]
        job.attempts = row.get("attempts", 0) or 0
        job.max_attempts = row.get("max_attempts", 3) or 3
        job.error_message = row.get("error_message")
        return job


_auto_reforecast_repository: Optional[AutoReforecastRepository] = None
_auto_reforecast_job_repository: Optional[AutoReforecastJobRepository] = None


def get_auto_reforecast_repository(settings: Optional[AurumSettings] = None) -> AutoReforecastRepository:
    global _auto_reforecast_repository
    if _auto_reforecast_repository is None:
        dao = TimescaleDAO(settings=settings)
        _auto_reforecast_repository = AutoReforecastRepository(dao)
    return _auto_reforecast_repository


def get_auto_reforecast_job_repository(settings: Optional[AurumSettings] = None) -> AutoReforecastJobRepository:
    global _auto_reforecast_job_repository
    if _auto_reforecast_job_repository is None:
        dao = TimescaleDAO(settings=settings)
        _auto_reforecast_job_repository = AutoReforecastJobRepository(dao)
    return _auto_reforecast_job_repository
