"""Model Scheduling Service - Handles automated model retraining schedules."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4

try:
    from aurum.telemetry.context import get_request_id, get_tenant_id, get_user_id
    from aurum.observability.telemetry_facade import get_telemetry_facade
except ImportError:
    # Fallback for demo
    def get_telemetry_facade():
        class MockTelemetry:
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
        return MockTelemetry()
    def get_request_id(): return "demo-request"
    def get_tenant_id(): return "demo-tenant"
    def get_user_id(): return "demo-user"
from .models import RetrainSchedule, ModelConfig
from .interfaces import IModelSchedulingService


class ModelSchedulingService(IModelSchedulingService):
    """Service for managing model retraining schedules."""

    def __init__(self, training_service=None):
        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()
        self.training_service = training_service
        self.schedules: Dict[str, RetrainSchedule] = {}
        self.scheduler_task: Optional[asyncio.Task] = None
        self._last_scheduler_heartbeat = None

    async def start(self) -> None:
        """Start the scheduling service."""
        if self.scheduler_task is None or self.scheduler_task.done():
            self.scheduler_task = asyncio.create_task(self._scheduler_loop())
            self.telemetry.info("model_scheduling.scheduler_started")

    async def stop(self) -> None:
        """Stop the scheduling service."""
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
            self.telemetry.info("model_scheduling.scheduler_stopped")

    async def create_retrain_schedule(
        self,
        model_name: str,
        cron_expression: str,
        config: ModelConfig,
        created_by: str
    ) -> RetrainSchedule:
        """Create a new retraining schedule."""
        try:
            schedule_id = str(uuid4())

            # Calculate next run time (simplified - would use cron parser in real implementation)
            next_run = datetime.utcnow() + timedelta(days=1)  # Daily for demo

            schedule = RetrainSchedule(
                schedule_id=schedule_id,
                model_name=model_name,
                cron_expression=cron_expression,
                enabled=True,
                next_run=next_run,
                config=config,
                created_by=created_by
            )

            self.schedules[schedule_id] = schedule

            self.telemetry.info(
                "model_scheduling.schedule_created",
                schedule_id=schedule_id,
                model_name=model_name,
                cron_expression=cron_expression,
                created_by=created_by
            )

            return schedule

        except Exception as exc:
            self.telemetry.error("Failed to create retrain schedule", error=str(exc))
            raise

    async def update_retrain_schedule(
        self,
        schedule_id: str,
        enabled: Optional[bool] = None,
        cron_expression: Optional[str] = None,
        config: Optional[ModelConfig] = None,
        updated_by: str = "system"
    ) -> bool:
        """Update an existing retraining schedule."""
        try:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                return False

            # Update fields
            if enabled is not None:
                schedule.enabled = enabled

            if cron_expression is not None:
                schedule.cron_expression = cron_expression
                # Recalculate next run (simplified)
                schedule.next_run = datetime.utcnow() + timedelta(days=1)

            if config is not None:
                schedule.config = config

            # Update metadata
            schedule.metadata["last_updated"] = datetime.utcnow().isoformat()
            schedule.metadata["updated_by"] = updated_by

            self.telemetry.info(
                "model_scheduling.schedule_updated",
                schedule_id=schedule_id,
                enabled=schedule.enabled,
                cron_expression=schedule.cron_expression,
                updated_by=updated_by
            )

            return True

        except Exception as exc:
            self.telemetry.error("Failed to update retrain schedule", error=str(exc))
            return False

    async def delete_retrain_schedule(
        self,
        schedule_id: str,
        deleted_by: str
    ) -> bool:
        """Delete a retraining schedule."""
        try:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                return False

            # Store deletion metadata before removing
            deletion_info = {
                "deleted_at": datetime.utcnow().isoformat(),
                "deleted_by": deleted_by,
                "model_name": schedule.model_name,
                "cron_expression": schedule.cron_expression,
            }

            # Remove schedule
            del self.schedules[schedule_id]

            self.telemetry.info(
                "model_scheduling.schedule_deleted",
                schedule_id=schedule_id,
                model_name=schedule.model_name,
                deleted_by=deleted_by
            )

            return True

        except Exception as exc:
            self.telemetry.error("Failed to delete retrain schedule", error=str(exc))
            return False

    async def list_retrain_schedules(
        self,
        model_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RetrainSchedule]:
        """List retraining schedules with optional filtering."""
        schedules = list(self.schedules.values())

        # Apply filters
        if model_name:
            schedules = [s for s in schedules if s.model_name == model_name]

        if enabled is not None:
            schedules = [s for s in schedules if s.enabled == enabled]

        # Sort by next run time
        schedules.sort(key=lambda s: (s.next_run or datetime.max, s.created_at))

        # Apply pagination
        start = min(offset, len(schedules))
        end = min(start + limit, len(schedules))

        return schedules[start:end]

    async def trigger_scheduled_retrain(
        self,
        schedule_id: str,
        triggered_by: str = "scheduler"
    ) -> Optional[str]:
        """Trigger a scheduled retrain manually."""
        try:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                return None

            if not schedule.enabled:
                self.telemetry.warning(
                    "model_scheduling.schedule_disabled",
                    schedule_id=schedule_id,
                    triggered_by=triggered_by
                )
                return None

            # Update last run time
            schedule.last_run = datetime.utcnow()

            # Trigger training job
            if self.training_service:
                job_id = await self.training_service.start_training_job(
                    model_name=schedule.model_name,
                    config=schedule.config,
                    created_by=triggered_by
                )

                self.telemetry.info(
                    "model_scheduling.retrain_triggered",
                    schedule_id=schedule_id,
                    job_id=job_id,
                    model_name=schedule.model_name,
                    triggered_by=triggered_by
                )

                return job_id
            else:
                self.telemetry.warning(
                    "model_scheduling.no_training_service",
                    schedule_id=schedule_id
                )
                return None

        except Exception as exc:
            self.telemetry.error("Failed to trigger scheduled retrain", error=str(exc))
            return None

    async def _scheduler_loop(self) -> None:
        """Background task for processing scheduled retrains."""
        try:
            while True:
                now = datetime.utcnow()

                # Update heartbeat
                self._last_scheduler_heartbeat = now

                # Check all enabled schedules
                for schedule_id, schedule in list(self.schedules.items()):
                    try:
                        if not schedule.enabled:
                            continue

                        # Check if it's time to run (simplified - would use proper cron parsing)
                        if schedule.next_run and now >= schedule.next_run:
                            # Trigger the retrain
                            await self.trigger_scheduled_retrain(schedule_id, "scheduler")

                            # Update next run time (simplified - would use cron parser)
                            schedule.next_run = now + timedelta(days=1)

                    except Exception as exc:
                        self.telemetry.error(
                            "model_scheduling.schedule_processing_error",
                            schedule_id=schedule_id,
                            error=str(exc)
                        )

                # Wait for next check (every minute for demo)
                await asyncio.sleep(60)

        except asyncio.CancelledError:
            self.telemetry.info("model_scheduling.scheduler_cancelled")
            raise
        except Exception as exc:
            self.telemetry.error("model_scheduling.scheduler_error", error=str(exc))
            raise

    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status information."""
        return {
            "scheduler_state": "running" if self.scheduler_task and not self.scheduler_task.done() else "stopped",
            "active_schedules": len([s for s in self.schedules.values() if s.enabled]),
            "total_schedules": len(self.schedules),
            "last_heartbeat": self._last_scheduler_heartbeat,
        }

    async def health_check(self) -> bool:
        """Health check for the model scheduling service."""
        try:
            # Check if we can access the schedules dictionary
            if not hasattr(self, 'schedules') or self.schedules is None:
                return False

            # Check if training service is available (if configured)
            if hasattr(self, 'training_service') and self.training_service is None:
                return False

            # Check scheduler task status
            if self.scheduler_task and self.scheduler_task.done():
                # Task completed (likely with error)
                if not self.scheduler_task.cancelled():
                    return False

            return True

        except Exception as exc:
            self.logger.error(f"Health check failed: {exc}")
            return False

    def get_service_health(self) -> Dict[str, Any]:
        """Get detailed health information for the service."""
        scheduler_status = self.get_scheduler_status()

        return {
            "healthy": True,  # Would be determined by health_check()
            "service_name": "ModelSchedulingService",
            "schedules_count": len(self.schedules),
            "training_service_available": self.training_service is not None,
            "scheduler_status": scheduler_status,
            "last_health_check": datetime.utcnow().isoformat()
        }
