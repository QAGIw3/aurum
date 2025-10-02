"""Retrain Scheduler Service.

This service handles scheduled model retraining, including cron-based
scheduling, trigger management, and retraining job execution.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from uuid import uuid4
from croniter import croniter

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class RetrainSchedule(BaseModel):
    """Schedule configuration for model retraining."""
    
    schedule_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    cron_expression: str
    enabled: bool = True
    config: Dict[str, Any] = Field(default_factory=dict)
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_run_status: Optional[str] = None
    last_run_job_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = "system"
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_for: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RetrainTrigger(BaseModel):
    """Trigger conditions for model retraining."""
    
    trigger_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    trigger_type: str  # "performance_degradation", "data_drift", "time_based", "manual"
    enabled: bool = True
    conditions: Dict[str, Any] = Field(default_factory=dict)
    last_evaluated: Optional[datetime] = None
    last_triggered: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SchedulerRepository(BaseRepository):
    """Repository interface for scheduler operations."""
    
    async def save_schedule(self, schedule: RetrainSchedule) -> RetrainSchedule:
        """Save or update a retrain schedule."""
        raise NotImplementedError
    
    async def get_schedule(self, schedule_id: str) -> Optional[RetrainSchedule]:
        """Get a schedule by ID."""
        raise NotImplementedError
    
    async def list_schedules(
        self,
        model_name: Optional[str] = None,
        enabled_only: bool = True,
        limit: int = 100
    ) -> List[RetrainSchedule]:
        """List schedules with optional filters."""
        raise NotImplementedError
    
    async def save_trigger(self, trigger: RetrainTrigger) -> RetrainTrigger:
        """Save or update a retrain trigger."""
        raise NotImplementedError
    
    async def list_triggers(
        self,
        model_name: Optional[str] = None,
        trigger_type: Optional[str] = None,
        enabled_only: bool = True
    ) -> List[RetrainTrigger]:
        """List triggers with optional filters."""
        raise NotImplementedError


class RetrainSchedulerService(BaseService):
    """
    Scheduled model retraining service.
    
    This service manages scheduled retraining jobs, evaluates triggers,
    and coordinates with the training job service to execute retraining.
    """
    
    def __init__(
        self,
        repository: Optional[SchedulerRepository] = None,
        training_service: Optional[Any] = None,  # Interface to training jobs service
        cache_enabled: bool = True,
        cache_ttl: int = 60,  # Short TTL for schedules
        scheduler_interval: int = 60  # Check schedules every minute
    ):
        """
        Initialize the retrain scheduler service.
        
        Args:
            repository: Repository for data persistence
            training_service: Service to start training jobs
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
            scheduler_interval: How often to check schedules (seconds)
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.training_service = training_service  # In real impl, inject from DI
        self.logger = logging.getLogger(__name__)
        self.scheduler_interval = scheduler_interval
        
        # Scheduler state
        self._scheduler_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._active_schedules: Dict[str, RetrainSchedule] = {}
        self._active_triggers: Dict[str, RetrainTrigger] = {}
    
    def _get_default_repository(self) -> SchedulerRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        # For now, return a mock repository
        class MockRepository(SchedulerRepository):
            def __init__(self):
                self.schedules = {}
                self.triggers = {}
            
            async def save_schedule(self, schedule: RetrainSchedule) -> RetrainSchedule:
                self.schedules[schedule.schedule_id] = schedule
                return schedule
            
            async def get_schedule(self, schedule_id: str) -> Optional[RetrainSchedule]:
                return self.schedules.get(schedule_id)
            
            async def list_schedules(self, **kwargs) -> List[RetrainSchedule]:
                schedules = list(self.schedules.values())
                if kwargs.get('model_name'):
                    schedules = [s for s in schedules if s.model_name == kwargs['model_name']]
                if kwargs.get('enabled_only'):
                    schedules = [s for s in schedules if s.enabled]
                return schedules[:kwargs.get('limit', 100)]
            
            async def save_trigger(self, trigger: RetrainTrigger) -> RetrainTrigger:
                self.triggers[trigger.trigger_id] = trigger
                return trigger
            
            async def list_triggers(self, **kwargs) -> List[RetrainTrigger]:
                triggers = list(self.triggers.values())
                if kwargs.get('model_name'):
                    triggers = [t for t in triggers if t.model_name == kwargs['model_name']]
                if kwargs.get('trigger_type'):
                    triggers = [t for t in triggers if t.trigger_type == kwargs['trigger_type']]
                if kwargs.get('enabled_only'):
                    triggers = [t for t in triggers if t.enabled]
                return triggers
        
        return MockRepository()
    
    async def start(self):
        """Start the scheduler background task."""
        if self._scheduler_task and not self._scheduler_task.done():
            self.logger.warning("Scheduler already running")
            return
        
        self._shutdown_event.clear()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.logger.info("Retrain scheduler started")
    
    async def stop(self):
        """Stop the scheduler background task."""
        self._shutdown_event.set()
        
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Retrain scheduler stopped")
    
    async def create_retrain_schedule(
        self,
        model_name: str,
        cron_expression: str,
        config: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        created_by: str = "system"
    ) -> RetrainSchedule:
        """
        Create a new retrain schedule for a model.
        
        Args:
            model_name: Name of the model to retrain
            cron_expression: Cron expression for scheduling
            config: Training configuration to use
            enabled: Whether schedule is active
            created_by: User creating the schedule
            
        Returns:
            Created RetrainSchedule
            
        Raises:
            ValueError: If cron expression is invalid
        """
        # Validate cron expression
        try:
            cron = croniter(cron_expression, datetime.utcnow())
            next_run = cron.get_next(datetime)
        except Exception as e:
            raise ValueError(f"Invalid cron expression: {e}")
        
        # Create schedule
        schedule = RetrainSchedule(
            model_name=model_name,
            cron_expression=cron_expression,
            config=config or {},
            enabled=enabled,
            created_by=created_by,
            next_run=next_run
        )
        
        # Save schedule
        schedule = await self.repository.save_schedule(schedule)
        self._active_schedules[schedule.schedule_id] = schedule
        
        self.logger.info(
            f"Created retrain schedule for model {model_name}",
            extra={
                "schedule_id": schedule.schedule_id,
                "cron": cron_expression,
                "next_run": next_run.isoformat()
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "retrain_schedule_created",
            tags={"model_name": model_name}
        )
        
        return schedule
    
    async def update_schedule(
        self,
        schedule_id: str,
        cron_expression: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        enabled: Optional[bool] = None
    ) -> RetrainSchedule:
        """
        Update an existing retrain schedule.
        
        Args:
            schedule_id: Schedule to update
            cron_expression: New cron expression
            config: New training configuration
            enabled: New enabled status
            
        Returns:
            Updated RetrainSchedule
        """
        schedule = await self.get_schedule(schedule_id)
        if not schedule:
            raise ValueError(f"Schedule {schedule_id} not found")
        
        # Update fields
        if cron_expression is not None:
            # Validate new expression
            try:
                cron = croniter(cron_expression, datetime.utcnow())
                schedule.cron_expression = cron_expression
                schedule.next_run = cron.get_next(datetime)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {e}")
        
        if config is not None:
            schedule.config.update(config)
        
        if enabled is not None:
            schedule.enabled = enabled
        
        schedule.updated_at = datetime.utcnow()
        
        # Save schedule
        schedule = await self.repository.save_schedule(schedule)
        self._active_schedules[schedule_id] = schedule
        
        self.logger.info(
            f"Updated retrain schedule {schedule_id}",
            extra={"schedule_id": schedule_id, "enabled": schedule.enabled}
        )
        
        return schedule
    
    async def get_schedule(self, schedule_id: str) -> Optional[RetrainSchedule]:
        """
        Get a retrain schedule by ID.
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            RetrainSchedule if found, None otherwise
        """
        # Check active schedules
        if schedule_id in self._active_schedules:
            return self._active_schedules[schedule_id]
        
        # Load from repository
        schedule = await self.repository.get_schedule(schedule_id)
        if schedule:
            self._active_schedules[schedule_id] = schedule
        
        return schedule
    
    async def list_schedules(
        self,
        model_name: Optional[str] = None,
        enabled_only: bool = False,
        limit: int = 50
    ) -> List[RetrainSchedule]:
        """
        List retrain schedules with optional filters.
        
        Args:
            model_name: Filter by model name
            enabled_only: Only return enabled schedules
            limit: Maximum results
            
        Returns:
            List of RetrainSchedule instances
        """
        return await self.repository.list_schedules(
            model_name=model_name,
            enabled_only=enabled_only,
            limit=limit
        )
    
    async def create_retrain_trigger(
        self,
        model_name: str,
        trigger_type: str,
        conditions: Dict[str, Any],
        enabled: bool = True
    ) -> RetrainTrigger:
        """
        Create a retrain trigger for a model.
        
        Args:
            model_name: Name of the model
            trigger_type: Type of trigger
            conditions: Trigger conditions
            enabled: Whether trigger is active
            
        Returns:
            Created RetrainTrigger
        """
        trigger = RetrainTrigger(
            model_name=model_name,
            trigger_type=trigger_type,
            conditions=conditions,
            enabled=enabled
        )
        
        # Save trigger
        trigger = await self.repository.save_trigger(trigger)
        self._active_triggers[trigger.trigger_id] = trigger
        
        self.logger.info(
            f"Created retrain trigger for model {model_name}",
            extra={
                "trigger_id": trigger.trigger_id,
                "trigger_type": trigger_type
            }
        )
        
        return trigger
    
    async def trigger_retrain(
        self,
        model_name: str,
        config: Optional[Dict[str, Any]] = None,
        triggered_by: str = "manual",
        reason: str = "Manual trigger"
    ) -> str:
        """
        Manually trigger a model retrain.
        
        Args:
            model_name: Name of the model to retrain
            config: Training configuration
            triggered_by: User or system triggering
            reason: Reason for retrain
            
        Returns:
            Training job ID
        """
        # In real implementation, would use injected training service
        job_id = str(uuid4())  # Simulate job creation
        
        self.logger.info(
            f"Triggered retrain for model {model_name}",
            extra={
                "model_name": model_name,
                "job_id": job_id,
                "triggered_by": triggered_by,
                "reason": reason
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "retrain_triggered",
            tags={
                "model_name": model_name,
                "trigger_type": triggered_by
            }
        )
        
        return job_id
    
    async def _scheduler_loop(self):
        """Background loop to check and execute scheduled retrains."""
        self.logger.info("Scheduler loop started")
        
        while not self._shutdown_event.is_set():
            try:
                # Load active schedules
                schedules = await self.list_schedules(enabled_only=True)
                
                # Check each schedule
                for schedule in schedules:
                    await self._check_schedule(schedule)
                
                # Check triggers
                triggers = await self.repository.list_triggers(enabled_only=True)
                for trigger in triggers:
                    await self._check_trigger(trigger)
                
                # Sleep until next check
                await asyncio.sleep(self.scheduler_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(self.scheduler_interval * 5)  # Back off on error
        
        self.logger.info("Scheduler loop stopped")
    
    async def _check_schedule(self, schedule: RetrainSchedule):
        """Check if a schedule needs to run."""
        if not schedule.enabled or not schedule.next_run:
            return
        
        now = datetime.utcnow()
        
        # Check if it's time to run
        if now >= schedule.next_run:
            try:
                # Trigger retrain
                job_id = await self.trigger_retrain(
                    model_name=schedule.model_name,
                    config=schedule.config,
                    triggered_by="scheduler",
                    reason=f"Scheduled retrain (schedule_id: {schedule.schedule_id})"
                )
                
                # Update schedule
                schedule.last_run = now
                schedule.last_run_status = "started"
                schedule.last_run_job_id = job_id
                
                # Calculate next run
                cron = croniter(schedule.cron_expression, now)
                schedule.next_run = cron.get_next(datetime)
                
                # Save schedule
                await self.repository.save_schedule(schedule)
                
            except Exception as e:
                self.logger.error(
                    f"Failed to trigger scheduled retrain: {e}",
                    extra={"schedule_id": schedule.schedule_id}
                )
                schedule.last_run_status = "failed"
                await self.repository.save_schedule(schedule)
    
    async def _check_trigger(self, trigger: RetrainTrigger):
        """Check if a trigger condition is met."""
        if not trigger.enabled:
            return
        
        now = datetime.utcnow()
        
        # Rate limit trigger evaluation (at most once per hour)
        if trigger.last_evaluated:
            time_since_eval = now - trigger.last_evaluated
            if time_since_eval < timedelta(hours=1):
                return
        
        trigger.last_evaluated = now
        
        try:
            # Evaluate trigger based on type
            should_trigger = await self._evaluate_trigger_condition(trigger)
            
            if should_trigger:
                # Rate limit actual triggering (at most once per day)
                if trigger.last_triggered:
                    time_since_trigger = now - trigger.last_triggered
                    min_interval = timedelta(
                        hours=trigger.conditions.get("min_interval_hours", 24)
                    )
                    if time_since_trigger < min_interval:
                        return
                
                # Trigger retrain
                await self.trigger_retrain(
                    model_name=trigger.model_name,
                    config=trigger.conditions.get("training_config", {}),
                    triggered_by=f"trigger_{trigger.trigger_type}",
                    reason=f"Trigger condition met (trigger_id: {trigger.trigger_id})"
                )
                
                trigger.last_triggered = now
            
            # Save trigger state
            await self.repository.save_trigger(trigger)
            
        except Exception as e:
            self.logger.error(
                f"Failed to evaluate trigger: {e}",
                extra={"trigger_id": trigger.trigger_id}
            )
    
    async def _evaluate_trigger_condition(self, trigger: RetrainTrigger) -> bool:
        """
        Evaluate if a trigger condition is met.
        
        In real implementation, would check actual metrics/conditions.
        """
        if trigger.trigger_type == "performance_degradation":
            # Check if model performance has degraded
            threshold = trigger.conditions.get("accuracy_threshold", 0.9)
            # In real impl, would fetch current model metrics
            current_accuracy = 0.85  # Simulated
            return current_accuracy < threshold
        
        elif trigger.trigger_type == "data_drift":
            # Check for data drift
            drift_threshold = trigger.conditions.get("drift_threshold", 0.1)
            # In real impl, would calculate actual drift
            current_drift = 0.15  # Simulated
            return current_drift > drift_threshold
        
        elif trigger.trigger_type == "time_based":
            # Check if enough time has passed
            days_threshold = trigger.conditions.get("days_since_last_train", 30)
            # In real impl, would check actual last training date
            return True  # Simulated
        
        return False
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
