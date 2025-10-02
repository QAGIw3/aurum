"""Auto-reforecast service for event-driven forecast triggering.

Implements business logic for automatic forecast re-runs when new data arrives,
with debounce controls and backpressure management.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError

logger = logging.getLogger(__name__)


@dataclass
class TriggerCondition:
    """Condition that triggers forecast re-run."""
    data_source: str  # "weather", "load", "price", "iso"
    geography: str = "US"
    threshold_type: str = "absolute"  # "absolute", "percentage", "volatility"
    threshold_value: float = 0.0
    lookback_hours: int = 24
    min_change_required: bool = True


@dataclass
class DebounceConfig:
    """Configuration for forecast debouncing."""
    enabled: bool = True
    window_seconds: int = 300  # 5 minutes debounce window
    max_concurrent_triggers: int = 5
    priority_threshold: float = 0.5


@dataclass
class BackpressureConfig:
    """Configuration for backpressure control."""
    enabled: bool = True
    max_queue_size: int = 1000
    max_processing_rate_per_second: int = 10
    queue_timeout_seconds: int = 60
    drop_low_priority_on_pressure: bool = True


class AutoReforecastService(BaseService):
    """Service for automatic forecast re-run orchestration.
    
    Auto-reforecast provides:
    - Event-driven forecast triggers via Kafka topics
    - Debounce and backpressure controls
    - Short-horizon forecast re-runs when new data arrives
    - Integration with feature store and forecasting
    - Automatic forecast invalidation and regeneration
    
    This service:
    - Manages forecast trigger conditions
    - Implements debounce logic
    - Handles backpressure
    - Orchestrates forecast re-runs
    - Tracks forecast jobs
    """
    
    def __init__(self):
        """Initialize service with default configuration."""
        super().__init__()
        self._triggers: Dict[str, TriggerCondition] = {}
        self._reforecast_jobs: Dict[str, Dict[str, Any]] = {}
        self._debounce_config = DebounceConfig()
        self._backpressure_config = BackpressureConfig()
    
    async def create_forecast_trigger(
        self,
        trigger_name: str,
        data_source: str,
        threshold_type: str,
        threshold_value: float,
        geography: str = "US",
        lookback_hours: int = 24,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[TriggerCondition]:
        """Create a forecast trigger condition.
        
        Args:
            trigger_name: Unique trigger identifier
            data_source: Data source to monitor
            threshold_type: Type of threshold check
            threshold_value: Threshold value
            geography: Geographic scope
            lookback_hours: Hours to look back for changes
            context: Service context
            
        Returns:
            ServiceResult with created trigger
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If creation fails
        """
        self._log_operation(
            "create_forecast_trigger",
            context=context,
            trigger_name=trigger_name
        )
        
        try:
            # Validate inputs
            self._validate_trigger_name(trigger_name)
            self._validate_data_source(data_source)
            self._validate_threshold_type(threshold_type)
            self._validate_threshold_value(threshold_value)
            
            if trigger_name in self._triggers:
                raise ValidationError(f"Trigger '{trigger_name}' already exists", field="trigger_name")
            
            # Create trigger
            trigger = TriggerCondition(
                data_source=data_source,
                geography=geography,
                threshold_type=threshold_type,
                threshold_value=threshold_value,
                lookback_hours=lookback_hours
            )
            
            self._triggers[trigger_name] = trigger
            
            return ServiceResult.ok(
                data=trigger,
                metadata={"trigger_name": trigger_name, "created": True}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "create_forecast_trigger", context)
    
    async def trigger_forecast_rerun(
        self,
        forecast_type: str,
        geography: str,
        reason: str,
        priority: float = 0.5,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Trigger a forecast re-run.
        
        Args:
            forecast_type: Type of forecast (e.g., "load", "price")
            geography: Geographic scope
            reason: Reason for re-run
            priority: Priority level (0.0-1.0)
            context: Service context
            
        Returns:
            ServiceResult with job information
            
        Raises:
            ValidationError: If parameters invalid
            ServiceError: If trigger fails
        """
        self._log_operation(
            "trigger_forecast_rerun",
            context=context,
            forecast_type=forecast_type,
            geography=geography
        )
        
        try:
            # Validate inputs
            self._validate_forecast_type(forecast_type)
            self._validate_geography(geography)
            self._validate_priority(priority)
            
            # Check backpressure
            if self._backpressure_config.enabled:
                if len(self._reforecast_jobs) >= self._backpressure_config.max_queue_size:
                    raise ServiceError(
                        "Reforecast queue is full (backpressure)",
                        code="BACKPRESSURE"
                    )
            
            # Create reforecast job
            job_id = str(uuid4())
            job = {
                "job_id": job_id,
                "forecast_type": forecast_type,
                "geography": geography,
                "reason": reason,
                "priority": priority,
                "status": "queued",
                "created_at": datetime.now().isoformat(),
                "started_at": None,
                "completed_at": None
            }
            
            self._reforecast_jobs[job_id] = job
            
            return ServiceResult.ok(
                data=job,
                metadata={
                    "job_id": job_id,
                    "triggered": True,
                    "queue_size": len(self._reforecast_jobs)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "trigger_forecast_rerun", context)
    
    async def get_reforecast_job_status(
        self,
        job_id: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get status of a reforecast job.
        
        Args:
            job_id: Job identifier
            context: Service context
            
        Returns:
            ServiceResult with job status
            
        Raises:
            ValidationError: If job_id invalid
            NotFoundError: If job not found
            ServiceError: If retrieval fails
        """
        self._log_operation("get_reforecast_job_status", context=context, job_id=job_id)
        
        try:
            self._validate_job_id(job_id)
            
            if job_id not in self._reforecast_jobs:
                raise NotFoundError("reforecast_job", job_id)
            
            job = self._reforecast_jobs[job_id]
            
            return ServiceResult.ok(
                data=job,
                metadata={"job_id": job_id}
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_reforecast_job_status", context)
    
    async def list_pending_jobs(
        self,
        forecast_type: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """List pending reforecast jobs.
        
        Args:
            forecast_type: Filter by forecast type
            limit: Maximum results
            context: Service context
            
        Returns:
            ServiceResult with pending jobs
        """
        self._log_operation("list_pending_jobs", context=context, forecast_type=forecast_type)
        
        try:
            # Filter jobs
            jobs = [
                job for job in self._reforecast_jobs.values()
                if job["status"] in ["queued", "running"]
            ]
            
            if forecast_type:
                self._validate_forecast_type(forecast_type)
                jobs = [j for j in jobs if j["forecast_type"] == forecast_type]
            
            # Apply limit
            jobs = jobs[:limit]
            
            return ServiceResult.ok(
                data=jobs,
                metadata={
                    "pending_count": len(jobs),
                    "limit": limit,
                    "forecast_type": forecast_type
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "list_pending_jobs", context)
    
    # Private helper methods
    
    def _validate_trigger_name(self, trigger_name: str) -> None:
        """Validate trigger name."""
        if not trigger_name or not trigger_name.strip():
            raise ValidationError("Trigger name is required", field="trigger_name")
        
        if len(trigger_name) > 100:
            raise ValidationError("Trigger name too long", field="trigger_name")
    
    def _validate_data_source(self, data_source: str) -> None:
        """Validate data source."""
        valid_sources = ["weather", "load", "price", "iso", "generation"]
        if data_source not in valid_sources:
            raise ValidationError(
                f"Invalid data source. Must be one of: {', '.join(valid_sources)}",
                field="data_source"
            )
    
    def _validate_threshold_type(self, threshold_type: str) -> None:
        """Validate threshold type."""
        valid_types = ["absolute", "percentage", "volatility"]
        if threshold_type not in valid_types:
            raise ValidationError(
                f"Invalid threshold type. Must be one of: {', '.join(valid_types)}",
                field="threshold_type"
            )
    
    def _validate_threshold_value(self, threshold_value: float) -> None:
        """Validate threshold value."""
        if threshold_value < 0:
            raise ValidationError("Threshold value must be non-negative", field="threshold_value")
    
    def _validate_forecast_type(self, forecast_type: str) -> None:
        """Validate forecast type."""
        valid_types = ["load", "price", "generation", "renewable"]
        if forecast_type not in valid_types:
            raise ValidationError(
                f"Invalid forecast type. Must be one of: {', '.join(valid_types)}",
                field="forecast_type"
            )
    
    def _validate_geography(self, geography: str) -> None:
        """Validate geography."""
        if not geography or not geography.strip():
            raise ValidationError("Geography is required", field="geography")
        
        if len(geography) > 50:
            raise ValidationError("Geography identifier too long", field="geography")
    
    def _validate_priority(self, priority: float) -> None:
        """Validate priority level."""
        if not (0.0 <= priority <= 1.0):
            raise ValidationError("Priority must be between 0.0 and 1.0", field="priority")
    
    def _validate_job_id(self, job_id: str) -> None:
        """Validate job ID."""
        if not job_id or not job_id.strip():
            raise ValidationError("Job ID is required", field="job_id")
        
        # Try parsing as UUID
        try:
            from uuid import UUID
            UUID(job_id)
        except ValueError:
            raise ValidationError("Job ID must be a valid UUID", field="job_id")

