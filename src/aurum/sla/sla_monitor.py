"""SLA monitoring for data ingestion pipelines."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any

from ..logging import StructuredLogger, LogLevel, create_logger


class SLAViolationType(str, Enum):
    """Types of SLA violations."""
    TIMEOUT = "timeout"
    ERROR_RATE = "error_rate"
    THROUGHPUT = "throughput"
    DATA_QUALITY = "data_quality"
    STALENESS = "staleness"
    AVAILABILITY = "availability"


@dataclass
class SLAConfig:
    """Configuration for SLA monitoring."""

    # Timing constraints
    max_execution_time_minutes: int = 60
    max_retry_delay_minutes: int = 10

    # Quality constraints
    max_error_rate_percent: float = 5.0
    min_records_per_hour: int = 100

    # Availability constraints
    max_downtime_minutes: int = 30
    min_uptime_percent: float = 99.0

    # Staleness constraints
    max_data_age_hours: int = 24

    # Monitoring configuration
    sla_check_interval_minutes: int = 5
    alert_cooldown_minutes: int = 15

    # Dataset-specific overrides
    dataset_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class SLAViolation:
    """SLA violation event."""

    timestamp: str
    dag_id: str
    task_id: str
    violation_type: SLAViolationType
    description: str
    severity: str
    actual_value: Any
    expected_value: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


class SLAMonitor:
    """SLA monitoring for data ingestion DAGs."""

    def __init__(self, config: SLAConfig):
        """Initialize SLA monitor.

        Args:
            config: SLA configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="sla_monitor",
            kafka_bootstrap_servers=None,  # Will be set later
            kafka_topic="aurum.sla.violations",
            dataset="sla_monitoring"
        )

        # SLA state tracking
        self.dag_states: Dict[str, Dict[str, Any]] = {}
        self.violation_history: Dict[str, List[SLAViolation]] = {}

        # Alert cooldown tracking
        self.last_alerts: Dict[str, datetime] = {}

    def check_dag_sla(
        self,
        dag_id: str,
        task_id: str,
        execution_time: float,
        records_processed: int = 0,
        error_count: int = 0,
        data_quality_score: Optional[float] = None,
        last_success_time: Optional[datetime] = None,
        **context
    ) -> List[SLAViolation]:
        """Check SLA compliance for a DAG task.

        Args:
            dag_id: DAG identifier
            task_id: Task identifier
            execution_time: Task execution time in seconds
            records_processed: Number of records processed
            error_count: Number of errors encountered
            data_quality_score: Data quality score (0.0 to 1.0)
            last_success_time: Last successful execution time
            **context: Additional context from Airflow

        Returns:
            List of SLA violations found
        """
        violations = []
        now = datetime.now()

        # Get dataset-specific configuration
        dataset = context.get('dataset', 'default')
        sla_config = self._get_sla_config_for_dataset(dataset)

        # Check execution time SLA
        execution_time_minutes = execution_time / 60
        if execution_time_minutes > sla_config.max_execution_time_minutes:
            violations.append(SLAViolation(
                timestamp=now.isoformat(),
                dag_id=dag_id,
                task_id=task_id,
                violation_type=SLAViolationType.TIMEOUT,
                description=f"Task exceeded maximum execution time",
                severity="HIGH",
                actual_value=execution_time_minutes,
                expected_value=sla_config.max_execution_time_minutes,
                metadata={
                    "execution_time_minutes": execution_time_minutes,
                    "max_allowed": sla_config.max_execution_time_minutes
                }
            ))

        # Check error rate SLA
        total_attempts = records_processed + error_count
        if total_attempts > 0:
            error_rate = (error_count / total_attempts) * 100
            if error_rate > sla_config.max_error_rate_percent:
                violations.append(SLAViolation(
                    timestamp=now.isoformat(),
                    dag_id=dag_id,
                    task_id=task_id,
                    violation_type=SLAViolationType.ERROR_RATE,
                    description=f"Error rate exceeded threshold",
                    severity="MEDIUM" if error_rate < 20 else "HIGH",
                    actual_value=error_rate,
                    expected_value=sla_config.max_error_rate_percent,
                    metadata={
                        "error_rate_percent": error_rate,
                        "errors": error_count,
                        "total_attempts": total_attempts
                    }
                ))

        # Check throughput SLA
        if records_processed < sla_config.min_records_per_hour:
            violations.append(SLAViolation(
                timestamp=now.isoformat(),
                dag_id=dag_id,
                task_id=task_id,
                violation_type=SLAViolationType.THROUGHPUT,
                description=f"Insufficient records processed per hour",
                severity="LOW",
                actual_value=records_processed,
                expected_value=sla_config.min_records_per_hour,
                metadata={
                    "records_processed": records_processed,
                    "min_required": sla_config.min_records_per_hour
                }
            ))

        # Check data quality SLA
        if data_quality_score is not None and data_quality_score < 0.95:
            violations.append(SLAViolation(
                timestamp=now.isoformat(),
                dag_id=dag_id,
                task_id=task_id,
                violation_type=SLAViolationType.DATA_QUALITY,
                description=f"Data quality score below threshold",
                severity="MEDIUM",
                actual_value=data_quality_score,
                expected_value=0.95,
                metadata={
                    "quality_score": data_quality_score,
                    "min_required": 0.95
                }
            ))

        # Check staleness SLA
        if last_success_time:
            hours_since_success = (now - last_success_time).total_seconds() / 3600
            if hours_since_success > sla_config.max_data_age_hours:
                violations.append(SLAViolation(
                    timestamp=now.isoformat(),
                    dag_id=dag_id,
                    task_id=task_id,
                    violation_type=SLAViolationType.STALENESS,
                    description=f"Data is stale (no recent successful runs)",
                    severity="HIGH",
                    actual_value=hours_since_success,
                    expected_value=sla_config.max_data_age_hours,
                    metadata={
                        "hours_since_success": hours_since_success,
                        "last_success_time": last_success_time.isoformat(),
                        "max_age_hours": sla_config.max_data_age_hours
                    }
                ))

        # Log violations
        for violation in violations:
            self.logger.log(
                LogLevel.ERROR,
                f"SLA violation: {violation.description}",
                "sla_violation",
                dag_id=dag_id,
                task_id=task_id,
                violation_type=violation.violation_type.value,
                severity=violation.severity,
                **violation.metadata
            )

        return violations

    def check_availability_sla(
        self,
        dag_id: str,
        uptime_percent: float,
        downtime_minutes: int
    ) -> Optional[SLAViolation]:
        """Check availability SLA.

        Args:
            dag_id: DAG identifier
            uptime_percent: Current uptime percentage
            downtime_minutes: Current downtime in minutes

        Returns:
            SLA violation if threshold exceeded, None otherwise
        """
        sla_config = self._get_sla_config_for_dataset(dag_id)

        if uptime_percent < sla_config.min_uptime_percent:
            return SLAViolation(
                timestamp=datetime.now().isoformat(),
                dag_id=dag_id,
                task_id="availability_check",
                violation_type=SLAViolationType.AVAILABILITY,
                description=f"Uptime below SLA threshold",
                severity="CRITICAL",
                actual_value=uptime_percent,
                expected_value=sla_config.min_uptime_percent,
                metadata={
                    "uptime_percent": uptime_percent,
                    "downtime_minutes": downtime_minutes,
                    "min_uptime_required": sla_config.min_uptime_percent
                }
            )

        return None

    def get_sla_status(self, dag_id: str) -> Dict[str, Any]:
        """Get current SLA status for a DAG.

        Args:
            dag_id: DAG identifier

        Returns:
            Dictionary with SLA status information
        """
        if dag_id not in self.dag_states:
            return {"status": "unknown", "violations": []}

        state = self.dag_states[dag_id]
        violations = self.violation_history.get(dag_id, [])

        # Calculate recent violations (last 24 hours)
        recent_violations = [
            v for v in violations
            if datetime.fromisoformat(v.timestamp) > datetime.now() - timedelta(hours=24)
        ]

        status = "healthy"
        if len(recent_violations) > 5:
            status = "critical"
        elif len(recent_violations) > 2:
            status = "warning"
        elif recent_violations:
            status = "minor"

        return {
            "dag_id": dag_id,
            "status": status,
            "last_check": state.get("last_check"),
            "recent_violations": len(recent_violations),
            "last_violation": recent_violations[-1].timestamp if recent_violations else None,
            "violation_summary": self._summarize_violations(recent_violations)
        }

    def _get_sla_config_for_dataset(self, dataset: str) -> SLAConfig:
        """Get SLA configuration for a specific dataset.

        Args:
            dataset: Dataset name

        Returns:
            SLA configuration with dataset-specific overrides
        """
        config = SLAConfig()

        # Apply dataset-specific overrides
        if dataset in self.config.dataset_overrides:
            overrides = self.config.dataset_overrides[dataset]
            for key, value in overrides.items():
                if hasattr(config, key):
                    setattr(config, key, value)

        return config

    def _summarize_violations(self, violations: List[SLAViolation]) -> Dict[str, int]:
        """Summarize violations by type.

        Args:
            violations: List of SLA violations

        Returns:
            Dictionary mapping violation types to counts
        """
        summary = {}
        for violation in violations:
            violation_type = violation.violation_type.value
            summary[violation_type] = summary.get(violation_type, 0) + 1
        return summary

    def record_dag_state(
        self,
        dag_id: str,
        state: str,
        execution_time: Optional[float] = None,
        records_processed: int = 0,
        **metadata
    ) -> None:
        """Record current DAG state for SLA tracking.

        Args:
            dag_id: DAG identifier
            state: Current state (running, success, failed, etc.)
            execution_time: Execution time in seconds
            records_processed: Number of records processed
            **metadata: Additional metadata
        """
        if dag_id not in self.dag_states:
            self.dag_states[dag_id] = {}

        self.dag_states[dag_id].update({
            "state": state,
            "last_check": datetime.now().isoformat(),
            "execution_time": execution_time,
            "records_processed": records_processed,
            **metadata
        })

    def should_alert(self, violation: SLAViolation) -> bool:
        """Determine if an alert should be sent for this violation.

        Args:
            violation: SLA violation

        Returns:
            True if alert should be sent
        """
        alert_key = f"{violation.dag_id}:{violation.violation_type.value}"

        now = datetime.now()
        last_alert = self.last_alerts.get(alert_key)

        if last_alert is None:
            self.last_alerts[alert_key] = now
            return True

        time_since_last_alert = now - last_alert
        if time_since_last_alert.total_seconds() > (self.config.alert_cooldown_minutes * 60):
            self.last_alerts[alert_key] = now
            return True

        return False
