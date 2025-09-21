"""Failure callbacks and error handling for data ingestion DAGs."""

from __future__ import annotations

import traceback
from typing import Callable, Dict, List, Optional, Any
from dataclasses import dataclass

from ..logging import StructuredLogger, LogLevel, create_logger


@dataclass
class FailureCallback:
    """Configuration for a failure callback."""

    name: str
    callback_func: Callable
    enabled: bool = True
    priority: int = 50  # Lower numbers = higher priority
    conditions: Dict[str, Any] = None  # Conditions for when to trigger


class FailureCallbackManager:
    """Manager for failure callbacks in data ingestion DAGs."""

    def __init__(self):
        """Initialize failure callback manager."""
        self.logger = create_logger(
            source_name="failure_callback_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.failure_callbacks",
            dataset="failure_handling"
        )

        # Default callbacks
        self.callbacks: List[FailureCallback] = [
            FailureCallback(
                name="log_failure",
                callback_func=self._log_failure,
                priority=10,
                conditions=None
            ),
            FailureCallback(
                name="update_sla_monitor",
                callback_func=self._update_sla_monitor,
                priority=20,
                conditions=None
            ),
            FailureCallback(
                name="send_alert",
                callback_func=self._send_alert,
                priority=30,
                conditions={"severity": ["HIGH", "CRITICAL"]}
            ),
            FailureCallback(
                name="cleanup_resources",
                callback_func=self._cleanup_resources,
                priority=90,
                conditions=None
            )
        ]

    def execute_callbacks(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> None:
        """Execute all applicable failure callbacks.

        Args:
            context: Airflow context dictionary
            exception: Exception that caused the failure
        """
        self.logger.log(
            LogLevel.ERROR,
            "Executing failure callbacks",
            "failure_callback_start",
            **context
        )

        # Sort callbacks by priority
        sorted_callbacks = sorted(
            [cb for cb in self.callbacks if cb.enabled],
            key=lambda x: x.priority
        )

        for callback in sorted_callbacks:
            try:
                # Check conditions
                if not self._check_conditions(callback, context, exception):
                    continue

                self.logger.log(
                    LogLevel.INFO,
                    f"Executing failure callback: {callback.name}",
                    "failure_callback_execute",
                    callback_name=callback.name,
                    priority=callback.priority
                )

                callback.callback_func(context, exception)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failure callback {callback.name} failed: {e}",
                    "failure_callback_error",
                    callback_name=callback.name,
                    error=str(e)
                )

    def add_callback(self, callback: FailureCallback) -> None:
        """Add a custom failure callback.

        Args:
            callback: Failure callback to add
        """
        self.callbacks.append(callback)
        self.logger.log(
            LogLevel.INFO,
            f"Added failure callback: {callback.name}",
            "failure_callback_added",
            callback_name=callback.name,
            priority=callback.priority
        )

    def remove_callback(self, name: str) -> bool:
        """Remove a failure callback by name.

        Args:
            name: Name of the callback to remove

        Returns:
            True if callback was removed
        """
        for i, callback in enumerate(self.callbacks):
            if callback.name == name:
                del self.callbacks[i]
                self.logger.log(
                    LogLevel.INFO,
                    f"Removed failure callback: {name}",
                    "failure_callback_removed",
                    callback_name=name
                )
                return True
        return False

    def _check_conditions(
        self,
        callback: FailureCallback,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> bool:
        """Check if callback conditions are met.

        Args:
            callback: Failure callback
            context: Airflow context
            exception: Exception that caused failure

        Returns:
            True if conditions are met
        """
        if callback.conditions is None:
            return True

        # Check severity condition
        if "severity" in callback.conditions:
            task_instance = context.get("task_instance")
            if task_instance:
                severity_levels = callback.conditions["severity"]
                # Determine severity based on failure context
                severity = self._determine_severity(context, exception)
                if severity not in severity_levels:
                    return False

        # Check error type condition
        if "error_type" in callback.conditions:
            if exception:
                error_types = callback.conditions["error_type"]
                error_type = exception.__class__.__name__
                if error_type not in error_types:
                    return False

        # Check task state condition
        if "task_state" in callback.conditions:
            task_instance = context.get("task_instance")
            if task_instance:
                task_states = callback.conditions["task_state"]
                current_state = task_instance.state
                if current_state not in task_states:
                    return False

        return True

    def _determine_severity(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> str:
        """Determine severity of a failure.

        Args:
            context: Airflow context
            exception: Exception that caused failure

        Returns:
            Severity level (LOW, MEDIUM, HIGH, CRITICAL)
        """
        # Critical if this is a repeated failure
        task_instance = context.get("task_instance")
        if task_instance and task_instance.try_number > 3:
            return "CRITICAL"

        # High if it's a data quality or timeout issue
        if exception:
            error_msg = str(exception).lower()
            if any(term in error_msg for term in ["timeout", "quality", "corruption"]):
                return "HIGH"

        # Medium for most operational issues
        return "MEDIUM"

    def _log_failure(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> None:
        """Log detailed failure information."""
        task_instance = context.get("task_instance")
        dag_id = context.get("dag", {}).dag_id
        task_id = task_instance.task_id if task_instance else "unknown"

        error_details = ""
        if exception:
            error_details = f"{exception.__class__.__name__}: {str(exception)}"

        self.logger.log(
            LogLevel.ERROR,
            f"DAG failure: {dag_id}.{task_id}",
            "dag_failure",
            dag_id=dag_id,
            task_id=task_id,
            error_details=error_details,
            try_number=task_instance.try_number if task_instance else 1,
            execution_date=context.get("execution_date"),
            **context
        )

    def _update_sla_monitor(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> None:
        """Update SLA monitor with failure information."""
        # This would integrate with the SLA monitor
        # For now, just log the intent
        self.logger.log(
            LogLevel.INFO,
            "Updating SLA monitor with failure information",
            "sla_update",
            **context
        )

    def _send_alert(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> None:
        """Send alert about failure."""
        task_instance = context.get("task_instance")
        dag_id = context.get("dag", {}).dag_id
        task_id = task_instance.task_id if task_instance else "unknown"

        severity = self._determine_severity(context, exception)

        self.logger.log(
            LogLevel.WARN,
            f"Sending {severity} alert for DAG failure: {dag_id}.{task_id}",
            "alert_sent",
            dag_id=dag_id,
            task_id=task_id,
            severity=severity,
            alert_type="dag_failure",
            **context
        )

    def _cleanup_resources(
        self,
        context: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> None:
        """Clean up resources after failure."""
        # This would handle cleanup of temporary files, connections, etc.
        self.logger.log(
            LogLevel.INFO,
            "Cleaning up resources after failure",
            "resource_cleanup",
            **context
        )
