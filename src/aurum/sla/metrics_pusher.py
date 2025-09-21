"""Metrics collection and pushing for DAG execution."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from ..logging import StructuredLogger, LogLevel, create_logger


@dataclass
class DAGMetrics:
    """Metrics for DAG execution."""

    dag_id: str
    task_id: str
    execution_date: str
    state: str
    try_number: int = 1
    max_tries: int = 1

    # Timing metrics
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    duration_seconds: Optional[float] = None

    # Performance metrics
    records_processed: int = 0
    bytes_processed: int = 0
    error_count: int = 0

    # Quality metrics
    data_quality_score: Optional[float] = None

    # Resource metrics
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None

    # Custom metrics
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        result = {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "execution_date": self.execution_date,
            "state": self.state,
            "try_number": self.try_number,
            "max_tries": self.max_tries,
        }

        for field in ["start_date", "end_date", "duration_seconds",
                     "records_processed", "bytes_processed", "error_count",
                     "data_quality_score", "memory_usage_mb", "cpu_usage_percent"]:
            value = getattr(self, field)
            if value is not None:
                result[field] = value

        if self.custom_metrics:
            result["custom_metrics"] = self.custom_metrics

        return result


class MetricsPusher:
    """Push metrics from DAG executions to monitoring systems."""

    def __init__(
        self,
        prometheus_gateway: Optional[str] = None,
        statsd_host: Optional[str] = None,
        statsd_port: int = 8125,
        kafka_bootstrap_servers: Optional[str] = None,
        kafka_topic: str = "aurum.dag_metrics"
    ):
        """Initialize metrics pusher.

        Args:
            prometheus_gateway: Prometheus PushGateway URL
            statsd_host: StatsD host
            statsd_port: StatsD port
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic for metrics
        """
        self.logger = create_logger(
            source_name="metrics_pusher",
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            dataset="metrics"
        )

        self.prometheus_gateway = prometheus_gateway
        self.statsd_host = statsd_host
        self.statsd_port = statsd_port

        # Metrics storage
        self.metrics_buffer: List[DAGMetrics] = []
        self.last_push = time.time()

    def collect_metrics(self, dag_id: str, task_id: str, **metrics) -> DAGMetrics:
        """Collect metrics for a DAG task.

        Args:
            dag_id: DAG identifier
            task_id: Task identifier
            **metrics: Metric values

        Returns:
            DAGMetrics instance
        """
        metric = DAGMetrics(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=metrics.get("execution_date", datetime.now().isoformat()),
            state=metrics.get("state", "unknown"),
            try_number=metrics.get("try_number", 1),
            max_tries=metrics.get("max_tries", 1),
            start_date=metrics.get("start_date"),
            end_date=metrics.get("end_date"),
            duration_seconds=metrics.get("duration_seconds"),
            records_processed=metrics.get("records_processed", 0),
            bytes_processed=metrics.get("bytes_processed", 0),
            error_count=metrics.get("error_count", 0),
            data_quality_score=metrics.get("data_quality_score"),
            memory_usage_mb=metrics.get("memory_usage_mb"),
            cpu_usage_percent=metrics.get("cpu_usage_percent"),
            custom_metrics=metrics.get("custom_metrics", {})
        )

        self.metrics_buffer.append(metric)

        self.logger.log(
            LogLevel.DEBUG,
            f"Collected metrics for {dag_id}.{task_id}",
            "metrics_collected",
            dag_id=dag_id,
            task_id=task_id,
            records_processed=metric.records_processed,
            duration_seconds=metric.duration_seconds
        )

        return metric

    def push_metrics(self, force: bool = False) -> None:
        """Push collected metrics to monitoring systems.

        Args:
            force: Force push even if buffer not full
        """
        now = time.time()

        # Check if we should push (either forced or buffer full or time elapsed)
        should_push = (
            force or
            len(self.metrics_buffer) >= 100 or
            now - self.last_push >= 60  # Push every 60 seconds
        )

        if not should_push:
            return

        if not self.metrics_buffer:
            return

        self.logger.log(
            LogLevel.INFO,
            f"Pushing {len(self.metrics_buffer)} metrics to monitoring systems",
            "metrics_push_start"
        )

        try:
            # Push to Prometheus
            if self.prometheus_gateway:
                self._push_to_prometheus()

            # Push to StatsD
            if self.statsd_host:
                self._push_to_statsd()

            # Push to Kafka
            if hasattr(self.logger, 'kafka_sink') and self.logger.kafka_sink:
                self._push_to_kafka()

            # Clear buffer
            self.metrics_buffer.clear()
            self.last_push = now

            self.logger.log(
                LogLevel.INFO,
                "Successfully pushed metrics",
                "metrics_push_success"
            )

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to push metrics: {e}",
                "metrics_push_error",
                error=str(e)
            )

    def _push_to_prometheus(self) -> None:
        """Push metrics to Prometheus PushGateway."""
        # Implementation would use prometheus_client library
        # This is a placeholder for the actual implementation
        self.logger.log(
            LogLevel.DEBUG,
            "Pushing metrics to Prometheus",
            "prometheus_push"
        )

    def _push_to_statsd(self) -> None:
        """Push metrics to StatsD."""
        # Implementation would use statsd client library
        # This is a placeholder for the actual implementation
        self.logger.log(
            LogLevel.DEBUG,
            "Pushing metrics to StatsD",
            "statsd_push"
        )

    def _push_to_kafka(self) -> None:
        """Push metrics to Kafka."""
        for metric in self.metrics_buffer:
            self.logger.log(
                LogLevel.INFO,
                f"DAG metrics: {metric.dag_id}.{metric.task_id} - {metric.state}",
                "dag_metrics",
                **metric.to_dict()
            )

    def get_dag_performance_summary(
        self,
        dag_id: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get performance summary for a DAG.

        Args:
            dag_id: DAG identifier
            hours: Time window in hours

        Returns:
            Dictionary with performance summary
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        relevant_metrics = [
            m for m in self.metrics_buffer
            if m.dag_id == dag_id and
            datetime.fromisoformat(m.execution_date) > cutoff_time
        ]

        if not relevant_metrics:
            return {"dag_id": dag_id, "error": "No recent metrics found"}

        successful_runs = [m for m in relevant_metrics if m.state == "success"]
        failed_runs = [m for m in relevant_metrics if m.state in ["failed", "upstream_failed"]]

        avg_duration = sum(
            m.duration_seconds for m in successful_runs if m.duration_seconds
        ) / len(successful_runs) if successful_runs else 0

        total_records = sum(m.records_processed for m in relevant_metrics)
        total_errors = sum(m.error_count for m in relevant_metrics)

        return {
            "dag_id": dag_id,
            "time_window_hours": hours,
            "total_runs": len(relevant_metrics),
            "successful_runs": len(successful_runs),
            "failed_runs": len(failed_runs),
            "success_rate": len(successful_runs) / len(relevant_metrics) if relevant_metrics else 0,
            "avg_duration_seconds": avg_duration,
            "total_records_processed": total_records,
            "total_errors": total_errors,
            "records_per_second": total_records / (avg_duration * len(successful_runs)) if successful_runs else 0
        }
