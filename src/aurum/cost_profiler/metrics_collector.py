"""Metrics collection for cost profiling."""

from __future__ import annotations

import time
import psutil
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from threading import Thread, Event

from .profiler import MetricType, PerformanceMetrics


@dataclass
class MetricSample:
    """A single metric sample."""

    metric_type: MetricType
    value: Union[int, float]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "metric_type": self.metric_type.value,
            "value": self.value,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }


@dataclass
class CollectionConfig:
    """Configuration for metrics collection."""

    collection_interval_seconds: float = 1.0
    enable_system_metrics: bool = True
    enable_api_metrics: bool = True
    buffer_size: int = 1000
    max_collection_time_seconds: float = 300.0  # 5 minutes
    callbacks: Dict[str, Callable] = field(default_factory=dict)


class MetricsCollector:
    """Collect performance metrics during data ingestion."""

    def __init__(self, config: CollectionConfig):
        """Initialize metrics collector.

        Args:
            config: Collection configuration
        """
        self.config = config

        # Collection state
        self.is_collecting = False
        self.collection_thread: Optional[Thread] = None
        self.stop_event = Event()

        # Metrics storage
        self.metrics_buffer: Dict[str, List[MetricSample]] = {}
        self.current_session_metrics = PerformanceMetrics(
            dataset_name="",
            data_source=""
        )

        # System monitoring
        self.process = psutil.Process()
        self.system_start_time = time.time()

    def start_collection(
        self,
        dataset_name: str,
        data_source: str,
        session_callbacks: Optional[Dict[str, Callable]] = None
    ) -> None:
        """Start metrics collection.

        Args:
            dataset_name: Name of the dataset being processed
            data_source: Data source identifier
            session_callbacks: Optional callbacks for this session
        """
        if self.is_collecting:
            self.stop_collection()

        # Initialize session
        self.current_session_metrics = PerformanceMetrics(
            dataset_name=dataset_name,
            data_source=data_source,
            start_time=datetime.now().isoformat()
        )

        self.is_collecting = True
        self.stop_event.clear()

        # Start collection thread
        self.collection_thread = Thread(
            target=self._collection_loop,
            args=(session_callbacks or {},)
        )
        self.collection_thread.daemon = True
        self.collection_thread.start()

    def stop_collection(self) -> PerformanceMetrics:
        """Stop metrics collection and return final metrics.

        Returns:
            Final performance metrics
        """
        if not self.is_collecting:
            return self.current_session_metrics

        # Stop collection
        self.is_collecting = False
        self.stop_event.set()

        if self.collection_thread:
            self.collection_thread.join(timeout=5.0)

        # Finalize metrics
        self.current_session_metrics.end_time = datetime.now().isoformat()

        return self.current_session_metrics

    def record_api_call(self, endpoint: str, response_time_seconds: float, success: bool = True) -> None:
        """Record an API call metric.

        Args:
            endpoint: API endpoint called
            response_time_seconds: Response time in seconds
            success: Whether the call was successful
        """
        # Update session metrics
        self.current_session_metrics.api_call_count += 1
        self.current_session_metrics.processing_time_seconds += response_time_seconds

        if not success:
            self.current_session_metrics.error_count += 1

        # Record samples
        latency_sample = MetricSample(
            metric_type=MetricType.LATENCY_SECONDS,
            value=response_time_seconds,
            metadata={"endpoint": endpoint, "success": success}
        )

        self._record_sample("api_calls", latency_sample)

    def record_retry(self, endpoint: str, retry_count: int) -> None:
        """Record a retry event.

        Args:
            endpoint: API endpoint that was retried
            retry_count: Number of retries
        """
        self.current_session_metrics.retry_count += retry_count

        retry_sample = MetricSample(
            metric_type=MetricType.RETRY_COUNT,
            value=retry_count,
            metadata={"endpoint": endpoint}
        )

        self._record_sample("retries", retry_sample)

    def record_data_processing(
        self,
        record_count: int,
        byte_count: int,
        processing_time_seconds: float
    ) -> None:
        """Record data processing metrics.

        Args:
            record_count: Number of records processed
            byte_count: Number of bytes processed
            processing_time_seconds: Processing time in seconds
        """
        self.current_session_metrics.record_count += record_count
        self.current_session_metrics.byte_count += byte_count
        self.current_session_metrics.processing_time_seconds += processing_time_seconds

        throughput_sample = MetricSample(
            metric_type=MetricType.THROUGHPUT_ROWS_PER_SECOND,
            value=record_count / max(processing_time_seconds, 0.001),
            metadata={"records": record_count, "bytes": byte_count}
        )

        self._record_sample("throughput", throughput_sample)

    def record_error(self, error_type: str, error_message: str) -> None:
        """Record an error event.

        Args:
            error_type: Type of error
            error_message: Error message
        """
        self.current_session_metrics.error_count += 1

        error_sample = MetricSample(
            metric_type=MetricType.ERROR_RATE,
            value=1.0,
            metadata={"error_type": error_type, "error_message": error_message}
        )

        self._record_sample("errors", error_sample)

    def get_current_metrics(self) -> PerformanceMetrics:
        """Get current performance metrics.

        Returns:
            Current performance metrics
        """
        # Update system metrics
        if self.config.enable_system_metrics:
            self._update_system_metrics()

        return self.current_session_metrics

    def _collection_loop(self, session_callbacks: Dict[str, Callable]) -> None:
        """Main collection loop.

        Args:
            session_callbacks: Callbacks for this session
        """
        last_collection_time = time.time()

        while not self.stop_event.is_set():
            try:
                # Collect system metrics
                if self.config.enable_system_metrics:
                    self._collect_system_metrics()

                # Call session callbacks
                for callback_name, callback in session_callbacks.items():
                    try:
                        callback(self)
                    except Exception as e:
                        # Log callback errors but don't stop collection
                        pass

                # Sleep for collection interval
                elapsed = time.time() - last_collection_time
                sleep_time = max(0, self.config.collection_interval_seconds - elapsed)

                if sleep_time > 0:
                    self.stop_event.wait(sleep_time)

                last_collection_time = time.time()

            except Exception as e:
                # Log collection errors but continue
                pass

    def _collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            # Memory usage
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            self.current_session_metrics.memory_peak_mb = max(
                self.current_session_metrics.memory_peak_mb,
                memory_mb
            )

            # CPU usage (expensive, only if enabled)
            if self.config.enable_system_metrics:
                try:
                    cpu_percent = self.process.cpu_percent(interval=0.1)
                    self.current_session_metrics.cpu_peak_percent = max(
                        self.current_session_metrics.cpu_peak_percent,
                        cpu_percent
                    )
                except Exception:
                    # CPU monitoring failed, skip
                    pass

        except Exception:
            # System metrics collection failed, skip
            pass

    def _update_system_metrics(self) -> None:
        """Update system metrics in current session."""
        try:
            # Update peak memory
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            self.current_session_metrics.memory_peak_mb = max(
                self.current_session_metrics.memory_peak_mb,
                memory_mb
            )

        except Exception:
            # System metrics update failed, skip
            pass

    def _record_sample(self, category: str, sample: MetricSample) -> None:
        """Record a metric sample.

        Args:
            category: Metric category
            sample: Metric sample to record
        """
        if category not in self.metrics_buffer:
            self.metrics_buffer[category] = []

        self.metrics_buffer[category].append(sample)

        # Trim buffer if too large
        if len(self.metrics_buffer[category]) > self.config.buffer_size:
            self.metrics_buffer[category] = self.metrics_buffer[category][-self.config.buffer_size:]

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics.

        Returns:
            Metrics summary
        """
        total_samples = sum(len(samples) for samples in self.metrics_buffer.values())

        summary = {
            "is_collecting": self.is_collecting,
            "total_samples": total_samples,
            "categories": list(self.metrics_buffer.keys()),
            "current_session": self.current_session_metrics.to_dict(),
            "samples_by_category": {
                category: len(samples) for category, samples in self.metrics_buffer.items()
            }
        }

        return summary

    def get_recent_samples(
        self,
        category: str,
        limit: int = 10
    ) -> List[MetricSample]:
        """Get recent samples for a category.

        Args:
            category: Metric category
            limit: Maximum number of samples to return

        Returns:
            List of recent metric samples
        """
        if category not in self.metrics_buffer:
            return []

        samples = self.metrics_buffer[category]
        return samples[-limit:]


class SeaTunnelMetricsCollector(MetricsCollector):
    """Metrics collector specifically for SeaTunnel jobs."""

    def __init__(self, config: CollectionConfig, job_config: Dict[str, Any]):
        """Initialize SeaTunnel metrics collector.

        Args:
            config: Collection configuration
            job_config: SeaTunnel job configuration
        """
        super().__init__(config)
        self.job_config = job_config
        self.source_metrics: Dict[str, Dict[str, Any]] = {}

    def record_seatunnel_source_metrics(
        self,
        source_name: str,
        record_count: int,
        processing_time: float
    ) -> None:
        """Record metrics from SeaTunnel source.

        Args:
            source_name: Name of the SeaTunnel source
            record_count: Number of records processed
            processing_time: Processing time in seconds
        """
        if source_name not in self.source_metrics:
            self.source_metrics[source_name] = {
                "record_count": 0,
                "total_time": 0.0,
                "call_count": 0
            }

        self.source_metrics[source_name]["record_count"] += record_count
        self.source_metrics[source_name]["total_time"] += processing_time
        self.source_metrics[source_name]["call_count"] += 1

        # Record as regular data processing
        self.record_data_processing(record_count, 0, processing_time)  # 0 bytes for now

    def record_seatunnel_sink_metrics(
        self,
        sink_name: str,
        record_count: int,
        processing_time: float
    ) -> None:
        """Record metrics from SeaTunnel sink.

        Args:
            sink_name: Name of the SeaTunnel sink
            record_count: Number of records processed
            processing_time: Processing time in seconds
        """
        # Similar to source metrics but for sinks
        self.record_data_processing(record_count, 0, processing_time)

    def record_kafka_metrics(
        self,
        topic: str,
        partition_count: int,
        message_count: int,
        byte_count: int
    ) -> None:
        """Record Kafka-specific metrics.

        Args:
            topic: Kafka topic name
            partition_count: Number of partitions
            message_count: Number of messages
            byte_count: Number of bytes
        """
        self.current_session_metrics.byte_count += byte_count

        kafka_sample = MetricSample(
            metric_type=MetricType.THROUGHPUT_BYTES_PER_SECOND,
            value=byte_count,
            metadata={
                "topic": topic,
                "partition_count": partition_count,
                "message_count": message_count
            }
        )

        self._record_sample("kafka", kafka_sample)

    def get_seatunnel_performance_report(self) -> Dict[str, Any]:
        """Get SeaTunnel-specific performance report.

        Returns:
            Performance report for SeaTunnel job
        """
        base_metrics = self.get_current_metrics()

        # Calculate per-source metrics
        source_performance = {}
        for source_name, metrics in self.source_metrics.items():
            avg_time_per_call = (
                metrics["total_time"] / max(metrics["call_count"], 1)
            )
            avg_records_per_call = (
                metrics["record_count"] / max(metrics["call_count"], 1)
            )

            source_performance[source_name] = {
                "total_calls": metrics["call_count"],
                "total_records": metrics["record_count"],
                "total_time": metrics["total_time"],
                "avg_time_per_call": avg_time_per_call,
                "avg_records_per_call": avg_records_per_call,
                "records_per_second": avg_records_per_call / max(avg_time_per_call, 0.001)
            }

        return {
            "job_config_summary": {
                "sources": len(self.source_metrics),
                "job_mode": self.job_config.get("env", {}).get("job.mode", "unknown"),
                "parallelism": self.job_config.get("env", {}).get("parallelism", 1)
            },
            "source_performance": source_performance,
            "base_metrics": base_metrics.to_dict(),
            "recommendations": self._generate_seatunnel_recommendations()
        }

    def _generate_seatunnel_recommendations(self) -> List[str]:
        """Generate SeaTunnel-specific recommendations.

        Returns:
            List of recommendation strings
        """
        recommendations = []

        # Check for performance issues
        if self.current_session_metrics.api_call_count > 1000:
            recommendations.append(
                "High API call count detected - consider batch processing"
            )

        if self.current_session_metrics.retry_count > self.current_session_metrics.api_call_count * 0.1:
            recommendations.append(
                "High retry rate - check API rate limits and error handling"
            )

        if self.current_session_metrics.memory_peak_mb > 1000:
            recommendations.append(
                "High memory usage - consider increasing SeaTunnel memory allocation"
            )

        # Check source performance
        for source_name, metrics in self.source_metrics.items():
            if metrics["avg_records_per_call"] < 10:
                recommendations.append(
                    f"Low records per call for source {source_name} - consider increasing page size"
                )

        return recommendations
