"""Core cost profiler for data ingestion pipelines."""

from __future__ import annotations

import time
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from pathlib import Path

from ..logging import StructuredLogger, LogLevel, create_logger


class MetricType(str, Enum):
    """Types of metrics collected by the profiler."""
    THROUGHPUT_ROWS_PER_SECOND = "throughput_rows_per_second"
    THROUGHPUT_BYTES_PER_SECOND = "throughput_bytes_per_second"
    LATENCY_SECONDS = "latency_seconds"
    RETRY_COUNT = "retry_count"
    ERROR_RATE = "error_rate"
    API_CALL_COUNT = "api_call_count"
    DATA_QUALITY_SCORE = "data_quality_score"
    PROCESSING_TIME_SECONDS = "processing_time_seconds"
    MEMORY_USAGE_MB = "memory_usage_mb"
    CPU_USAGE_PERCENT = "cpu_usage_percent"


@dataclass
class PerformanceMetrics:
    """Performance metrics for a dataset ingestion."""

    dataset_name: str
    data_source: str
    record_count: int = 0
    byte_count: int = 0
    processing_time_seconds: float = 0.0
    api_call_count: int = 0
    retry_count: int = 0
    error_count: int = 0
    memory_peak_mb: float = 0.0
    cpu_peak_percent: float = 0.0
    start_time: str = field(default_factory=lambda: datetime.now().isoformat())
    end_time: Optional[str] = None

    def calculate_throughput(self) -> Dict[str, float]:
        """Calculate throughput metrics.

        Returns:
            Dictionary with throughput metrics
        """
        if self.processing_time_seconds == 0:
            return {
                "rows_per_second": 0.0,
                "bytes_per_second": 0.0
            }

        return {
            "rows_per_second": self.record_count / self.processing_time_seconds,
            "bytes_per_second": self.byte_count / self.processing_time_seconds
        }

    def calculate_efficiency(self) -> Dict[str, float]:
        """Calculate efficiency metrics.

        Returns:
            Dictionary with efficiency metrics
        """
        throughput = self.calculate_throughput()

        return {
            "error_rate": self.error_count / max(self.api_call_count, 1),
            "retry_rate": self.retry_count / max(self.api_call_count, 1),
            "records_per_api_call": self.record_count / max(self.api_call_count, 1),
            "data_quality_score": 1.0 - (self.error_count / max(self.record_count, 1))
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        throughput = self.calculate_throughput()
        efficiency = self.calculate_efficiency()

        return {
            "dataset_name": self.dataset_name,
            "data_source": self.data_source,
            "record_count": self.record_count,
            "byte_count": self.byte_count,
            "processing_time_seconds": self.processing_time_seconds,
            "api_call_count": self.api_call_count,
            "retry_count": self.retry_count,
            "error_count": self.error_count,
            "memory_peak_mb": self.memory_peak_mb,
            "cpu_peak_percent": self.cpu_peak_percent,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "throughput": throughput,
            "efficiency": efficiency
        }


@dataclass
class DatasetProfile:
    """Profile for a specific dataset."""

    dataset_name: str
    data_source: str
    profile_id: str
    metrics: List[PerformanceMetrics] = field(default_factory=list)
    baseline_metrics: Optional[PerformanceMetrics] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def add_metrics(self, metrics: PerformanceMetrics) -> None:
        """Add performance metrics to this profile.

        Args:
            metrics: Performance metrics to add
        """
        self.metrics.append(metrics)

        # Update end time
        if metrics.end_time:
            metrics.end_time = metrics.end_time

        # Update baseline if this is better than current
        if not self.baseline_metrics:
            self.baseline_metrics = metrics
        elif self._is_better_metrics(metrics, self.baseline_metrics):
            self.baseline_metrics = metrics

    def _is_better_metrics(self, new: PerformanceMetrics, baseline: PerformanceMetrics) -> bool:
        """Check if new metrics are better than baseline.

        Args:
            new: New metrics to compare
            baseline: Current baseline metrics

        Returns:
            True if new metrics are better
        """
        new_throughput = new.calculate_throughput()
        baseline_throughput = baseline.calculate_throughput()

        # Better if higher throughput and lower error rate
        return (
            new_throughput["rows_per_second"] > baseline_throughput["rows_per_second"] and
            new.error_count <= baseline.error_count
        )

    def get_average_metrics(self) -> Optional[PerformanceMetrics]:
        """Get average metrics across all runs.

        Returns:
            Average performance metrics or None if no metrics
        """
        if not self.metrics:
            return None

        total_metrics = PerformanceMetrics(
            dataset_name=self.dataset_name,
            data_source=self.data_source
        )

        for metrics in self.metrics:
            total_metrics.record_count += metrics.record_count
            total_metrics.byte_count += metrics.byte_count
            total_metrics.processing_time_seconds += metrics.processing_time_seconds
            total_metrics.api_call_count += metrics.api_call_count
            total_metrics.retry_count += metrics.retry_count
            total_metrics.error_count += metrics.error_count
            total_metrics.memory_peak_mb = max(total_metrics.memory_peak_mb, metrics.memory_peak_mb)
            total_metrics.cpu_peak_percent = max(total_metrics.cpu_peak_percent, metrics.cpu_peak_percent)

        # Calculate averages
        count = len(self.metrics)
        total_metrics.processing_time_seconds /= count
        total_metrics.api_call_count = int(total_metrics.api_call_count / count)
        total_metrics.retry_count = int(total_metrics.retry_count / count)
        total_metrics.error_count = int(total_metrics.error_count / count)

        return total_metrics

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for this dataset.

        Returns:
            Performance summary dictionary
        """
        if not self.metrics:
            return {}

        avg_metrics = self.get_average_metrics()
        if not avg_metrics:
            return {}

        summary = {
            "dataset_name": self.dataset_name,
            "data_source": self.data_source,
            "run_count": len(self.metrics),
            "average_metrics": avg_metrics.to_dict() if avg_metrics else {},
            "baseline_metrics": self.baseline_metrics.to_dict() if self.baseline_metrics else {},
            "improvement_over_baseline": self._calculate_improvement()
        }

        return summary

    def _calculate_improvement(self) -> Dict[str, float]:
        """Calculate improvement over baseline.

        Returns:
            Improvement metrics
        """
        if not self.baseline_metrics or not self.metrics:
            return {}

        avg_metrics = self.get_average_metrics()
        if not avg_metrics:
            return {}

        baseline_throughput = self.baseline_metrics.calculate_throughput()
        avg_throughput = avg_metrics.calculate_throughput()

        return {
            "rows_per_second_improvement": (
                avg_throughput["rows_per_second"] / baseline_throughput["rows_per_second"] - 1
            ) * 100,
            "error_rate_improvement": (
                (self.baseline_metrics.error_count / max(self.baseline_metrics.record_count, 1)) /
                (avg_metrics.error_count / max(avg_metrics.record_count, 1)) - 1
            ) * 100
        }


@dataclass
class ProfilerConfig:
    """Configuration for cost profiler."""

    # Collection settings
    collection_interval_seconds: float = 1.0
    enable_memory_monitoring: bool = True
    enable_cpu_monitoring: bool = False  # Usually too expensive
    max_metric_history: int = 1000

    # Cost estimation
    cost_per_api_call_usd: float = 0.001  # Cost per API call
    cost_per_gb_processed_usd: float = 0.02  # Cost per GB processed
    cost_per_hour_compute_usd: float = 0.05  # Cost per hour of compute

    # Analysis thresholds
    throughput_warning_threshold: float = 100.0  # Rows per second
    error_rate_warning_threshold: float = 0.05   # 5% error rate
    retry_rate_warning_threshold: float = 0.1    # 10% retry rate

    # Reporting
    report_interval_minutes: int = 15
    enable_detailed_logging: bool = True

    # Data source specific settings
    data_source_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class CostProfiler:
    """Main cost profiler for data ingestion pipelines."""

    def __init__(self, config: ProfilerConfig):
        """Initialize cost profiler.

        Args:
            config: Profiler configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="cost_profiler",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.cost_profiler.metrics",
            dataset="cost_profiling"
        )

        # Profile storage
        self.dataset_profiles: Dict[str, DatasetProfile] = {}
        self.active_profiling_sessions: Dict[str, str] = {}

        # Metrics tracking
        self.metrics_history: Dict[str, List[PerformanceMetrics]] = {}

        self.logger.log(
            LogLevel.INFO,
            "Initialized cost profiler",
            "cost_profiler_initialized",
            collection_interval_seconds=config.collection_interval_seconds,
            enable_memory_monitoring=config.enable_memory_monitoring
        )

    def start_profiling(
        self,
        dataset_name: str,
        data_source: str,
        session_id: Optional[str] = None
    ) -> str:
        """Start profiling a dataset ingestion.

        Args:
            dataset_name: Name of the dataset being profiled
            data_source: Data source (eia, fred, cpi, noaa, iso)
            session_id: Optional session identifier

        Returns:
            Profile session ID
        """
        if session_id is None:
            session_id = f"{dataset_name}_{data_source}_{int(time.time())}"

        # Create or get dataset profile
        if dataset_name not in self.dataset_profiles:
            self.dataset_profiles[dataset_name] = DatasetProfile(
                dataset_name=dataset_name,
                data_source=data_source,
                profile_id=session_id
            )

        self.active_profiling_sessions[session_id] = dataset_name

        self.logger.log(
            LogLevel.INFO,
            f"Started profiling session {session_id} for {dataset_name}",
            "profiling_session_started",
            session_id=session_id,
            dataset_name=dataset_name,
            data_source=data_source
        )

        return session_id

    def end_profiling(self, session_id: str) -> Optional[DatasetProfile]:
        """End a profiling session.

        Args:
            session_id: Profiling session ID

        Returns:
            Dataset profile if found
        """
        if session_id not in self.active_profiling_sessions:
            self.logger.log(
                LogLevel.WARNING,
                f"Profiling session {session_id} not found",
                "profiling_session_not_found",
                session_id=session_id
            )
            return None

        dataset_name = self.active_profiling_sessions[session_id]
        profile = self.dataset_profiles.get(dataset_name)

        if profile:
            profile.end_time = datetime.now().isoformat()

        # Clean up active session
        del self.active_profiling_sessions[session_id]

        self.logger.log(
            LogLevel.INFO,
            f"Ended profiling session {session_id} for {dataset_name}",
            "profiling_session_ended",
            session_id=session_id,
            dataset_name=dataset_name
        )

        return profile

    def record_metrics(
        self,
        session_id: str,
        metrics: PerformanceMetrics
    ) -> None:
        """Record performance metrics for a profiling session.

        Args:
            session_id: Profiling session ID
            metrics: Performance metrics to record
        """
        if session_id not in self.active_profiling_sessions:
            self.logger.log(
                LogLevel.WARNING,
                f"Cannot record metrics for inactive session {session_id}",
                "invalid_session_metrics",
                session_id=session_id
            )
            return

        dataset_name = self.active_profiling_sessions[session_id]

        # Add to dataset profile
        if dataset_name in self.dataset_profiles:
            self.dataset_profiles[dataset_name].add_metrics(metrics)

        # Add to metrics history
        if dataset_name not in self.metrics_history:
            self.metrics_history[dataset_name] = []

        self.metrics_history[dataset_name].append(metrics)

        # Trim history if too large
        if len(self.metrics_history[dataset_name]) > self.config.max_metric_history:
            self.metrics_history[dataset_name] = self.metrics_history[dataset_name][-self.config.max_metric_history:]

        if self.config.enable_detailed_logging:
            throughput = metrics.calculate_throughput()
            efficiency = metrics.calculate_efficiency()

            self.logger.log(
                LogLevel.DEBUG,
                f"Recorded metrics for {dataset_name}: {throughput['rows_per_second']".1f"} rows/s, {efficiency['error_rate']".2%"} error rate",
                "metrics_recorded",
                session_id=session_id,
                dataset_name=dataset_name,
                record_count=metrics.record_count,
                throughput_rows_per_second=throughput["rows_per_second"],
                error_rate=efficiency["error_rate"]
            )

    def get_dataset_profile(self, dataset_name: str) -> Optional[DatasetProfile]:
        """Get profile for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dataset profile or None if not found
        """
        return self.dataset_profiles.get(dataset_name)

    def analyze_performance_issues(self) -> List[Dict[str, Any]]:
        """Analyze performance issues across all datasets.

        Returns:
            List of performance issues found
        """
        issues = []

        for dataset_name, profile in self.dataset_profiles.items():
            avg_metrics = profile.get_average_metrics()
            if not avg_metrics:
                continue

            throughput = avg_metrics.calculate_throughput()
            efficiency = avg_metrics.calculate_efficiency()

            # Check throughput
            if throughput["rows_per_second"] < self.config.throughput_warning_threshold:
                issues.append({
                    "dataset_name": dataset_name,
                    "issue_type": "LOW_THROUGHPUT",
                    "severity": "WARNING",
                    "message": f"Low throughput: {throughput['rows_per_second']".1f"} rows/s",
                    "current_value": throughput["rows_per_second"],
                    "threshold": self.config.throughput_warning_threshold,
                    "metrics": avg_metrics.to_dict()
                })

            # Check error rate
            if efficiency["error_rate"] > self.config.error_rate_warning_threshold:
                issues.append({
                    "dataset_name": dataset_name,
                    "issue_type": "HIGH_ERROR_RATE",
                    "severity": "ERROR",
                    "message": f"High error rate: {efficiency['error_rate']".2%"}",
                    "current_value": efficiency["error_rate"],
                    "threshold": self.config.error_rate_warning_threshold,
                    "metrics": avg_metrics.to_dict()
                })

            # Check retry rate
            if efficiency["retry_rate"] > self.config.retry_rate_warning_threshold:
                issues.append({
                    "dataset_name": dataset_name,
                    "issue_type": "HIGH_RETRY_RATE",
                    "severity": "WARNING",
                    "message": f"High retry rate: {efficiency['retry_rate']".2%"}",
                    "current_value": efficiency["retry_rate"],
                    "threshold": self.config.retry_rate_warning_threshold,
                    "metrics": avg_metrics.to_dict()
                })

        return issues

    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report.

        Returns:
            Performance report dictionary
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_datasets": len(self.dataset_profiles),
            "total_sessions": len(self.active_profiling_sessions),
            "performance_issues": self.analyze_performance_issues(),
            "dataset_summaries": {}
        }

        # Generate summaries for each dataset
        for dataset_name, profile in self.dataset_profiles.items():
            summary = profile.get_performance_summary()
            if summary:
                report["dataset_summaries"][dataset_name] = summary

        # Calculate overall statistics
        total_records = sum(
            profile.get_average_metrics().record_count
            for profile in self.dataset_profiles.values()
            if profile.get_average_metrics()
        )

        avg_throughput = sum(
            profile.get_average_metrics().calculate_throughput()["rows_per_second"]
            for profile in self.dataset_profiles.values()
            if profile.get_average_metrics()
        ) / max(len(self.dataset_profiles), 1)

        report["overall_statistics"] = {
            "total_records_processed": total_records,
            "average_throughput_rows_per_second": avg_throughput,
            "datasets_with_issues": len([
                issue for issue in report["performance_issues"]
                if issue["severity"] in ["ERROR", "CRITICAL"]
            ])
        }

        return report

    def save_profile(self, dataset_name: str, file_path: Path) -> bool:
        """Save dataset profile to file.

        Args:
            dataset_name: Name of the dataset
            file_path: Path to save profile

        Returns:
            True if saved successfully
        """
        profile = self.get_dataset_profile(dataset_name)
        if not profile:
            return False

        try:
            with open(file_path, 'w') as f:
                json.dump(profile.get_performance_summary(), f, indent=2, default=str)

            self.logger.log(
                LogLevel.INFO,
                f"Saved profile for {dataset_name} to {file_path}",
                "profile_saved",
                dataset_name=dataset_name,
                file_path=str(file_path)
            )

            return True

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to save profile for {dataset_name}: {e}",
                "profile_save_failed",
                dataset_name=dataset_name,
                error=str(e)
            )

            return False

    def load_profile(self, file_path: Path) -> Optional[str]:
        """Load dataset profile from file.

        Args:
            file_path: Path to profile file

        Returns:
            Dataset name if loaded successfully, None otherwise
        """
        try:
            with open(file_path, 'r') as f:
                profile_data = json.load(f)

            dataset_name = profile_data.get("dataset_name")
            if not dataset_name:
                return None

            # Create profile object (simplified for loading)
            profile = DatasetProfile(
                dataset_name=dataset_name,
                data_source=profile_data.get("data_source", ""),
                profile_id=f"loaded_{int(time.time())}"
            )

            self.dataset_profiles[dataset_name] = profile

            self.logger.log(
                LogLevel.INFO,
                f"Loaded profile for {dataset_name} from {file_path}",
                "profile_loaded",
                dataset_name=dataset_name,
                file_path=str(file_path)
            )

            return dataset_name

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to load profile from {file_path}: {e}",
                "profile_load_failed",
                file_path=str(file_path),
                error=str(e)
            )

            return None
