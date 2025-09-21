"""Metrics collection for structured logging."""

from __future__ import annotations

import time
from typing import Dict, Optional
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class LoggingMetrics:
    """Metrics collection for logging operations."""

    # Counters
    log_events_total: int = 0
    log_events_by_level: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    log_events_by_source: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    log_events_by_type: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Histograms
    log_processing_time: Dict[str, list] = field(default_factory=lambda: defaultdict(list))

    # Gauges
    active_sessions: int = 0
    kafka_backlog: int = 0
    clickhouse_backlog: int = 0

    def record_log_event(
        self,
        level: str,
        source_name: str,
        event_type: str,
        processing_time_ms: Optional[float] = None
    ) -> None:
        """Record a log event metric.

        Args:
            level: Log level
            source_name: Source name
            event_type: Event type
            processing_time_ms: Processing time in milliseconds
        """
        self.log_events_total += 1
        self.log_events_by_level[level] += 1
        self.log_events_by_source[source_name] += 1
        self.log_events_by_type[event_type] += 1

        if processing_time_ms is not None:
            self.log_processing_time[f"{source_name}.{event_type}"].append(processing_time_ms)

    def get_summary(self) -> Dict[str, any]:
        """Get metrics summary.

        Returns:
            Dictionary with metrics summary
        """
        # Calculate percentiles for processing times
        processing_percentiles = {}
        for key, times in self.log_processing_time.items():
            if times:
                sorted_times = sorted(times)
                processing_percentiles[key] = {
                    "p50": sorted_times[len(sorted_times) // 2] if sorted_times else 0,
                    "p95": sorted_times[int(len(sorted_times) * 0.95)] if len(sorted_times) > 1 else 0,
                    "p99": sorted_times[int(len(sorted_times) * 0.99)] if len(sorted_times) > 1 else 0,
                    "mean": sum(times) / len(times) if times else 0,
                    "count": len(times)
                }

        return {
            "log_events_total": self.log_events_total,
            "log_events_by_level": dict(self.log_events_by_level),
            "log_events_by_source": dict(self.log_events_by_source),
            "log_events_by_type": dict(self.log_events_by_type),
            "processing_time_percentiles": processing_percentiles,
            "active_sessions": self.active_sessions,
            "kafka_backlog": self.kafka_backlog,
            "clickhouse_backlog": self.clickhouse_backlog
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self.log_events_total = 0
        self.log_events_by_level.clear()
        self.log_events_by_source.clear()
        self.log_events_by_type.clear()
        self.log_processing_time.clear()
        self.active_sessions = 0
        self.kafka_backlog = 0
        self.clickhouse_backlog = 0


# Global metrics instance
global_metrics = LoggingMetrics()
