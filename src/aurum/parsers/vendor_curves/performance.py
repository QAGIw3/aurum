"""Performance analytics helpers for vendor parser execution."""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterator


@dataclass
class PerformanceMetrics:
    """Captures execution timings and row counts for a parser run."""

    durations: Dict[str, float] = field(default_factory=dict)
    row_count: int = 0

    def mark_rows(self, count: int) -> None:
        self.row_count = count


class PerformanceTracker:
    """Lightweight instrumentation for parser pipeline sections."""

    def __init__(self) -> None:
        self.metrics = PerformanceMetrics()

    @contextmanager
    def track(self, label: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            end = time.perf_counter()
            self.metrics.durations[label] = end - start


__all__ = ["PerformanceTracker", "PerformanceMetrics"]
