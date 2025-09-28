"""Unsupervised anomaly detection for trading opportunities."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterable, Literal, Mapping, Optional

import numpy as np
import pandas as pd


AnomalySide = Literal["long", "short", "neutral"]


@dataclass(frozen=True)
class AnomalyEvent:
    index: int
    timestamp: object
    value: float | None
    z_score: float
    side: AnomalySide
    method: str = "zscore"
    baseline: float | None = None
    metadata: Optional[Mapping[str, float]] = None


class AdaptiveAnomalyDetector:
    """Hybrid z-score and MAD-based anomaly detector with adaptive thresholds."""

    def __init__(
        self,
        *,
        window: int = 48,
        z_threshold: float = 3.0,
        mad_threshold: float = 3.5,
        cooldown: int = 0,
    ) -> None:
        if window < 5:
            raise ValueError("window must be >= 5")
        self.window = int(window)
        self.z_threshold = float(z_threshold)
        self.mad_threshold = float(mad_threshold)
        self.cooldown = int(max(0, cooldown))
        self._buffer: deque[float] = deque(maxlen=self.window)
        self._count = 0
        self._last_emit_index = -self.cooldown - 1

    def reset(self) -> None:
        self._buffer.clear()
        self._count = 0
        self._last_emit_index = -self.cooldown - 1

    def fit(self, series: pd.Series) -> None:
        self.reset()
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        for value in s.iloc[-self.window :]:
            self._buffer.append(float(value))
        self._count = len(s)

    def detect(self, series: pd.Series) -> pd.DataFrame:
        self.reset()
        s = pd.to_numeric(series, errors="coerce").astype(float)
        s = s.dropna()
        events: list[AnomalyEvent] = []
        for idx, (ts, value) in enumerate(s.items()):
            event = self.update(float(value), timestamp=ts, index_position=idx)
            if event is not None:
                events.append(event)
        return _events_to_frame(events)

    def update(self, value: float, *, timestamp: object | None = None, index_position: Optional[int] = None) -> AnomalyEvent | None:
        if not np.isfinite(value):
            return None
        if len(self._buffer) < self.window:
            self._buffer.append(float(value))
            self._count += 1
            return None

        buffer_arr = np.array(self._buffer, dtype=float)
        mean = float(np.mean(buffer_arr))
        std = float(np.std(buffer_arr, ddof=1)) if len(buffer_arr) > 1 else 0.0
        z_score = float((value - mean) / std) if std > 0 else 0.0

        median = float(np.median(buffer_arr))
        mad = float(np.median(np.abs(buffer_arr - median)))
        mad_score = 0.0 if mad == 0 else 0.6745 * (value - median) / mad

        triggered = abs(z_score) >= self.z_threshold or abs(mad_score) >= self.mad_threshold
        current_index = index_position if index_position is not None else self._count
        if triggered:
            if self.cooldown and (current_index - self._last_emit_index) <= self.cooldown:
                triggered = False

        side: AnomalySide = "neutral"
        if triggered:
            side = "short" if value > mean else ("long" if value < mean else "neutral")
            metadata = {
                "mad_score": float(mad_score),
                "window_mean": mean,
                "window_std": std,
            }
            event = AnomalyEvent(
                index=current_index,
                timestamp=timestamp if timestamp is not None else current_index,
                value=float(value),
                z_score=float(z_score),
                side=side,
                method="adaptive",
                baseline=mean,
                metadata=metadata,
            )
            self._last_emit_index = current_index
        else:
            event = None

        self._buffer.append(float(value))
        self._count = current_index + 1
        return event


def detect_anomalies(
    series: pd.Series,
    *,
    window: int = 24,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Detect outliers using the adaptive detector for backward compatibility."""
    detector = AdaptiveAnomalyDetector(window=max(window, 5), z_threshold=z_threshold)
    return detector.detect(series)


def _events_to_frame(events: Iterable[AnomalyEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(
            columns=["index", "timestamp", "value", "z_score", "side", "method", "baseline", "metadata"]
        )
    rows = []
    for event in events:
        row = {
            "index": event.index,
            "timestamp": event.timestamp,
            "value": event.value,
            "z_score": event.z_score,
            "side": event.side,
            "method": event.method,
            "baseline": event.baseline,
            "metadata": dict(event.metadata or {}),
        }
        rows.append(row)
    return pd.DataFrame(rows)


__all__ = ["AnomalyEvent", "AdaptiveAnomalyDetector", "detect_anomalies"]

