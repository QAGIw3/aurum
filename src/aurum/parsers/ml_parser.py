"""Machine learning inspired anomaly detection for vendor curve data."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Anomaly:
    """Representation of a single anomaly candidate."""

    index: int
    curve_key: Optional[str]
    tenor_label: Optional[str]
    column: str
    score: float
    value: Optional[float]
    threshold: float


@dataclass(frozen=True)
class AnomalyResult:
    """Detection output containing anomalies and global confidence score."""

    anomalies: pd.DataFrame
    confidence_score: float
    summary: Mapping[str, float]


class CurveAnomalyDetector:
    """Detect anomalous price points using robust statistics.

    The detector computes a median absolute deviation (MAD) score per curve and
    marks observations whose robust z-score exceeds the configured threshold.
    This keeps the implementation dependency-light, while providing behaviour
    similar to an unsupervised anomaly detection model.
    """

    def __init__(
        self,
        *,
        value_columns: Sequence[str] | None = None,
        group_column: str = "curve_key",
        zscore_threshold: float = 3.0,
        min_points: int = 6,
    ) -> None:
        self.value_columns = tuple(value_columns or ("mid", "value", "bid", "ask"))
        self.group_column = group_column
        self.zscore_threshold = zscore_threshold
        self.min_points = min_points

    def detect(self, frame: pd.DataFrame) -> AnomalyResult:
        if frame.empty or (self.group_column not in frame.columns) or frame[self.group_column].dropna().empty:
            return AnomalyResult(
                anomalies=pd.DataFrame(columns=[
                    "index",
                    self.group_column,
                    "tenor_label",
                    "column",
                    "score",
                    "value",
                    "threshold",
                ]),
                confidence_score=1.0,
                summary={},
            )

        anomalies: list[Anomaly] = []
        total_points = 0
        group_items = [(None, frame)]
        for group_key, group_df in group_items:
            total_points += len(group_df)
            for column in self.value_columns:
                if column not in group_df:
                    continue
                series = pd.to_numeric(group_df[column], errors="coerce")
                valid = series.dropna()
                if valid.empty or len(valid) < self.min_points:
                    continue

                median = valid.median()
                mad = np.median(np.abs(valid - median))
                if mad == 0:
                    std = valid.std(ddof=0)
                    if std == 0 or np.isnan(std):
                        continue
                    scale = std
                else:
                    scale = 1.4826 * mad  # ~std for normal dist

                deviations = (series - median) / scale
                mask = deviations.abs() > self.zscore_threshold
                for idx, deviation in deviations[mask].dropna().items():
                    value = series.loc[idx]
                    anomalies.append(
                        Anomaly(
                            index=int(idx),
                            curve_key=str(group_key) if group_key is not None else None,
                            tenor_label=_safe_str(frame.at[idx, "tenor_label"], default=None),
                            column=column,
                            score=float(abs(deviation)),
                            value=float(value) if pd.notna(value) else None,
                            threshold=float(self.zscore_threshold),
                        )
                    )

        anomalies_df = pd.DataFrame([a.__dict__ for a in anomalies])
        if not anomalies_df.empty:
            anomalies_df.sort_values(by="score", ascending=False, inplace=True)

        count = len(anomalies_df)
        confidence = 1.0 if total_points == 0 else max(0.0, 1 - (count / total_points))
        summary = {
            "total_points": float(total_points),
            "anomaly_count": float(count),
            "threshold": float(self.zscore_threshold),
        }
        return AnomalyResult(anomalies=anomalies_df, confidence_score=confidence, summary=summary)


def _safe_str(value: object, default: Optional[str] = "") -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    return str(value)


__all__ = ["CurveAnomalyDetector", "AnomalyResult", "Anomaly"]
