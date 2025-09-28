"""Curve shape prediction via nearest-neighbor pattern matching.

This approach normalizes sliding windows by mean/std and uses Euclidean
distance to find the most similar historical window. The subsequent horizon
after the matched window becomes the predicted shape, rescaled to the current
window's level and variance.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ShapeForecastResult:
    predictions: pd.Series
    match_start: int
    match_end: int
    match_distance: float
    model_name: str


def _zscore(x: np.ndarray) -> Tuple[np.ndarray, float, float]:
    mu = float(np.mean(x))
    sigma = float(np.std(x))
    if sigma == 0:
        return np.zeros_like(x), mu, 0.0
    return (x - mu) / sigma, mu, sigma


class NearestNeighborShapeForecaster:
    """Nearest-neighbor forecaster over normalized shapes.

    Parameters
    - window_size: number of steps used to characterize the current shape
    - horizon: number of steps to predict ahead
    - min_separation: minimal index gap between current window and match to avoid leakage
    - distance: only 'euclidean' supported (kept for future extension)
    """

    def __init__(
        self,
        window_size: int = 24,
        horizon: int = 6,
        *,
        min_separation: int = 1,
        distance: str = "euclidean",
    ) -> None:
        if window_size < 2:
            raise ValueError("window_size must be >= 2")
        if horizon < 1:
            raise ValueError("horizon must be >= 1")
        self.window_size = int(window_size)
        self.horizon = int(horizon)
        self.min_separation = int(min_separation)
        self.distance = distance
        self._series: Optional[pd.Series] = None

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if len(s) < self.window_size + self.horizon + 5:
            raise ValueError("Insufficient history for shape forecaster")
        self._series = s

    def forecast(self, steps: int | None = None, freq: Optional[str] = None) -> ShapeForecastResult:
        if self._series is None:
            raise RuntimeError("Model must be fit before forecasting")
        horizon = self.horizon if steps is None else int(steps)
        s = self._series

        x = s.to_numpy()
        cur = x[-self.window_size :]
        cur_z, cur_mu, cur_sigma = _zscore(cur)

        best_d = float("inf")
        best_i = None
        # Search all candidate windows that have a full horizon following them
        limit = len(x) - self.window_size - horizon
        for i in range(0, limit + 1):
            if len(x) - (i + self.window_size) < self.min_separation:
                continue
            cand = x[i : i + self.window_size]
            cz, _, _ = _zscore(cand)
            d = float(np.sqrt(np.sum((cur_z - cz) ** 2)))
            if d < best_d:
                best_d = d
                best_i = i

        if best_i is None:
            # Fallback to naive
            preds = pd.Series([float(cur[-1])] * horizon, index=pd.RangeIndex(start=0, stop=horizon))
            return ShapeForecastResult(
                predictions=preds,
                match_start=-1,
                match_end=-1,
                match_distance=float("nan"),
                model_name=f"NNShape(window={self.window_size},h={horizon})",
            )

        hist_next = x[best_i + self.window_size : best_i + self.window_size + horizon]
        # Rescale matched future shape using current window mean/std
        # Use z of matched future relative to matched window
        matched_window = x[best_i : best_i + self.window_size]
        mz, mmu, msig = _zscore(matched_window)
        if msig == 0:
            # Constant matched window; predict constant equal to current mean
            pred_arr = np.full(horizon, cur_mu)
        else:
            # Express hist_next relative to matched mean/std, then scale by current
            hist_next_z = (hist_next - mmu) / msig
            pred_arr = cur_mu + hist_next_z * (cur_sigma if cur_sigma > 0 else 1.0)

        # Build index like the input
        last_idx = s.index
        if isinstance(last_idx, pd.DatetimeIndex):
            if freq is None:
                freq = last_idx.freqstr if last_idx.freq is not None else "D"
            idx = pd.date_range(
                start=last_idx[-1] + pd.tseries.frequencies.to_offset(freq), periods=horizon, freq=freq
            )
        else:
            idx = pd.RangeIndex(start=0, stop=horizon)

        preds = pd.Series(pred_arr, index=idx)
        return ShapeForecastResult(
            predictions=preds,
            match_start=int(best_i),
            match_end=int(best_i + self.window_size - 1),
            match_distance=best_d,
            model_name=f"NNShape(window={self.window_size},h={horizon})",
        )


__all__ = ["NearestNeighborShapeForecaster", "ShapeForecastResult"]

