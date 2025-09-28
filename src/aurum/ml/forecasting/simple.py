"""Simple forecasting models and results container."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .models import ForecastResult


class SimpleExpSmoothingForecaster:
    """Lightweight exponential smoothing forecaster (additive, level-only)."""

    def __init__(self, alpha: float = 0.2):
        if not (0.0 < alpha <= 1.0):
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = float(alpha)
        self._fitted: Optional[float] = None
        self._residual_std: Optional[float] = None
        self._last_index: Optional[pd.Index] = None

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if len(s) < 5:
            raise ValueError("Need at least 5 observations to fit SES forecaster")
        level = s.iloc[0]
        residuals = []
        for value in s.iloc[1:]:
            level = self.alpha * value + (1 - self.alpha) * level
            residuals.append(value - level)
        self._fitted = float(level)
        self._residual_std = float(np.std(residuals)) if residuals else 0.0
        self._last_index = s.index

    def forecast(self, steps: int, freq: Optional[str] = None) -> ForecastResult:
        if self._fitted is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")
        last_idx = self._last_index
        if isinstance(last_idx, pd.DatetimeIndex):
            if freq is None:
                freq = last_idx.freqstr if last_idx.freq is not None else "D"
            index = pd.date_range(start=last_idx[-1] + pd.tseries.frequencies.to_offset(freq), periods=steps, freq=freq)
        else:
            index = pd.RangeIndex(start=0, stop=steps, step=1)

        preds = pd.Series([self._fitted] * steps, index=index)
        if self._residual_std and self._residual_std > 0:
            z = 1.96
            lower = preds - z * self._residual_std
            upper = preds + z * self._residual_std
        else:
            lower = upper = None
        return ForecastResult(predictions=preds, lower=lower, upper=upper, model_name="SES(alpha=%.2f)" % self.alpha)


__all__ = ["SimpleExpSmoothingForecaster", "ForecastResult"]

