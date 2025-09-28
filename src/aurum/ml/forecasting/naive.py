"""Baseline forecasting models with minimal dependencies.

These forecasters provide simple, robust baselines for time series tasks:
- NaiveLastValueForecaster: repeats the last observed value
- MovingAverageForecaster: uses trailing moving average
- SeasonalNaiveForecaster: repeats the last seasonal cycle
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .models import ForecastResult


class NaiveLastValueForecaster:
    """Forecast by repeating the last observed value."""

    def __init__(self) -> None:
        self._last: Optional[float] = None
        self._resid_std: float = 0.0
        self._index: Optional[pd.Index] = None

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if len(s) < 2:
            raise ValueError("Need at least 2 observations to fit naive forecaster")
        self._last = float(s.iloc[-1])
        resid = s.diff().dropna()
        self._resid_std = float(resid.std() or 0.0)
        self._index = s.index

    def forecast(self, steps: int, freq: Optional[str] = None) -> ForecastResult:
        if self._last is None or self._index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")

        last_idx = self._index
        if isinstance(last_idx, pd.DatetimeIndex):
            if freq is None:
                freq = last_idx.freqstr if last_idx.freq is not None else "D"
            idx = pd.date_range(start=last_idx[-1] + pd.tseries.frequencies.to_offset(freq), periods=steps, freq=freq)
        else:
            idx = pd.RangeIndex(start=0, stop=steps)

        preds = pd.Series([self._last] * steps, index=idx)
        z = 1.96
        std = self._resid_std
        lower = preds - z * std if std > 0 else None
        upper = preds + z * std if std > 0 else None
        return ForecastResult(predictions=preds, lower=lower, upper=upper, model_name="NaiveLastValue")


class MovingAverageForecaster:
    """Forecast using trailing moving average over a fixed window."""

    def __init__(self, window: int = 12) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self.window = int(window)
        self._mean: Optional[float] = None
        self._resid_std: float = 0.0
        self._index: Optional[pd.Index] = None

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if len(s) < self.window:
            raise ValueError("Insufficient history for moving average window")
        window_vals = s.iloc[-self.window :]
        self._mean = float(window_vals.mean())
        resid = (window_vals - self._mean).dropna()
        self._resid_std = float(resid.std() or 0.0)
        self._index = s.index

    def forecast(self, steps: int, freq: Optional[str] = None) -> ForecastResult:
        if self._mean is None or self._index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")

        last_idx = self._index
        if isinstance(last_idx, pd.DatetimeIndex):
            if freq is None:
                freq = last_idx.freqstr if last_idx.freq is not None else "D"
            idx = pd.date_range(start=last_idx[-1] + pd.tseries.frequencies.to_offset(freq), periods=steps, freq=freq)
        else:
            idx = pd.RangeIndex(start=0, stop=steps)

        preds = pd.Series([self._mean] * steps, index=idx)
        z = 1.96
        std = self._resid_std
        lower = preds - z * std if std > 0 else None
        upper = preds + z * std if std > 0 else None
        return ForecastResult(
            predictions=preds, lower=lower, upper=upper, model_name=f"MovingAverage(window={self.window})"
        )


class SeasonalNaiveForecaster:
    """Forecast by repeating the last observed seasonal cycle.

    For example, with hourly data and daily seasonality, use season_length=24.
    """

    def __init__(self, season_length: int) -> None:
        if season_length < 1:
            raise ValueError("season_length must be >= 1")
        self.season_length = int(season_length)
        self._season: Optional[np.ndarray] = None
        self._index: Optional[pd.Index] = None
        self._resid_std: float = 0.0

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if len(s) < self.season_length:
            raise ValueError("Insufficient history for seasonal naive forecaster")
        self._season = s.iloc[-self.season_length :].to_numpy(copy=True)
        seasonal_history = s.iloc[-2 * self.season_length : -self.season_length]
        resid = seasonal_history.reset_index(drop=True) - pd.Series(self._season).reset_index(drop=True)
        self._resid_std = float(resid.std() or 0.0)
        self._index = s.index

    def forecast(self, steps: int, freq: Optional[str] = None) -> ForecastResult:
        if self._season is None or self._index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")

        last_idx = self._index
        if isinstance(last_idx, pd.DatetimeIndex):
            if freq is None:
                freq = last_idx.freqstr if last_idx.freq is not None else "D"
            idx = pd.date_range(start=last_idx[-1] + pd.tseries.frequencies.to_offset(freq), periods=steps, freq=freq)
        else:
            idx = pd.RangeIndex(start=0, stop=steps)

        reps = int(np.ceil(steps / self.season_length))
        preds_arr = np.tile(self._season, reps)[:steps]
        preds = pd.Series(preds_arr, index=idx)
        z = 1.96
        std = self._resid_std
        lower = preds - z * std if std > 0 else None
        upper = preds + z * std if std > 0 else None
        return ForecastResult(
            predictions=preds, lower=lower, upper=upper, model_name=f"SeasonalNaive(season={self.season_length})"
        )


__all__ = [
    "ForecastResult",
    "NaiveLastValueForecaster",
    "MovingAverageForecaster",
    "SeasonalNaiveForecaster",
]

