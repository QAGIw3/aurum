"""Advanced forecasting models and shared interfaces."""
from __future__ import annotations

import inspect

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Protocol, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ForecastResult:
    """Container for forecast outputs and optional prediction intervals."""

    predictions: pd.Series
    lower: Optional[pd.Series]
    upper: Optional[pd.Series]
    model_name: str
    metadata: Optional[dict[str, object]] = field(default=None)


class BaseForecaster(Protocol):
    """Protocol implemented by all forecasters in the platform."""

    model_name: str

    def fit(self, series: pd.Series, features: Optional[pd.DataFrame] = None) -> None:
        """Fit the model on historical data and optional feature frame."""

    def forecast(self, steps: int, future_features: Optional[pd.DataFrame] = None) -> ForecastResult:
        """Forecast `steps` ahead using optional future feature frame."""


def _to_float_series(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if isinstance(s.index, pd.MultiIndex):
        raise ValueError("MultiIndex series are not supported")
    return s.dropna()


class HoltWintersForecaster:
    """Additive Holt-Winters (triple exponential smoothing) forecaster."""

    def __init__(
        self,
        season_length: int,
        *,
        alpha: float = 0.2,
        beta: float = 0.1,
        gamma: float = 0.1,
        seasonal_smoothing: Optional[float] = None,
        damped_trend: bool = False,
    ) -> None:
        if season_length < 2:
            raise ValueError("season_length must be >= 2")
        self.season_length = int(season_length)
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.gamma = float(seasonal_smoothing if seasonal_smoothing is not None else gamma)
        self.damped_trend = bool(damped_trend)
        self.model_name = f"HoltWinters(m={self.season_length})"

        self._level: float | None = None
        self._trend: float | None = None
        self._seasonals: np.ndarray | None = None
        self._resid_std: float = 0.0
        self._last_index: pd.Index | None = None
        self._phi: float = 0.98 if self.damped_trend else 1.0

    def fit(self, series: pd.Series, features: Optional[pd.DataFrame] = None) -> None:
        s = _to_float_series(series)
        if len(s) < 2 * self.season_length + 2:
            raise ValueError("Insufficient history for Holt-Winters forecaster")

        data = s.to_numpy(copy=False)
        seasonals = self._initial_seasonals(data)
        level = float(np.mean(data[: self.season_length]))
        trend = float((np.mean(data[self.season_length : 2 * self.season_length]) - np.mean(data[: self.season_length])) / self.season_length)

        alpha = self.alpha
        beta = self.beta
        gamma = self.gamma
        phi = self._phi

        residuals: list[float] = []
        for idx, y in enumerate(data):
            season = seasonals[idx % self.season_length]
            pred = level + phi * trend + season
            residuals.append(float(y - pred))
            prev_level = level
            level = alpha * (y - season) + (1.0 - alpha) * (level + phi * trend)
            trend = beta * (level - prev_level) + (1.0 - beta) * phi * trend
            seasonals[idx % self.season_length] = gamma * (y - level) + (1.0 - gamma) * season

        self._level = level
        self._trend = trend
        self._seasonals = seasonals
        self._resid_std = float(np.std(residuals, ddof=1)) if len(residuals) > 1 else 0.0
        self._last_index = s.index

    def _initial_seasonals(self, data: np.ndarray) -> np.ndarray:
        m = self.season_length
        n_seasons = len(data) // m
        if n_seasons < 2:
            raise ValueError("Need at least two full seasons to initialize Holt-Winters")
        season_averages = [np.mean(data[i * m : (i + 1) * m]) for i in range(n_seasons)]
        seasonals = np.zeros(m, dtype=float)
        for i in range(m):
            vals = [data[i + j * m] - season_averages[j] for j in range(n_seasons)]
            seasonals[i] = float(np.mean(vals))
        return seasonals

    def forecast(self, steps: int, future_features: Optional[pd.DataFrame] = None) -> ForecastResult:
        if self._level is None or self._trend is None or self._seasonals is None or self._last_index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")
        alpha = 1.96 * self._resid_std if self._resid_std > 0 else None

        preds: list[float] = []
        lowers: list[float] = []
        uppers: list[float] = []

        level = self._level
        trend = self._trend
        seasonals = self._seasonals
        phi = self._phi
        m = self.season_length
        start_index = len(self._last_index)

        for k in range(1, steps + 1):
            season = seasonals[(start_index + k - 1) % m]
            if phi == 1.0:
                value = level + k * trend + season
            else:
                damp_sum = (1.0 - phi ** k) / (1.0 - phi)
                value = level + damp_sum * trend + season
            preds.append(float(value))
            if alpha is not None:
                lowers.append(float(value - alpha))
                uppers.append(float(value + alpha))

        future_index = self._forecast_index(steps)
        pred_series = pd.Series(preds, index=future_index)
        lower_series = pd.Series(lowers, index=future_index) if lowers else None
        upper_series = pd.Series(uppers, index=future_index) if uppers else None

        return ForecastResult(predictions=pred_series, lower=lower_series, upper=upper_series, model_name=self.model_name)

    def _forecast_index(self, steps: int) -> pd.Index:
        idx = self._last_index
        if isinstance(idx, pd.DatetimeIndex):
            freq = idx.freq or getattr(idx, "inferred_freq", None) or "D"
            start = idx[-1] + pd.tseries.frequencies.to_offset(freq)
            return pd.date_range(start=start, periods=steps, freq=freq)
        if isinstance(idx, pd.RangeIndex):
            start = idx.stop
            return pd.RangeIndex(start=start, stop=start + steps)
        return pd.RangeIndex(start=0, stop=steps)


class DampedTrendForecaster:
    """Holt's linear method with optional damping for smoother long horizons."""

    def __init__(self, *, alpha: float = 0.3, beta: float = 0.1, phi: float = 0.95) -> None:
        if not (0.0 < alpha <= 1.0 and 0.0 < beta <= 1.0):
            raise ValueError("alpha and beta must be in (0, 1]")
        if not (0.0 < phi <= 1.0):
            raise ValueError("phi must be in (0, 1]")
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.phi = float(phi)
        self.model_name = "DampedTrend"
        self._level: float | None = None
        self._trend: float | None = None
        self._resid_std: float = 0.0
        self._last_index: pd.Index | None = None

    def fit(self, series: pd.Series, features: Optional[pd.DataFrame] = None) -> None:
        s = _to_float_series(series)
        if len(s) < 5:
            raise ValueError("Need at least 5 observations to fit damped trend forecaster")
        data = s.to_numpy(copy=False)
        level = float(data[0])
        trend = float(data[1] - data[0])
        residuals: list[float] = []
        alpha = self.alpha
        beta = self.beta
        phi = self.phi

        for y in data:
            pred = level + phi * trend
            residuals.append(float(y - pred))
            prev_level = level
            level = alpha * y + (1.0 - alpha) * (level + phi * trend)
            trend = beta * (level - prev_level) + (1.0 - beta) * phi * trend

        self._level = level
        self._trend = trend
        self._resid_std = float(np.std(residuals, ddof=1)) if len(residuals) > 1 else 0.0
        self._last_index = s.index

    def forecast(self, steps: int, future_features: Optional[pd.DataFrame] = None) -> ForecastResult:
        if self._level is None or self._trend is None or self._last_index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")
        preds: list[float] = []
        lowers: list[float] = []
        uppers: list[float] = []
        level = self._level
        trend = self._trend
        phi = self.phi
        std = self._resid_std
        horizon_multiplier = 1.96 * std if std > 0 else None

        for k in range(1, steps + 1):
            damp_sum = (1.0 - phi ** k) / (1.0 - phi)
            value = level + damp_sum * trend
            preds.append(float(value))
            if horizon_multiplier is not None:
                lowers.append(float(value - horizon_multiplier))
                uppers.append(float(value + horizon_multiplier))

        future_index = self._forecast_index(steps)
        pred_series = pd.Series(preds, index=future_index)
        lower_series = pd.Series(lowers, index=future_index) if lowers else None
        upper_series = pd.Series(uppers, index=future_index) if uppers else None
        return ForecastResult(predictions=pred_series, lower=lower_series, upper=upper_series, model_name=self.model_name)

    def _forecast_index(self, steps: int) -> pd.Index:
        idx = self._last_index
        if isinstance(idx, pd.DatetimeIndex):
            freq = idx.freq or getattr(idx, "inferred_freq", None) or "D"
            start = idx[-1] + pd.tseries.frequencies.to_offset(freq)
            return pd.date_range(start=start, periods=steps, freq=freq)
        if isinstance(idx, pd.RangeIndex):
            start = idx.stop
            return pd.RangeIndex(start=start, stop=start + steps)
        return pd.RangeIndex(start=0, stop=steps)


class RegressionForecaster:
    """Linear regression forecaster with autoregressive lags and exogenous features."""

    def __init__(
        self,
        *,
        lags: Iterable[int] = (1, 24),
        regularization: float = 1e-6,
        include_trend: bool = True,
    ) -> None:
        self.lags = sorted({int(abs(l)) for l in lags if int(abs(l)) > 0})
        if not self.lags:
            raise ValueError("At least one positive lag is required")
        if regularization < 0:
            raise ValueError("regularization must be non-negative")
        self.regularization = float(regularization)
        self.include_trend = bool(include_trend)
        self.model_name = "RegressionForecaster"

        self._coef: np.ndarray | None = None
        self._columns: list[str] | None = None
        self._resid_std: float = 0.0
        self._last_values: pd.Series | None = None
        self._feature_means: pd.Series | None = None
        self._fitted_index: pd.Index | None = None

    def fit(self, series: pd.Series, features: Optional[pd.DataFrame] = None) -> None:
        s = _to_float_series(series)
        df = pd.DataFrame({"y": s})
        for lag in self.lags:
            df[f"lag_{lag}"] = s.shift(lag)
        if features is not None:
            aligned_features = features.reindex(s.index)
            df = df.join(aligned_features)
        if self.include_trend:
            df["time_index"] = np.arange(len(df), dtype=float)
        df = df.dropna()
        if df.empty:
            raise ValueError("No training samples available after applying lags/features")

        y = df.pop("y").to_numpy(dtype=float)
        X = df.to_numpy(dtype=float)
        cols = list(df.columns)
        ones = np.ones((X.shape[0], 1), dtype=float)
        X_design = np.hstack([ones, X])
        ridge = self.regularization * np.eye(X_design.shape[1], dtype=float)
        beta = np.linalg.solve(X_design.T @ X_design + ridge, X_design.T @ y)
        preds = X_design @ beta
        resid = y - preds

        self._coef = beta
        self._columns = cols
        self._resid_std = float(np.sqrt(np.mean(resid**2))) if len(resid) > 0 else 0.0
        self._last_values = s.iloc[-max(self.lags) :].copy()
        self._feature_means = df.mean(axis=0)
        self._fitted_index = s.index

    def forecast(self, steps: int, future_features: Optional[pd.DataFrame] = None) -> ForecastResult:
        if self._coef is None or self._columns is None or self._last_values is None or self._fitted_index is None:
            raise RuntimeError("Model must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")

        lag_buffer = self._last_values.to_list()
        max_lag = max(self.lags)
        preds: list[float] = []
        lowers: list[float] = []
        uppers: list[float] = []
        std = self._resid_std
        offset = 1.96 * std if std > 0 else None
        coef = self._coef
        columns = self._columns

        if future_features is not None:
            future_df = future_features.copy()
            horizon_features = future_df.iloc[:steps].reindex(columns=columns, fill_value=np.nan)
        else:
            horizon_features = pd.DataFrame(np.nan, index=range(steps), columns=columns)

        if self._feature_means is not None:
            horizon_features = horizon_features.fillna(self._feature_means)
        else:
            horizon_features = horizon_features.fillna(0.0)

        for step in range(steps):
            row_values: list[float] = []
            for col in columns:
                if col.startswith("lag_"):
                    lag = int(col.split("_")[-1])
                    value_idx = -lag
                    if len(lag_buffer) + value_idx < 0:
                        raise RuntimeError("Insufficient lag history for forecasting")
                    row_values.append(float(lag_buffer[value_idx]))
                elif col == "time_index":
                    row_values.append(float(len(self._fitted_index) + step))
                else:
                    row_values.append(float(horizon_features.iloc[step][col]))
            row = np.array([1.0] + row_values, dtype=float)
            value = float(row @ coef)
            preds.append(value)
            lag_buffer.append(value)
            if len(lag_buffer) > max_lag:
                lag_buffer = lag_buffer[-max_lag:]
            if offset is not None:
                lowers.append(float(value - offset))
                uppers.append(float(value + offset))

        future_index = self._forecast_index(steps)
        pred_series = pd.Series(preds, index=future_index)
        lower_series = pd.Series(lowers, index=future_index) if lowers else None
        upper_series = pd.Series(uppers, index=future_index) if uppers else None
        return ForecastResult(predictions=pred_series, lower=lower_series, upper=upper_series, model_name=self.model_name)

    def _forecast_index(self, steps: int) -> pd.Index:
        idx = self._fitted_index
        if isinstance(idx, pd.DatetimeIndex):
            freq = idx.freq or getattr(idx, "inferred_freq", None) or "D"
            start = idx[-1] + pd.tseries.frequencies.to_offset(freq)
            return pd.date_range(start=start, periods=steps, freq=freq)
        if isinstance(idx, pd.RangeIndex):
            start = idx.stop
            return pd.RangeIndex(start=start, stop=start + steps)
        return pd.RangeIndex(start=0, stop=steps)


class EnsembleForecaster:
    """Combines multiple forecasters via weighted averaging."""

    def __init__(
        self,
        members: Sequence[Callable[[], BaseForecaster]],
        *,
        weights: Optional[Sequence[float]] = None,
        name: str = "Ensemble",
    ) -> None:
        if not members:
            raise ValueError("At least one member forecaster is required")
        if weights is not None and len(weights) != len(members):
            raise ValueError("weights length must match members length")
        self.member_factories = list(members)
        self.weights = list(weights) if weights is not None else None
        self.model_name = name
        self._trained_members: list[BaseForecaster] = []

    def fit(self, series: pd.Series, features: Optional[pd.DataFrame] = None) -> None:
        self._trained_members.clear()
        for factory in self.member_factories:
            model = factory()
            self._call_fit(model, series, features)
            self._trained_members.append(model)

    def forecast(self, steps: int, future_features: Optional[pd.DataFrame] = None) -> ForecastResult:
        if not self._trained_members:
            raise RuntimeError("Ensemble forecaster must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")

        member_results = [self._call_forecast(model, steps, future_features) for model in self._trained_members]
        preds = None
        lowers = None
        uppers = None
        weights = self._normalized_weights(len(member_results))

        for weight, result in zip(weights, member_results):
            series_pred = result.predictions.astype(float)
            preds = series_pred * weight if preds is None else preds + series_pred * weight
            if result.lower is not None and result.upper is not None:
                lower_series = result.lower.astype(float)
                upper_series = result.upper.astype(float)
                lowers = lower_series * weight if lowers is None else lowers + lower_series * weight
                uppers = upper_series * weight if uppers is None else uppers + upper_series * weight
            else:
                lowers = None
                uppers = None

        pred_series = preds
        lower_series = lowers if lowers is not None else None
        upper_series = uppers if uppers is not None else None
        metadata = {
            "members": [res.model_name for res in member_results],
            "weights": weights,
        }
        return ForecastResult(predictions=pred_series, lower=lower_series, upper=upper_series, model_name=self.model_name, metadata=metadata)

    def _normalized_weights(self, n: int) -> list[float]:
        if self.weights is None:
            return [1.0 / n] * n
        total = float(sum(self.weights))
        if total <= 0:
            raise ValueError("weights must sum to a positive value")
        return [float(w) / total for w in self.weights]

    @staticmethod
    def _call_fit(model: BaseForecaster, series: pd.Series, features: Optional[pd.DataFrame]) -> None:
        params = inspect.signature(model.fit).parameters
        if "features" in params:
            model.fit(series, features)
        else:
            model.fit(series)

    @staticmethod
    def _call_forecast(
        model: BaseForecaster, steps: int, future_features: Optional[pd.DataFrame]
    ) -> ForecastResult:
        params = inspect.signature(model.forecast).parameters
        if "future_features" in params:
            return model.forecast(steps, future_features=future_features)
        return model.forecast(steps)


__all__ = [
    "BaseForecaster",
    "ForecastResult",
    "HoltWintersForecaster",
    "DampedTrendForecaster",
    "RegressionForecaster",
    "EnsembleForecaster",
]
