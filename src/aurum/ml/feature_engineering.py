"""Feature engineering utilities for time series forecasting and analytics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


def compute_realized_volatility(series: pd.Series, window: int = 24) -> pd.Series:
    """Compute realized volatility over a rolling window using squared returns."""
    s = pd.to_numeric(series, errors="coerce").astype(float)
    log_ret = np.log(s / s.shift(1))
    vol = log_ret.rolling(window=window, min_periods=max(2, window // 3)).std()
    return vol.fillna(0.0)


@dataclass(frozen=True)
class FeaturePipelineConfig:
    """Configuration for feature engineering pipeline."""

    lags: Sequence[int] = field(default_factory=lambda: (1, 2, 24))
    rolling_windows: Sequence[int] = field(default_factory=lambda: (3, 12, 24))
    ewma_spans: Sequence[int] = field(default_factory=lambda: (6, 24))
    differences: Sequence[int] = field(default_factory=lambda: (1,))
    include_returns: bool = True
    include_log_returns: bool = True
    include_calendar: bool = True
    include_volatility: bool = True
    include_value: bool = True
    calendar_features: Sequence[str] = field(default_factory=lambda: ("hour", "dayofweek", "month", "is_weekend"))
    dropna: bool = False
    exogenous_prefix: str = "exo"


class FeaturePipeline:
    """Builds consistent feature frames for training and inference scenarios."""

    def __init__(self, config: FeaturePipelineConfig | None = None, *, freq: str | None = None) -> None:
        self.config = config or FeaturePipelineConfig()
        self.freq = freq
        self._series: pd.Series | None = None
        self._features: pd.DataFrame | None = None
        self._feature_columns: list[str] = []
        self._last_feature_row: Optional[pd.Series] = None
        self._exogenous_columns: list[str] = []

    def fit(self, series: pd.Series, exogenous: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        features = self.transform(series, exogenous=exogenous)
        s = pd.to_numeric(series, errors="coerce").astype(float).dropna()
        if s.empty:
            raise ValueError("FeaturePipeline requires non-empty series")
        self._series = s
        self._features = features
        self._feature_columns = list(features.columns)
        self._last_feature_row = features.iloc[-1] if not features.empty else None
        return features

    def transform(self, series: pd.Series, exogenous: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        s = pd.to_numeric(series, errors="coerce").astype(float)
        s = s.dropna()
        if s.empty:
            return pd.DataFrame(index=s.index)
        features = _build_feature_frame(s, self.config, exogenous=exogenous, exogenous_columns=self._exogenous_columns)
        if self.config.dropna:
            features = features.dropna()
        return features

    def make_future_features(
        self,
        steps: int,
        exogenous_future: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        if self._series is None:
            raise RuntimeError("FeaturePipeline must be fit before generating future features")
        if steps <= 0:
            raise ValueError("steps must be positive")
        future_index = _future_index(self._series.index, steps, freq_hint=self.freq)
        df = pd.DataFrame(index=future_index, columns=self._feature_columns, dtype=float)

        # Calendar features from the future index
        if self.config.include_calendar and isinstance(future_index, pd.DatetimeIndex):
            cal = _calendar_feature_frame(future_index, self.config.calendar_features)
            for col in cal.columns:
                if col in df.columns:
                    df[col] = cal[col]

        # Exogenous future data
        if exogenous_future is not None:
            exo = exogenous_future.copy()
            if not isinstance(exo.index, pd.Index):
                raise TypeError("exogenous_future must be indexed")
            exo = exo.reindex(future_index)
            renamed = {col: f"{self.config.exogenous_prefix}_{col}" for col in exo.columns}
            exo = exo.rename(columns=renamed)
            for col in exo.columns:
                if col in df.columns:
                    df[col] = exo[col]

        # Carry forward last known feature values for the remaining columns
        if self._last_feature_row is not None:
            for col in df.columns:
                if df[col].notna().any():
                    continue
                if col in self._last_feature_row.index:
                    df[col] = self._last_feature_row[col]

        return df

    @property
    def feature_columns(self) -> Sequence[str]:
        return tuple(self._feature_columns)

    def to_frame(self) -> pd.DataFrame:
        if self._features is None:
            return pd.DataFrame()
        return self._features.copy()


def _build_feature_frame(
    series: pd.Series,
    config: FeaturePipelineConfig,
    *,
    exogenous: Optional[pd.DataFrame] = None,
    exogenous_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    df = pd.DataFrame(index=series.index)
    if config.include_value:
        df["value"] = series

    if config.include_returns:
        df["return_1"] = series.pct_change()
    if config.include_log_returns:
        with np.errstate(divide="ignore", invalid="ignore"):
            df["log_return_1"] = np.log(series / series.shift(1))

    for lag in sorted({int(abs(l)) for l in config.lags if int(abs(l)) > 0}):
        df[f"lag_{lag}"] = series.shift(lag)

    for order in sorted({int(abs(d)) for d in config.differences if int(abs(d)) > 0}):
        df[f"diff_{order}"] = series.diff(order)

    for w in sorted({int(abs(w)) for w in config.rolling_windows if int(abs(w)) > 1}):
        df[f"roll_mean_{w}"] = series.rolling(window=w, min_periods=max(2, w // 3)).mean()
        df[f"roll_std_{w}"] = series.rolling(window=w, min_periods=max(2, w // 3)).std()

    for span in sorted({int(abs(s)) for s in config.ewma_spans if int(abs(s)) > 1}):
        df[f"ewma_{span}"] = series.ewm(span=span, min_periods=1, adjust=False).mean()

    if config.include_volatility:
        for w in sorted({int(abs(w)) for w in config.rolling_windows if int(abs(w)) > 1}):
            df[f"realized_vol_{w}"] = compute_realized_volatility(series, window=w)

    if config.include_calendar and isinstance(series.index, pd.DatetimeIndex):
        cal = _calendar_feature_frame(series.index, config.calendar_features)
        df = df.join(cal)

    if exogenous is not None:
        exo = exogenous.copy()
        exo = exo.reindex(series.index)
        rename_map = {col: f"{config.exogenous_prefix}_{col}" for col in exo.columns}
        exo = exo.rename(columns=rename_map)
        df = df.join(exo)
        if exogenous_columns is not None:
            exogenous_columns[:] = list(exo.columns)

    return df


def _calendar_feature_frame(index: pd.DatetimeIndex, features: Sequence[str]) -> pd.DataFrame:
    cal: dict[str, Iterable[float]] = {}
    if "hour" in features:
        cal["hour"] = index.hour.astype(float)
    if "dayofweek" in features:
        cal["dayofweek"] = index.dayofweek.astype(float)
    if "weekofyear" in features:
        cal["weekofyear"] = index.isocalendar().week.astype(float)
    if "month" in features:
        cal["month"] = index.month.astype(float)
    if "dayofmonth" in features:
        cal["dayofmonth"] = index.day.astype(float)
    if "is_weekend" in features:
        cal["is_weekend"] = index.dayofweek.isin([5, 6]).astype(float)
    if "is_month_start" in features:
        cal["is_month_start"] = index.is_month_start.astype(float)
    if "is_month_end" in features:
        cal["is_month_end"] = index.is_month_end.astype(float)
    return pd.DataFrame(cal, index=index)


def _future_index(index: pd.Index, steps: int, *, freq_hint: str | None = None) -> pd.Index:
    if isinstance(index, pd.DatetimeIndex):
        freq = freq_hint or index.freq or getattr(index, "inferred_freq", None) or "D"
        offset = pd.tseries.frequencies.to_offset(freq)
        start = index[-1] + offset
        return pd.date_range(start=start, periods=steps, freq=freq)
    if isinstance(index, pd.RangeIndex):
        start = index.stop
        return pd.RangeIndex(start=start, stop=start + steps)
    return pd.RangeIndex(start=0, stop=steps)


def build_features(
    series: pd.Series,
    *,
    windows: Iterable[int] = (3, 12, 24),
    include_volatility: bool = True,
) -> pd.DataFrame:
    """Backward compatible helper that mirrors the original feature builder."""
    config = FeaturePipelineConfig(
        lags=(),
        rolling_windows=tuple(windows),
        ewma_spans=tuple(windows),
        differences=(),
        include_returns=True,
        include_log_returns=True,
        include_calendar=False,
        include_volatility=include_volatility,
        include_value=True,
        dropna=False,
    )
    pipeline = FeaturePipeline(config)
    return pipeline.transform(series)


__all__ = [
    "compute_realized_volatility",
    "FeaturePipeline",
    "FeaturePipelineConfig",
    "build_features",
]

