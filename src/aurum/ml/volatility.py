"""Market volatility analysis utilities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd


def realized_volatility(close: pd.Series, window: int = 24) -> pd.Series:
    s = pd.to_numeric(close, errors="coerce").astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        lr = np.log(s / s.shift(1))
    return lr.rolling(window=window, min_periods=max(2, window // 3)).std().fillna(0.0)


def parkinson_volatility(high: pd.Series, low: pd.Series, window: int = 24) -> pd.Series:
    h = pd.to_numeric(high, errors="coerce").astype(float)
    l = pd.to_numeric(low, errors="coerce").astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = (1.0 / (4.0 * np.log(2.0))) * (np.log(h / l) ** 2)
    return rs.rolling(window=window, min_periods=max(2, window // 3)).mean().apply(np.sqrt).fillna(0.0)


def garman_klass_volatility(open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, window: int = 24) -> pd.Series:
    o = pd.to_numeric(open_, errors="coerce").astype(float)
    h = pd.to_numeric(high, errors="coerce").astype(float)
    l = pd.to_numeric(low, errors="coerce").astype(float)
    c = pd.to_numeric(close, errors="coerce").astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        term1 = 0.5 * (np.log(h / l) ** 2)
        term2 = (2.0 * np.log(2.0) - 1.0) * (np.log(c / o) ** 2)
        rs = term1 - term2
    return rs.rolling(window=window, min_periods=max(2, window // 3)).mean().apply(np.sqrt).fillna(0.0)


@dataclass(frozen=True)
class VolatilityRegime:
    regime: str
    vol_value: float
    threshold_low: float
    threshold_high: float
    timestamp: Optional[pd.Timestamp]
    threshold_extreme: float = float("nan")


def classify_regime(
    vol_series: pd.Series,
    *,
    low_quantile: float = 0.25,
    high_quantile: float = 0.75,
    extreme_quantile: float = 0.95,
) -> VolatilityRegime:
    v = pd.to_numeric(vol_series, errors="coerce").astype(float).dropna()
    if v.empty:
        return VolatilityRegime("normal", float("nan"), float("nan"), float("nan"), None)
    q_low = float(v.quantile(low_quantile))
    q_high = float(v.quantile(high_quantile))
    q_ext = float(v.quantile(extreme_quantile))
    curr = float(v.iloc[-1])
    regime = "normal"
    if curr <= q_low:
        regime = "low"
    elif curr >= q_ext:
        regime = "extreme"
    elif curr >= q_high:
        regime = "high"
    ts = v.index[-1] if isinstance(v.index, pd.DatetimeIndex) else None
    return VolatilityRegime(
        regime=regime,
        vol_value=curr,
        threshold_low=q_low,
        threshold_high=q_high,
        timestamp=ts,
        threshold_extreme=q_ext,
    )


class VolatilityEngine:
    """Aggregates multiple volatility estimators and produces diagnostics."""

    def __init__(
        self,
        *,
        window: int = 24,
        regime_quantiles: Sequence[float] = (0.25, 0.75, 0.95),
    ) -> None:
        if len(regime_quantiles) != 3:
            raise ValueError("regime_quantiles must be (low, high, extreme)")
        self.window = int(window)
        self.low_q, self.high_q, self.ext_q = [float(q) for q in regime_quantiles]
        self._frame: Optional[pd.DataFrame] = None
        self._index: Optional[pd.Index] = None

    def fit(
        self,
        close: pd.Series,
        *,
        high: Optional[pd.Series] = None,
        low: Optional[pd.Series] = None,
        open_: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        c = pd.to_numeric(close, errors="coerce").astype(float)
        frame = pd.DataFrame(index=c.index)
        frame["realized_vol"] = realized_volatility(c, window=self.window)
        frame["ewm_vol"] = c.pct_change().ewm(span=self.window, min_periods=1, adjust=False).std().fillna(0.0)

        if high is not None and low is not None:
            frame["parkinson_vol"] = parkinson_volatility(high, low, window=self.window)
        if all(series is not None for series in (open_, high, low, close)):
            frame["garman_klass_vol"] = garman_klass_volatility(open_, high, low, close, window=self.window)

        self._frame = frame
        self._index = c.index
        return frame

    def to_frame(self) -> pd.DataFrame:
        if self._frame is None:
            return pd.DataFrame()
        return self._frame.copy()

    def classify(self) -> VolatilityRegime:
        if self._frame is None or self._frame.empty:
            return VolatilityRegime("normal", float("nan"), float("nan"), float("nan"), None)
        series = self._frame["realized_vol"].dropna()
        if series.empty:
            return VolatilityRegime("normal", float("nan"), float("nan"), float("nan"), None)
        return classify_regime(
            series,
            low_quantile=self.low_q,
            high_quantile=self.high_q,
            extreme_quantile=self.ext_q,
        )

    def diagnostics(self) -> dict[str, float | str]:
        if self._frame is None or self._frame.empty:
            return {}
        latest = self._frame.iloc[-1]
        regime = self.classify()
        return {
            "regime": regime.regime,
            "realized_vol": float(latest.get("realized_vol", float("nan"))),
            "ewm_vol": float(latest.get("ewm_vol", float("nan"))),
            "parkinson_vol": float(latest.get("parkinson_vol", float("nan"))),
            "garman_klass_vol": float(latest.get("garman_klass_vol", float("nan"))),
            "threshold_low": regime.threshold_low,
            "threshold_high": regime.threshold_high,
            "threshold_extreme": regime.threshold_extreme,
        }


__all__ = [
    "realized_volatility",
    "parkinson_volatility",
    "garman_klass_volatility",
    "VolatilityRegime",
    "classify_regime",
    "VolatilityEngine",
]

