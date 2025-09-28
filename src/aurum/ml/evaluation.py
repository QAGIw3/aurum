"""Model evaluation utilities for time series forecasting and signals."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional

import numpy as np
import pandas as pd


def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    diff = (y_true - y_pred).dropna()
    return float(np.sqrt(np.mean(np.square(diff)))) if not diff.empty else float("nan")


def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    diff = (y_true - y_pred).abs().dropna()
    return float(diff.mean()) if not diff.empty else float("nan")


def mape(y_true: pd.Series, y_pred: pd.Series) -> float:
    mask = (y_true != 0).astype(bool)
    diff = ((y_true[mask] - y_pred[mask]).abs() / y_true[mask].abs()).dropna()
    return float(diff.mean()) if not diff.empty else float("nan")


def smape(y_true: pd.Series, y_pred: pd.Series) -> float:
    y = pd.concat([y_true, y_pred], axis=1).dropna()
    if y.empty:
        return float("nan")
    a = y.iloc[:, 0].abs()
    b = y.iloc[:, 1].abs()
    denom = (a + b).replace(0, np.nan)
    return float((2.0 * (a - b).abs() / denom).mean())


Metric = Callable[[pd.Series, pd.Series], float]
DEFAULT_METRICS: Mapping[str, Metric] = {"rmse": rmse, "mae": mae, "mape": mape, "smape": smape}


@dataclass(frozen=True)
class BacktestResult:
    metrics: Mapping[str, float]
    horizon: int
    folds: int


def rolling_origin_backtest(
    series: pd.Series,
    forecaster_factory: Callable[[], any],
    *,
    horizon: int = 6,
    initial_train_size: int = 100,
    step: int = 1,
    metrics: Optional[Mapping[str, Metric]] = None,
) -> BacktestResult:
    """Perform a simple rolling-origin backtest to evaluate a forecaster.

    For each fold, expands the training window by `step`, forecasts `horizon`, and
    compares against the actuals. Aggregates metrics across folds as a mean.
    """
    s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
    if len(s) < initial_train_size + horizon + 2:
        raise ValueError("Insufficient data for backtest")
    metrics = metrics or DEFAULT_METRICS

    results: dict[str, list[float]] = {k: [] for k in metrics}
    folds = 0

    for start in range(initial_train_size, len(s) - horizon, step):
        train = s.iloc[:start]
        test = s.iloc[start : start + horizon]
        model = forecaster_factory()
        model.fit(train)
        fc = model.forecast(horizon)
        preds = fc.predictions
        aligned = pd.DataFrame({"y": test, "yhat": preds}).dropna()
        if aligned.empty:
            continue
        folds += 1
        for name, fn in metrics.items():
            results[name].append(fn(aligned["y"], aligned["yhat"]))

    agg = {name: float(np.nanmean(vals)) if vals else float("nan") for name, vals in results.items()}
    return BacktestResult(metrics=agg, horizon=horizon, folds=folds)


__all__ = [
    "rmse",
    "mae",
    "mape",
    "smape",
    "rolling_origin_backtest",
    "BacktestResult",
]

