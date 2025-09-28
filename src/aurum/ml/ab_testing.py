"""A/B testing utilities to compare forecasting or signal models."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ABTestResult:
    winner: str
    metrics: Mapping[str, float]
    metrics_a: Mapping[str, float]
    metrics_b: Mapping[str, float]


@dataclass(frozen=True)
class LoggedExperiment:
    name: str
    winner: str
    metrics: Mapping[str, float]
    challenger: str
    champion: str


def _rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    diff = (y_true - y_pred).dropna()
    if diff.empty:
        return float("nan")
    return float(np.sqrt(np.mean(np.square(diff))))


def _mape(y_true: pd.Series, y_pred: pd.Series) -> float:
    mask = (y_true != 0).astype(bool)
    diff = (y_true[mask] - y_pred[mask]).abs() / y_true[mask].abs()
    if diff.empty:
        return float("nan")
    return float(diff.mean())


def _mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    diff = (y_true - y_pred).abs().dropna()
    if diff.empty:
        return float("nan")
    return float(diff.mean())


_METRIC_FUNCS = {
    "rmse": _rmse,
    "mape": _mape,
    "mae": _mae,
}


def run_ab_test(
    actuals: pd.Series,
    pred_a: pd.Series,
    pred_b: pd.Series,
    *,
    metrics: Sequence[str] = ("rmse", "mape"),
    names: tuple[str, str] = ("A", "B"),
) -> ABTestResult:
    """Compare two prediction series and declare a winner based on metrics."""
    aligned = pd.DataFrame({"y": actuals, "a": pred_a, "b": pred_b}).dropna()
    if aligned.empty:
        return ABTestResult(winner="tie", metrics={}, metrics_a={}, metrics_b={})

    metrics_a: dict[str, float] = {}
    metrics_b: dict[str, float] = {}
    for m in metrics:
        fn = _METRIC_FUNCS.get(m)
        if fn is None:
            continue
        metrics_a[m] = fn(aligned["y"], aligned["a"])
        metrics_b[m] = fn(aligned["y"], aligned["b"])

    def _better(a: Mapping[str, float], b: Mapping[str, float]) -> int:
        for key in ("rmse", "mape", "mae"):
            va = a.get(key)
            vb = b.get(key)
            if va is None or vb is None or np.isnan(va) or np.isnan(vb):
                continue
            if va < vb - 1e-12:
                return -1
            if vb < va - 1e-12:
                return 1
        return 0

    cmp = _better(metrics_a, metrics_b)
    winner = "tie"
    if cmp < 0:
        winner = names[0]
    elif cmp > 0:
        winner = names[1]

    merged_metrics = {f"{names[0]}_{k}": v for k, v in metrics_a.items()}
    merged_metrics.update({f"{names[1]}_{k}": v for k, v in metrics_b.items()})

    return ABTestResult(winner=winner, metrics=merged_metrics, metrics_a=metrics_a, metrics_b=metrics_b)


class ABTestingService:
    """Tracks sequential A/B experiments and produces leaderboards."""

    def __init__(self, *, metrics: Sequence[str] = ("rmse", "mape"), primary: str = "rmse") -> None:
        self.metrics = tuple(metrics)
        self.primary = primary
        self._history: list[LoggedExperiment] = []

    def run(
        self,
        name: str,
        actuals: pd.Series,
        champion_pred: pd.Series,
        challenger_pred: pd.Series,
        *,
        champion_name: str = "champion",
        challenger_name: str = "challenger",
    ) -> ABTestResult:
        result = run_ab_test(actuals, champion_pred, challenger_pred, metrics=self.metrics, names=(champion_name, challenger_name))
        self._history.append(
            LoggedExperiment(
                name=name,
                winner=result.winner,
                metrics=result.metrics,
                champion=champion_name,
                challenger=challenger_name,
            )
        )
        return result

    def record_metrics(
        self,
        name: str,
        champion_name: str,
        challenger_name: str,
        champion_metrics: Mapping[str, float],
        challenger_metrics: Mapping[str, float],
    ) -> LoggedExperiment:
        champ_score = champion_metrics.get(self.primary, float("nan"))
        chall_score = challenger_metrics.get(self.primary, float("nan"))
        winner = champion_name
        if np.isnan(champ_score) and not np.isnan(chall_score):
            winner = challenger_name
        elif chall_score < champ_score:
            winner = challenger_name
        metrics = {f"{champion_name}_{k}": v for k, v in champion_metrics.items()}
        metrics.update({f"{challenger_name}_{k}": v for k, v in challenger_metrics.items()})
        experiment = LoggedExperiment(
            name=name,
            winner=winner,
            metrics=metrics,
            champion=champion_name,
            challenger=challenger_name,
        )
        self._history.append(experiment)
        return experiment

    def history(self) -> Sequence[LoggedExperiment]:
        return list(self._history)

    def leaderboard(self) -> pd.DataFrame:
        if not self._history:
            return pd.DataFrame(columns=["experiment", "winner", "champion", "challenger"])
        rows = []
        for exp in self._history:
            row = {
                "experiment": exp.name,
                "winner": exp.winner,
                "champion": exp.champion,
                "challenger": exp.challenger,
            }
            row.update(exp.metrics)
            rows.append(row)
        return pd.DataFrame(rows)


__all__ = ["run_ab_test", "ABTestResult", "ABTestingService", "LoggedExperiment"]

