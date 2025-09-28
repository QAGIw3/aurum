"""Forecasting pipeline orchestrating feature generation, model selection, and evaluation."""
from __future__ import annotations

import inspect

from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from .models import BaseForecaster, ForecastResult
from ..evaluation import DEFAULT_METRICS, Metric


@dataclass(frozen=True)
class CandidateEvaluation:
    name: str
    metrics: Mapping[str, float]
    folds: int


FeatureBuilder = Callable[[pd.Series], pd.DataFrame]


class ForecastingPipeline:
    """High-level pipeline that evaluates candidate forecasters and selects the best."""

    def __init__(
        self,
        candidates: Sequence[Callable[[], BaseForecaster]],
        *,
        horizon: int = 24,
        initial_train_size: Optional[int] = None,
        step: int = 1,
        metrics: Optional[Mapping[str, Metric]] = None,
        primary_metric: str = "rmse",
        feature_builder: Optional[FeatureBuilder] = None,
        name: str = "forecasting_pipeline",
    ) -> None:
        if not candidates:
            raise ValueError("At least one candidate forecaster is required")
        if horizon <= 0:
            raise ValueError("horizon must be positive")
        self.candidate_factories = list(candidates)
        self.horizon = int(horizon)
        self.initial_train_size = initial_train_size
        self.step = int(step)
        self.metrics = dict(metrics or DEFAULT_METRICS)
        if primary_metric not in self.metrics:
            raise ValueError(f"primary_metric '{primary_metric}' not in metrics")
        self.primary_metric = primary_metric
        self.feature_builder = feature_builder
        self.name = name

        self._series: pd.Series | None = None
        self._features: pd.DataFrame | None = None
        self._evaluations: list[CandidateEvaluation] = []
        self._trained_models: dict[str, BaseForecaster] = {}
        self._best_model_name: str | None = None

    @property
    def best_model_name(self) -> str | None:
        return self._best_model_name

    @property
    def evaluations(self) -> Sequence[CandidateEvaluation]:
        return list(self._evaluations)

    def fit(self, series: pd.Series) -> None:
        s = pd.to_numeric(series, errors="coerce").astype(float)
        s = s.dropna()
        if len(s) < (self.initial_train_size or 10) + self.horizon + 2:
            raise ValueError("Insufficient history for forecasting pipeline")
        self._series = s
        features = self.feature_builder(s) if self.feature_builder else None
        if features is not None:
            if not isinstance(features, pd.DataFrame):
                raise TypeError("feature_builder must return a pandas DataFrame")
            features = features.reindex(s.index)
        self._features = features

        self._evaluations = []
        self._trained_models.clear()
        best_score = float("inf")
        best_name: str | None = None

        for factory in self.candidate_factories:
            candidate_name = self._candidate_name(factory)
            eval_result = self._evaluate_candidate(factory, candidate_name, s, features)
            self._evaluations.append(eval_result)
            score = eval_result.metrics.get(self.primary_metric, float("nan"))
            if np.isnan(score):
                continue
            if score < best_score:
                best_score = score
                best_name = eval_result.name

            model = factory()
            self._call_fit(model, s, features)
            self._trained_models[candidate_name] = model

        if best_name is None:
            raise RuntimeError("No candidate produced a valid evaluation score")
        self._best_model_name = best_name

    def forecast(
        self,
        steps: int,
        *,
        future_features: Optional[pd.DataFrame] = None,
        model_name: Optional[str] = None,
    ) -> ForecastResult:
        if self._series is None:
            raise RuntimeError("Pipeline must be fit before forecasting")
        if steps <= 0:
            raise ValueError("steps must be positive")
        name = model_name or self._best_model_name
        if name is None:
            raise RuntimeError("No trained model available for forecasting")
        model = self._trained_models.get(name)
        if model is None:
            raise KeyError(f"Model '{name}' not found. Available: {list(self._trained_models)}")
        return self._call_forecast(model, steps, future_features)

    def evaluation_report(self) -> pd.DataFrame:
        if not self._evaluations:
            return pd.DataFrame()
        rows = []
        for ev in self._evaluations:
            row = {"model": ev.name, "folds": ev.folds}
            row.update(ev.metrics)
            rows.append(row)
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.sort_values(by=self.primary_metric, na_position="last")

    def _candidate_name(self, factory: Callable[[], BaseForecaster]) -> str:
        model = factory()
        name = getattr(model, "model_name", type(model).__name__)
        return str(name)

    def _evaluate_candidate(
        self,
        factory: Callable[[], BaseForecaster],
        candidate_name: str,
        series: pd.Series,
        features: Optional[pd.DataFrame],
    ) -> CandidateEvaluation:
        horizon = self.horizon
        step = max(1, self.step)
        initial = self.initial_train_size or max(horizon * 2, 50)
        metrics = {key: [] for key in self.metrics}
        folds = 0
        total_length = len(series)

        for split in range(initial, total_length - horizon, step):
            train = series.iloc[:split]
            test = series.iloc[split : split + horizon]
            if train.empty or test.empty:
                continue
            train_features = features.iloc[:split] if features is not None else None
            future_features = features.iloc[split : split + horizon] if features is not None else None
            model = factory()
            try:
                self._call_fit(model, train, train_features)
                forecast = self._call_forecast(model, horizon, future_features)
            except Exception:
                continue
            aligned = pd.DataFrame({"y": test, "yhat": forecast.predictions}).dropna()
            if aligned.empty:
                continue
            folds += 1
            for metric_name, fn in self.metrics.items():
                try:
                    metrics[metric_name].append(float(fn(aligned["y"], aligned["yhat"])))
                except Exception:
                    metrics[metric_name].append(float("nan"))

        agg = {name: float(np.nanmean(vals)) if vals else float("nan") for name, vals in metrics.items()}
        return CandidateEvaluation(name=candidate_name, metrics=agg, folds=folds)

    def get_model(self, name: Optional[str] = None) -> BaseForecaster:
        if not self._trained_models:
            raise RuntimeError("No models have been trained. Call fit() first.")
        key = name or self._best_model_name
        if key is None:
            raise RuntimeError("No best model available")
        model = self._trained_models.get(key)
        if model is None:
            raise KeyError(f"Model '{key}' not found. Available: {list(self._trained_models)}")
        return model

    def _call_fit(self, model: BaseForecaster, series: pd.Series, features: Optional[pd.DataFrame]) -> None:
        params = inspect.signature(model.fit).parameters
        if "features" in params:
            model.fit(series, features)
        else:
            model.fit(series)

    def _call_forecast(
        self, model: BaseForecaster, steps: int, future_features: Optional[pd.DataFrame]
    ) -> ForecastResult:
        params = inspect.signature(model.forecast).parameters
        if "future_features" in params:
            return model.forecast(steps, future_features=future_features)
        return model.forecast(steps)


__all__ = ["ForecastingPipeline", "CandidateEvaluation"]

