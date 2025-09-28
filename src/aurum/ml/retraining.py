"""Automated model retraining pipeline primitives."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from .ab_testing import ABTestingService
from .evaluation import DEFAULT_METRICS
from .feature_engineering import FeaturePipeline, FeaturePipelineConfig
from .forecasting import (
    ForecastingPipeline,
    HoltWintersForecaster,
    DampedTrendForecaster,
    RegressionForecaster,
    MovingAverageForecaster,
    NaiveLastValueForecaster,
    SeasonalNaiveForecaster,
)
from .registry import ModelRegistry


@dataclass(frozen=True)
class RetrainOutcome:
    registered_name: str
    registered_version: str
    metrics: Mapping[str, float]
    candidate_name: str
    timestamp: str
    evaluation_report: Optional[Sequence[Mapping[str, float]]] = None
    ab_tests: Optional[Sequence[Mapping[str, object]]] = None


def _infer_season_length(series: pd.Series, freq_hint: Optional[str]) -> int:
    if freq_hint:
        key = freq_hint.upper()
        if key.startswith("H"):
            return 24
        if key.startswith("D"):
            return 7
        if key.startswith("W"):
            return 52
    if isinstance(series.index, pd.DatetimeIndex):
        inferred = series.index.inferred_freq
        if inferred and inferred.upper().startswith("H"):
            return 24
    return 7


def default_candidates(series: pd.Series, freq_hint: Optional[str] = None) -> list[Callable[[], any]]:
    season = _infer_season_length(series, freq_hint)
    return [
        lambda: NaiveLastValueForecaster(),
        lambda: MovingAverageForecaster(window=max(3, season // 2)),
        lambda: SeasonalNaiveForecaster(season_length=season),
        lambda: HoltWintersForecaster(season_length=season),
        lambda: DampedTrendForecaster(phi=0.95),
        lambda: RegressionForecaster(lags=(1, season)),
    ]


def retrain_best_forecaster(
    fetch_series: Callable[[], pd.Series],
    *,
    registry: Optional[ModelRegistry] = None,
    model_name: str = "energy_price_forecaster",
    version: Optional[str] = None,
    horizon: int = 6,
    initial_train_size: int = 100,
    step: int = 1,
    metrics: Optional[Mapping[str, Callable[[pd.Series, pd.Series], float]]] = None,
    candidates: Optional[Sequence[Callable[[], any]]] = None,
    freq_hint: Optional[str] = None,
    feature_config: Optional[FeaturePipelineConfig] = None,
    enable_ab_testing: bool = True,
) -> RetrainOutcome:
    """Fetch latest data, evaluate candidate forecasters, and register the best."""
    series = fetch_series()
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    series = pd.to_numeric(series, errors="coerce").astype(float).dropna()
    if series.empty:
        raise ValueError("Fetched series is empty")

    registry = registry or ModelRegistry()
    metrics = metrics or DEFAULT_METRICS
    feature_pipeline = FeaturePipeline(feature_config, freq=freq_hint)
    feature_pipeline.fit(series)

    candidate_factories = list(candidates) if candidates else default_candidates(series, freq_hint=freq_hint)
    pipeline = ForecastingPipeline(
        candidate_factories,
        horizon=horizon,
        initial_train_size=initial_train_size,
        step=step,
        metrics=metrics,
        feature_builder=lambda s: feature_pipeline.transform(s),
        primary_metric="rmse",
        name=model_name,
    )
    pipeline.fit(series)
    report = pipeline.evaluation_report()
    best_name = pipeline.best_model_name
    if best_name is None:
        raise RuntimeError("Forecasting pipeline did not select a best model")

    best_row = report[report["model"] == best_name].iloc[0].to_dict() if not report.empty else {}
    best_metrics = {
        key: float(value)
        for key, value in best_row.items()
        if key not in {"model", "folds"}
    }
    best_metrics = best_metrics or {"rmse": float("nan")}

    model = pipeline.get_model(best_name)

    ver = version or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    metadata_extra = {
        "evaluation": report.to_dict(orient="records") if not report.empty else [],
        "feature_config": asdict(feature_config) if feature_config else asdict(FeaturePipelineConfig()),
        "feature_columns": list(feature_pipeline.feature_columns),
    }

    ab_service = ABTestingService()
    ab_records: list[Mapping[str, object]] = []
    if enable_ab_testing and len(report) >= 2:
        runner_up = report.iloc[1]
        champion_metrics = {key: float(best_row.get(key, float("nan"))) for key in best_metrics}
        challenger_metrics = {
            key: float(runner_up.get(key, float("nan")))
            for key in report.columns
            if key not in {"model", "folds"}
        }
        experiment = ab_service.record_metrics(
            name=f"{model_name}_leaderboard",
            champion_name=best_name,
            challenger_name=str(runner_up["model"]),
            champion_metrics=champion_metrics,
            challenger_metrics=challenger_metrics,
        )
        ab_records.append(
            {
                "experiment": experiment.name,
                "winner": experiment.winner,
                "metrics": experiment.metrics,
                "champion": experiment.champion,
                "challenger": experiment.challenger,
            }
        )
        metadata_extra["ab_tests"] = ab_records

    registry.save(
        model_name,
        ver,
        model,
        metrics=best_metrics,
        **metadata_extra,
    )

    return RetrainOutcome(
        registered_name=model_name,
        registered_version=ver,
        metrics=best_metrics,
        candidate_name=best_name,
        timestamp=datetime.utcnow().isoformat() + "Z",
        evaluation_report=metadata_extra["evaluation"],
        ab_tests=ab_records or None,
    )


__all__ = ["retrain_best_forecaster", "RetrainOutcome"]

