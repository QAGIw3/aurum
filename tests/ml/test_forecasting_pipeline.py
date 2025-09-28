import numpy as np
import pandas as pd

from aurum.ml import FeaturePipeline, FeaturePipelineConfig
from aurum.ml.forecasting import ForecastingPipeline, HoltWintersForecaster, NaiveLastValueForecaster


def test_forecasting_pipeline_selects_best_model():
    # Synthetic seasonal signal
    idx = pd.date_range("2024-01-01", periods=120, freq="h")
    values = 20 + 5 * np.sin(np.linspace(0, 10 * np.pi, len(idx)))
    series = pd.Series(values, index=idx)

    feature_pipeline = FeaturePipeline(FeaturePipelineConfig(lags=(1, 24), rolling_windows=(12,), include_calendar=False))
    feature_pipeline.fit(series)

    pipeline = ForecastingPipeline(
        [lambda: NaiveLastValueForecaster(), lambda: HoltWintersForecaster(season_length=24)],
        horizon=6,
        initial_train_size=48,
        step=6,
        feature_builder=lambda s: feature_pipeline.transform(s),
    )
    pipeline.fit(series)

    assert pipeline.best_model_name in {"NaiveLastValueForecaster", "HoltWinters(m=24)"}
    report = pipeline.evaluation_report()
    assert not report.empty
    best_model = pipeline.get_model()
    result = pipeline.forecast(steps=6)
    assert len(result.predictions) == 6
    assert best_model is not None

