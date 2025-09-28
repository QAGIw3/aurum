import numpy as np
import pandas as pd

from aurum.ml.forecasting import (
    DampedTrendForecaster,
    EnsembleForecaster,
    HoltWintersForecaster,
    NaiveLastValueForecaster,
    RegressionForecaster,
)


def test_holt_winters_forecaster_predicts_horizon():
    idx = pd.date_range("2024-01-01", periods=72, freq="h")
    values = 20 + 5 * np.sin(np.linspace(0, 3 * np.pi, len(idx)))
    series = pd.Series(values, index=idx)
    model = HoltWintersForecaster(season_length=24)
    model.fit(series)
    result = model.forecast(steps=12)
    assert len(result.predictions) == 12
    assert result.lower is not None and len(result.lower) == 12


def test_damped_trend_forecaster_handles_linear_series():
    idx = pd.RangeIndex(start=0, stop=50)
    series = pd.Series(2 * idx + 5, index=idx)
    model = DampedTrendForecaster(alpha=0.4, beta=0.3, phi=0.9)
    model.fit(series)
    result = model.forecast(steps=5)
    assert isinstance(result.predictions.index, pd.RangeIndex)
    assert len(result.predictions) == 5


def test_regression_forecaster_with_future_features():
    idx = pd.date_range("2024-01-01", periods=72, freq="h")
    base = np.linspace(30, 32, len(idx))
    series = pd.Series(base + np.sin(np.linspace(0, 6 * np.pi, len(idx))), index=idx)
    exogenous = pd.DataFrame({"exo": np.cos(np.linspace(0, 6 * np.pi, len(idx)))}, index=idx)
    model = RegressionForecaster(lags=(1, 24))
    model.fit(series, exogenous)
    future_idx = pd.date_range(idx[-1] + pd.Timedelta(hours=1), periods=6, freq="h")
    future_exogenous = pd.DataFrame({"exo": np.cos(np.linspace(0, np.pi, len(future_idx)))}, index=future_idx)
    result = model.forecast(steps=6, future_features=future_exogenous)
    assert result.predictions.index.equals(future_idx)
    assert result.lower is not None and result.upper is not None


def test_ensemble_forecaster_blends_members():
    idx = pd.date_range("2024-01-01", periods=40, freq="h")
    series = pd.Series(50 + np.sin(np.linspace(0, 2 * np.pi, len(idx))), index=idx)

    def make_naive():
        return NaiveLastValueForecaster()

    def make_damped():
        return DampedTrendForecaster()

    ensemble = EnsembleForecaster([make_naive, make_damped], weights=[0.7, 0.3])
    ensemble.fit(series)
    result = ensemble.forecast(steps=4)
    assert len(result.predictions) == 4
    assert result.metadata["members"]
