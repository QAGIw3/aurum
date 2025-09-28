import numpy as np
import pandas as pd

from aurum.ml import mae, mape, rmse, rolling_origin_backtest, smape
from aurum.ml.forecasting import NaiveLastValueForecaster


def test_basic_metrics_behaviour():
    y_true = pd.Series([10, 12, 14, 16])
    y_pred = pd.Series([11, 11, 15, 15])
    assert np.isclose(rmse(y_true, y_pred), 1.0)
    assert np.isclose(mae(y_true, y_pred), 1.0)
    assert mape(y_true, y_pred) > 0
    assert smape(y_true, y_pred) > 0


def test_rolling_origin_backtest_produces_folds():
    idx = pd.date_range("2024-01-01", periods=40, freq="h")
    series = pd.Series(np.linspace(10, 20, len(idx)), index=idx)
    result = rolling_origin_backtest(
        series,
        forecaster_factory=NaiveLastValueForecaster,
        horizon=3,
        initial_train_size=12,
        step=3,
    )
    assert result.folds > 0
    assert "rmse" in result.metrics
