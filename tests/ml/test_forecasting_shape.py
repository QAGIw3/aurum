import numpy as np
import pandas as pd

from aurum.ml.forecasting import NearestNeighborShapeForecaster


def test_shape_forecaster_finds_similar_pattern():
    idx = pd.date_range("2024-01-01", periods=120, freq="h")
    base = np.sin(np.linspace(0, 6 * np.pi, len(idx)))
    series = pd.Series(100 + 10 * base, index=idx)
    model = NearestNeighborShapeForecaster(window_size=24, horizon=6, min_separation=12)
    model.fit(series)
    result = model.forecast()
    assert len(result.predictions) == 6
    assert result.match_start >= 0


def test_shape_forecaster_fallback_when_min_separation_blocks_match():
    series = pd.Series(np.linspace(1, 10, 40))
    model = NearestNeighborShapeForecaster(window_size=5, horizon=3, min_separation=100)
    model.fit(series)
    result = model.forecast()
    assert result.match_start == -1
    assert len(result.predictions) == 3
