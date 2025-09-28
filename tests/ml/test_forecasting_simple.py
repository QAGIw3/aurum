import pandas as pd
from aurum.ml import SimpleExpSmoothingForecaster


def test_ses_forecaster_forecast_shape():
    idx = pd.date_range("2024-01-01", periods=20, freq="H")
    s = pd.Series([i % 5 + 10 for i in range(20)], index=idx)
    f = SimpleExpSmoothingForecaster(alpha=0.3)
    f.fit(s)
    res = f.forecast(steps=5, freq="H")
    assert len(res.predictions) == 5
    assert res.lower is None or len(res.lower) == 5
    assert res.upper is None or len(res.upper) == 5

