import pandas as pd

from aurum.ml import VolatilityEngine


def test_volatility_engine_diagnostics():
    idx = pd.date_range("2024-01-01", periods=50, freq="H")
    prices = pd.Series(50 + (idx.hour % 12), index=idx).astype(float)
    engine = VolatilityEngine(window=6)
    engine.fit(prices)
    summary = engine.diagnostics()
    assert "realized_vol" in summary
    regime = engine.classify()
    assert regime.regime in {"low", "normal", "high", "extreme"}

