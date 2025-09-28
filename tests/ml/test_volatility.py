import numpy as np
import pandas as pd

from aurum.ml import (
    VolatilityEngine,
    garman_klass_volatility,
    parkinson_volatility,
    realized_volatility,
)


def test_volatility_engine_diagnostics():
    idx = pd.date_range("2024-01-01", periods=50, freq="h")
    prices = pd.Series(50 + (idx.hour % 12), index=idx).astype(float)
    highs = prices + 0.5
    lows = prices - 0.5
    opens = prices.copy()
    engine = VolatilityEngine(window=6)
    engine.fit(prices, high=highs, low=lows, open_=opens)
    summary = engine.diagnostics()
    assert "realized_vol" in summary
    assert "parkinson_vol" in summary
    regime = engine.classify()
    assert regime.regime in {"low", "normal", "high", "extreme"}


def test_volatility_estimators_behave():
    idx = pd.date_range("2024-01-01", periods=30, freq="h")
    base = pd.Series(np.linspace(40, 42, len(idx)), index=idx)
    high = base + 1.0
    low = base - 1.0
    close = base + 0.2
    open_ = base - 0.2
    rv = realized_volatility(close)
    pv = parkinson_volatility(high, low)
    gk = garman_klass_volatility(open_, high, low, close)
    assert (rv >= 0).all()
    assert (pv >= 0).all()
    assert (gk >= 0).all()
