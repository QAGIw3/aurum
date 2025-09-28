import pandas as pd
from aurum.ml import build_features, compute_realized_volatility


def test_build_features_basic():
    s = pd.Series([10, 11, 12, 13, 14, 15], index=pd.date_range("2024-01-01", periods=6, freq="D"))
    df = build_features(s, windows=(3,))
    assert set(["value", "return_1", "log_return_1", "roll_mean_3", "roll_std_3", "ewma_3", "realized_vol_3"]).issubset(df.columns)
    assert len(df) == 6


def test_compute_realized_volatility_no_nan():
    s = pd.Series([1, 1.1, 1.2, 1.15, 1.25, 1.3])
    vol = compute_realized_volatility(s, window=3)
    assert len(vol) == 6
    assert (vol >= 0).all()

