import numpy as np
import pandas as pd

from aurum.ml import (
    FeaturePipeline,
    FeaturePipelineConfig,
    build_features,
    compute_realized_volatility,
)


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


def test_feature_pipeline_with_exogenous_and_calendar():
    idx = pd.date_range("2024-01-01", periods=48, freq="h")
    series = pd.Series(np.linspace(10, 15, len(idx)), index=idx)
    exogenous = pd.DataFrame({"temp": np.linspace(30, 35, len(idx))}, index=idx)
    config = FeaturePipelineConfig(
        lags=(1, 2),
        rolling_windows=(3,),
        ewma_spans=(3,),
        differences=(1,),
        include_calendar=True,
        include_volatility=True,
    )
    pipeline = FeaturePipeline(config, freq="h")
    features = pipeline.fit(series, exogenous=exogenous)
    assert {"lag_1", "diff_1", "roll_mean_3", "hour", "exo_temp"}.issubset(features.columns)
    future_idx = pd.date_range(idx[-1] + pd.Timedelta(hours=1), periods=2, freq="h")
    exo_future = pd.DataFrame({"temp": [36, 37]}, index=future_idx)
    future_features = pipeline.make_future_features(2, exogenous_future=exo_future)
    assert future_features.index.equals(future_idx)
    assert future_features.isna().sum().sum() == 0


def test_feature_pipeline_range_index_future():
    series = pd.Series([1, 2, 3, 4, 5, 6])
    config = FeaturePipelineConfig(
        lags=(1,),
        rolling_windows=(2,),
        include_calendar=False,
        include_volatility=False,
    )
    pipeline = FeaturePipeline(config)
    pipeline.fit(series)
    future = pipeline.make_future_features(3)
    assert isinstance(future.index, pd.RangeIndex)
    assert future.index.equals(pd.RangeIndex(start=0, stop=3))
