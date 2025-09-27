from __future__ import annotations

from datetime import datetime, timedelta
import math
import sys
import types

import pytest


def _ensure_api_stubs() -> None:
    """Provide lightweight stubs for optional service dependencies."""
    if "aurum.api.observability" not in sys.modules:
        observability_pkg = types.ModuleType("aurum.api.observability")
        sys.modules["aurum.api.observability"] = observability_pkg

    metrics_module = sys.modules.get("aurum.api.observability.metrics")
    if metrics_module is None:
        metrics_module = types.ModuleType("aurum.api.observability.metrics")
        sys.modules["aurum.api.observability.metrics"] = metrics_module

    if not hasattr(metrics_module, "get_metrics_client"):
        def _noop_metrics_client(*_args: object, **_kwargs: object) -> object:
            class _NoopClient:
                def __getattr__(self, _name: str) -> object:
                    return lambda *_a, **_k: None

            return _NoopClient()

        metrics_module.get_metrics_client = _noop_metrics_client  # type: ignore[attr-defined]

    if "aurum.api.logging" not in sys.modules:
        logging_pkg = types.ModuleType("aurum.api.logging")
        sys.modules["aurum.api.logging"] = logging_pkg

    structured_module = sys.modules.get("aurum.api.logging.structured_logger")
    if structured_module is None:
        structured_module = types.ModuleType("aurum.api.logging.structured_logger")
        sys.modules["aurum.api.logging.structured_logger"] = structured_module

    if not hasattr(structured_module, "get_logger"):
        class _NoopLogger:
            def __getattr__(self, _name: str) -> object:
                return lambda *_args, **_kwargs: None

        structured_module.get_logger = lambda *_a, **_k: _NoopLogger()  # type: ignore[attr-defined]


_ensure_api_stubs()

from aurum.api.services.feature_store_service import (
    FeatureStoreService,
    FeatureConfig,
    get_feature_store_service,
    get_features_for_scenario,
)


def _make_service() -> FeatureStoreService:
    return FeatureStoreService(FeatureConfig(enable_caching=False))


@pytest.mark.asyncio
async def test_create_cross_asset_features_includes_engineered_blocks() -> None:
    service = _make_service()
    end = datetime.utcnow()
    start = end - timedelta(hours=72)

    features = await service.create_cross_asset_features(start, end, geography="US")

    assert "temperature" in features
    assert "load_mw" in features
    assert len(features["timestamp"]) == len(features["load_mw"])

    engineered = features.get("engineered_features")
    assert engineered is not None
    assert "time_windows" in engineered and "lags" in engineered and "seasonal" in engineered
    assert "load_mw_rolling_24_mean" in engineered["time_windows"]
    assert "temperature_lag_24" in engineered["lags"]


@pytest.mark.asyncio
async def test_create_time_window_features_basic_operations() -> None:
    service = _make_service()
    base = {
        "timestamp": [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(4)],
        "load_mw": [10.0, 14.0, 18.0, 22.0],
    }

    result = await service.create_time_window_features(
        base,
        window_sizes=[2],
        aggregation_methods=("mean", "sum"),
        columns=["load_mw"],
    )

    mean_series = result["load_mw_rolling_2_mean"]
    sum_series = result["load_mw_rolling_2_sum"]

    assert mean_series == [10.0, 12.0, 16.0, 20.0]
    assert sum_series == [10.0, 24.0, 32.0, 40.0]


@pytest.mark.asyncio
async def test_create_lag_features_forward_fill() -> None:
    service = _make_service()
    base = {
        "timestamp": [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(4)],
        "load_mw": [10.0, 14.0, 18.0, 22.0],
    }

    result = await service.create_lag_features(
        base,
        lag_periods=[1, 2],
        columns=["load_mw"],
    )

    lag_1 = result["load_mw_lag_1"]
    lag_2 = result["load_mw_lag_2"]

    assert math.isnan(lag_1[0])
    assert lag_1[1:] == [10.0, 14.0, 18.0]
    assert math.isnan(lag_2[0]) and math.isnan(lag_2[1])
    assert lag_2[2:] == [10.0, 14.0]


@pytest.mark.asyncio
async def test_get_features_for_modeling_provides_default_engineered_features() -> None:
    service = _make_service()
    end = datetime.utcnow()
    start = end - timedelta(days=5)

    X, y = await service.get_features_for_modeling(start, end, geography="US")

    expected_keys = {
        "temperature",
        "load_mw",
        "load_mw_rolling_24_mean",
        "lmp_price_rolling_24_mean",
        "temperature_lag_24",
        "load_mw_lag_24",
        "lmp_price_lag_1",
        "hour_of_day",
    }

    assert expected_keys.issubset(set(X.keys()))
    lengths = {len(series) for series in X.values()}
    assert len(lengths) == 1
    (series_length,) = tuple(lengths)
    assert len(y) == series_length


@pytest.mark.asyncio
async def test_get_features_for_scenario_includes_engineered_groups() -> None:
    service = get_feature_store_service()
    end = datetime.utcnow()
    start = end - timedelta(days=3)

    features = await get_features_for_scenario(
        scenario_id="scenario-123",
        curve_families=["weather", "load", "price"],
        start_date=start,
        end_date=end,
        geography="US",
    )

    assert "scenario_metadata" in features
    assert features["scenario_metadata"]["scenario_id"] == "scenario-123"

    engineered = features.get("engineered_features")
    assert engineered is not None
    assert "time_windows" in engineered and "lags" in engineered and "seasonal" in engineered
    assert "load_mw_rolling_24_mean" in engineered["time_windows"]
    assert "temperature_lag_24" in engineered["lags"]
