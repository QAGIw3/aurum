"""Tests for enhanced ScenarioService functionality."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest

from aurum.api.services.scenario_service import ScenarioService
from aurum.api.scenarios.scenario_service import InMemoryScenarioStore


@pytest.fixture()
def stub_store() -> InMemoryScenarioStore:
    return InMemoryScenarioStore()


async def _feature_fetcher(
    scenario_id: str,
    curve_families: List[str],
    start_date: datetime,
    end_date: datetime,
    geography: str,
) -> Dict[str, Any]:
    horizon_hours = min(48, int((end_date - start_date).total_seconds() // 3600))
    timestamps = [start_date + timedelta(hours=i) for i in range(horizon_hours + 1)]
    values = [50.0 + i for i in range(len(timestamps))]
    return {
        "lmp_price": values,
        "metadata": {
            "timestamps": [ts.isoformat() for ts in timestamps],
            "geography": geography,
            "curve_families": curve_families,
        },
    }


@dataclass
class _StubForecastResult:
    model_type: str
    predictions: np.ndarray
    confidence_intervals: Any
    forecast_dates: pd.DatetimeIndex
    model_params: Dict[str, Any]
    training_time: float
    prediction_time: float
    accuracy_metrics: Any = None
    model_confidence: float = 0.0


class _StubForecastEngine:
    async def enhanced_forecast(self, series: pd.Series, config) -> _StubForecastResult:
        base = float(series.iloc[-1])
        predictions = np.array([base + idx + 1 for idx in range(config.forecast_horizon)], dtype=float)
        forecast_dates = pd.date_range(
            series.index[-1] + pd.Timedelta(hours=1),
            periods=config.forecast_horizon,
            freq="H",
        )
        confidence = (predictions - 2.0, predictions + 2.0)
        return _StubForecastResult(
            model_type="stub",
            predictions=predictions,
            confidence_intervals=confidence,
            forecast_dates=forecast_dates,
            model_params={"stub": True},
            training_time=0.1,
            prediction_time=0.05,
            accuracy_metrics=None,
            model_confidence=0.9,
        )


@dataclass
class _StubModelVersion:
    version_id: str
    model_name: str
    version_number: str
    description: str
    metadata: Dict[str, Any]

    def model_dump(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "model_name": self.model_name,
            "version_number": self.version_number,
            "description": self.description,
            "metadata": self.metadata,
        }


class _StubModelRegistry:
    def __init__(self) -> None:
        self._model = _StubModelVersion(
            version_id="stub-version",
            model_name="tenant-1_lmp_price",
            version_number="v1",
            description="stub",
            metadata={"calibration_offset": 1.0, "scaling_factor": 1.1},
        )

    def get_current_champion_model(self, model_name: str) -> _StubModelVersion:
        return self._model


class _StubAnalytics:
    async def analyze_scenario_results(self, results, include_comparisons: bool = True):
        comparisons: List[Any] = []
        if include_comparisons and len(results) >= 2:
            comparisons.append(
                type(
                    "Comparison",
                    (),
                    {
                        "scenario_a_id": results[0].scenario_id,
                        "scenario_b_id": results[1].scenario_id,
                        "metrics": type("Metrics", (), {"divergence_score": 0.0})(),
                        "significant_differences": [],
                    },
                )()
            )

        return type(
            "Report",
            (),
            {
                "scenario_count": len(results),
                "comparisons": comparisons,
            },
        )()


@pytest.mark.asyncio()
async def test_clone_scenario_applies_overrides(stub_store: InMemoryScenarioStore) -> None:
    service = ScenarioService(
        store=stub_store,
        feature_fetcher=_feature_fetcher,
        forecasting_engine=_StubForecastEngine(),
        model_registry=_StubModelRegistry(),
        analytics=_StubAnalytics(),
    )

    created = await service.create_scenario(
        {
            "tenant_id": "tenant-1",
            "name": "Baseline",
            "description": "Baseline scenario",
            "assumptions": [
                {"driver_type": "load_growth", "payload": {"annual_growth_pct": 2.0}},
            ],
            "parameters": {"volume": 10.0},
            "tags": ["original"],
        }
    )

    clone = await service.clone_scenario(
        created.id,
        new_name="Baseline Copy",
        overrides={"parameters": {"volume": 20.0}, "tags": ["clone"]},
    )

    assert clone.id != created.id
    assert clone.name == "Baseline Copy"
    assert clone.parameters["volume"] == 20.0
    assert set(clone.tags) == {"original", "clone"}
    assert clone.metadata.get("cloned_from") == created.id


@pytest.mark.asyncio()
async def test_forecast_and_valuation_adjusted_by_model_registry(stub_store: InMemoryScenarioStore) -> None:
    service = ScenarioService(
        store=stub_store,
        feature_fetcher=_feature_fetcher,
        forecasting_engine=_StubForecastEngine(),
        model_registry=_StubModelRegistry(),
        analytics=_StubAnalytics(),
    )

    scenario = await service.create_scenario(
        {
            "tenant_id": "tenant-1",
            "name": "Forecasted",
            "assumptions": [
                {"driver_type": "load_growth", "payload": {"annual_growth_pct": 1.5}},
            ],
            "parameters": {"volume": 5.0},
        }
    )

    payload = await service.forecast_scenario(
        scenario.id,
        forecast_options={"forecast_horizon": 4, "target_variable": "lmp_price"},
        valuation_options={"discount_rate": 0.02},
    )

    predictions = payload["forecast"]["predictions"]
    # Each step should reflect the scaling factor applied on the unit step growth
    assert predictions[1] - predictions[0] == pytest.approx(1.1)
    assert payload["valuation"]["npv"] != 0
    assert payload["model_version"]["version_id"] == "stub-version"


@pytest.mark.asyncio()
async def test_compare_scenarios_generates_comparisons(stub_store: InMemoryScenarioStore) -> None:
    service = ScenarioService(
        store=stub_store,
        feature_fetcher=_feature_fetcher,
        forecasting_engine=_StubForecastEngine(),
        model_registry=_StubModelRegistry(),
        analytics=_StubAnalytics(),
    )

    scenarios = []
    for idx in range(2):
        scenario = await service.create_scenario(
            {
                "tenant_id": "tenant-1",
                "name": f"Scenario {idx}",
                "assumptions": [
                    {"driver_type": "load_growth", "payload": {"annual_growth_pct": 1.0 + idx}}
                ],
            }
        )
        scenarios.append(scenario.id)

    report, comparisons = await service.compare_scenarios(scenarios)

    assert report.scenario_count == len(scenarios)
    assert comparisons
    ids = {comparison.scenario_a_id for comparison in comparisons} | {comparison.scenario_b_id for comparison in comparisons}
    assert ids.issubset(set(scenarios))
