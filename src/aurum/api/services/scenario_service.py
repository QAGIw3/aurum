"""High-level scenario service with forecasting, valuation, and comparison."""

from __future__ import annotations

import asyncio
import inspect
import logging
from copy import deepcopy
from enum import Enum
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING
from uuid import uuid4

import numpy as np
import pandas as pd

from ..scenario_models import (
    ScenarioData,
    ScenarioRunData,
    ScenarioRunOptions,
    ScenarioRunPriority,
    ScenarioRunStatus,
)
from ..telemetry.context import get_request_id
from aurum.scenarios.models import ScenarioAssumption
from aurum.scenarios.analytics import ScenarioAnalytics, AnalyticsReport, ScenarioComparison
from aurum.scenarios.parallel_engine import ScenarioExecutionResult
from aurum.scenarios.validation import SimulationResult
from aurum.api.scenarios.scenario_service import InMemoryScenarioStore
from aurum.scenarios.storage import get_scenario_store
from .feature_store_service import get_features_for_scenario as _fetch_features_for_scenario

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .model_registry_service import ModelRegistryService, ModelVersion
else:  # Fallback types when optional module not available at import time
    ModelRegistryService = Any  # type: ignore[assignment]
    ModelVersion = Any  # type: ignore[assignment]


FeatureFetcher = Callable[[str, List[str], datetime, datetime, str], Awaitable[Dict[str, Any]]]


@dataclass
class _FallbackForecastConfig:
    """Lightweight config used when enhanced forecasting extras are unavailable."""

    model_type: str
    forecast_horizon: int = 24
    seasonality_periods: Optional[int] = None
    confidence_level: float = 0.95
    cross_validation_folds: int = 5
    ensemble_method: str = "weighted_average"
    adaptive_parameters: bool = True


class ScenarioService:
    """Facade providing orchestration across scenario data, features, and models."""

    def __init__(
        self,
        *,
        store: Optional[object] = None,
        feature_fetcher: Optional[FeatureFetcher] = None,
        model_registry: Optional[ModelRegistryService] = None,
        analytics: Optional[ScenarioAnalytics] = None,
        forecasting_engine: Optional[EnhancedForecastingEngine] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._store = store or self._resolve_store()
        self._feature_fetcher = feature_fetcher or _fetch_features_for_scenario
        self._model_registry = model_registry or self._resolve_model_registry()
        self._analytics = analytics or ScenarioAnalytics()
        self._forecast_config_factory = _FallbackForecastConfig
        self._forecasting_engine = forecasting_engine or self._load_default_forecasting_engine()
        self._clock = clock or datetime.utcnow

    # ------------------------------------------------------------------
    # Core CRUD operations
    # ------------------------------------------------------------------

    async def list_scenarios(
        self,
        *,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        name_contains: Optional[str] = None,
        tag: Optional[str] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Tuple[List[ScenarioData], int, Dict[str, Any]]:
        """Return paginated scenarios with simple metadata."""

        result = await self._call_store(
            "list_scenarios",
            tenant_id=tenant_id,
            status=status,
            limit=limit,
            offset=offset,
            name_contains=name_contains,
            tag=tag,
            created_after=created_after,
            created_before=created_before,
        )

        if isinstance(result, tuple) and len(result) == 2:
            records, total = result
        else:
            records = result or []
            total = len(records)

        scenarios = [self._to_scenario_data(record) for record in records]
        meta = {"request_id": get_request_id(), "count": len(scenarios)}
        return scenarios, total, meta

    async def create_scenario(self, payload: Dict[str, Any]) -> ScenarioData:
        """Create a scenario, handling heterogeneous store interfaces."""

        normalized = dict(payload)
        assumptions_payload = normalized.get("assumptions", []) or []
        assumptions = [ScenarioAssumption(**assumption) for assumption in assumptions_payload]

        method = getattr(self._store, "create_scenario")
        params = list(inspect.signature(method).parameters.values())

        if any(param.name == "scenario_data" for param in params):
            record = await self._call_store("create_scenario", normalized)
        else:
            record = await self._call_store(
                "create_scenario",
                normalized.get("tenant_id"),
                normalized.get("name"),
                normalized.get("description"),
                assumptions,
            )

        if hasattr(record, "tags"):
            setattr(record, "tags", list(normalized.get("tags", [])))
        if hasattr(record, "parameters"):
            setattr(record, "parameters", dict(normalized.get("parameters", {})))
        if hasattr(record, "metadata"):
            metadata = getattr(record, "metadata", {}) or {}
            metadata.update(normalized.get("metadata", {}))
            setattr(record, "metadata", metadata)

        scenario = self._to_scenario_data(record)
        scenario.tags = normalized.get("tags", [])
        scenario.parameters = normalized.get("parameters", {})
        scenario.metadata.update(normalized.get("metadata", {}))
        return scenario

    async def get_scenario(self, scenario_id: str, tenant_id: Optional[str] = None) -> Optional[ScenarioData]:
        """Return scenario by identifier."""

        method = getattr(self._store, "get_scenario", None)
        if method is None:
            return None

        try:
            record = await self._call_store("get_scenario", scenario_id, tenant_id=tenant_id)
        except TypeError:
            record = await self._call_store("get_scenario", scenario_id)

        return self._to_scenario_data(record) if record else None

    async def clone_scenario(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        new_name: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
        include_tags: bool = True,
        include_parameters: bool = True,
    ) -> ScenarioData:
        """Clone an existing scenario applying optional overrides."""

        source = await self.get_scenario(scenario_id, tenant_id=tenant_id)
        if source is None:
            raise ValueError(f"Scenario {scenario_id} not found")

        overrides = deepcopy(overrides or {})
        clone_payload: Dict[str, Any] = {
            "tenant_id": overrides.get("tenant_id", source.tenant_id),
            "name": new_name or overrides.get("name") or f"{source.name} (Clone)",
            "description": overrides.get("description", source.description),
            "assumptions": overrides.get("assumptions", deepcopy(source.assumptions)),
            "parameters": deepcopy(source.parameters) if include_parameters else {},
            "tags": list(source.tags) if include_tags else [],
            "metadata": deepcopy(source.metadata),
        }

        clone_payload["parameters"].update(overrides.get("parameters", {}))
        if include_tags:
            extra_tags = overrides.get("tags")
            if extra_tags:
                clone_payload["tags"].extend(tag for tag in extra_tags if tag not in clone_payload["tags"])
        else:
            clone_payload["tags"] = overrides.get("tags", [])

        clone_payload.setdefault("metadata", {})
        clone_payload["metadata"].update(
            {
                "cloned_from": source.id,
                "cloned_at": self._clock().isoformat(),
                "source_version": source.version,
            }
        )
        if overrides.get("metadata"):
            clone_payload["metadata"].update(overrides["metadata"])

        return await self.create_scenario(clone_payload)

    async def delete_scenario(self, scenario_id: str) -> bool:
        """Delete scenario by identifier."""

        method = getattr(self._store, "delete_scenario", None)
        if method is None:
            return False
        result = await self._call_store("delete_scenario", scenario_id)
        return bool(result)

    async def list_scenario_runs(
        self,
        *,
        scenario_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        state: Optional[str] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Tuple[List[ScenarioRunData], int, Dict[str, Any]]:
        """Return scenario run records."""

        result = await self._call_store(
            "list_runs",
            tenant_id=None,
            scenario_id=scenario_id,
            state=state,
            limit=limit,
            offset=offset,
            created_after=created_after,
            created_before=created_before,
        )

        if isinstance(result, tuple) and len(result) == 2:
            records, total = result
        else:
            records = result or []
            total = len(records)

        runs = [self._to_run_data(run) for run in records]
        meta = {"request_id": get_request_id(), "count": len(runs)}
        return runs, total, meta

    async def create_scenario_run(
        self,
        *,
        scenario_id: str,
        options: Dict[str, Any],
        tenant_id: Optional[str] = None,
    ) -> ScenarioRunData:
        """Create a scenario run entry via the backing store."""

        run_options = options if isinstance(options, ScenarioRunOptions) else ScenarioRunOptions(**options)

        record = await self._call_store(
            "create_run",
            scenario_id,
            tenant_id=tenant_id,
            code_version=run_options.code_version,
            seed=run_options.seed,
            parameters=run_options.parameters,
            environment=run_options.environment,
            priority=run_options.priority,
            max_retries=run_options.max_retries,
            idempotency_key=run_options.idempotency_key,
        )

        return self._to_run_data(record)

    async def get_scenario_run(self, scenario_id: str, run_id: str) -> Optional[ScenarioRunData]:
        """Return run by scenario/run identifiers."""

        method = getattr(self._store, "get_run_for_scenario", None)
        if method is None:
            return None

        try:
            record = await self._call_store("get_run_for_scenario", scenario_id, run_id, tenant_id=None)
        except TypeError:
            record = await self._call_store("get_run_for_scenario", scenario_id, run_id)

        return self._to_run_data(record) if record else None

    async def cancel_scenario_run(self, run_id: str) -> Optional[ScenarioRunData]:
        """Transition a run to CANCELLED state if supported by the store."""

        method = getattr(self._store, "update_run_state", None)
        if method is None:
            return None

        record = await self._call_store(
            "update_run_state",
            run_id,
            state=ScenarioRunStatus.CANCELLED.value,
            tenant_id=None,
        )

        return self._to_run_data(record) if record else None

    # ------------------------------------------------------------------
    # Forecasting & valuation
    # ------------------------------------------------------------------

    async def forecast_scenario(
        self,
        scenario_id: str,
        *,
        tenant_id: Optional[str] = None,
        forecast_options: Optional[Dict[str, Any]] = None,
        valuation_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Generate forecast and valuation payload for the scenario."""

        scenario = await self.get_scenario(scenario_id, tenant_id=tenant_id)
        if scenario is None:
            raise ValueError(f"Scenario {scenario_id} not found")

        forecast_options = forecast_options or {}
        valuation_options = valuation_options or {}

        start_date = forecast_options.get("start_date") or scenario.parameters.get("start_date")
        end_date = forecast_options.get("end_date") or scenario.parameters.get("end_date")
        geography = forecast_options.get("geography") or scenario.parameters.get("geography") or "US"

        end_dt = _coerce_datetime(end_date) or self._clock()
        default_window_days = int(forecast_options.get("lookback_days", 30))
        start_dt = _coerce_datetime(start_date) or (end_dt - timedelta(days=default_window_days))

        curve_families = _coerce_curve_families(scenario.metadata)
        features = await self._feature_fetcher(
            scenario_id,
            curve_families or ["price"],
            start_dt,
            end_dt,
            geography,
        )

        target_variable = forecast_options.get("target_variable", "lmp_price")
        series = _build_series_from_features(features, target_variable)

        config_cls = self._forecast_config_factory
        config = config_cls(
            model_type=str(forecast_options.get("model_type", "xgboost")),
            forecast_horizon=int(forecast_options.get("forecast_horizon", 24)),
            seasonality_periods=forecast_options.get("seasonality_periods", 24),
            confidence_level=float(forecast_options.get("confidence_level", 0.95)),
            cross_validation_folds=int(forecast_options.get("cross_validation_folds", 5)),
            ensemble_method=forecast_options.get("ensemble_method", "weighted_average"),
            adaptive_parameters=forecast_options.get("adaptive_parameters", True),
        )

        forecast_result = await self._forecasting_engine.enhanced_forecast(series, config)
        adjusted_predictions, model_version = self._apply_model_registry_adjustments(
            scenario,
            forecast_result,
            forecast_options,
        )

        valuation = _calculate_valuation(
            adjusted_predictions,
            forecast_result,
            scenario,
            valuation_options,
        )

        payload = {
            "scenario": scenario,
            "forecast": _serialize_forecast(forecast_result, adjusted_predictions),
            "valuation": valuation,
            "features": {
                "metadata": features.get("metadata", {}),
                "feature_count": len(features),
            },
            "model_version": model_version.model_dump() if model_version else None,
            "meta": {
                "request_id": get_request_id(),
                "generated_at": self._clock().isoformat(),
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "geography": geography,
            },
        }

        return payload

    async def compare_scenarios(
        self,
        scenario_ids: Sequence[str],
        *,
        tenant_id: Optional[str] = None,
        forecast_options: Optional[Dict[str, Any]] = None,
        valuation_options: Optional[Dict[str, Any]] = None,
    ) -> Tuple[AnalyticsReport, List[ScenarioComparison]]:
        """Compare scenarios using analytics generated from forecasted valuations."""

        forecast_options = forecast_options or {}
        valuation_options = valuation_options or {}

        execution_results: List[ScenarioExecutionResult] = []
        for scenario_id in scenario_ids:
            payload = await self.forecast_scenario(
                scenario_id,
                tenant_id=tenant_id,
                forecast_options=forecast_options,
                valuation_options=valuation_options,
            )

            forecast = payload["forecast"]
            valuation = payload["valuation"]
            predictions = np.array(forecast["predictions"], dtype=float)

            stats = {
                "mean": float(np.mean(predictions)),
                "std": float(np.std(predictions)),
                "p95": float(np.percentile(predictions, 95)),
                "p05": float(np.percentile(predictions, 5)),
            }
            confidence_intervals = {
                "p95": (float(np.percentile(predictions, 2.5)), float(np.percentile(predictions, 97.5)))
            }

            simulation_result = SimulationResult(
                simulation_id=str(uuid4()),
                scenario_id=scenario_id,
                results=predictions,
                statistics=stats,
                confidence_intervals=confidence_intervals,
                execution_time=forecast.get("prediction_time_seconds", 0.0),
                metadata={"valuation": valuation},
            )

            execution_results.append(
                ScenarioExecutionResult(
                    scenario_id=scenario_id,
                    status="success",
                    result=simulation_result,
                    execution_time=forecast.get("prediction_time_seconds", 0.0),
                    metadata={"forecast": forecast, "valuation": valuation},
                )
            )

        report = await self._analytics.analyze_scenario_results(execution_results, include_comparisons=True)
        return report, report.comparisons

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _call_store(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self._store, method_name)
        fn = getattr(method, "__func__", None)
        signature_target = fn or method
        if inspect.iscoroutinefunction(signature_target):
            return await method(*args, **kwargs)
        return await asyncio.to_thread(method, *args, **kwargs)

    def _to_scenario_data(self, record: Any) -> ScenarioData:
        if isinstance(record, ScenarioData):
            return record
        if record is None:
            raise ValueError("Scenario record cannot be None")

        if isinstance(record, dict):
            data = dict(record)
        elif is_dataclass(record):
            data = asdict(record)
        elif hasattr(record, "as_dict"):
            data = record.as_dict()
        else:
            data = vars(record)

        # Normalize status string to enum value expected by ScenarioData
        status_value = data.get("status") or data.get("state", ScenarioRunStatus.QUEUED.value)
        if isinstance(status_value, ScenarioRunStatus):
            status_value = status_value.value

        assumptions = [_coerce_assumption_payload(item) for item in data.get("assumptions", [])]

        scenario = ScenarioData(
            id=str(data.get("id")),
            tenant_id=str(data.get("tenant_id")),
            name=data.get("name", ""),
            description=data.get("description"),
            status=status_value,
            unique_key=data.get("unique_key", str(uuid4())),
            assumptions=assumptions,
            parameters=data.get("parameters", {}),
            tags=data.get("tags", []),
            created_at=data.get("created_at", self._clock()),
            updated_at=data.get("updated_at"),
            created_by=data.get("created_by"),
            version=data.get("version", 1),
            metadata=data.get("metadata", {}),
        )
        return scenario

    def _to_run_data(self, record: Any) -> ScenarioRunData:
        if isinstance(record, ScenarioRunData):
            return record
        if record is None:
            raise ValueError("Scenario run record cannot be None")

        if isinstance(record, dict):
            data = dict(record)
        elif is_dataclass(record):
            data = asdict(record)
        else:
            data = vars(record)

        status_value = data.get("status") or data.get("state") or ScenarioRunStatus.QUEUED
        if isinstance(status_value, ScenarioRunStatus):
            status_enum = status_value
        else:
            status_enum = ScenarioRunStatus(str(status_value))

        priority_value = data.get("priority", ScenarioRunPriority.NORMAL)
        if not isinstance(priority_value, ScenarioRunPriority):
            priority_value = ScenarioRunPriority(str(priority_value))

        run_data = ScenarioRunData(
            id=str(data.get("run_id") or data.get("id")),
            scenario_id=str(data.get("scenario_id")),
            status=status_enum,
            priority=priority_value,
            run_key=data.get("run_key"),
            input_hash=data.get("input_hash"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            duration_seconds=data.get("duration_seconds"),
            error_message=data.get("error_message"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            progress_percent=data.get("progress_percent"),
            parameters=data.get("parameters", {}),
            environment=data.get("environment", {}),
            created_at=data.get("created_at", self._clock()),
            queued_at=data.get("queued_at"),
            cancelled_at=data.get("cancelled_at"),
        )
        return run_data

    def _resolve_store(self) -> object:
        try:
            return get_scenario_store()
        except RuntimeError:
            self._logger.debug("ScenarioStore not initialized; using in-memory store for fallback")
            return InMemoryScenarioStore()

    def _resolve_model_registry(self) -> Optional[ModelRegistryService]:
        try:
            from .model_registry_service import get_model_registry_service  # pylint: disable=import-error
            return get_model_registry_service()
        except Exception:  # pragma: no cover - optional registry
            self._logger.debug("Model registry service unavailable", exc_info=True)
            return None

    def _load_default_forecasting_engine(self):
        try:
            from aurum.scenarios.enhanced_forecasting import (
                EnhancedForecastingEngine,
                EnhancedForecastConfig,
            )
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency missing
            raise ModuleNotFoundError(
                "Enhanced forecasting extras are not installed. Provide a forecasting_engine instance or install "
                "the 'aurum[forecasting]' extras."
            ) from exc

        self._forecast_config_factory = EnhancedForecastConfig
        return EnhancedForecastingEngine()

    def _apply_model_registry_adjustments(
        self,
        scenario: ScenarioData,
        forecast_result: Any,
        forecast_options: Dict[str, Any],
    ) -> Tuple[np.ndarray, Optional[ModelVersion]]:
        predictions = np.array(forecast_result.predictions, dtype=float)
        if self._model_registry is None:
            return predictions, None

        model_name = forecast_options.get("model_name") or f"{scenario.tenant_id}_{forecast_options.get('target_variable', 'lmp_price')}"
        try:
            model_version = self._model_registry.get_current_champion_model(model_name)
        except Exception:  # pragma: no cover - registry failure fallback
            self._logger.debug("Failed to load champion model", exc_info=True)
            return predictions, None

        if model_version is None:
            return predictions, None

        calibration_offset = float(model_version.metadata.get("calibration_offset", 0.0))
        scaling_factor = float(model_version.metadata.get("scaling_factor", 1.0))
        adjusted = (predictions + calibration_offset) * scaling_factor
        return adjusted, model_version


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _coerce_curve_families(metadata: Dict[str, Any]) -> List[str]:
    if not metadata:
        return []
    families = metadata.get("curve_families")
    if isinstance(families, list):
        return [str(item) for item in families]
    if isinstance(families, str):
        return [families]
    return []


def _build_series_from_features(features: Dict[str, Any], target_variable: str) -> pd.Series:
    data = features.get(target_variable)
    if data is None:
        raise ValueError(f"Target variable '{target_variable}' not present in feature payload")

    timestamps = None
    metadata = features.get("metadata", {})
    if metadata:
        timestamps = metadata.get("timestamps")
    if timestamps is None:
        timestamps = features.get("timestamps")

    if timestamps:
        index = pd.to_datetime(timestamps)
        if len(index) == len(data):
            return pd.Series(data, index=index)

    return pd.Series(data)


def _serialize_forecast(forecast: EnhancedForecastResult, adjusted_predictions: np.ndarray) -> Dict[str, Any]:
    lower, upper = (None, None)
    if forecast.confidence_intervals:
        lower_arr, upper_arr = forecast.confidence_intervals
        lower = lower_arr.tolist()
        upper = upper_arr.tolist()

    return {
        "model_type": forecast.model_type,
        "predictions": adjusted_predictions.tolist(),
        "forecast_dates": [ts.isoformat() for ts in forecast.forecast_dates] if forecast.forecast_dates is not None else None,
        "confidence_intervals": {"lower": lower, "upper": upper} if lower is not None else None,
        "training_time_seconds": forecast.training_time,
        "prediction_time_seconds": forecast.prediction_time,
        "accuracy_metrics": forecast.accuracy_metrics.model_dump() if getattr(forecast, "accuracy_metrics", None) else None,
        "model_confidence": getattr(forecast, "model_confidence", None),
    }


def _calculate_valuation(
    predictions: np.ndarray,
    forecast: EnhancedForecastResult,
    scenario: ScenarioData,
    valuation_options: Dict[str, Any],
) -> Dict[str, Any]:
    volume = float(valuation_options.get("volume", scenario.parameters.get("volume", 1.0)))
    discount_rate = float(valuation_options.get("discount_rate", scenario.parameters.get("discount_rate", 0.05)))
    periods_per_year = int(valuation_options.get("periods_per_year", 12))
    initial_investment = float(valuation_options.get("initial_investment", scenario.parameters.get("initial_investment", 0.0)))

    cashflows = predictions * volume
    discount_factors = (1 + discount_rate / periods_per_year) ** (-np.arange(1, len(cashflows) + 1))
    npv = float(np.dot(cashflows, discount_factors)) - initial_investment

    irr_func = getattr(np, "irr", None)
    irr = None
    if irr_func and initial_investment:
        try:  # numpy irr can fail if cashflows do not change sign
            irr = float(irr_func(np.concatenate(([-initial_investment], cashflows))))
        except Exception:  # pragma: no cover - numerical edge cases
            irr = None

    va_r_95 = float(np.percentile(cashflows, 5))

    return {
        "volume": volume,
        "cashflows": cashflows.tolist(),
        "npv": npv,
        "average_price": float(np.mean(predictions)),
        "volatility": float(np.std(predictions)),
        "value_at_risk_95": va_r_95,
        "discount_rate": discount_rate,
        "initial_investment": initial_investment,
        "irr": irr,
        "prediction_time_seconds": forecast.prediction_time,
    }


def _coerce_assumption_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        payload = dict(value)
    elif hasattr(value, "model_dump"):
        payload = value.model_dump()
    else:
        payload = {
            "driver_type": getattr(value, "driver_type", None),
            "payload": getattr(value, "payload", {}),
            "version": getattr(value, "version", None),
        }

    driver_type = payload.get("driver_type")
    if isinstance(driver_type, Enum):
        driver_type = driver_type.value

    return {
        "driver_type": driver_type,
        "payload": payload.get("payload", {}),
        "version": payload.get("version"),
    }
