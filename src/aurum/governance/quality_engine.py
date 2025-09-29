"""Data quality orchestration for governance workflows."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence

import pandas as pd

try:  # pragma: no cover - optional dependency
    import great_expectations as ge
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.dataset import PandasDataset
except Exception:  # pragma: no cover - preserve runtime when GE not installed
    ge = None  # type: ignore
    AbstractDataContext = object  # type: ignore
    PandasDataset = pd.DataFrame  # type: ignore

logger = logging.getLogger(__name__)


class QualityResultPublisher(Protocol):
    def publish(self, result: "DataQualitySuiteResult", score: float) -> None:  # pragma: no cover - protocol
        ...


@dataclass
class DataQualityCheckResult:
    name: str
    passed: bool
    score: float
    severity: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataQualitySuiteResult:
    asset_ref: str
    suite_name: str
    started_at: datetime
    completed_at: datetime
    results: List[DataQualityCheckResult]
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.results)


@dataclass
class BuiltinCheck:
    """Configuration for a built-in check."""

    name: str
    check_type: str
    column: Optional[str] = None
    threshold: Optional[float] = None
    reference_schema: Optional[Mapping[str, str]] = None
    window_minutes: Optional[int] = None


@dataclass
class TestSuiteConfig:
    """Generic suite configuration for the engine."""

    suite_name: str
    ge_expectation_suite: Optional[str] = None
    ge_context: Optional[str] = None
    builtin_checks: Sequence[BuiltinCheck] = ()
    weights: Mapping[str, float] = field(default_factory=dict)


class DataQualityEngine:
    """Coordinates execution of data quality test suites."""

    def __init__(
        self,
        *,
        publishers: Sequence[QualityResultPublisher] = (),
        default_weights: Optional[Mapping[str, float]] = None,
    ) -> None:
        self.publishers = list(publishers)
        self.default_weights = dict(default_weights or {})

    def run_tests(
        self,
        *,
        asset_ref: str,
        dataframe: pd.DataFrame,
        suite: TestSuiteConfig,
    ) -> DataQualitySuiteResult:
        started = datetime.now(timezone.utc)
        results: List[DataQualityCheckResult] = []

        if suite.ge_expectation_suite:
            ge_results = self._run_ge_suite(
                dataframe=dataframe,
                ge_suite=suite.ge_expectation_suite,
                context_root=suite.ge_context,
            )
            results.extend(ge_results)

        if suite.builtin_checks:
            results.extend(self._run_builtin_checks(dataframe, suite.builtin_checks))

        completed = datetime.now(timezone.utc)
        run_result = DataQualitySuiteResult(
            asset_ref=asset_ref,
            suite_name=suite.suite_name,
            started_at=started,
            completed_at=completed,
            results=results,
        )

        overall_score = self.compute_score(run_result, weights=suite.weights)
        self.publish_results(run_result, overall_score)
        return run_result

    def compute_score(
        self,
        suite_result: DataQualitySuiteResult,
        *,
        weights: Optional[Mapping[str, float]] = None,
    ) -> float:
        check_weights = {**self.default_weights, **(weights or {})}
        weighted_scores: List[float] = []
        total_weight = 0.0
        for check in suite_result.results:
            weight = check_weights.get(check.name, 1.0)
            weighted_scores.append(check.score * weight)
            total_weight += weight
        if not weighted_scores or total_weight == 0:
            return 0.0
        return round(sum(weighted_scores) / total_weight, 2)

    def publish_results(self, result: DataQualitySuiteResult, score: float) -> None:
        for publisher in self.publishers:
            try:
                publisher.publish(result, score)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("Failed to publish data quality result via %s: %s", publisher, exc)

    def _run_ge_suite(
        self,
        *,
        dataframe: pd.DataFrame,
        ge_suite: str,
        context_root: Optional[str],
    ) -> List[DataQualityCheckResult]:
        if ge is None:
            raise RuntimeError("great_expectations is not installed")

        context: AbstractDataContext
        if context_root:
            context = ge.get_context(context_root_dir=context_root)  # type: ignore[attr-defined]
        else:
            context = ge.get_context()  # type: ignore[attr-defined]

        dataset: PandasDataset = ge.dataset.PandasDataset(dataframe.copy())  # type: ignore[attr-defined]
        expectation_suite = context.get_expectation_suite(ge_suite)  # type: ignore[attr-defined]
        dataset._append_expectation_suite(expectation_suite)  # type: ignore[attr-defined]
        validation = dataset.validate(result_format="COMPLETE")  # type: ignore[attr-defined]

        results: List[DataQualityCheckResult] = []
        for result in validation.get("results", []):
            expectation = result.get("expectation_config", {}).get("expectation_type", "unknown")
            success = bool(result.get("success"))
            metrics = result.get("result", {})
            severity = "high" if expectation.startswith("expect_table") else "medium"
            score = 1.0 if success else 0.0
            results.append(
                DataQualityCheckResult(
                    name=expectation,
                    passed=success,
                    score=score,
                    severity=severity,
                    metrics=metrics,
                    details={"expectation": expectation},
                )
            )
        return results

    def _run_builtin_checks(
        self,
        dataframe: pd.DataFrame,
        checks: Sequence[BuiltinCheck],
    ) -> List[DataQualityCheckResult]:
        results: List[DataQualityCheckResult] = []
        for check in checks:
            if check.check_type == "freshness":
                results.append(self._check_freshness(dataframe, check))
            elif check.check_type == "completeness":
                results.append(self._check_completeness(dataframe, check))
            elif check.check_type == "schema_drift":
                results.append(self._check_schema_drift(dataframe, check))
            else:
                logger.warning("Unknown built-in check type %s", check.check_type)
        return results

    def _check_freshness(self, dataframe: pd.DataFrame, check: BuiltinCheck) -> DataQualityCheckResult:
        if check.column is None:
            raise ValueError("Freshness check requires a column")
        if dataframe.empty:
            return DataQualityCheckResult(check.name, False, 0.0, "high", {"reason": "empty dataset"})

        if check.column not in dataframe.columns:
            return DataQualityCheckResult(check.name, False, 0.0, "high", {"reason": "missing column"})

        window_minutes = check.window_minutes or 1440
        timestamps = pd.to_datetime(dataframe[check.column], utc=True, errors="coerce")
        latest = timestamps.max()
        if pd.isna(latest):
            return DataQualityCheckResult(check.name, False, 0.0, "high", {"reason": "no valid timestamps"})
        age_minutes = (datetime.now(timezone.utc) - latest.to_pydatetime()).total_seconds() / 60
        passed = age_minutes <= window_minutes
        score = max(0.0, min(1.0, 1 - (age_minutes / window_minutes))) if window_minutes else 0.0
        metrics = {"age_minutes": round(age_minutes, 2), "window_minutes": window_minutes}
        return DataQualityCheckResult(check.name, passed, round(score, 2), "high", metrics)

    def _check_completeness(self, dataframe: pd.DataFrame, check: BuiltinCheck) -> DataQualityCheckResult:
        if check.column is None:
            raise ValueError("Completeness check requires a column")
        if check.column not in dataframe.columns or dataframe.empty:
            return DataQualityCheckResult(check.name, False, 0.0, "medium", {"reason": "missing column or empty"})

        completeness = 1.0 - dataframe[check.column].isna().mean()
        threshold = check.threshold or 0.99
        passed = completeness >= threshold
        metrics = {"completeness": round(float(completeness), 4), "threshold": threshold}
        return DataQualityCheckResult(check.name, passed, round(float(completeness), 2), "medium", metrics)

    def _check_schema_drift(self, dataframe: pd.DataFrame, check: BuiltinCheck) -> DataQualityCheckResult:
        if not check.reference_schema:
            raise ValueError("Schema drift check requires a reference schema")

        expected = {col: dtype for col, dtype in check.reference_schema.items()}
        actual = {col: str(dtype) for col, dtype in dataframe.dtypes.items()}

        mismatches: Dict[str, Dict[str, str]] = {}
        for column, expected_dtype in expected.items():
            actual_dtype = actual.get(column)
            if actual_dtype != expected_dtype:
                mismatches[column] = {"expected": expected_dtype, "actual": actual_dtype}

        extra_columns = set(actual).difference(expected)
        missing_columns = set(expected).difference(actual)

        passed = not mismatches and not extra_columns and not missing_columns
        metrics = {
            "mismatches": mismatches,
            "extraColumns": sorted(extra_columns),
            "missingColumns": sorted(missing_columns),
        }
        score = 1.0 if passed else max(0.0, 1 - 0.2 * (len(mismatches) + len(extra_columns) + len(missing_columns)))
        return DataQualityCheckResult(check.name, passed, round(score, 2), "high", metrics)


__all__ = [
    "BuiltinCheck",
    "DataQualityCheckResult",
    "DataQualityEngine",
    "DataQualitySuiteResult",
    "QualityResultPublisher",
    "TestSuiteConfig",
]
