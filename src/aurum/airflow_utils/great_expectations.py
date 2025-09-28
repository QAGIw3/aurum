"""Great Expectations helpers for Airflow tasks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

try:  # pragma: no cover - ge optional in some environments
    from aurum.dq import enforce_expectation_suite
except Exception as exc:  # pragma: no cover - allow missing dependency during linting
    enforce_expectation_suite = None  # type: ignore[assignment]

try:  # pragma: no cover - Airflow optional in unit tests
    from airflow.models.xcom_arg import XComArg  # type: ignore
except Exception:  # pragma: no cover - maintain compatibility without Airflow
    XComArg = None  # type: ignore


__all__ = ["ExpectationSuiteConfig", "validate_dataframe"]


@dataclass(frozen=True)
class ExpectationSuiteConfig:
    """Configuration describing a validation suite to enforce."""

    suite_path: Path
    suite_name: str
    cache_results: bool = False
    fail_on_empty: bool = False


def _load_dataframe(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(list(frames), ignore_index=True)


def _normalise_frames(df: pd.DataFrame | Iterable[pd.DataFrame] | None) -> Sequence[pd.DataFrame]:
    if df is None:
        return []
    if isinstance(df, pd.DataFrame):
        return [df]
    if isinstance(df, Iterable):
        frames: list[pd.DataFrame] = []
        for item in df:
            if not isinstance(item, pd.DataFrame):
                raise TypeError("Expected iterable of pandas.DataFrame")
            frames.append(item)
        return frames
    raise TypeError("Expected pandas.DataFrame or iterable of DataFrame")


def validate_dataframe(
    df: pd.DataFrame | Iterable[pd.DataFrame] | None,
    *,
    suite: ExpectationSuiteConfig,
    context: Optional[dict[str, Any]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Validate dataframe(s) against the configured expectation suite.

    Returns a payload describing pass/fail status suitable for XCom. When
    ``df`` is ``None`` or empty the behaviour depends on ``suite.fail_on_empty``.
    """

    if enforce_expectation_suite is None:
        raise RuntimeError("Great Expectations dependency not available")

    frames = _normalise_frames(df)
    data = _load_dataframe(frames)

    if data.empty:
        status = "skipped" if not suite.fail_on_empty else "failed"
        payload = {
            "suite": suite.suite_name,
            "status": status,
            "rows": 0,
        }
    else:
        enforce_expectation_suite(data, suite.suite_path, suite_name=suite.suite_name)
        payload = {
            "suite": suite.suite_name,
            "status": "passed",
            "rows": int(len(data)),
        }

    if metadata:
        payload["metadata"] = dict(metadata)

    if context and "ti" in context:
        ti = context["ti"]
        try:  # pragma: no cover - depends on Airflow env
            ti.xcom_push(key=f"ge::{suite.suite_name}", value=payload)
        except Exception:
            pass

    return payload

