"""Minimal expectation-suite executor for pandas dataframes."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import json
import os
import pandas as pd


@dataclass
class ExpectationResult:
    expectation_type: str
    kwargs: Dict[str, Any]
    success: bool
    details: str | None = None


class ExpectationFailedError(RuntimeError):
    """Raised when one or more expectations fail."""

    def __init__(self, suite_name: str, failures: List[ExpectationResult]) -> None:
        messages = [
            f"{failure.expectation_type}({failure.kwargs}) failed: {failure.details or 'no details'}"
            for failure in failures
        ]
        message = f"Expectation suite '{suite_name}' failed: " + "; ".join(messages)
        super().__init__(message)
        self.suite_name = suite_name
        self.failures = failures


def _fraction(valid: pd.Series, total_count: int) -> float:
    if total_count == 0:
        return 1.0
    return valid.sum() / total_count


def _filter_dataframe(df: pd.DataFrame, kwargs: Dict[str, Any]) -> pd.DataFrame:
    condition = kwargs.get("row_condition")
    if not condition:
        return df
    parser = kwargs.get("condition_parser", "pandas")
    if parser != "pandas":
        raise ValueError(f"Unsupported condition_parser '{parser}'")
    try:
        return df.query(condition)
    except Exception as exc:
        raise ValueError(f"Failed to evaluate row_condition '{condition}': {exc}") from exc


def _evaluate_expectation(df: pd.DataFrame, expectation: Dict[str, Any]) -> ExpectationResult:
    expectation_type = expectation["expectation_type"]
    kwargs = expectation.get("kwargs", {})

    if expectation_type == "expect_table_columns_to_match_set":
        expected = set(kwargs.get("column_set", []))
        actual = set(df.columns)
        success = actual == expected
        details = f"expected={sorted(expected)}, actual={sorted(actual)}" if not success else None
        return ExpectationResult(expectation_type, kwargs, success, details)

    column = kwargs.get("column")
    filtered_df = _filter_dataframe(df, kwargs)
    series = filtered_df[column] if column in filtered_df.columns else pd.Series(dtype="object")
    total_count = len(series)

    if expectation_type == "expect_column_values_to_not_be_null":
        success = series.notna().all()
        details = None if success else f"{series.isna().sum()} null values found"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_values_to_be_in_set":
        value_set = set(kwargs.get("value_set", []))
        non_null = series.dropna()
        success = non_null.isin(value_set).all()
        details = None
        if not success:
            invalid = non_null[~non_null.isin(value_set)].unique().tolist()
            details = f"invalid values: {invalid}"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_values_to_be_between":
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        mostly = kwargs.get("mostly", 1.0)
        non_null = series.dropna()
        if non_null.empty:
            return ExpectationResult(expectation_type, kwargs, True)
        mask = pd.Series(True, index=non_null.index)
        if min_value is not None:
            mask &= non_null >= min_value
        if max_value is not None:
            mask &= non_null <= max_value
        ratio = _fraction(mask, len(non_null))
        success = ratio >= mostly
        details = None if success else f"only {ratio:.3f} within bounds"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_values_to_be_unique":
        non_null = series.dropna()
        success = non_null.is_unique
        details = None
        if not success:
            duplicate_count = len(non_null) - len(non_null.drop_duplicates())
            details = f"{duplicate_count} duplicates found"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_values_to_match_regex":
        regex = kwargs.get("regex")
        if regex is None:
            raise ValueError("expect_column_values_to_match_regex requires 'regex'")
        non_null = series.dropna().astype(str)
        if non_null.empty:
            return ExpectationResult(expectation_type, kwargs, True)
        mask = non_null.str.match(regex)
        success = mask.all()
        details = None
        if not success:
            details = f"values not matching regex: {non_null[~mask].unique().tolist()}"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_proportion_of_nonnulls_to_be_between":
        min_value = kwargs.get("min_value", 0.0)
        max_value = kwargs.get("max_value", 1.0)
        proportion = _fraction(series.notna(), total_count)
        success = proportion >= min_value and proportion <= max_value
        details = None if success else f"observed proportion {proportion:.3f}"
        return ExpectationResult(expectation_type, kwargs, success, details)

    raise NotImplementedError(f"Expectation type '{expectation_type}' not supported")


def validate_dataframe(df: pd.DataFrame, suite_path: Path | str) -> List[ExpectationResult]:
    """Evaluate the expectations defined in ``suite_path`` against ``df``."""
    suite_path = Path(suite_path)
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    expectations = suite.get("expectations", [])

    processed: List[Dict[str, Any]] = []
    for expectation in expectations:
        overrides = expectation.get("meta", {}).get("env_overrides", {})
        if overrides:
            expectation = expectation.copy()
            kwargs = dict(expectation.get("kwargs", {}))
            for key, env_var in overrides.items():
                raw = os.getenv(env_var)
                if raw is None:
                    continue
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError:
                    value = raw
                kwargs[key] = value
            expectation["kwargs"] = kwargs
        processed.append(expectation)

    results = [_evaluate_expectation(df, expectation) for expectation in processed]
    return results


def enforce_expectation_suite(
    df: pd.DataFrame,
    suite_path: Path | str,
    *,
    suite_name: str | None = None,
) -> None:
    """Raise ``ExpectationFailedError`` if any expectation from ``suite_path`` fails."""
    suite_path = Path(suite_path)
    suite_name = suite_name or suite_path.stem
    results = validate_dataframe(df, suite_path)
    failures = [result for result in results if not result.success]
    if failures:
        raise ExpectationFailedError(suite_name, failures)
