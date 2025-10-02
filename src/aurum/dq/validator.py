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

    if expectation_type == "expect_column_values_to_be_of_type":
        # Minimal type checker to support common GE types seen in suites
        type_ = kwargs.get("type_") or kwargs.get("type")
        if type_ is None:
            raise ValueError("expect_column_values_to_be_of_type requires 'type_'")
        non_null = series.dropna()
        if non_null.empty:
            return ExpectationResult(expectation_type, kwargs, True)

        # Normalize target type
        target = str(type_).upper()

        def _is_type(s: pd.Series, target_type: str) -> bool:
            dtype = s.dtype
            if target_type in {"FLOAT", "FLOAT64", "DOUBLE"}:
                return pd.api.types.is_float_dtype(dtype)
            if target_type in {"INTEGER", "INT", "INT64"}:
                return pd.api.types.is_integer_dtype(dtype)
            if target_type in {"BOOLEAN", "BOOL"}:
                return pd.api.types.is_bool_dtype(dtype)
            if target_type in {"STRING", "STR", "OBJECT"}:
                return pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype)
            if target_type in {"DATE", "DATETIME", "TIMESTAMP"}:
                return pd.api.types.is_datetime64_any_dtype(dtype)
            # Fallback: exact match check on dtype name
            return str(dtype).lower() == target_type.lower()

        success = _is_type(non_null, target)
        details = None if success else f"observed dtype '{non_null.dtype}' not compatible with '{target}'"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_pair_values_to_be_equal":
        col_a = kwargs.get("column_A") or kwargs.get("column_a")
        col_b = kwargs.get("column_B") or kwargs.get("column_b")
        mostly = kwargs.get("mostly", 1.0)
        if not col_a or not col_b:
            raise ValueError("expect_column_pair_values_to_be_equal requires 'column_A' and 'column_B'")

        filtered_df = _filter_dataframe(df, kwargs)
        missing = [c for c in (col_a, col_b) if c not in filtered_df.columns]
        if missing:
            raise ValueError(f"Columns {missing} missing from dataframe for expect_column_pair_values_to_be_equal")

        a = filtered_df[col_a]
        b = filtered_df[col_b]
        # Consider pairs equal when both are NaN; compare non-null pairs strictly
        both_null = a.isna() & b.isna()
        non_null = ~(both_null) & ~(a.isna() | b.isna())
        eq_mask = both_null | (non_null & (a[non_null].values == b[non_null].values))
        ratio = _fraction(eq_mask, len(filtered_df))
        success = ratio >= mostly
        details = None if success else f"only {ratio:.3f} pairs equal between '{col_a}' and '{col_b}'"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type == "expect_column_pair_values_A_to_be_greater_than_B":
        col_a = kwargs.get("column_A") or kwargs.get("column_a")
        col_b = kwargs.get("column_B") or kwargs.get("column_b")
        or_equal = bool(kwargs.get("or_equal", False))
        mostly = kwargs.get("mostly", 1.0)
        if not col_a or not col_b:
            raise ValueError("expect_column_pair_values_A_to_be_greater_than_B requires 'column_A' and 'column_B'")

        filtered_df = _filter_dataframe(df, kwargs)
        missing = [c for c in (col_a, col_b) if c not in filtered_df.columns]
        if missing:
            raise ValueError(f"Columns {missing} missing from dataframe for expect_column_pair_values_A_to_be_greater_than_B")

        a = filtered_df[col_a]
        b = filtered_df[col_b]
        # Treat both null as pass; compare only rows with both non-null
        both_null = a.isna() & b.isna()
        both_non_null = ~(a.isna() | b.isna())
        if or_equal:
            comp = a[both_non_null] >= b[both_non_null]
        else:
            comp = a[both_non_null] > b[both_non_null]
        ok_mask = both_null.copy()
        ok_mask[both_non_null] = comp
        ratio = _fraction(ok_mask, len(filtered_df))
        success = ratio >= mostly
        details = None if success else f"only {ratio:.3f} rows satisfy {col_a} {'>=' if or_equal else '>'} {col_b}"
        return ExpectationResult(expectation_type, kwargs, success, details)

    if expectation_type in {
        "expect_compound_columns_to_be_unique",
        "expect_multicolumn_values_to_be_unique",
    }:
        columns = kwargs.get("column_list")
        if not columns:
            raise ValueError(f"{expectation_type} requires 'column_list'")
        missing = [col for col in columns if col not in filtered_df.columns]
        if missing:
            raise ValueError(f"Columns {missing} missing from dataframe for {expectation_type}")
        subset = filtered_df[columns]
        duplicates_mask = subset.duplicated(keep=False)
        success = not duplicates_mask.any()
        details = None if success else f"duplicate rows for columns {columns}"
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
