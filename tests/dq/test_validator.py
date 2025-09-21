from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from aurum.dq import ExpectationFailedError, enforce_expectation_suite, validate_dataframe


def _write_suite(tmp_path: Path, expectations):
    suite = {
        "expectation_suite_name": "test_suite",
        "expectations": expectations,
    }
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(suite), encoding="utf-8")
    return path


def test_compound_columns_unique_pass(tmp_path: Path):
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
        }
    )
    suite_path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {"column_list": ["a", "b"]},
            }
        ],
    )
    results = validate_dataframe(df, suite_path)
    assert all(result.success for result in results)


def test_compound_columns_unique_fail(tmp_path: Path):
    df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": ["x", "x"],
        }
    )
    suite_path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_multicolumn_values_to_be_unique",
                "kwargs": {"column_list": ["a", "b"]},
            }
        ],
    )
    with pytest.raises(ExpectationFailedError) as excinfo:
        enforce_expectation_suite(df, suite_path)
    assert "duplicate" in str(excinfo.value).lower()


def test_expectation_missing_columns(tmp_path: Path):
    df = pd.DataFrame({"a": [1]})
    suite_path = _write_suite(
        tmp_path,
        [
            {
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {"column_list": ["a", "b"]},
            }
        ],
    )
    with pytest.raises(ValueError):
        validate_dataframe(df, suite_path)
