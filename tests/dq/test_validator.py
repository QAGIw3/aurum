from pathlib import Path

import json
import pandas as pd
import pytest

from aurum.dq import validator


def test_env_overrides(monkeypatch, tmp_path: Path):
    suite = {
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "value", "min_value": 0, "max_value": 5},
                "meta": {
                    "env_overrides": {
                        "min_value": "GE_TEST_MIN",
                        "max_value": "GE_TEST_MAX",
                    }
                },
            }
        ]
    }
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(json.dumps(suite))

    df = pd.DataFrame({"value": [1, 2, 3]})
    monkeypatch.setenv("GE_TEST_MIN", "2")
    monkeypatch.setenv("GE_TEST_MAX", "4")

    results = validator.validate_dataframe(df, suite_path)
    assert len(results) == 1
    assert not results[0].success
    assert "only" in (results[0].details or "")


def test_row_condition_proportion(tmp_path: Path):
    suite = {
        "expectations": [
            {
                "expectation_type": "expect_column_proportion_of_nonnulls_to_be_between",
                "kwargs": {
                    "column": "value",
                    "row_condition": "segment == 'A'",
                    "min_value": 0.5,
                    "max_value": 1.0,
                },
            }
        ]
    }
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(json.dumps(suite))

    df = pd.DataFrame(
        {
            "segment": ["A", "A", "B"],
            "value": [1.0, None, 2.0],
        }
    )

    results = validator.validate_dataframe(df, suite_path)
    assert len(results) == 1
    # Only rows in segment A considered => one value, one null => proportion 0.5 satisfies bounds
    assert results[0].success
