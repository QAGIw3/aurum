from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from aurum.dq import ExpectationFailedError, enforce_expectation_suite, validate_dataframe

SUITE_PATH = Path("ge/expectations/curve_schema.json")


def _sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "asof_date": "2024-01-01",
                "source_file": "test.xlsx",
                "sheet_name": "Fixed Prices - Mid",
                "asset_class": "power",
                "region": "US",
                "iso": "PJM",
                "location": "WEST",
                "market": "DAY_AHEAD",
                "product": "OnPeak",
                "block": "ON_PEAK",
                "spark_location": None,
                "price_type": "MID",
                "units_raw": "$/MWh",
                "currency": "USD",
                "per_unit": "MWh",
                "tenor_type": "MONTHLY",
                "contract_month": "2024-01-01",
                "tenor_label": "2024-01",
                "value": 45.0,
                "bid": 44.5,
                "ask": 45.5,
                "mid": 45.0,
                "curve_key": "curve-key",
                "version_hash": "hash",
                "_ingest_ts": "2024-01-01T00:00:00Z",
            }
        ]
    )


def test_validate_dataframe_returns_success() -> None:
    df = _sample_dataframe()
    results = validate_dataframe(df, SUITE_PATH)
    assert all(result.success for result in results)


def test_enforce_expectation_suite_raises_on_failure() -> None:
    df = _sample_dataframe()
    df.loc[0, "currency"] = None
    with pytest.raises(ExpectationFailedError):
        enforce_expectation_suite(df, SUITE_PATH)
