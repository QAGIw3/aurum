from __future__ import annotations

from datetime import date

import pandas as pd

from aurum.parsers.enrichment import build_dlq_records, enrich_units_currency, partition_quarantine


def test_enrich_units_currency_fills_missing_values() -> None:
    df = pd.DataFrame(
        {
            "units_raw": ["USD/MWh", "CAD/MWh"],
            "currency": [None, "CAD"],
            "per_unit": [None, None],
            "iso": ["PJM", "AESO"],
            "region": ["US", "CA"],
        }
    )

    enriched = enrich_units_currency(df)

    assert enriched.loc[0, "currency"] == "USD"
    assert enriched.loc[0, "per_unit"] == "MWh"
    assert enriched.loc[1, "currency"] == "CAD"
    assert enriched.loc[1, "per_unit"] == "MWh"


def test_partition_quarantine_flags_missing_currency() -> None:
    df = pd.DataFrame(
        {
            "units_raw": ["USD/MWh", "USD/MWh"],
            "currency": ["USD", None],
            "per_unit": ["MWh", "MWh"],
            "curve_key": ["key1", "key2"],
            "price_type": ["MID", "MID"],
            "tenor_type": ["MONTHLY", "MONTHLY"],
        }
    )

    clean_df, quarantine_df = partition_quarantine(df)

    assert len(clean_df) == 1
    assert len(quarantine_df) == 1
    assert quarantine_df.iloc[0]["quarantine_reason"] and "missing_currency" in quarantine_df.iloc[0]["quarantine_reason"]


def test_build_dlq_records_produces_expected_payload() -> None:
    df = pd.DataFrame(
        {
            "source_file": ["book.xlsx"],
            "sheet_name": ["Fixed Prices"],
            "curve_key": ["abc"],
            "units_raw": ["USD/MWh"],
            "currency": ["USD"],
            "per_unit": ["MWh"],
            "price_type": ["MID"],
            "tenor_type": ["MONTHLY"],
            "tenor_label": ["2024-01"],
            "iso": ["PJM"],
            "market": ["DA"],
            "region": ["US"],
            "asof_date": [date(2024, 1, 1)],
            "quarantine_reason": ["missing_measurement"],
        }
    )

    records = list(build_dlq_records(df))
    assert len(records) == 1
    payload = records[0]
    assert payload["source"] == "book.xlsx"
    assert payload["error_message"] == "missing_measurement"
    assert payload["context"]["curve_key"] == "abc"
    assert payload["context"]["quarantine_reason"] == "missing_measurement"
