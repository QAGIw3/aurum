from __future__ import annotations

from datetime import date

import pandas as pd

from aurum.parsers.enrichment import build_dlq_records, enrich_units_currency, partition_quarantine


def test_pipeline_enrichment_and_quarantine_flow() -> None:
    df = pd.DataFrame(
        {
            "asof_date": [date(2025, 1, 1), date(2025, 1, 1)],
            "source_file": ["pw.xlsx", "pw.xlsx"],
            "sheet_name": ["Fixed Prices", "Fixed Prices"],
            "asset_class": ["power", "power"],
            "region": [None, "US"],
            "iso": [None, "PJM"],
            "location": ["UNKNOWN", "AECO"],
            "market": ["DA", "DA"],
            "product": ["power", "power"],
            "block": ["ON_PEAK", "ON_PEAK"],
            "spark_location": [None, None],
            "price_type": ["MID", "MID"],
            "units_raw": [None, "USD/MWh"],
            "currency": [None, "USD"],
            "per_unit": [None, "MWh"],
            "tenor_type": ["MONTHLY", "MONTHLY"],
            "contract_month": [date(2025, 2, 1), date(2025, 3, 1)],
            "tenor_label": ["2025-02", "2025-03"],
            "value": [45.5, None],
            "bid": [45.0, 44.5],
            "ask": [46.0, 45.5],
            "mid": [None, None],
            "curve_key": ["aaa", "bbb"],
            "version_hash": ["xxx", "yyy"],
            "_ingest_ts": pd.Timestamp("2025-01-01T12:00:00Z"),
        }
    )

    enriched = enrich_units_currency(df)
    clean_df, quarantine_df = partition_quarantine(enriched)

    assert "quarantine_reason" in clean_df.columns
    assert clean_df["quarantine_reason"].isna().all()
    assert len(quarantine_df) == 1
    reason = quarantine_df.iloc[0]["quarantine_reason"]
    assert reason and "missing_currency" in reason

    records = list(build_dlq_records(quarantine_df))
    assert len(records) == 1
    payload = records[0]
    assert payload["source"] == "pw.xlsx"
    assert "missing_currency" in payload["error_message"]
    assert payload["context"]["currency"] is None
