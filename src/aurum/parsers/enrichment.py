"""Enrichment helpers for vendor curve ingestion pipelines."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Iterable, Tuple

import pandas as pd

from aurum.reference.curve_schema import CurvePriceType, CurveTenorType
from aurum.reference.units import UnitsMapper, infer_default_units

_QUARANTINE_REASON_COLUMN = "quarantine_reason"
_VALID_PRICE_TYPES = {item.value for item in CurvePriceType}
_VALID_TENOR_TYPES = {item.value for item in CurveTenorType}


def enrich_units_currency(
    df: pd.DataFrame,
    *,
    mapper: UnitsMapper | None = None,
) -> pd.DataFrame:
    """Fill missing currency/per_unit columns using shared mapping heuristics.

    The function preserves the original index and returns a copy with updated values.
    """

    if df.empty:
        return df.copy()

    mapper = mapper or UnitsMapper()

    working = df.copy()
    currency_series = working.get("currency")
    per_unit_series = working.get("per_unit")
    if currency_series is None:
        working["currency"] = pd.Series([None] * len(working), index=working.index)
    if per_unit_series is None:
        working["per_unit"] = pd.Series([None] * len(working), index=working.index)

    mask_missing = (
        working["currency"].isna()
        | (working["currency"].astype(str).str.strip() == "")
        | working["per_unit"].isna()
        | (working["per_unit"].astype(str).str.strip() == "")
    )

    if not mask_missing.any():
        return working

    for idx in working.index[mask_missing]:
        units_raw = working.at[idx, "units_raw"] if "units_raw" in working.columns else None
        iso = working.at[idx, "iso"] if "iso" in working.columns else None
        region = working.at[idx, "region"] if "region" in working.columns else None
        currency, per_unit = mapper.map(units_raw)
        if not currency or not per_unit:
            fallback_currency, fallback_unit = infer_default_units(iso, region)
            currency = currency or fallback_currency
            per_unit = per_unit or fallback_unit
        if currency:
            working.at[idx, "currency"] = currency
        if per_unit:
            working.at[idx, "per_unit"] = per_unit

    return working


def partition_quarantine(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataframe into clean and quarantined sets based on basic quality rules."""

    if df.empty:
        return df.copy(), df.copy()

    working = df.copy()
    masks: dict[str, pd.Series] = {}

    def _empty_mask(column: str) -> pd.Series:
        if column not in working.columns:
            return pd.Series(False, index=working.index)
        series = working[column]
        if pd.api.types.is_string_dtype(series):
            return series.fillna("").str.strip() == ""
        return series.isna()

    masks["missing_units_raw"] = _empty_mask("units_raw")
    masks["missing_currency"] = _empty_mask("currency")
    masks["missing_per_unit"] = _empty_mask("per_unit")
    masks["missing_curve_key"] = _empty_mask("curve_key")

    price_series = working.get("price_type")
    if price_series is not None:
        invalid_price = ~price_series.fillna("").isin(_VALID_PRICE_TYPES)
    else:
        invalid_price = pd.Series(False, index=working.index)
    masks["invalid_price_type"] = invalid_price

    tenor_series = working.get("tenor_type")
    if tenor_series is not None:
        invalid_tenor = ~tenor_series.fillna("").isin(_VALID_TENOR_TYPES)
    else:
        invalid_tenor = pd.Series(False, index=working.index)
    masks["invalid_tenor_type"] = invalid_tenor

    value_series = working.get("value")
    bid_series = working.get("bid")
    ask_series = working.get("ask")
    mid_series = working.get("mid")
    if all(series is not None for series in (value_series, bid_series, ask_series, mid_series)):
        missing_measure = (
            value_series.isna()
            & bid_series.isna()
            & ask_series.isna()
            & mid_series.isna()
        )
    elif mid_series is not None:
        missing_measure = mid_series.isna()
    else:
        missing_measure = pd.Series(False, index=working.index)
    masks["missing_measurement"] = missing_measure

    reason_lists: list[str | None] = []
    for idx in working.index:
        reasons = [name for name, mask in masks.items() if mask.iloc[idx]]
        reason_lists.append("|".join(reasons) if reasons else None)

    reason_series = pd.Series(reason_lists, index=working.index, dtype="object")
    working[_QUARANTINE_REASON_COLUMN] = reason_series

    quarantine_mask = reason_series.notna()
    quarantine_df = working.loc[quarantine_mask].copy()
    clean_df = working.loc[~quarantine_mask].copy()
    if _QUARANTINE_REASON_COLUMN in clean_df.columns:
        clean_df = clean_df.drop(columns=[_QUARANTINE_REASON_COLUMN])

    return clean_df, quarantine_df


def build_dlq_records(quarantine_df: pd.DataFrame) -> Iterable[dict]:
    """Generate DLQ payloads mirroring ``aurum.ingest.error.v1`` semantics."""

    if quarantine_df.empty:
        return []

    now_micros = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    records: list[dict] = []
    for row in quarantine_df.to_dict(orient="records"):
        context_keys = [
            "sheet_name",
            "curve_key",
            "units_raw",
            "currency",
            "per_unit",
            "price_type",
            "tenor_type",
            "tenor_label",
            "iso",
            "market",
            "region",
        ]
        context = {
            key: _stringify(row.get(key))
            for key in context_keys
            if key in row
        }
        context["quarantine_reason"] = row.get(_QUARANTINE_REASON_COLUMN) or "unknown"
        context["asof_date"] = _stringify(row.get("asof_date"))
        record = {
            "source": _stringify(row.get("source_file")) or "unknown",
            "error_message": row.get(_QUARANTINE_REASON_COLUMN) or "unknown",
            "context": context,
            "ingest_ts": now_micros,
        }
        records.append(record)
    return records


def _stringify(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


__all__ = [
    "enrich_units_currency",
    "partition_quarantine",
    "build_dlq_records",
]
