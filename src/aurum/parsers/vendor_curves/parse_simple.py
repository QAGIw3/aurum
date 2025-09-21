"""Simple vendor parser used for internal testing and onboarding new formats.

Expected workbook layout:
- A sheet named "Fixed Prices - Mid" with header rows in the first column
  containing labels like ISO, Market, Hours, Location, Product, Units followed
  by values in the same row's third column.
- Data starts where column 2 contains tenor labels (dates or strings) and
  subsequent columns contain numeric mid prices.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable, Dict, List, Optional

import logging
import pandas as pd

from . import register
from .schema import CANONICAL_COLUMNS
from ...reference import infer_default_units, map_units
from ...reference.curve_schema import CurveAssetClass, CurvePriceType
from ..utils import (
    compute_curve_key,
    compute_version_hash,
    derive_region,
    infer_tenor_type,
    normalise_tenor_label,
    safe_str,
    to_float,
)

LOGGER = logging.getLogger(__name__)


HEADERS_TO_CAPTURE = {
    "iso": "iso",
    "market": "market",
    "hours": "block",
    "location": "location",
    "product": "product",
    "units": "units",
}


def parse(path: str, asof: date) -> pd.DataFrame:
    book = pd.ExcelFile(path)
    source_file = Path(path).name
    frames: List[pd.DataFrame] = []
    if "Fixed Prices - Mid" in book.sheet_names:
        df_mid = _parse_mid_sheet(book.parse("Fixed Prices - Mid", header=None), source_file, asof)
        if not df_mid.empty:
            frames.append(df_mid)

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined["value"] = combined["mid"]
    combined["_ingest_ts"] = pd.Timestamp.utcnow()
    combined["contract_month"] = pd.to_datetime(combined["contract_month"], errors="coerce").dt.date
    for column in CANONICAL_COLUMNS:
        if column not in combined.columns:
            combined[column] = pd.NA
    return combined[CANONICAL_COLUMNS]


def _parse_mid_sheet(df: pd.DataFrame, source_file: str, asof: date) -> pd.DataFrame:
    return _extract_records(df, source_file, asof)


def _extract_records(
    df: pd.DataFrame,
    source_file: str,
    asof: date,
) -> pd.DataFrame:
    headers = _collect_header_rows(df)
    data_start = _find_data_start(df)
    if data_start is None:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    data_block = df.iloc[data_start:, :]
    tenor_series = data_block.iloc[:, 1]
    records: List[Dict[str, object]] = []

    for col in range(2, df.shape[1]):
        column_values = data_block.iloc[:, col]
        if column_values.dropna().empty:
            continue

        identity = _build_identity(headers, col)
        if identity is None:
            continue

        region = derive_region(identity.get("iso"), identity.get("location"))
        version_hash = compute_version_hash(source_file, "Fixed Prices - Mid", asof)

        units_raw = identity.get("units")
        currency, per_unit = map_units(units_raw)
        if currency is None or per_unit is None:
            fallback_currency, fallback_unit = infer_default_units(identity.get("iso"), region)
            currency = currency or fallback_currency
            per_unit = per_unit or fallback_unit
            if currency is None or per_unit is None:
                LOGGER.debug("No unit mapping for '%s'", units_raw)

        for tenor_value, cell_value in zip(tenor_series, column_values):
            if pd.isna(cell_value):
                continue
            mid = to_float(cell_value)
            if mid is None:
                continue

            tenor_type = infer_tenor_type(tenor_value)
            tenor_label = normalise_tenor_label(tenor_value)
            contract_month = pd.to_datetime(tenor_value, errors="coerce")

            row: Dict[str, object] = {
                "asof_date": asof,
                "source_file": source_file,
                "sheet_name": "Fixed Prices - Mid",
                "asset_class": CurveAssetClass.POWER.value,
                "region": region,
                "iso": identity.get("iso"),
                "location": identity.get("location"),
                "market": identity.get("market"),
                "product": identity.get("product"),
                "block": identity.get("block"),
                "spark_location": None,
                "price_type": CurvePriceType.MID.value,
                "units_raw": units_raw,
                "currency": currency,
                "per_unit": per_unit,
                "tenor_type": tenor_type,
                "contract_month": contract_month,
                "tenor_label": tenor_label,
                "value": None,
                "bid": None,
                "ask": None,
                "mid": mid,
                "curve_key": compute_curve_key(
                    {
                        "asset_class": CurveAssetClass.POWER.value,
                        "region": region,
                        "iso": identity.get("iso"),
                        "location": identity.get("location"),
                        "market": identity.get("market"),
                        "product": identity.get("product"),
                        "block": identity.get("block"),
                        "spark_location": None,
                    }
                ),
                "version_hash": version_hash,
            }
            records.append(row)

    if not records:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return pd.DataFrame.from_records(records)


def _collect_header_rows(df: pd.DataFrame) -> Dict[str, pd.Series]:
    headers: Dict[str, pd.Series] = {}
    for idx in range(min(12, len(df))):
        row = df.iloc[idx, :]
        label = safe_str(row.iloc[0]) or safe_str(row.iloc[1])
        if not label:
            continue
        label_norm = label.lower().strip(":")
        for key, alias in HEADERS_TO_CAPTURE.items():
            if key in label_norm and alias not in headers:
                headers[alias] = row
    return headers


def _find_data_start(df: pd.DataFrame) -> Optional[int]:
    for idx in range(len(df)):
        cell = df.iat[idx, 1] if df.shape[1] > 1 else None
        label = safe_str(df.iat[idx, 0]) or safe_str(df.iat[idx, 1])
        if isinstance(cell, pd.Timestamp) and (not label or "date" not in label.lower()):
            return idx
        if isinstance(cell, str) and cell.strip() and (not label or "date" not in label.lower()):
            return idx
    return None


def _value_from_header(headers: Dict[str, pd.Series], key: str, col: int) -> Optional[str]:
    series = headers.get(key)
    if series is None or col >= len(series):
        return None
    return safe_str(series.iloc[col])


def _build_identity(headers: Dict[str, pd.Series], col: int) -> Optional[Dict[str, Optional[str]]]:
    iso = _value_from_header(headers, "iso", col)
    market = _value_from_header(headers, "market", col)
    block = _value_from_header(headers, "block", col)
    units = _value_from_header(headers, "units", col)
    product = _value_from_header(headers, "product", col)
    location = _value_from_header(headers, "location", col)

    if not location:
        location = market or iso
    if not market:
        market = iso or location
    if not product:
        product = "power"

    return {
        "iso": iso,
        "market": market,
        "location": location,
        "product": product,
        "block": block,
        "units": units,
    }


register("simple", parse)
