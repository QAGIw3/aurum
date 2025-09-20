"""Parser for the RP vendor workbook format."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable, Dict, List, Optional

import logging
import pandas as pd

from . import register
from .schema import CANONICAL_COLUMNS
from ...reference import infer_default_units, map_units
from ..utils import (
    compute_curve_key,
    compute_version_hash,
    derive_region,
    infer_tenor_type,
    normalise_tenor_label,
    parse_bid_ask,
    safe_str,
    to_float,
)

HEADERS_TO_CAPTURE = {
    "location": "location",
    "product": "product",
    "units": "units",
}


LOGGER = logging.getLogger(__name__)


def parse(path: str, asof: date) -> pd.DataFrame:
    book = pd.ExcelFile(path)
    source_file = Path(path).name

    mid_frames: List[pd.DataFrame] = []
    bid_frames: List[pd.DataFrame] = []

    sheet_pairs = [
        ("Fixed Prices - Mid", "Fixed Prices - BidAsk"),
        ("Seasonal Blocks - Mid", "Seasonal Blocks - BidAsk"),
    ]

    for mid_sheet, bid_sheet in sheet_pairs:
        if mid_sheet in book.sheet_names:
            df_mid = _parse_mid_sheet(book.parse(mid_sheet, header=None), source_file, mid_sheet, asof)
            if not df_mid.empty:
                mid_frames.append(df_mid)
        if bid_sheet in book.sheet_names:
            df_bid = _parse_bid_sheet(book.parse(bid_sheet, header=None), source_file, bid_sheet, asof)
            if not df_bid.empty:
                bid_frames.append(df_bid)

    if not mid_frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    mid_df = pd.concat(mid_frames, ignore_index=True)
    if bid_frames:
        bid_df = pd.concat(bid_frames, ignore_index=True)
        keys = ["curve_key", "tenor_type", "tenor_label", "contract_month"]
        bid_df = bid_df[keys + ["bid", "ask", "mid"]]
        mid_df = mid_df.merge(bid_df, on=keys, how="left", suffixes=("", "_bid"))
        for col in ["bid", "ask", "mid"]:
            bid_col = f"{col}_bid"
            if bid_col in mid_df.columns:
                fallback = mid_df.pop(bid_col)
                mid_df[col] = mid_df[col].where(mid_df[col].notna(), fallback)
    else:
        mid_df["bid"] = pd.NA
        mid_df["ask"] = pd.NA

    mid_df["value"] = mid_df["mid"]
    mid_df["_ingest_ts"] = pd.Timestamp.utcnow()
    mid_df["contract_month"] = pd.to_datetime(mid_df["contract_month"], errors="coerce").dt.date
    for column in CANONICAL_COLUMNS:
        if column not in mid_df.columns:
            mid_df[column] = pd.NA
    return mid_df[CANONICAL_COLUMNS]


def _parse_mid_sheet(df: pd.DataFrame, source_file: str, sheet_name: str, asof: date) -> pd.DataFrame:
    return _extract_records(df, source_file, sheet_name, asof, _mid_value_parser)


def _parse_bid_sheet(df: pd.DataFrame, source_file: str, sheet_name: str, asof: date) -> pd.DataFrame:
    return _extract_records(df, source_file, sheet_name, asof, _bid_value_parser)


def _extract_records(
    df: pd.DataFrame,
    source_file: str,
    sheet_name: str,
    asof: date,
    value_parser: Callable[[object], Dict[str, Optional[float]]],
) -> pd.DataFrame:
    headers = _collect_header_rows(df)
    data_start = _find_data_start(df)
    if data_start is None:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    region_series = headers.get("region")
    data_block = df.iloc[data_start:, :]
    tenor_series = data_block.iloc[:, 1]
    records: List[Dict[str, object]] = []

    for col in range(2, df.shape[1]):
        column_values = data_block.iloc[:, col]
        if column_values.dropna().empty:
            continue

        identity = _build_identity(headers, col, sheet_name)
        if identity is None:
            continue

        region_override = safe_str(region_series.iloc[col]) if region_series is not None and col < len(region_series) else None
        region = region_override or derive_region(identity.get("iso"), identity.get("location"))
        version_hash = compute_version_hash(source_file, sheet_name, asof)

        units_raw = identity.get("units")
        currency, per_unit = map_units(units_raw)
        if currency is None or per_unit is None:
            fallback_currency, fallback_unit = infer_default_units(identity.get("iso"), region)
            currency = currency or fallback_currency
            per_unit = per_unit or fallback_unit
            if currency is None or per_unit is None:
                LOGGER.debug("No unit mapping for '%s' in sheet %s", units_raw, sheet_name)

        for tenor_value, cell_value in zip(tenor_series, column_values):
            if pd.isna(cell_value):
                continue
            metrics = value_parser(cell_value)
            if not any(val is not None for val in metrics.values()):
                continue

            tenor_type = infer_tenor_type(tenor_value)
            tenor_label = normalise_tenor_label(tenor_value)
            contract_month = pd.to_datetime(tenor_value, errors="coerce")

            row: Dict[str, object] = {
                "asof_date": asof,
                "source_file": source_file,
                "sheet_name": sheet_name,
                "asset_class": "renewable",
                "region": region,
                "iso": identity.get("iso"),
                "location": identity.get("location"),
                "market": identity.get("market"),
                "product": identity.get("product"),
                "block": identity.get("block"),
                "spark_location": None,
                "price_type": "MID",
                "units_raw": units_raw,
                "currency": currency,
                "per_unit": per_unit,
                "tenor_type": tenor_type,
                "contract_month": contract_month,
                "tenor_label": tenor_label,
                "value": None,
                "bid": None,
                "ask": None,
                "mid": None,
                "curve_key": compute_curve_key(
                    {
                        "asset_class": "renewable",
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
            row.update(metrics)
            records.append(row)

    if not records:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return pd.DataFrame.from_records(records)


def _collect_header_rows(df: pd.DataFrame) -> Dict[str, pd.Series]:
    headers: Dict[str, pd.Series] = {}
    last_label = None
    for idx in range(min(12, len(df))):
        row = df.iloc[idx, :]
        label = safe_str(row.iloc[0]) or safe_str(row.iloc[1])
        label_norm = label.lower().strip(":") if label else None
        if not label_norm:
            if last_label == "location" and "region" not in headers:
                headers["region"] = row
            continue
        last_label = label_norm
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


def _value_from_headers(headers: Dict[str, pd.Series], key: str, col: int) -> Optional[str]:
    """Return header cell at or before column index to handle sparse headers."""
    series = headers.get(key)
    if series is None:
        return None
    i = min(col, len(series) - 1)
    while i >= 0:
        val = safe_str(series.iloc[i])
        if val:
            if val.strip().endswith(":"):
                i -= 1
                continue
            return val
        i -= 1
    return None


def _build_identity(headers: Dict[str, pd.Series], col: int, sheet_name: str) -> Optional[Dict[str, Optional[str]]]:
    location = _value_from_headers(headers, "location", col)
    product_name = _value_from_headers(headers, "product", col)
    units = _value_from_headers(headers, "units", col)

    iso = "US" if location and "canada" not in location.lower() else None
    market = location
    product = product_name or sheet_name.split(" - ")[0].lower()

    return {
        "iso": iso,
        "market": market,
        "location": location,
        "product": product,
        "block": None,
        "units": units,
    }


def _mid_value_parser(value: object) -> Dict[str, Optional[float]]:
    mid = to_float(value)
    return {"mid": mid}


def _bid_value_parser(value: object) -> Dict[str, Optional[float]]:
    bid, ask = parse_bid_ask(value)
    mid = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2
    return {"bid": bid, "ask": ask, "mid": mid}


register("rp", parse)
