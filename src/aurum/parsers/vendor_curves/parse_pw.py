"""Parser for the PW vendor workbook format."""
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
    CANONICAL_COLUMNS as BASE_COLUMNS,
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
    "iso": "iso",
    "market": "market",
    "hours": "block",
    "location": "location",
    "product": "product",
    "spark location": "spark_location",
    "spark": "spark_location",
    "units": "units",
}


LOGGER = logging.getLogger(__name__)


def parse(path: str, asof: date) -> pd.DataFrame:
    """Parse PW workbook into the canonical dataframe schema."""
    book = pd.ExcelFile(path)
    source_file = Path(path).name

    mid_frames: List[pd.DataFrame] = []
    bid_frames: List[pd.DataFrame] = []

    sheet_pairs = [
        ("Fixed Prices - Mid", "Fixed Prices - BidAsk"),
        ("Strips - Mid", "Strips - BidAsk"),
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
        return pd.DataFrame(columns=BASE_COLUMNS)

    combined = pd.concat(mid_frames, ignore_index=True)
    if bid_frames:
        bid_df = pd.concat(bid_frames, ignore_index=True)
        keys = [
            "curve_key",
            "tenor_type",
            "tenor_label",
            "contract_month",
            "price_type",
        ]
        bid_df = bid_df[keys + ["bid", "ask", "mid"]]
        combined = combined.merge(bid_df, on=keys, how="left", suffixes=("", "_bid"))
        for column in ("bid", "ask", "mid"):
            extra = f"{column}_bid"
            if extra in combined:
                fallback = combined.pop(extra)
                combined[column] = combined[column].where(combined[column].notna(), fallback)
    else:
        combined["bid"] = pd.NA
        combined["ask"] = pd.NA

    combined["value"] = combined["mid"]
    combined["_ingest_ts"] = pd.Timestamp.utcnow()
    combined["contract_month"] = pd.to_datetime(combined["contract_month"], errors="coerce").dt.date
    for column in CANONICAL_COLUMNS:
        if column not in combined.columns:
            combined[column] = pd.NA
    return combined[CANONICAL_COLUMNS]


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

        region = derive_region(identity.get("iso"), identity.get("location"))
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
                "asset_class": "power",
                "region": region,
                "iso": identity.get("iso"),
                "location": identity.get("location"),
                "market": identity.get("market"),
                "product": identity.get("product"),
                "block": identity.get("block"),
                "spark_location": identity.get("spark_location"),
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
                        "asset_class": "power",
                        "region": region,
                        "iso": identity.get("iso"),
                        "location": identity.get("location"),
                        "market": identity.get("market"),
                        "product": identity.get("product"),
                        "block": identity.get("block"),
                        "spark_location": identity.get("spark_location"),
                    }
                ),
                "version_hash": version_hash,
            }
            row.update(metrics)
            records.append(row)

    if not records:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return pd.DataFrame.from_records(records)



def _value_from_header(headers: Dict[str, pd.Series], key: str, col: int) -> Optional[str]:
    series = headers.get(key)
    if series is None or col >= len(series):
        return None
    return safe_str(series.iloc[col])


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


def _build_identity(headers: Dict[str, pd.Series], col: int, sheet_name: str) -> Optional[Dict[str, Optional[str]]]:
    iso = _value_from_header(headers, "iso", col)
    market = _value_from_header(headers, "market", col)
    block = _value_from_header(headers, "block", col)
    units = _value_from_header(headers, "units", col)
    product = _value_from_header(headers, "product", col)
    location = _value_from_header(headers, "location", col)
    spark_location = _value_from_header(headers, "spark_location", col)

    if not location:
        location = market or iso
    if not market:
        market = iso or location
    if not product:
        product = sheet_name.split(" - ")[0].lower()

    return {
        "iso": iso,
        "market": market,
        "location": location,
        "product": product,
        "block": block,
        "units": units,
        "spark_location": spark_location,
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


register("pw", parse)
