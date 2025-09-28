"""Generic vendor plugin handling multiple flat data formats."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import re

import pandas as pd

from .formats import FileFormat, LoadedDocument
from .plugin import ParserContext, VendorParserPlugin, register

_DATE_PATTERN = re.compile(r"^(\d{4})[-/](\d{1,2})(?:[-/](\d{1,2}))?")


@dataclass
class GenericTabularPlugin(VendorParserPlugin):
    """Fallback parser that auto-loads tabular vendor formats."""

    name: str = "generic"
    supported_formats = (
        FileFormat.EXCEL,
        FileFormat.CSV,
        FileFormat.JSON,
        FileFormat.XML,
        FileFormat.PDF,
    )

    def parse(self, context: ParserContext, document: LoadedDocument) -> pd.DataFrame:
        loader = _FORMAT_HANDLERS.get(document.format)
        if loader is None:
            raise ValueError(f"Unsupported document format for generic parser: {document.format}")
        frame = loader(document.payload, context)
        if frame is None:
            return pd.DataFrame()
        if "asof_date" not in frame.columns:
            frame["asof_date"] = context.asof
        if "sheet_name" not in frame.columns:
            frame["sheet_name"] = "auto"
        if "curve_key" not in frame.columns:
            frame["curve_key"] = f"{context.vendor}|{context.format.value}"
        return frame


def _from_excel(payload: Mapping[str, pd.DataFrame], context: ParserContext) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame()
    name, frame = next(iter(payload.items()))
    if frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame["sheet_name"] = name
    return frame


def _from_csv(payload: pd.DataFrame, context: ParserContext) -> pd.DataFrame:  # noqa: ARG001
    return payload.copy()


def _from_json(payload: object, context: ParserContext) -> pd.DataFrame:  # noqa: ARG001
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return pd.DataFrame(payload["data"])
        return pd.DataFrame([payload])
    return pd.DataFrame()


def _from_xml(payload: object, context: ParserContext) -> pd.DataFrame:  # noqa: ARG001
    try:
        import xml.etree.ElementTree as ET
    except ImportError:  # pragma: no cover - stdlib always present
        return pd.DataFrame()
    if not hasattr(payload, "iter"):
        return pd.DataFrame()
    rows = []
    for element in payload:
        row = {child.tag: child.text for child in element}
        if row:
            rows.append(row)
    return pd.DataFrame(rows)


def _from_pdf(payload: Mapping[str, pd.DataFrame], context: ParserContext) -> pd.DataFrame:
    text_df = payload.get("text")
    if text_df is None or text_df.empty:
        return pd.DataFrame()
    records = []
    for _, row in text_df.iterrows():
        line = str(row.get("content", "")).strip()
        if not line:
            continue
        tenor = _extract_tenor(line)
        value = _extract_numeric(line)
        if tenor is None or value is None:
            continue
        records.append({"tenor_label": tenor, "mid": value})
    return pd.DataFrame.from_records(records)


def _extract_tenor(line: str) -> str | None:
    match = _DATE_PATTERN.search(line)
    if match:
        year, month, day = match.groups(default="01")
        return f"{year}-{int(month):02d}"
    parts = line.split()
    return parts[0] if parts else None


def _extract_numeric(line: str) -> float | None:
    tokens = re.findall(r"[-+]?[0-9]*\.?[0-9]+", line)
    if not tokens:
        return None
    try:
        return float(tokens[-1])
    except ValueError:
        return None


_FORMAT_HANDLERS = {
    FileFormat.EXCEL: _from_excel,
    FileFormat.CSV: _from_csv,
    FileFormat.JSON: _from_json,
    FileFormat.XML: _from_xml,
    FileFormat.PDF: _from_pdf,
}


register("generic", GenericTabularPlugin())


__all__ = ["GenericTabularPlugin"]
