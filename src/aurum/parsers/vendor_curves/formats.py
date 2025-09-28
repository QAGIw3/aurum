"""File format detection and loading utilities for vendor curves."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Mapping, Optional, List
import json

import pandas as pd
import xml.etree.ElementTree as ET


class FileFormat(str, Enum):
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PDF = "pdf"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class LoadedDocument:
    """Container for raw loaded content used by downstream parsers."""

    format: FileFormat
    payload: object


class FormatDetectionError(RuntimeError):
    pass


def detect_format(path: str) -> FileFormat:
    suffix = Path(path).suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls", ".xlsb"}:
        return FileFormat.EXCEL
    if suffix in {".csv", ".tsv"}:
        return FileFormat.CSV
    if suffix in {".json"}:
        return FileFormat.JSON
    if suffix in {".xml"}:
        return FileFormat.XML
    if suffix in {".pdf"}:
        return FileFormat.PDF
    return FileFormat.UNKNOWN


def load_document(path: str, *, fmt: Optional[FileFormat] = None) -> LoadedDocument:
    fmt = fmt or detect_format(path)
    try:
        loader = _LOADER_MAP[fmt]
    except KeyError as exc:
        raise FormatDetectionError(f"Unsupported vendor document format '{fmt}'") from exc
    payload = loader(path)
    return LoadedDocument(format=fmt, payload=payload)


def _load_excel(path: str) -> Mapping[str, pd.DataFrame]:
    book = pd.ExcelFile(path)
    return {name: book.parse(name) for name in book.sheet_names}


def _load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def _load_json(path: str) -> object:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_xml(path: str) -> ET.Element:
    parser = ET.XMLParser()
    with open(path, "rb") as fh:
        tree = ET.parse(fh, parser=parser)
    return tree.getroot()


def _load_pdf(path: str) -> Dict[str, pd.DataFrame]:
    """Load a PDF using the best available backend.

    Priority:
    1) docling (structured tables)
    2) pdfminer.six (plain text lines)
    """
    # 1) Try structured extraction with docling (if installed)
    try:  # pragma: no cover - optional dependency
        from docling.document_converter import DocumentConverter  # type: ignore

        converter = DocumentConverter(allowed_formats=None)
        result = converter.convert(path)
        # Prefer legacy_document tables for convenient DataFrame export
        tables: List[pd.DataFrame] = []
        legacy_doc = getattr(result, "legacy_document", None)
        if legacy_doc is not None and getattr(legacy_doc, "tables", None):
            for table in legacy_doc.tables:
                try:
                    df = table.export_to_dataframe()
                except Exception:
                    # Fallback to building from payload if available
                    payload = getattr(table, "payload", None)
                    if payload and hasattr(payload, "data"):
                        df = pd.DataFrame(payload.data)
                    else:
                        continue
                if df is not None and not df.empty:
                    tables.append(df)
        if tables:
            return {f"table_{i}": df for i, df in enumerate(tables)}
    except Exception:
        # If docling is not available or fails, fall back to pdfminer
        pass

    # 2) Fallback: extract raw text lines with pdfminer.six
    try:
        from pdfminer.high_level import extract_text  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise FormatDetectionError(
            "PDF parsing requires either 'docling' or 'pdfminer.six' dependency"
        ) from exc

    text = extract_text(path)
    if not text:
        return {"text": pd.DataFrame(columns=["content"])}

    rows = [line.strip() for line in text.splitlines() if line.strip()]
    df = pd.DataFrame({"content": rows})
    return {"text": df}


_LOADER_MAP: Mapping[FileFormat, Callable[[str], object]] = {
    FileFormat.EXCEL: _load_excel,
    FileFormat.CSV: _load_csv,
    FileFormat.JSON: _load_json,
    FileFormat.XML: _load_xml,
    FileFormat.PDF: _load_pdf,
}


__all__ = ["FileFormat", "detect_format", "load_document", "LoadedDocument", "FormatDetectionError"]
