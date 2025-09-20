"""Utility helpers shared by vendor workbook parsers."""
from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

from .vendor_curves.schema import CANONICAL_COLUMNS

_ASOF_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"as[-_\s]*of\s*[:=-]?\s*(?P<value>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", re.IGNORECASE),
    re.compile(r"(?P<value>\d{4}[-/]\d{1,2}[-/]\d{1,2})"),
)
_ASOF_KEYWORD = re.compile(r"as[-_\s]*of", re.IGNORECASE)

_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d")

_CURRENCY_ALIASES: Mapping[str, str] = {
    "C$": "CAD",
    "US$": "USD",
    "$": "USD",
    "£": "GBP",
    "€": "EUR",
    "¥": "JPY",
    "A$": "AUD",
    "AU$": "AUD",
    "NZ$": "NZD",
}

_UNIT_TOKENS: tuple[str, ...] = (
    "MWH",
    "KWH",
    "MMBTU",
    "MW-DAY",
    "MW-YR",
    "MW-MONTH",
    "MW-WEEK",
    "MW",
    "KW",
    "TONNE",
    "TON",
    "BBL",
    "GAL",
    "THERM",
)


def normalise_token(value: Optional[str]) -> str:
    """Return a lowercase trimmed representation suitable for key construction."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value.strip()).lower()


def normalise_units_token(text: Optional[str]) -> Optional[str]:
    """Normalise vendor-provided units text to aid lookup heuristics."""
    if not text:
        return None
    s = str(text).strip()
    if not s:
        return None
    for alias, canonical in _CURRENCY_ALIASES.items():
        s = s.replace(alias, canonical)
    s = s.replace(" - ", "/")
    s = s.replace("—", "/")
    s_compact = s.upper().replace(" ", "")
    currencies = ("CAD", "USD", "GBP", "EUR", "JPY", "AUD", "NZD", "CHF", "SEK", "NOK", "DKK", "ZAR", "MXN")
    currency = next((code for code in currencies if code in s_compact), None)
    unit = next((token for token in _UNIT_TOKENS if token in s_compact), None)
    if currency and unit:
        return f"{currency}/{unit}"
    return s


def looks_like_unit_token(text: Optional[str]) -> bool:
    if not text:
        return False
    upper = text.upper()
    return any(token in upper for token in _UNIT_TOKENS)


def detect_units_row(df: pd.DataFrame, *, header_rows: int = 12, scan_columns: int = 6) -> Optional[pd.Series]:
    """Best-effort detection of a header row that contains unit hints."""
    limit = min(header_rows, len(df))
    for idx in range(limit):
        row = df.iloc[idx, :]
        for value in row.tolist()[:scan_columns]:
            token = normalise_units_token(safe_str(value))
            if token and looks_like_unit_token(token):
                return row
    return None


def compute_curve_key(identity: Mapping[str, Optional[str]], *, separator: str = "|") -> str:
    """Compute a deterministic curve key from identity components."""
    ordered_fields = (
        "asset_class",
        "iso",
        "region",
        "location",
        "market",
        "product",
        "block",
        "spark_location",
    )
    pieces = [normalise_token(identity.get(field)) for field in ordered_fields]
    raw = separator.join(pieces)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest


def compute_version_hash(*values: object) -> str:
    """Compute a stable hash representing the version of the ingested workbook slice."""
    digest = hashlib.sha256()
    for value in values:
        digest.update(str(value or "").encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()


def parse_bid_ask(value: object) -> tuple[Optional[float], Optional[float]]:
    """Return bid and ask floats from a string such as ``"45.25 / 47.75"``."""
    if value is None:
        return None, None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value), float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None, None
        parts = re.split(r"\s*/\s*", cleaned)
        if len(parts) == 1:
            f = _safe_float(parts[0])
            return f, f
        bid = _safe_float(parts[0])
        ask = _safe_float(parts[1])
        return bid, ask
    return None, None


def detect_asof(text_rows: Iterable[str]) -> date:
    """Detect the as-of date from workbook text rows."""
    fallback_match: Optional[date] = None

    for row in text_rows:
        if not row:
            continue
        for pattern in _ASOF_PATTERNS:
            match = pattern.search(row)
            if not match:
                continue
            candidate = match.group("value")
            parsed = _try_parse_date(candidate)
            if not parsed:
                continue
            if _ASOF_KEYWORD.search(row):
                return parsed
            if fallback_match is None:
                fallback_match = parsed

    if fallback_match:
        return fallback_match
    raise ValueError("Could not detect as-of date in provided rows")


def first_string(*values: object) -> Optional[str]:
    """Return the first non-empty string from the provided iterable."""
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def safe_str(value: object) -> Optional[str]:
    """Return a trimmed string representation or ``None`` if not available."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def infer_tenor_type(label: object) -> str:
    if isinstance(label, (datetime, pd.Timestamp)):
        return "MONTHLY"
    if not isinstance(label, str):
        return "MONTHLY"
    upper = label.upper()
    if upper.startswith("CALENDAR"):
        return "CALENDAR"
    if any(
        upper.startswith(prefix)
        for prefix in ("WINTER", "SUMMER", "SPRING", "FALL", "AUTUMN")
    ):
        return "SEASON"
    if any(token in upper for token in ("BAL", "STRIP")):
        return "SEASON"
    if re.match(r"^Q\d", upper):
        return "QUARTER"
    return "MONTHLY"


def normalise_tenor_label(label: object) -> str:
    if isinstance(label, (datetime, pd.Timestamp)):
        return label.strftime("%Y-%m")
    if label is None:
        return ""
    return str(label).strip()


def derive_region(iso: Optional[str], location: Optional[str]) -> Optional[str]:
    mapping = {
        "PJM": "US",
        "MISO": "US",
        "SPP": "US",
        "ERCOT": "US",
        "NYISO": "US",
        "ISONE": "US",
        "CAISO": "US",
        "IESO": "CA",
        "AESO": "CA",
        "NBP": "EU",
        "TTF": "EU",
        "EUA": "EU",
        "CEGH": "EU",
        "PEG": "EU",
        "UKA": "EU",
        "EU": "EU",
        "AP": "APAC",
    }
    if iso:
        key = iso.strip().upper()
        if key in mapping:
            return mapping[key]
    if location:
        loc = location.strip().upper()
        if loc in mapping:
            return mapping[loc]
    return None


def _safe_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value.replace(",", ""))
    except (AttributeError, ValueError):
        return None


def _try_parse_date(value: str) -> Optional[date]:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None
