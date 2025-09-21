"""Helpers for deriving EIA API window parameters from scheduling metadata."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from dateutil import parser as dateutil_parser
from dateutil.relativedelta import relativedelta


class WindowConfigError(ValueError):
    """Raised when window configuration cannot be interpreted."""


@dataclass(frozen=True)
class WindowBounds:
    start_token: str
    end_token: str


def _parse_int(value: Optional[str], name: str) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
        raise WindowConfigError(f"Invalid integer for {name}: {value}") from exc


def _default_delta(frequency: str) -> relativedelta:
    freq = frequency.upper()
    if freq == "HOURLY" or freq.endswith("HOURLY"):
        return relativedelta(hours=1)
    if freq == "DAILY":
        return relativedelta(days=1)
    if freq == "WEEKLY":
        return relativedelta(days=7)
    if freq == "MONTHLY":
        return relativedelta(months=1)
    if freq == "QUARTERLY":
        return relativedelta(months=3)
    if freq == "ANNUAL":
        return relativedelta(years=1)
    return relativedelta(days=1)


def _normalize_date_format(date_format: Optional[str]) -> Optional[str]:
    if not date_format:
        return None
    token = date_format.replace('"', "")
    replacements = [
        ("HH24", "%H"),
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MM", "%m"),
        ("DD", "%d"),
    ]
    for old, new in replacements:
        token = token.replace(old, new)
    # If the pattern still contains unsupported tokens (like Q), give up
    if any(letter in token for letter in "Qq"):
        return None
    return token


def _format_token(moment: datetime, frequency: str, date_format: Optional[str]) -> str:
    fmt = _normalize_date_format(date_format)
    if fmt:
        return moment.strftime(fmt)

    freq = frequency.upper()
    if freq == "ANNUAL":
        return f"{moment.year}"
    if freq == "QUARTERLY":
        quarter = ((moment.month - 1) // 3) + 1
        return f"{moment.year}-Q{quarter}"
    if freq == "MONTHLY":
        return moment.strftime("%Y-%m")
    if freq == "WEEKLY":
        return moment.strftime("%Y-%m-%d")
    if freq == "DAILY":
        return moment.strftime("%Y-%m-%d")
    if freq == "HOURLY" or freq.endswith("HOURLY"):
        return moment.strftime("%Y-%m-%dT%H")
    return moment.strftime("%Y-%m-%d")


def compute_window_bounds(
    *,
    window_end: Optional[str],
    frequency: Optional[str],
    date_format: Optional[str],
    hours: Optional[str] = None,
    days: Optional[str] = None,
    months: Optional[str] = None,
    years: Optional[str] = None,
) -> WindowBounds:
    """Return formatted start/end tokens suitable for the EIA API."""

    if not frequency:
        raise WindowConfigError("EIA_FREQUENCY is required to compute window bounds")
    if not window_end:
        raise WindowConfigError("EIA_WINDOW_END must be provided when deriving bounds")

    try:
        end_dt = dateutil_parser.isoparse(window_end)
    except (ValueError, TypeError) as exc:  # pragma: no cover - defensive
        raise WindowConfigError(f"Invalid ISO timestamp for EIA_WINDOW_END: {window_end}") from exc

    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = end_dt.astimezone(timezone.utc)

    delta = relativedelta(
        years=_parse_int(years, "EIA_WINDOW_YEARS") or 0,
        months=_parse_int(months, "EIA_WINDOW_MONTHS") or 0,
        days=_parse_int(days, "EIA_WINDOW_DAYS") or 0,
        hours=_parse_int(hours, "EIA_WINDOW_HOURS") or 0,
    )
    if delta == relativedelta():
        delta = _default_delta(frequency)

    start_dt = end_dt - delta
    if start_dt >= end_dt:
        raise WindowConfigError("Derived window start is not before end; adjust window parameters")

    # Avoid formatting the exclusive end boundary directly to prevent rolling into next period
    epsilon = timedelta(seconds=1)
    end_for_token = end_dt - epsilon

    start_token = _format_token(start_dt, frequency, date_format)
    end_token = _format_token(end_for_token, frequency, date_format)
    return WindowBounds(start_token, end_token)


__all__ = ["WindowBounds", "WindowConfigError", "compute_window_bounds"]
