"""Vendor curve parsing adapters."""
from __future__ import annotations
from datetime import date
from typing import Protocol

import pandas as pd


class CurveParser(Protocol):
    """Adapter protocol for vendor workbook parsers."""

    def __call__(self, path: str, asof: date) -> pd.DataFrame:
        ...


PARSERS: dict[str, CurveParser] = {}


def register(name: str, parser: CurveParser) -> None:
    """Register a parser implementation under the provided vendor key."""
    PARSERS[name] = parser


def parse(vendor: str, path: str, asof: date) -> pd.DataFrame:
    """Dispatch to a vendor-specific parser."""
    try:
        parser = PARSERS[vendor]
    except KeyError as exc:
        raise ValueError(f"No parser registered for vendor '{vendor}'") from exc
    return parser(path, asof)

# Register built-in parsers
from . import parse_pw  # noqa: F401
from . import parse_eugp  # noqa: F401
from . import parse_rp  # noqa: F401
