"""Utilities for mapping raw unit strings to normalized currency/unit codes."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Mapping, Tuple

import csv
import logging

LOGGER = logging.getLogger(__name__)


DEFAULT_UNITS_PATH = Path(__file__).resolve().parents[3] / "config" / "units_map.csv"


def _normalise(token: str) -> str:
    """Normalise a unit token for lookups."""
    return token.strip().upper().replace(" ", "")


@dataclass
class UnitRecord:
    raw: str
    currency: str
    per_unit: str


class UnitsMapper:
    """Look up canonical currency/unit pairs from raw unit strings."""

    def __init__(self, csv_path: Path | str | None = None, *, strict: bool = False) -> None:
        self._path = Path(csv_path) if csv_path else DEFAULT_UNITS_PATH
        self._strict = strict
        self._mapping = self._load_mapping(self._path)

    @staticmethod
    @lru_cache(maxsize=1)
    def _load_mapping(csv_path: Path) -> Mapping[str, UnitRecord]:
        if not csv_path.exists():
            raise FileNotFoundError(f"Units mapping file not found: {csv_path}")
        mapping: dict[str, UnitRecord] = {}
        with csv_path.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            required = {"units_raw", "currency", "per_unit"}
            if not required.issubset(reader.fieldnames or {}):
                raise ValueError(
                    f"Units mapping file {csv_path} missing required columns {sorted(required)}"
                )
            for row in reader:
                raw = row["units_raw"].strip()
                if not raw:
                    continue
                key = _normalise(raw)
                mapping[key] = UnitRecord(raw=raw, currency=row["currency"].strip(), per_unit=row["per_unit"].strip())
        return mapping

    def map(self, units_raw: str | None) -> Tuple[str | None, str | None]:
        """Return canonical `(currency, per_unit)` for the provided raw string."""
        if units_raw is None:
            if self._strict:
                raise ValueError("Unit string cannot be None")
            return (None, None)
        key = _normalise(units_raw)
        record = self._mapping.get(key)
        if record is None:
            if self._strict:
                raise KeyError(f"Unknown unit mapping for '{units_raw}'")
            LOGGER.debug("No unit mapping for '%s'", units_raw)
            return (None, None)
        currency = record.currency or None
        per_unit = record.per_unit or None
        return (currency, per_unit)


_default_mapper = UnitsMapper()


def map_units(units_raw: str | None, *, strict: bool = False) -> Tuple[str | None, str | None]:
    """Helper using the default mapper instance."""
    if strict:
        return UnitsMapper(strict=True).map(units_raw)
    return _default_mapper.map(units_raw)


_ISO_DEFAULTS: dict[str, tuple[str, str]] = {
    "US": ("USD", "MWh"),
    "UK": ("GBP", "MWh"),
    "NBP": ("GBP", "MWh"),
    "TTF": ("EUR", "MWh"),
    "EUA": ("EUR", "MWh"),
}

_REGION_DEFAULTS: dict[str, tuple[str, str]] = {
    "US": ("USD", "MWh"),
    "CA": ("CAD", "MWh"),
    "EU": ("EUR", "MWh"),
    "APAC": ("USD", "MWh"),
}


def infer_default_units(iso: str | None, region: str | None) -> tuple[str | None, str | None]:
    """Infer default currency/unit when explicit mapping is missing."""
    if iso:
        key = iso.strip().upper()
        if key in _ISO_DEFAULTS:
            return _ISO_DEFAULTS[key]
    if region:
        key = region.strip().upper()
        if key in _REGION_DEFAULTS:
            return _REGION_DEFAULTS[key]
    return (None, None)


__all__ = [
    "UnitsMapper",
    "map_units",
    "UnitRecord",
    "infer_default_units",
]
