"""Automatic schema inference utilities for vendor curve ingestion."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Optional

import logging

import pandas as pd

from aurum.reference.curve_schema import CANONICAL_CURVE_COLUMNS

LOGGER = logging.getLogger(__name__)

_CANONICAL_SET = {c.lower(): c for c in CANONICAL_CURVE_COLUMNS}
_DEFAULT_REQUIRED = {
    "asof_date",
    "sheet_name",
    "tenor_label",
    "curve_key",
    "mid",
}

_ALIAS_CANDIDATES: Mapping[str, Sequence[str]] = {
    "asof_date": ("as of", "pricingdate", "valuation_date", "trade_date", "asof"),
    "sheet_name": ("sheet", "tab", "worksheet", "source_tab"),
    "tenor_label": ("tenor", "bucket", "period", "month", "contract"),
    "curve_key": ("curve", "key", "identifier", "curve_id"),
    "mid": ("mid", "midpoint", "price", "value", "settle", "mtm"),
    "bid": ("bid", "bidoffer", "bid_price"),
    "ask": ("ask", "offer", "ask_price"),
    "value": ("value", "mtm", "settle", "price"),
    "price_type": ("price_type", "type", "quote_type"),
    "iso": ("iso", "hub", "market", "pool"),
    "location": ("location", "zone", "hub", "node"),
    "market": ("market", "region"),
    "product": ("product", "instrument"),
    "block": ("block", "hour", "hours", "shape"),
    "currency": ("currency", "curr"),
    "per_unit": ("unit", "perunit", "measure"),
    "contract_month": ("contract_month", "contract", "delivery_month"),
    "tenor_type": ("tenor_type", "bucket_type", "granularity"),
}


@dataclass(frozen=True)
class SchemaInferenceResult:
    """Outcome of schema inference."""

    column_mapping: Mapping[str, str]
    missing_columns: Sequence[str]
    unexpected_columns: Sequence[str]
    confidence: float
    field_confidence: Mapping[str, float]

    def rename(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return a copy with columns renamed according to the mapping."""
        renamed = frame.rename(columns=self.column_mapping)
        return renamed


class SchemaInferenceEngine:
    """Infer mappings between vendor-provided columns and canonical schema."""

    def __init__(
        self,
        *,
        required_columns: Iterable[str] | None = None,
        alias_candidates: Mapping[str, Sequence[str]] | None = None,
    ) -> None:
        self.required_columns = {c.lower() for c in (required_columns or _DEFAULT_REQUIRED)}
        if alias_candidates is None:
            alias_candidates = _ALIAS_CANDIDATES
        self.alias_candidates = {
            canonical.lower(): tuple(value for value in values)
            for canonical, values in alias_candidates.items()
        }

    def infer(self, data: pd.DataFrame | Mapping[str, pd.DataFrame]) -> SchemaInferenceResult:
        frame = self._select_frame(data)
        if frame is None or frame.empty:
            LOGGER.warning("Schema inference received no data")
            return SchemaInferenceResult({}, tuple(self.required_columns), tuple(), 0.0, {})

        column_mapping: MutableMapping[str, str] = {}
        field_scores: MutableMapping[str, float] = {}

        for original in frame.columns:
            canonical, score = self._match_column(original)
            if canonical is None:
                continue
            column_mapping[str(original)] = canonical
            field_scores[canonical] = max(field_scores.get(canonical, 0.0), score)

        missing = [col for col in self.required_columns if col not in (c.lower() for c in column_mapping.values())]
        unexpected = [col for col in frame.columns if col not in column_mapping]

        matched_required = len(self.required_columns) - len(missing)
        confidence = 0.0 if not self.required_columns else matched_required / len(self.required_columns)

        return SchemaInferenceResult(
            column_mapping=column_mapping,
            missing_columns=tuple(sorted(missing)),
            unexpected_columns=tuple(str(col) for col in unexpected),
            confidence=confidence,
            field_confidence=dict(field_scores),
        )

    def _match_column(self, name: str) -> tuple[Optional[str], float]:
        key = _canonicalise(name)
        if key in _CANONICAL_SET:
            return _CANONICAL_SET[key], 1.0

        for canonical, aliases in self.alias_candidates.items():
            if key == canonical:
                return _CANONICAL_SET.get(canonical, canonical), 0.9
            if any(key == _canonicalise(alias) for alias in aliases):
                return _CANONICAL_SET.get(canonical, canonical), 0.75
            if any(_canonicalise(alias) in key for alias in aliases):
                return _CANONICAL_SET.get(canonical, canonical), 0.5

        return None, 0.0

    @staticmethod
    def _select_frame(data: pd.DataFrame | Mapping[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, Mapping):
            # pick largest non-empty sheet
            viable = [df for df in data.values() if isinstance(df, pd.DataFrame) and not df.empty]
            if not viable:
                return None
            viable.sort(key=lambda df: df.shape[0] * df.shape[1], reverse=True)
            return viable[0]
        return None


def _canonicalise(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


__all__ = ["SchemaInferenceEngine", "SchemaInferenceResult"]
