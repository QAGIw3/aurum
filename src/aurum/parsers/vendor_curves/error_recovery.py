"""Error recovery and data correction utilities for vendor curves."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, MutableMapping

import pandas as pd


@dataclass(frozen=True)
class RecoveryReport:
    """Summary of corrections performed during recovery."""

    corrections: Mapping[str, int]


class ErrorRecoveryEngine:
    """Apply best-effort corrections to parsed curve data."""

    def apply(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, RecoveryReport]:
        if frame.empty:
            return frame, RecoveryReport(corrections={})

        corrections: MutableMapping[str, int] = {}

        corrected = frame.copy()

        if "mid" in corrected.columns and "value" in corrected.columns:
            missing_mid = corrected["mid"].isna() & corrected["value"].notna()
            if missing_mid.any():
                corrected["mid"] = corrected["mid"].where(~missing_mid, corrected["value"])
                corrections["mid_from_value"] = int(missing_mid.sum())

        if "value" in corrected.columns and "mid" in corrected.columns:
            missing_value = corrected["value"].isna() & corrected["mid"].notna()
            if missing_value.any():
                corrected["value"] = corrected["value"].where(~missing_value, corrected["mid"])
                corrections["value_from_mid"] = int(missing_value.sum())

        for column in ("currency", "per_unit"):
            if column not in corrected.columns:
                continue
            missing = corrected[column].isna()
            if missing.any():
                mode = corrected[column].dropna().mode()
                if not mode.empty:
                    corrected.loc[missing, column] = mode.iloc[0]
                    corrections[f"filled_{column}"] = int(missing.sum())

        if {"curve_key", "tenor_label", "price_type"}.issubset(corrected.columns):
            id_mask = corrected["curve_key"].notna() & corrected["tenor_label"].notna() & corrected["price_type"].notna()
            if id_mask.any():
                before = len(corrected)
                corrected = corrected.sort_values("_ingest_ts") if "_ingest_ts" in corrected else corrected
                corrected = corrected.drop_duplicates(subset=["curve_key", "tenor_label", "price_type"], keep="last")
                removed = before - len(corrected)
                if removed > 0:
                    corrections["deduplicated_rows"] = removed

        return corrected, RecoveryReport(corrections=dict(corrections))


__all__ = ["ErrorRecoveryEngine", "RecoveryReport"]
