"""Real-time validation engine for vendor curve parsing."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, MutableMapping, Optional, Sequence

import pandas as pd

from aurum.reference.curve_schema import CURVE_ENUMS, CurvePriceType

_DEFAULT_REQUIRED = (
    "asof_date",
    "tenor_label",
    "curve_key",
    "price_type",
    "mid",
)


@dataclass(frozen=True)
class ValidationIssue:
    """Represents a validation error or warning."""

    severity: str  # "error" or "warning"
    message: str
    context: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validation."""

    passed: bool
    confidence: float
    issues: Sequence[ValidationIssue]
    stats: Mapping[str, float]


class ValidationEngine:
    """Performs lightweight validation with confidence scoring."""

    def __init__(
        self,
        *,
        required_columns: Iterable[str] | None = None,
        min_confidence: float = 0.6,
    ) -> None:
        self.required_columns = tuple(required_columns or _DEFAULT_REQUIRED)
        self.min_confidence = min_confidence

    def validate(self, frame: pd.DataFrame) -> ValidationResult:
        if frame.empty:
            issue = ValidationIssue("error", "Parsed dataframe is empty")
            return ValidationResult(False, 0.0, (issue,), {"row_count": 0.0})

        issues: list[ValidationIssue] = []
        stats: MutableMapping[str, float] = {
            "row_count": float(len(frame)),
        }
        penalties = 0.0

        # Required column coverage
        missing_columns = [col for col in self.required_columns if col not in frame.columns]
        if missing_columns:
            issues.append(
                ValidationIssue(
                    "error",
                    f"Missing required columns: {', '.join(missing_columns)}",
                )
            )
            penalties += 0.25

        # Null ratios
        for column in self.required_columns:
            if column not in frame.columns:
                continue
            null_ratio = float(frame[column].isna().mean())
            stats[f"null_ratio::{column}"] = null_ratio
            if null_ratio > 0.4:
                issues.append(
                    ValidationIssue(
                        "warning",
                        f"Column '{column}' contains {null_ratio:.0%} nulls",
                    )
                )
                penalties += 0.05

        # Enumerations
        for column, enum in CURVE_ENUMS.items():
            if column not in frame.columns:
                continue
            valid_values = {item.value for item in enum}
            valid_mask = frame[column].dropna().isin(valid_values)
            valid_ratio = float(valid_mask.mean()) if not valid_mask.empty else 1.0
            invalid_ratio = 1 - valid_ratio
            stats[f"invalid_ratio::{column}"] = invalid_ratio
            if invalid_ratio > 0.05:
                issues.append(
                    ValidationIssue(
                        "error",
                        f"Column '{column}' contains {invalid_ratio:.0%} invalid enum values",
                    )
                )
                penalties += 0.1

        # Curve key uniqueness (only when identifiers are present)
        if {"curve_key", "tenor_label"}.issubset(frame.columns):
            ck = frame["curve_key"]
            tl = frame["tenor_label"]
            pairs = [(c, t) for c, t in zip(ck, tl) if pd.notna(c) and pd.notna(t)]
            if pairs:
                unique_pairs = set(pairs)
                dup_ratio = 0.0 if not pairs else 1.0 - (len(unique_pairs) / len(pairs))
                stats["duplicate_ratio::curve_key_tenor"] = dup_ratio
                if dup_ratio > 0.05:
                    issues.append(
                        ValidationIssue(
                            "warning",
                            f"{dup_ratio:.0%} duplicate curve/tenor combinations",
                        )
                    )
                    penalties += 0.05

        # Price type coverage
        if "price_type" in frame.columns:
            unique_price_types = frame["price_type"].dropna().unique()
            stats["price_type_unique"] = float(len(unique_price_types))
            if CurvePriceType.MID.value not in unique_price_types:
                issues.append(
                    ValidationIssue(
                        "warning",
                        "Missing MID price type in parsed data",
                    )
                )
                penalties += 0.02

        base_confidence = max(0.0, 1.0 - penalties)
        passed = base_confidence >= self.min_confidence and not any(issue.severity == "error" for issue in issues)

        return ValidationResult(
            passed=passed,
            confidence=base_confidence,
            issues=tuple(issues),
            stats=dict(stats),
        )


__all__ = ["ValidationEngine", "ValidationResult", "ValidationIssue"]
