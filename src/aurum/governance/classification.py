"""Automated classification utilities for data governance."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from .catalog import CatalogService


@dataclass
class ClassificationRule:
    name: str
    tag: str
    patterns: Sequence[str]
    sample_regexes: Sequence[str] = ()


@dataclass
class ClassificationResult:
    column: str
    tag: str
    confidence: float
    reason: str


class ColumnClassifier:
    """Performs lightweight PII detection using heuristics."""

    DEFAULT_RULES: Sequence[ClassificationRule] = (
        ClassificationRule(
            name="email",
            tag="pii.email",
            patterns=("email", "e_mail", "contact_email"),
            sample_regexes=(r"^[^@]+@[^@]+\.[^@]+$",),
        ),
        ClassificationRule(
            name="phone",
            tag="pii.phone",
            patterns=("phone", "phone_number", "mobile", "contact_number"),
            sample_regexes=(r"^\+?[0-9\-() ]{7,}$",),
        ),
        ClassificationRule(
            name="ssn",
            tag="pii.ssn",
            patterns=("ssn", "social_security"),
            sample_regexes=(r"^[0-9]{3}-[0-9]{2}-[0-9]{4}$",),
        ),
        ClassificationRule(
            name="name",
            tag="pii.name",
            patterns=("first_name", "last_name", "full_name"),
        ),
    )

    def __init__(
        self,
        *,
        rules: Optional[Sequence[ClassificationRule]] = None,
        sample_size: int = 200,
        catalog: Optional[CatalogService] = None,
    ) -> None:
        self.rules = rules or self.DEFAULT_RULES
        self.sample_size = sample_size
        self.catalog = catalog

    def classify_dataframe(
        self,
        dataframe: pd.DataFrame,
        *,
        fqn: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> List[ClassificationResult]:
        results: List[ClassificationResult] = []
        for column in dataframe.columns:
            column_lower = column.lower()
            sample = dataframe[column].dropna().astype(str).head(self.sample_size)
            for rule in self.rules:
                if self._matches_rule(column_lower, sample, rule):
                    confidence = self._calculate_confidence(column_lower, sample, rule)
                    results.append(
                        ClassificationResult(
                            column=column,
                            tag=rule.tag,
                            confidence=confidence,
                            reason=f"matched rule {rule.name}",
                        )
                    )
                    break
        if fqn and self.catalog and results:
            self._apply_tags(fqn, results)
        return results

    def _matches_rule(self, column: str, sample: pd.Series, rule: ClassificationRule) -> bool:
        if any(pattern in column for pattern in rule.patterns):
            return True
        for regex in rule.sample_regexes:
            pattern = re.compile(regex)
            if sample.apply(lambda value: bool(pattern.match(value))).mean() > 0.6:
                return True
        return False

    def _calculate_confidence(self, column: str, sample: pd.Series, rule: ClassificationRule) -> float:
        confidence = 0.0
        if any(pattern in column for pattern in rule.patterns):
            confidence += 0.6
        hits = 0
        total = max(1, len(sample))
        for regex in rule.sample_regexes:
            pattern = re.compile(regex)
            hits = max(hits, int(sample.apply(lambda value: bool(pattern.match(value))).sum()))
        confidence += min(0.4, hits / total)
        return round(min(confidence, 0.99), 2)

    def _apply_tags(self, fqn: str, results: Iterable[ClassificationResult]) -> None:
        tags = {result.tag for result in results}
        if not tags:
            return
        if not self.catalog:
            return
        self.catalog.apply_tags(fqn, tags)


__all__ = [
    "ClassificationResult",
    "ColumnClassifier",
    "ClassificationRule",
]
