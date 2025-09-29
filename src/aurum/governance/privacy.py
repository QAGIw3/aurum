"""Privacy enforcement helpers for governance workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional

import pandas as pd

from .catalog import CatalogService


@dataclass
class PrivacyRule:
    tag: str
    action: str  # e.g. "mask", "restrict"
    mask_value: str = "***"


class PrivacyPolicyManager:
    """Applies privacy rules based on catalog tags."""

    def __init__(self, catalog: CatalogService, rules: Iterable[PrivacyRule]) -> None:
        self.catalog = catalog
        self.rules = list(rules)

    def fetch_tags(self, fqn: str) -> Mapping[str, str]:
        dataset = self.catalog.get_dataset(fqn, fields=("tags", "columns"))
        column_tags = {}
        for column in dataset.get("columns", []):
            column_tags[column.get("name")] = [tag.get("tagFQN") for tag in column.get("tags", [])]
        dataset_tags = [tag.get("tagFQN") for tag in dataset.get("tags", [])]
        return {"dataset": dataset_tags, **column_tags}

    def mask_dataframe(self, dataframe: pd.DataFrame, *, fqn: str) -> pd.DataFrame:
        tags = self.fetch_tags(fqn)
        masked = dataframe.copy()
        for column, column_tags in tags.items():
            if column == "dataset":
                continue
            if column not in masked.columns:
                continue
            for rule in self.rules:
                if rule.tag in column_tags and rule.action == "mask":
                    masked[column] = rule.mask_value
        return masked

    def restricted_columns(self, *, fqn: str) -> set[str]:
        tags = self.fetch_tags(fqn)
        restricted: set[str] = set()
        for column, column_tags in tags.items():
            if column == "dataset":
                continue
            for rule in self.rules:
                if rule.tag in column_tags and rule.action == "restrict":
                    restricted.add(column)
        return restricted


__all__ = ["PrivacyPolicyManager", "PrivacyRule"]
