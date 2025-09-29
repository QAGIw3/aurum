"""Monitoring utilities for freshness and completeness."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd

from .quality_engine import BuiltinCheck, DataQualityEngine, TestSuiteConfig


@dataclass
class MonitorConfig:
    dataset_fqn: str
    freshness_column: Optional[str] = None
    freshness_window_minutes: int = 1440
    completeness_columns: Sequence[str] = ()
    completeness_threshold: float = 0.99


class FreshnessCompletenessMonitor:
    """Wraps the data quality engine with pre-defined monitor configs."""

    def __init__(
        self,
        engine: DataQualityEngine,
        loader: Callable[[str], pd.DataFrame],
    ) -> None:
        self.engine = engine
        self.loader = loader

    def run(self, config: MonitorConfig) -> Dict[str, float]:
        dataframe = self.loader(config.dataset_fqn)
        builtin_checks: List[BuiltinCheck] = []
        if config.freshness_column:
            builtin_checks.append(
                BuiltinCheck(
                    name=f"freshness::{config.freshness_column}",
                    check_type="freshness",
                    column=config.freshness_column,
                    window_minutes=config.freshness_window_minutes,
                )
            )
        for column in config.completeness_columns:
            builtin_checks.append(
                BuiltinCheck(
                    name=f"completeness::{column}",
                    check_type="completeness",
                    column=column,
                    threshold=config.completeness_threshold,
                )
            )

        suite = TestSuiteConfig(
            suite_name=f"monitor::{config.dataset_fqn}",
            builtin_checks=builtin_checks,
        )
        result = self.engine.run_tests(
            asset_ref=config.dataset_fqn,
            dataframe=dataframe,
            suite=suite,
        )
        return {check.name: check.score for check in result.results}


__all__ = ["FreshnessCompletenessMonitor", "MonitorConfig"]
