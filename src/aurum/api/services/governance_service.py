"""Governance service wiring lineage, quality, and catalog capabilities."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

import pandas as pd

from ...governance.catalog import CatalogService
from ...governance.classification import ClassificationResult, ColumnClassifier
from ...governance.impact_analysis import ImpactAnalyzer, ImpactSummary
from ...governance.lineage_tracker import LineageDirection, LineageGraph, LineageTracker
from ...governance.monitors import FreshnessCompletenessMonitor, MonitorConfig
from ...governance.privacy import PrivacyPolicyManager, PrivacyRule
from ...governance.quality_engine import DataQualityEngine, DataQualitySuiteResult, TestSuiteConfig
from ...governance.schema_tracker import SchemaEvolutionTracker, SchemaSnapshot

DEFAULT_DIRECTION = {
    "UPSTREAM": LineageDirection.UPSTREAM,
    "DOWNSTREAM": LineageDirection.DOWNSTREAM,
    "BOTH": LineageDirection.BOTH,
}


def _ensure_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(func):  # pragma: no cover - rarely async
        return func(*args, **kwargs)
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, lambda: func(*args, **kwargs))


@dataclass
class GovernanceConfig:
    default_depth: int = 2
    freshness_window_minutes: int = 1440
    completeness_threshold: float = 0.99


class GovernanceService:
    """Service façade orchestrating governance operations."""

    def __init__(
        self,
        *,
        lineage_tracker: LineageTracker,
        catalog_service: CatalogService,
        quality_engine: DataQualityEngine,
        schema_tracker: SchemaEvolutionTracker,
        impact_analyzer: ImpactAnalyzer,
        monitor: FreshnessCompletenessMonitor,
        classifier: ColumnClassifier,
        privacy_manager: PrivacyPolicyManager,
        dataframe_loader: Callable[[str], pd.DataFrame],
        config: Optional[GovernanceConfig] = None,
    ) -> None:
        self.lineage_tracker = lineage_tracker
        self.catalog_service = catalog_service
        self.quality_engine = quality_engine
        self.schema_tracker = schema_tracker
        self.impact_analyzer = impact_analyzer
        self.monitor = monitor
        self.classifier = classifier
        self.privacy_manager = privacy_manager
        self.dataframe_loader = dataframe_loader
        self.config = config or GovernanceConfig()

    async def get_lineage(
        self,
        *,
        dataset_fqn: str,
        depth: Optional[int] = None,
        direction: str = "BOTH",
    ) -> Dict[str, Any]:
        depth = depth or self.config.default_depth
        lineage = await _ensure_async(
            self.lineage_tracker.get_lineage_graph,
            node_urn=dataset_fqn,
            depth=depth,
            direction=DEFAULT_DIRECTION.get(direction.upper(), LineageDirection.BOTH),
        )
        return self._serialise_lineage(lineage)

    async def get_quality_score(self, *, dataset_fqn: str) -> Dict[str, Any]:
        metadata = await _ensure_async(self.catalog_service.get_dataset, dataset_fqn, fields=("customProperties",))
        props = metadata.get("customProperties") or {}
        score = props.get("quality_score")
        return {
            "dataset": dataset_fqn,
            "score": float(score) if score is not None else None,
            "properties": props,
        }

    async def run_quality_suite(
        self,
        *,
        dataset_fqn: str,
        suite: TestSuiteConfig,
    ) -> DataQualitySuiteResult:
        dataframe = await _ensure_async(self.dataframe_loader, dataset_fqn)
        return self.quality_engine.run_tests(
            asset_ref=dataset_fqn,
            dataframe=dataframe,
            suite=suite,
        )

    async def monitor_dataset(self, *, dataset_fqn: str, freshness_column: Optional[str], completeness_columns: Iterable[str]) -> Dict[str, float]:
        config = MonitorConfig(
            dataset_fqn=dataset_fqn,
            freshness_column=freshness_column,
            freshness_window_minutes=self.config.freshness_window_minutes,
            completeness_columns=tuple(completeness_columns),
            completeness_threshold=self.config.completeness_threshold,
        )
        return await _ensure_async(self.monitor.run, config)

    async def classify_dataset(self, *, dataset_fqn: str) -> List[ClassificationResult]:
        dataframe = await _ensure_async(self.dataframe_loader, dataset_fqn)
        return self.classifier.classify_dataframe(dataframe, fqn=dataset_fqn)

    async def privacy_overview(self, *, dataset_fqn: str) -> Dict[str, Any]:
        restricted = await _ensure_async(self.privacy_manager.restricted_columns, fqn=dataset_fqn)
        masked_preview_df = await _ensure_async(self.dataframe_loader, dataset_fqn)
        masked_preview = self.privacy_manager.mask_dataframe(masked_preview_df, fqn=dataset_fqn).head(5).to_dict(orient="records")
        return {
            "dataset": dataset_fqn,
            "restrictedColumns": sorted(restricted),
            "maskedPreview": masked_preview,
        }

    async def analyze_schema_change(
        self,
        *,
        dataset_fqn: str,
        new_snapshot: SchemaSnapshot,
        previous_snapshot: Optional[SchemaSnapshot] = None,
        severity: str = "medium",
    ) -> ImpactSummary:
        prev_snapshot = previous_snapshot
        if prev_snapshot is None:
            dataset = await _ensure_async(self.catalog_service.get_dataset, dataset_fqn, fields=("columns",))
            prev_snapshot = self.schema_tracker.snapshot_from_metadata(dataset) if dataset else SchemaSnapshot()
        diff = self.schema_tracker.diff(prev_snapshot, new_snapshot)
        await _ensure_async(
            self.schema_tracker.record,
            dataset_fqn,
            new_snapshot=new_snapshot,
            previous_snapshot=prev_snapshot,
        )
        return await _ensure_async(
            self.impact_analyzer.analyze_schema_change,
            fqn=dataset_fqn,
            diff=diff,
            severity=severity,
        )

    async def lineage_gaps(self, *, dataset_fqn: str, depth: Optional[int] = None) -> Dict[str, Any]:
        depth = depth or self.config.default_depth
        return await _ensure_async(
            self.impact_analyzer.detect_lineage_gaps,
            fqn=dataset_fqn,
            depth=depth,
        )

    def _serialise_lineage(self, lineage: LineageGraph) -> Dict[str, Any]:
        return {
            "nodes": [
                {
                    "urn": node.urn,
                    "type": node.type,
                    "attributes": node.attributes,
                }
                for node in lineage.nodes.values()
            ],
            "edges": [
                {
                    "source": edge.source,
                    "target": edge.target,
                    "type": edge.type,
                }
                for edge in lineage.edges
            ],
        }


_governance_service: Optional[GovernanceService] = None


def get_governance_service() -> GovernanceService:
    global _governance_service
    if _governance_service is None:
        raise RuntimeError("GovernanceService has not been initialised")
    return _governance_service


def initialise_governance_service(
    *,
    lineage_tracker: LineageTracker,
    catalog_service: CatalogService,
    quality_engine: DataQualityEngine,
    schema_tracker: SchemaEvolutionTracker,
    impact_analyzer: ImpactAnalyzer,
    monitor: FreshnessCompletenessMonitor,
    classifier: ColumnClassifier,
    privacy_manager: PrivacyPolicyManager,
    dataframe_loader: Callable[[str], pd.DataFrame],
    config: Optional[GovernanceConfig] = None,
) -> GovernanceService:
    global _governance_service
    _governance_service = GovernanceService(
        lineage_tracker=lineage_tracker,
        catalog_service=catalog_service,
        quality_engine=quality_engine,
        schema_tracker=schema_tracker,
        impact_analyzer=impact_analyzer,
        monitor=monitor,
        classifier=classifier,
        privacy_manager=privacy_manager,
        dataframe_loader=dataframe_loader,
        config=config,
    )
    return _governance_service


__all__ = [
    "GovernanceService",
    "GovernanceConfig",
    "get_governance_service",
    "initialise_governance_service",
]
