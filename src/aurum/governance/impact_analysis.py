"""Impact analysis helpers leveraging lineage and catalog metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from .catalog import CatalogService
from .lineage_tracker import LineageDirection, LineageTracker
from .schema_tracker import SchemaDiff


@dataclass
class ImpactSummary:
    subject: str
    severity: str
    impacted_assets: list[Dict[str, Any]]
    schema_changes: Dict[str, Any]


class ImpactAnalyzer:
    """Aggregates lineage and schema diffs to highlight downstream impact."""

    def __init__(self, catalog: CatalogService, lineage: LineageTracker) -> None:
        self.catalog = catalog
        self.lineage = lineage

    def analyze_schema_change(
        self,
        *,
        fqn: str,
        diff: SchemaDiff,
        severity: str = "medium",
        depth: int = 2,
    ) -> ImpactSummary:
        impacted = self.catalog.list_downstream(fqn, depth=depth)
        change_spec = diff.summary()
        return ImpactSummary(
            subject=fqn,
            severity=severity,
            impacted_assets=[entity.__dict__ for entity in impacted],
            schema_changes=change_spec,
        )

    def detect_lineage_gaps(self, *, fqn: str, depth: int = 2) -> Dict[str, Any]:
        graph = self.lineage.get_lineage_graph(
            node_urn=fqn,
            direction=LineageDirection.BOTH,
            depth=depth,
        )
        missing_nodes = [node.urn for node in graph.nodes.values() if not node.attributes]
        return {
            "subject": fqn,
            "missingMetadata": missing_nodes,
            "edgeCount": len(graph.edges),
        }


__all__ = ["ImpactAnalyzer", "ImpactSummary"]
