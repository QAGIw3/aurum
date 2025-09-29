"""Lineage tracking utilities built on top of OpenLineage and Marquez."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote

try:  # pragma: no cover - optional dependency
    from openlineage.client import OpenLineageClient
except Exception:  # pragma: no cover - allow operation without SDK
    OpenLineageClient = None  # type: ignore

import requests

from aurum.airflow_utils.lineage import LineageDataset, emit_lineage_event

logger = logging.getLogger(__name__)


class LineageDirection(str, Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"


@dataclass(frozen=True)
class DatasetURN:
    """Standardised dataset URN for governance services."""

    system: str
    database: str
    schema: str
    name: str

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"urn:{self.system}:{self.database}.{self.schema}.{self.name}"


@dataclass
class LineageNode:
    """Node metadata within a lineage graph."""

    urn: str
    type: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    source: str
    target: str
    type: str


@dataclass
class LineageGraph:
    """Container for lineage nodes and edges."""

    nodes: Dict[str, LineageNode] = field(default_factory=dict)
    edges: List[LineageEdge] = field(default_factory=list)

    def merge(self, other: "LineageGraph") -> None:
        self.nodes.update(other.nodes)
        self.edges.extend(other.edges)


class LineageTracker:
    """Facade for emitting OpenLineage events and querying Marquez lineage graphs."""

    def __init__(
        self,
        *,
        openlineage_url: Optional[str],
        namespace: str,
        marquez_url: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.openlineage_url = openlineage_url.rstrip("/") if openlineage_url else None
        self.namespace = namespace
        self.marquez_url = marquez_url.rstrip("/") if marquez_url else None
        self.session = session or requests.Session()
        self._client = self._build_openlineage_client()

    def _build_openlineage_client(self) -> Optional[OpenLineageClient]:
        if OpenLineageClient is None or not self.openlineage_url:
            return None
        try:
            return OpenLineageClient(url=self.openlineage_url)
        except Exception as exc:  # pragma: no cover - best effort initialisation
            logger.warning("Failed to initialise OpenLineage client: %s", exc)
            return None

    def emit_job_event(
        self,
        *,
        job_name: str,
        run_id: str,
        event_type: str,
        inputs: Iterable[LineageDataset] = (),
        outputs: Iterable[LineageDataset] = (),
        run_facets: Optional[Mapping[str, Any]] = None,
        job_facets: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not self.openlineage_url:
            logger.debug("OpenLineage disabled; skipping event emission")
            return

        try:
            if self._client is not None:
                self._client.emit(
                    {
                        "eventType": event_type,
                        "eventTime": datetime.utcnow().isoformat(),
                        "run": {
                            "runId": run_id,
                            "facets": dict(run_facets or {}),
                        },
                        "job": {
                            "namespace": self.namespace,
                            "name": job_name,
                            "facets": dict(job_facets or {}),
                        },
                        "inputs": [
                            {
                                "namespace": dataset.namespace,
                                "name": dataset.name,
                                "facets": dict(dataset.facets),
                            }
                            for dataset in inputs
                        ],
                        "outputs": [
                            {
                                "namespace": dataset.namespace,
                                "name": dataset.name,
                                "facets": dict(dataset.facets),
                            }
                            for dataset in outputs
                        ],
                        "producer": "urn:aurum:openlineage:sdk",
                        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
                    }
                )
                return
        except Exception as exc:
            logger.error("OpenLineage SDK emit failed: %s", exc)

        emit_lineage_event(
            endpoint=f"{self.openlineage_url}/api/v1/lineage" if self.openlineage_url else None,
            namespace=self.namespace,
            job_name=job_name,
            run_id=run_id,
            event_type=event_type,
            inputs=inputs,
            outputs=outputs,
            extra_run_facets=run_facets,
            extra_job_facets=job_facets,
        )

    @staticmethod
    def build_dataset_urn(system: str, database: str, schema: str, name: str) -> str:
        return str(DatasetURN(system=system, database=database, schema=schema, name=name))

    def get_lineage_graph(
        self,
        *,
        node_urn: str,
        direction: LineageDirection = LineageDirection.BOTH,
        depth: int = 3,
    ) -> LineageGraph:
        if not self.marquez_url:
            raise RuntimeError("Marquez URL not configured")

        params = {
            "node": node_urn,
            "depth": depth,
            "upstream": "true" if direction in (LineageDirection.UPSTREAM, LineageDirection.BOTH) else "false",
            "downstream": "true" if direction in (LineageDirection.DOWNSTREAM, LineageDirection.BOTH) else "false",
        }
        url = f"{self.marquez_url}/api/v1/lineage"
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        return self._parse_lineage_response(payload)

    def _parse_lineage_response(self, payload: Mapping[str, Any]) -> LineageGraph:
        graph = LineageGraph()
        for node in payload.get("graph", {}).get("nodes", []):
            node_urn = node.get("id") or node.get("urn")
            if not node_urn:
                continue
            graph.nodes[node_urn] = LineageNode(
                urn=node_urn,
                type=node.get("type", "unknown"),
                attributes=node.get("data", {}),
            )
        for edge in payload.get("graph", {}).get("edges", []):
            source = edge.get("source")
            target = edge.get("target")
            if not source or not target:
                continue
            graph.edges.append(
                LineageEdge(
                    source=source,
                    target=target,
                    type=edge.get("type", "unknown"),
                )
            )
        return graph

    def get_dataset_runs(
        self,
        *,
        dataset_name: str,
        namespace: Optional[str] = None,
        limit: int = 20,
    ) -> Sequence[Mapping[str, Any]]:
        if not self.marquez_url:
            raise RuntimeError("Marquez URL not configured")
        ns = namespace or self.namespace
        dataset_id = quote(dataset_name, safe="")
        url = f"{self.marquez_url}/api/v1/namespaces/{ns}/datasets/{dataset_id}/runs"
        response = self.session.get(url, params={"limit": limit}, timeout=10)
        response.raise_for_status()
        runs = response.json().get("runs", [])
        return runs

    def upsert_job_facets(
        self,
        *,
        job_name: str,
        facets: Mapping[str, Any],
    ) -> None:
        if not self.marquez_url:
            raise RuntimeError("Marquez URL not configured")
        payload = {"facets": dict(facets)}
        job_id = quote(job_name, safe="")
        url = f"{self.marquez_url}/api/v1/namespaces/{self.namespace}/jobs/{job_id}/facets"
        response = self.session.put(url, json=payload, timeout=10)
        response.raise_for_status()

    def upsert_dataset_facets(
        self,
        *,
        dataset_name: str,
        facets: Mapping[str, Any],
    ) -> None:
        if not self.marquez_url:
            raise RuntimeError("Marquez URL not configured")
        payload = {"facets": dict(facets)}
        dataset_id = quote(dataset_name, safe="")
        url = f"{self.marquez_url}/api/v1/namespaces/{self.namespace}/datasets/{dataset_id}/facets"
        response = self.session.put(url, json=payload, timeout=10)
        response.raise_for_status()


__all__ = [
    "DatasetURN",
    "LineageDirection",
    "LineageEdge",
    "LineageGraph",
    "LineageNode",
    "LineageTracker",
]
