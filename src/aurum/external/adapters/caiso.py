"""CAISO adapter for OASIS endpoints.

Notes:
- CAISO "ALL" nodes queries must be limited in hours; enforce chunking <= 24h
- Timestamps are provided in GMT; normalize to UTC
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence, Tuple, List

from .base import IsoAdapter, IsoAdapterConfig, IsoRequestChunk
from ..collect import HttpRequest
from ...data.iso_catalog import canonicalize_iso_observation_record


DEFAULT_BASE = os.getenv("AURUM_CAISO_BASE", "https://oasis.caiso.com/oasisapi")


@dataclass(frozen=True)
class CaisoConfig:
    market_run_id: str = os.getenv("AURUM_CAISO_MARKET_RUN", "RTM")
    node_id: str = os.getenv("AURUM_CAISO_NODE", "ALL")
    max_hours_per_request: int = int(os.getenv("AURUM_CAISO_MAX_HOURS", "24"))


class CaisoAdapter(IsoAdapter):
    """Adapter for CAISO OASIS JSON endpoints."""

    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        config = IsoAdapterConfig(
            provider="iso.caiso",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers={"Accept": "application/json"},
        )
        super().__init__(config, series_id=series_id)
        self._cfg = CaisoConfig()

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        # CAISO expects GMT timestamps without offset
        # E.g. startdatetime=2024-01-01T00:00Z
        def _fmt(dt: datetime) -> str:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")

        # enforce hour limit
        if (chunk.end - chunk.start) > timedelta(hours=self._cfg.max_hours_per_request):
            raise ValueError("chunk exceeds CAISO hour-limit; pre-chunking required")

        params = {
            "resultformat": "6",  # JSON
            "queryname": "PRC_LMP",
            "version": "1",
            "market_run_id": self._cfg.market_run_id,
            "node": self._cfg.node_id,
            "startdatetime": _fmt(chunk.start),
            "enddatetime": _fmt(chunk.end),
        }
        return HttpRequest(method="GET", path="/Group_Prices/PRC_LMP", params=params)

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        # CAISO returns {PRC_LMP: {results: {item: [...]}}}
        try:
            items = (
                payload.get("PRC_LMP", {})
                .get("results", {})
                .get("items", payload.get("items"))
                or []
            )
        except Exception:
            items = []
        out: List[Mapping[str, Any]] = []
        for it in items:
            rec: dict[str, Any] = dict(it)
            rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
            metadata = dict(rec.get("metadata") or {})
            metadata.setdefault("market", self._cfg.market_run_id)
            metadata.setdefault("product", metadata.get("product") or "LMP")
            node_id = (
                rec.get("node")
                or rec.get("NODE")
                or rec.get("NODE_ID")
                or rec.get("node_id")
                or rec.get("pnode")
            )
            metadata.setdefault("location_id", node_id)
            metadata.setdefault(
                "location_type",
                rec.get("node_type")
                or rec.get("NODE_TYPE")
                or metadata.get("location_type")
                or "NODE",
            )
            metadata.setdefault(
                "interval_minutes",
                metadata.get("interval_minutes")
                or (5 if self._cfg.market_run_id.upper() == "RTM" else 60),
            )
            metadata.setdefault("unit", rec.get("uom") or metadata.get("unit") or "USD/MWh")
            rec["metadata"] = metadata
            if self.series_id:
                rec.setdefault("series_id", self.series_id)
            out.append(canonicalize_iso_observation_record("iso.caiso", rec))
        return (out, None)
