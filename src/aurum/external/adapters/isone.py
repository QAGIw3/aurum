"""ISO-NE adapter for Web Services endpoints (JSON mode).

For ingestion we stick to JSON responses (not XML) and normalize to Avro.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Tuple, List

from .base import IsoAdapter, IsoAdapterConfig, IsoRequestChunk
from ..collect import HttpRequest


DEFAULT_BASE = os.getenv("AURUM_ISONE_BASE", "https://webservices.iso-ne.com/api/v1.1")


@dataclass(frozen=True)
class IsoneConfig:
    endpoint: str = os.getenv("AURUM_ISONE_LMP_ENDPOINT", "/markets/real-time/hourly-lmp")


class IsoneAdapter(IsoAdapter):
    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        config = IsoAdapterConfig(
            provider="iso.isone",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers={"Accept": "application/json"},
        )
        super().__init__(config, series_id=series_id)
        self._cfg = IsoneConfig()

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        params = {
            "start": chunk.start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": chunk.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        return HttpRequest(method="GET", path=self._cfg.endpoint, params=params)

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        items = payload.get("items") or payload.get("data") or []
        out: List[Mapping[str, Any]] = []
        for it in items:
            rec: dict[str, Any] = dict(it)
            rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
            out.append(rec)
        return (out, None)

