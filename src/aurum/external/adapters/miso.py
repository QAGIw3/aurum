"""MISO adapter using Data Exchange / RT data endpoints.

Implements headers per product subscriptions (via env-provided headers) and
JSON paging if present. Datasets use Avro subjects declared in contracts.yml.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence, Tuple, List

from .base import IsoAdapter, IsoAdapterConfig, IsoRequestChunk
from ..collect import HttpRequest


DEFAULT_BASE = os.getenv("AURUM_MISO_RTDB_BASE", "https://api.misoenergy.org")
DEFAULT_ENDPOINT = os.getenv("AURUM_MISO_RTDB_ENDPOINT", "/MISORTDBService/rest/data/getLMPConsolidatedTable")
DEFAULT_HEADERS = os.getenv("AURUM_MISO_RTDB_HEADERS", "").strip()


def _parse_kv_headers(raw: str) -> Mapping[str, str]:
    headers: dict[str, str] = {}
    for line in (raw or "").split(","):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
    return headers


@dataclass(frozen=True)
class MisoConfig:
    endpoint: str = DEFAULT_ENDPOINT
    region: str = os.getenv("AURUM_MISO_RTDB_REGION", "ALL")
    market: str = os.getenv("AURUM_MISO_RTDB_MARKET", "RTM")


class MisoAdapter(IsoAdapter):
    """Adapter for MISO RT Data Broker LMP/Load/Genmix, JSON responses only."""

    def __init__(self, *, series_id: str, kafka_topic: str, schema_registry_url: Optional[str] = None) -> None:
        headers = _parse_kv_headers(DEFAULT_HEADERS)
        config = IsoAdapterConfig(
            provider="iso.miso",
            base_url=DEFAULT_BASE,
            kafka_topic=kafka_topic,
            schema_registry_url=schema_registry_url,
            default_headers=headers,
        )
        super().__init__(config, series_id=series_id)
        self._miso = MisoConfig()

    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:
        params = {
            "marketType": self._miso.market,
            "region": self._miso.region,
            "startDateTime": chunk.start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": chunk.end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if chunk.cursor:
            params["next"] = chunk.cursor
        return HttpRequest(method="GET", path=self._miso.endpoint, params=params)

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
        # MISO RT DB tables place data under response.data; support both {items:[], next:...} and simple list
        if not payload:
            return ([], None)
        data = payload.get("items") or payload.get("data") or []
        next_cursor = payload.get("next")
        out: List[Mapping[str, Any]] = []
        for item in data:
            # Normalize to iso.lmp/load/genmix envelopes where possible
            rec: dict[str, Any] = dict(item)
            # Ensure ingest_ts and basic required fields exist
            rec.setdefault("ingest_ts", int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000))
            out.append(rec)
        return (out, next_cursor)

