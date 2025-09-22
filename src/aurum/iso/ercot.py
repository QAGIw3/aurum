"""ERCOT data extractor (placeholder stub).

Production ingestion is implemented via scripts/ingest/ercot_mis_to_kafka.py
which downloads MIS zip payloads, normalizes CSV rows, and publishes Avro.
This class aligns the interface with other ISOs for potential future use.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base import IsoBaseExtractor, IsoConfig


class ErcotExtractor(IsoBaseExtractor):
    """ERCOT MIS extractor (interface stub)."""

    def __init__(self, config: IsoConfig):
        super().__init__(config)
        self.markets = {"DAM": "DAM", "RTM": "RTM"}

    def _get_auth_headers(self) -> Dict[str, str]:
        # ERCOT MIS often requires a bearer token for file API access.
        token = self.config.api_key or ""
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _setup_rate_limiting(self) -> None:
        self.config.requests_per_minute = 60
        self.config.requests_per_hour = 3600

    def get_lmp_data(
        self,
        start_date: str,
        end_date: str,
        market: str = "DAM",
        nodes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError(
            "Use scripts/ingest/ercot_mis_to_kafka.py for ERCOT LMP ingestion"
        )

    def get_load_data(
        self, start_date: str, end_date: str, zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        return []

    def get_generation_mix(
        self, start_date: str, end_date: str, zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        return []

    def get_ancillary_services(
        self, start_date: str, end_date: str, zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        return []

