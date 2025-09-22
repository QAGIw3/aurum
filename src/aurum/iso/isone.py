"""ISO-NE data extractor (placeholder stub).

This extractor documents intended integration points for ISO-NE Web Services.
For production ingestion, see scripts/ingest/isone_ws_to_kafka.py and the
ingest_iso_prices_isone Airflow DAG.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base import IsoBaseExtractor, IsoConfig


class IsoneExtractor(IsoBaseExtractor):
    """ISO New England extractor.

    Note: The official integration in this repository uses a Python helper that
    handles authentication and schema mapping. This class exists to provide a
    unified interface alongside other ISO extractors and may be implemented in
    the future.
    """

    def __init__(self, config: IsoConfig):
        super().__init__(config)
        self.markets = {"DAM": "DA", "RTM": "RT"}

    def _get_auth_headers(self) -> Dict[str, str]:
        # ISO-NE typically uses basic auth for Web Services; headers are handled
        # by the requests client via auth tuples in the helper script. Return an
        # empty header set here.
        return {}

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
            "Use scripts/ingest/isone_ws_to_kafka.py for ISO-NE LMP ingestion"
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

