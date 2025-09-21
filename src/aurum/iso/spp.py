"""SPP data extractor implementation."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .base import IsoBaseExtractor, IsoConfig, IsoDataType


class SppExtractor(IsoBaseExtractor):
    """Southwest Power Pool data extractor."""

    def __init__(self, config: IsoConfig):
        """Initialize SPP extractor.

        Args:
            config: SPP configuration
        """
        super().__init__(config)

        # SPP specific configuration
        self.markets = {
            "DAM": "DAM",
            "RTM": "RTBM"
        }

    def _get_auth_headers(self) -> Dict[str, str]:
        """SPP doesn't require authentication headers."""
        return {}

    def _setup_rate_limiting(self) -> None:
        """SPP has specific rate limiting requirements."""
        # SPP allows 30 requests per minute
        self.config.requests_per_minute = 30
        self.config.requests_per_hour = 1800

    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get SPP Locational Marginal Price data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            market: Market type (DAM/RTM)
            nodes: List of node IDs to fetch

        Returns:
            List of LMP records
        """
        if market not in self.markets:
            raise ValueError(f"Unsupported market: {market}. Supported: {list(self.markets.keys())}")

        endpoint = "api/market/lmp"

        params = {
            "startDate": start_date,
            "endDate": end_date,
            "market": self.markets[market]
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_lmp_record(item, market)
            records.append(record)

        return records

    def get_load_data(self, start_date: str, end_date: str,
                     zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get SPP system load data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of load records
        """
        endpoint = "api/market/load"

        params = {
            "startDate": start_date,
            "endDate": end_date
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_load_record(item)
            records.append(record)

        return records

    def get_generation_mix(self, start_date: str, end_date: str,
                          zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get SPP generation mix data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of generation mix records
        """
        endpoint = "api/market/generation"

        params = {
            "startDate": start_date,
            "endDate": end_date
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_genmix_record(item)
            records.append(record)

        return records

    def get_ancillary_services(self, start_date: str, end_date: str,
                              zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get SPP ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        # SPP doesn't provide a direct ancillary services endpoint
        # This would need to be derived from other data or use a different approach
        return []

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get SPP price node definitions.

        Returns:
            List of price node definitions
        """
        endpoint = "api/market/nodes"

        params = {}

        response_data = self._make_request(endpoint, params)

        nodes = []
        for item in response_data.get("data", []):
            node = {
                "node_id": item.get("node_id"),
                "node_name": item.get("node_name"),
                "zone": item.get("zone"),
                "type": item.get("node_type"),
                "voltage": item.get("voltage"),
                "substation": item.get("substation"),
                "region": "SPP"
            }
            nodes.append(node)

        return nodes

    def _normalize_lmp_record(self, item: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Normalize SPP LMP data record.

        Args:
            item: Raw API response item
            market: Market type

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("timestamp"),
            "node_id": item.get("node_id"),
            "node_name": item.get("node_name"),
            "zone": item.get("zone"),
            "market": market,
            "lmp": float(item.get("lmp", 0)),
            "congestion": float(item.get("congestion", 0)),
            "losses": float(item.get("losses", 0)),
            "source": "SPP",
            "data_type": "lmp"
        }

    def _normalize_load_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize SPP load data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("timestamp"),
            "zone": item.get("zone"),
            "load_mw": float(item.get("load", 0)),
            "source": "SPP",
            "data_type": "load"
        }

    def _normalize_genmix_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize SPP generation mix data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("timestamp"),
            "fuel_type": item.get("fuel_type"),
            "generation_mw": float(item.get("generation", 0)),
            "zone": item.get("zone"),
            "source": "SPP",
            "data_type": "generation_mix"
        }
