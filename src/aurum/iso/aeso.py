"""AESO data extractor implementation."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .base import IsoBaseExtractor, IsoConfig, IsoDataType


class AesoExtractor(IsoBaseExtractor):
    """Alberta Electric System Operator data extractor."""

    def __init__(self, config: IsoConfig):
        """Initialize AESO extractor.

        Args:
            config: AESO configuration
        """
        super().__init__(config)

        # AESO specific configuration
        # AESO uses different terminology for markets
        self.markets = {
            "DAM": "energy",
            "RTM": "dispatch"
        }

    def _get_auth_headers(self) -> Dict[str, str]:
        """AESO uses Bearer token authentication."""
        return {
            "Authorization": f"Bearer {self.config.api_key or ''}"
        }

    def _setup_rate_limiting(self) -> None:
        """AESO has specific rate limiting requirements."""
        # AESO allows 100 requests per minute
        self.config.requests_per_minute = 100
        self.config.requests_per_hour = 6000

    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get AESO Locational Marginal Price data.

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

        endpoint = "api/market/poolprice"

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
        """Get AESO system load data.

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
        """Get AESO generation mix data.

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
        """Get AESO ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        # AESO doesn't provide a direct ancillary services endpoint
        # This would need to be derived from other data or use a different approach
        return []

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get AESO price node definitions.

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
                "region": "AESO"
            }
            nodes.append(node)

        return nodes

    def _normalize_lmp_record(self, item: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Normalize AESO LMP data record.

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
            "lmp": float(item.get("price", 0)),
            "congestion": 0.0,  # AESO doesn't separate congestion
            "losses": 0.0,      # AESO doesn't separate losses
            "source": "AESO",
            "data_type": "lmp"
        }

    def _normalize_load_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize AESO load data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("timestamp"),
            "zone": item.get("zone", "Alberta"),
            "load_mw": float(item.get("load", 0)),
            "source": "AESO",
            "data_type": "load"
        }

    def _normalize_genmix_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize AESO generation mix data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("timestamp"),
            "fuel_type": item.get("fuel_type"),
            "generation_mw": float(item.get("generation", 0)),
            "zone": item.get("zone", "Alberta"),
            "source": "AESO",
            "data_type": "generation_mix"
        }
