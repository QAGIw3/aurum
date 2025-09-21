"""CAISO data extractor implementation."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .base import IsoBaseExtractor, IsoConfig, IsoDataType


class CaisoExtractor(IsoBaseExtractor):
    """California Independent System Operator data extractor."""

    def __init__(self, config: IsoConfig):
        """Initialize CAISO extractor.

        Args:
            config: CAISO configuration
        """
        super().__init__(config)

        # CAISO specific configuration
        self.markets = {
            "DAM": "DAM",
            "RTM": "RTM"
        }

        # CAISO uses different query names for different data types
        self.query_names = {
            "lmp": "PRC_LMP",
            "load": "SLD_FCST",
            "genmix": "ENE_SLRS",
            "asm": "AS_CAISO_EXP"
        }

    def _get_auth_headers(self) -> Dict[str, str]:
        """CAISO doesn't require authentication headers for most APIs."""
        return {}

    def _setup_rate_limiting(self) -> None:
        """CAISO has specific rate limiting requirements."""
        # CAISO allows 100 requests per minute
        self.config.requests_per_minute = 100
        self.config.requests_per_hour = 6000

    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get CAISO Locational Marginal Price data.

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

        endpoint = "oasisapi/singlepublic/api/v1/query"

        params = {
            "queryname": self.query_names["lmp"],
            "startdatetime": f"{start_date}T00:00-0000",
            "enddatetime": f"{end_date}T23:59-0000",
            "market_run_id": self.markets[market],
            "version": 1
        }

        if nodes:
            params["node"] = ",".join(nodes)

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_lmp_record(item, market)
            records.append(record)

        return records

    def get_load_data(self, start_date: str, end_date: str,
                     zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get CAISO system load data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of load records
        """
        endpoint = "oasisapi/singlepublic/api/v1/query"

        params = {
            "queryname": self.query_names["load"],
            "startdatetime": f"{start_date}T00:00-0000",
            "enddatetime": f"{end_date}T23:59-0000",
            "version": 1
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_load_record(item)
            records.append(record)

        return records

    def get_generation_mix(self, start_date: str, end_date: str,
                          zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get CAISO generation mix data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of generation mix records
        """
        endpoint = "oasisapi/singlepublic/api/v1/query"

        params = {
            "queryname": self.query_names["genmix"],
            "startdatetime": f"{start_date}T00:00-0000",
            "enddatetime": f"{end_date}T23:59-0000",
            "version": 1
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_genmix_record(item)
            records.append(record)

        return records

    def get_ancillary_services(self, start_date: str, end_date: str,
                              zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get CAISO ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        endpoint = "oasisapi/singlepublic/api/v1/query"

        params = {
            "queryname": self.query_names["asm"],
            "startdatetime": f"{start_date}T00:00-0000",
            "enddatetime": f"{end_date}T23:59-0000",
            "version": 1
        }

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("data", []):
            record = self._normalize_asm_record(item)
            records.append(record)

        return records

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get CAISO price node definitions.

        Returns:
            List of price node definitions
        """
        endpoint = "oasisapi/singlepublic/api/v1/query"

        params = {
            "queryname": "ATL_PNODE",
            "startdatetime": "2024-01-01T00:00-0000",  # Static query, use recent date
            "enddatetime": "2024-01-01T23:59-0000",
            "version": 1
        }

        response_data = self._make_request(endpoint, params)

        nodes = []
        for item in response_data.get("data", []):
            node = {
                "node_id": item.get("PNODE_ID"),
                "node_name": item.get("PNODE_NAME"),
                "zone": item.get("TAC_ZONE"),
                "type": item.get("PNODE_TYPE"),
                "voltage": item.get("VOLTAGE"),
                "substation": item.get("SUBSTATION"),
                "region": "CAISO"
            }
            nodes.append(node)

        return nodes

    def _normalize_lmp_record(self, item: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Normalize CAISO LMP data record.

        Args:
            item: Raw API response item
            market: Market type

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("INTERVAL_START_GMT"),
            "node_id": item.get("PNODE_ID"),
            "node_name": item.get("PNODE_NAME"),
            "zone": item.get("TAC_ZONE"),
            "market": market,
            "lmp": float(item.get("LMP_CONGESTION", 0)) + float(item.get("LMP_LOSS", 0)) + float(item.get("LMP_ENERGY", 0)),
            "congestion": float(item.get("LMP_CONGESTION", 0)),
            "losses": float(item.get("LMP_LOSS", 0)),
            "energy": float(item.get("LMP_ENERGY", 0)),
            "source": "CAISO",
            "data_type": "lmp"
        }

    def _normalize_load_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CAISO load data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("INTERVAL_START_GMT"),
            "zone": item.get("TAC_ZONE"),
            "load_mw": float(item.get("MW", 0)),
            "source": "CAISO",
            "data_type": "load"
        }

    def _normalize_genmix_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CAISO generation mix data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("INTERVAL_START_GMT"),
            "fuel_type": item.get("FUEL_TYPE"),
            "generation_mw": float(item.get("MW", 0)),
            "zone": item.get("TAC_ZONE"),
            "source": "CAISO",
            "data_type": "generation_mix"
        }

    def _normalize_asm_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CAISO ancillary services data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("INTERVAL_START_GMT"),
            "service_type": item.get("AS_TYPE"),
            "zone": item.get("TAC_ZONE"),
            "price": float(item.get("COST", 0)),
            "quantity": float(item.get("MW", 0)),
            "source": "CAISO",
            "data_type": "ancillary_services"
        }
