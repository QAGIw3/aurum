"""PJM data extractor implementation."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .base import IsoBaseExtractor, IsoConfig, IsoDataType


class PjmExtractor(IsoBaseExtractor):
    """PJM Interconnection data extractor."""

    def __init__(self, config: IsoConfig):
        """Initialize PJM extractor.

        Args:
            config: PJM configuration
        """
        super().__init__(config)

        # PJM specific configuration
        self.markets = {
            "DAM": "DAY_AHEAD_HOURLY",
            "RTM": "REAL_TIME_5_MIN"
        }

    def _get_auth_headers(self) -> Dict[str, str]:
        """PJM uses API key authentication."""
        return {
            "Ocp-Apim-Subscription-Key": self.config.api_key or ""
        }

    def _setup_rate_limiting(self) -> None:
        """PJM has specific rate limiting requirements."""
        # PJM allows 200 requests per minute
        self.config.requests_per_minute = 200
        self.config.requests_per_hour = 12000

    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get PJM Locational Marginal Price data.

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

        endpoint = "lmp_by_bus"

        params = {
            "startRow": 1,
            "rowCount": 100000,  # Large number to get all results
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "time_zone": "UTC",
            "market_type": self.markets[market],
            "datetime_beginning_utc": f"{start_date}T00:00:00",
            "datetime_ending_utc": f"{end_date}T23:59:59"
        }

        if nodes:
            params["bus_name"] = ",".join(nodes)

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("items", []):
            record = self._normalize_lmp_record(item, market)
            records.append(record)

        return records

    def get_load_data(self, start_date: str, end_date: str,
                     zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get PJM system load data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of load records
        """
        endpoint = "load_actual"

        params = {
            "startRow": 1,
            "rowCount": 100000,
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "time_zone": "UTC",
            "datetime_beginning_utc": f"{start_date}T00:00:00",
            "datetime_ending_utc": f"{end_date}T23:59:59"
        }

        if zones:
            params["area"] = ",".join(zones)

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("items", []):
            record = self._normalize_load_record(item)
            records.append(record)

        return records

    def get_generation_mix(self, start_date: str, end_date: str,
                          zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get PJM generation mix data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of generation mix records
        """
        endpoint = "generation_mix"

        params = {
            "startRow": 1,
            "rowCount": 100000,
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "time_zone": "UTC",
            "datetime_beginning_utc": f"{start_date}T00:00:00",
            "datetime_ending_utc": f"{end_date}T23:59:59"
        }

        if zones:
            params["area"] = ",".join(zones)

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("items", []):
            record = self._normalize_genmix_record(item)
            records.append(record)

        return records

    def get_ancillary_services(self, start_date: str, end_date: str,
                              zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get PJM ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        endpoint = "ancillary_services"

        params = {
            "startRow": 1,
            "rowCount": 100000,
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "time_zone": "UTC",
            "datetime_beginning_utc": f"{start_date}T00:00:00",
            "datetime_ending_utc": f"{end_date}T23:59:59"
        }

        if zones:
            params["area"] = ",".join(zones)

        response_data = self._make_request(endpoint, params)

        records = []
        for item in response_data.get("items", []):
            record = self._normalize_asm_record(item)
            records.append(record)

        return records

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get PJM price node definitions.

        Returns:
            List of price node definitions
        """
        endpoint = "bus_info"

        params = {
            "startRow": 1,
            "rowCount": 100000
        }

        response_data = self._make_request(endpoint, params)

        nodes = []
        for item in response_data.get("items", []):
            node = {
                "node_id": item.get("bus_id"),
                "node_name": item.get("bus_name"),
                "zone": item.get("area"),
                "type": item.get("bus_type"),
                "voltage": item.get("voltage"),
                "substation": item.get("substation"),
                "region": "PJM"
            }
            nodes.append(node)

        return nodes

    def _normalize_lmp_record(self, item: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Normalize PJM LMP data record.

        Args:
            item: Raw API response item
            market: Market type

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("datetime_beginning_utc"),
            "node_id": item.get("bus_id"),
            "node_name": item.get("bus_name"),
            "zone": item.get("area"),
            "market": market,
            "lmp": float(item.get("total_lmp", 0)),
            "congestion": float(item.get("congestion_price", 0)),
            "losses": float(item.get("marginal_loss_price", 0)),
            "source": "PJM",
            "data_type": "lmp"
        }

    def _normalize_load_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize PJM load data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("datetime_beginning_utc"),
            "zone": item.get("area"),
            "load_mw": float(item.get("actual_load", 0)),
            "source": "PJM",
            "data_type": "load"
        }

    def _normalize_genmix_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize PJM generation mix data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("datetime_beginning_utc"),
            "fuel_type": item.get("fuel_type"),
            "generation_mw": float(item.get("generation", 0)),
            "zone": item.get("area"),
            "source": "PJM",
            "data_type": "generation_mix"
        }

    def _normalize_asm_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize PJM ancillary services data record.

        Args:
            item: Raw API response item

        Returns:
            Normalized record
        """
        return {
            "timestamp": item.get("datetime_beginning_utc"),
            "service_type": item.get("ancillary_service_type"),
            "zone": item.get("area"),
            "price": float(item.get("clearing_price", 0)),
            "quantity": float(item.get("cleared_quantity", 0)),
            "source": "PJM",
            "data_type": "ancillary_services"
        }
