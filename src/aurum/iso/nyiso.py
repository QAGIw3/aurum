"""NYISO data extractor implementation."""

from __future__ import annotations

import csv
import io
from typing import Any, Dict, List, Optional

from .base import IsoBaseExtractor, IsoConfig, IsoDataType


class NyisoExtractor(IsoBaseExtractor):
    """New York Independent System Operator data extractor."""

    def __init__(self, config: IsoConfig):
        """Initialize NYISO extractor.

        Args:
            config: NYISO configuration
        """
        super().__init__(config)

        # NYISO specific configuration
        self.markets = {
            "DAM": "damlbmp_zone",
            "RTM": "realtime_zone"
        }

    def _get_auth_headers(self) -> Dict[str, str]:
        """NYISO doesn't require authentication headers."""
        return {}

    def _setup_rate_limiting(self) -> None:
        """NYISO has specific rate limiting requirements."""
        # NYISO allows 10 requests per minute
        self.config.requests_per_minute = 10
        self.config.requests_per_hour = 600

    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get NYISO Locational Marginal Price data.

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

        endpoint = self.markets[market]

        params = {
            "startdate": start_date,
            "enddate": end_date
        }

        # NYISO API returns CSV, not JSON
        response_text = self._make_csv_request(endpoint, params)

        records = []
        csv_reader = csv.DictReader(io.StringIO(response_text))

        for row in csv_reader:
            record = self._normalize_lmp_record(row, market)
            records.append(record)

        return records

    def get_load_data(self, start_date: str, end_date: str,
                     zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get NYISO system load data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of load records
        """
        endpoint = "realtime_zone_load"

        params = {
            "startdate": start_date,
            "enddate": end_date
        }

        response_text = self._make_csv_request(endpoint, params)

        records = []
        csv_reader = csv.DictReader(io.StringIO(response_text))

        for row in csv_reader:
            record = self._normalize_load_record(row)
            records.append(record)

        return records

    def get_generation_mix(self, start_date: str, end_date: str,
                          zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get NYISO generation mix data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of generation mix records
        """
        endpoint = "fuel_mix_realtime"

        params = {
            "startdate": start_date,
            "enddate": end_date
        }

        response_text = self._make_csv_request(endpoint, params)

        records = []
        csv_reader = csv.DictReader(io.StringIO(response_text))

        for row in csv_reader:
            record = self._normalize_genmix_record(row)
            records.append(record)

        return records

    def get_ancillary_services(self, start_date: str, end_date: str,
                              zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get NYISO ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        # NYISO doesn't provide a direct ancillary services endpoint
        # This would need to be derived from other data or use a different approach
        return []

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get NYISO price node definitions.

        Returns:
            List of price node definitions
        """
        endpoint = "ptids"

        try:
            response_text = self._make_csv_request(endpoint, {})

            nodes = []
            csv_reader = csv.DictReader(io.StringIO(response_text))

            for row in csv_reader:
                node = {
                    "node_id": row.get("PTID"),
                    "node_name": row.get("Name"),
                    "zone": row.get("Zone"),
                    "type": row.get("Type"),
                    "voltage": row.get("Voltage"),
                    "substation": row.get("Substation"),
                    "region": "NYISO"
                }
                nodes.append(node)

            return nodes

        except Exception as e:
            print(f"Error fetching price nodes: {e}")
            return []

    def _make_csv_request(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Make request to NYISO CSV API.

        Args:
            endpoint: API endpoint
            params: Request parameters

        Returns:
            CSV response text
        """
        url = f"{self.config.base_url}/{endpoint}"

        for attempt in range(self.config.max_retries + 1):
            try:
                # Rate limiting
                self._wait_for_rate_limit()

                response = self.session.get(
                    url=url,
                    params=params,
                    timeout=self.config.timeout
                )

                response.raise_for_status()
                return response.text

            except Exception as e:
                if attempt == self.config.max_retries:
                    raise e

                # Exponential backoff
                wait_time = self.config.backoff_factor * (2 ** attempt)
                print(f"Request failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

        # This should never be reached
        raise RuntimeError("Max retries exceeded")

    def _normalize_lmp_record(self, row: Dict[str, Any], market: str) -> Dict[str, Any]:
        """Normalize NYISO LMP data record.

        Args:
            row: Raw CSV row
            market: Market type

        Returns:
            Normalized record
        """
        return {
            "timestamp": self._parse_nyiso_timestamp(row.get("Time Stamp", "")),
            "node_id": row.get("PTID"),
            "node_name": row.get("Name"),
            "zone": row.get("Zone"),
            "market": market,
            "lmp": float(row.get("LBMP ($/MWHr)", 0)),
            "congestion": float(row.get("Marginal Cost Congestion ($/MWHr)", 0)),
            "losses": float(row.get("Marginal Cost Losses ($/MWHr)", 0)),
            "source": "NYISO",
            "data_type": "lmp"
        }

    def _normalize_load_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize NYISO load data record.

        Args:
            row: Raw CSV row

        Returns:
            Normalized record
        """
        return {
            "timestamp": self._parse_nyiso_timestamp(row.get("Time Stamp", "")),
            "zone": row.get("Zone"),
            "load_mw": float(row.get("Load (MW)", 0)),
            "source": "NYISO",
            "data_type": "load"
        }

    def _normalize_genmix_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize NYISO generation mix data record.

        Args:
            row: Raw CSV row

        Returns:
            Normalized record
        """
        return {
            "timestamp": self._parse_nyiso_timestamp(row.get("Time Stamp", "")),
            "fuel_type": row.get("Fuel Category"),
            "generation_mw": float(row.get("Gen (MW)", 0)),
            "zone": row.get("Zone"),
            "source": "NYISO",
            "data_type": "generation_mix"
        }

    def _parse_nyiso_timestamp(self, timestamp_str: str) -> str:
        """Parse NYISO timestamp format.

        Args:
            timestamp_str: Timestamp string from NYISO

        Returns:
            ISO formatted timestamp
        """
        try:
            # NYISO format: "01/01/2024 00:00:00"
            dt = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            return dt.isoformat()
        except ValueError:
            return timestamp_str


# Import required modules at the end to avoid circular imports
import time
from datetime import datetime
