"""Base ISO extractor interface and common functionality."""

from __future__ import annotations

import abc
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests


class IsoDataType(Enum):
    """ISO data types."""
    LMP = "lmp"              # Locational Marginal Pricing
    LOAD = "load"            # System load/demand
    GENERATION_MIX = "genmix"  # Generation mix by fuel type
    ANCILLARY_SERVICES = "asm"  # Ancillary services markets
    PRICE_NODES = "pnode"     # Price node definitions


@dataclass
class IsoConfig:
    """Configuration for ISO data extraction."""

    # API configuration
    base_url: str
    api_key: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3

    # Data configuration
    markets: List[str] = None  # Default: ["DAM", "RTM"] (Day-Ahead, Real-Time)
    nodes: List[str] = None     # Price nodes to extract
    zones: List[str] = None     # Zones to aggregate

    # Rate limiting
    requests_per_minute: int = 60
    requests_per_hour: int = 1000

    # Data filtering
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    data_types: List[IsoDataType] = None

    def __post_init__(self):
        if self.markets is None:
            self.markets = ["DAM", "RTM"]
        if self.data_types is None:
            self.data_types = [IsoDataType.LMP, IsoDataType.LOAD]


class IsoBaseExtractor(abc.ABC):
    """Base class for ISO data extractors."""

    def __init__(self, config: IsoConfig):
        """Initialize ISO extractor.

        Args:
            config: ISO configuration
        """
        self.config = config
        self.session = requests.Session()

        # Configure session
        self.session.timeout = config.timeout
        if config.api_key:
            self.session.headers.update(self._get_auth_headers())

        # Rate limiting setup
        self._setup_rate_limiting()

    @abc.abstractmethod
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        pass

    @abc.abstractmethod
    def _setup_rate_limiting(self) -> None:
        """Set up rate limiting for API requests."""
        pass

    @abc.abstractmethod
    def get_lmp_data(self, start_date: str, end_date: str, market: str = "DAM",
                    nodes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get Locational Marginal Price data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            market: Market type (DAM/RTM)
            nodes: List of node IDs to fetch

        Returns:
            List of LMP records
        """
        pass

    @abc.abstractmethod
    def get_load_data(self, start_date: str, end_date: str,
                     zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get system load data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of load records
        """
        pass

    @abc.abstractmethod
    def get_generation_mix(self, start_date: str, end_date: str,
                          zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get generation mix data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of generation mix records
        """
        pass

    @abc.abstractmethod
    def get_ancillary_services(self, start_date: str, end_date: str,
                              zones: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get ancillary services data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            zones: List of zones to fetch

        Returns:
            List of ancillary services records
        """
        pass

    def get_price_nodes(self) -> List[Dict[str, Any]]:
        """Get price node definitions.

        Returns:
            List of price node definitions
        """
        # Default implementation - some ISOs may not support this
        return []

    def extract_all_data_types(self, start_date: str, end_date: str) -> Dict[IsoDataType, List[Dict[str, Any]]]:
        """Extract all configured data types.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dictionary mapping data types to extracted data
        """
        results = {}

        for data_type in self.config.data_types:
            try:
                if data_type == IsoDataType.LMP:
                    data = []
                    for market in self.config.markets:
                        market_data = self.get_lmp_data(start_date, end_date, market)
                        data.extend(market_data)
                    results[data_type] = data

                elif data_type == IsoDataType.LOAD:
                    results[data_type] = self.get_load_data(start_date, end_date)

                elif data_type == IsoDataType.GENERATION_MIX:
                    results[data_type] = self.get_generation_mix(start_date, end_date)

                elif data_type == IsoDataType.ANCILLARY_SERVICES:
                    results[data_type] = self.get_ancillary_services(start_date, end_date)

                elif data_type == IsoDataType.PRICE_NODES:
                    results[data_type] = self.get_price_nodes()

            except Exception as e:
                print(f"Error extracting {data_type.value}: {e}")
                results[data_type] = []

        return results

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
                     method: str = "GET") -> Dict[str, Any]:
        """Make HTTP request with retry logic and rate limiting.

        Args:
            endpoint: API endpoint
            params: Request parameters
            method: HTTP method

        Returns:
            Response JSON data

        Raises:
            requests.RequestException: If request fails after retries
        """
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(self.config.max_retries + 1):
            try:
                # Rate limiting
                self._wait_for_rate_limit()

                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.config.timeout
                )

                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                if attempt == self.config.max_retries:
                    raise e

                # Exponential backoff
                wait_time = self.config.backoff_factor * (2 ** attempt)
                print(f"Request failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

        # This should never be reached
        raise RuntimeError("Max retries exceeded")

    def _wait_for_rate_limit(self) -> None:
        """Wait if rate limit would be exceeded."""
        # Simple implementation - in production, use a token bucket or similar
        # For now, just add a small delay between requests
        time.sleep(1.0 / (self.config.requests_per_minute / 60.0))

    def _normalize_timestamp(self, timestamp: Union[str, int, datetime]) -> str:
        """Normalize timestamp to ISO format.

        Args:
            timestamp: Timestamp in various formats

        Returns:
            ISO formatted timestamp string
        """
        if isinstance(timestamp, datetime):
            return timestamp.isoformat()
        elif isinstance(timestamp, int):
            return datetime.fromtimestamp(timestamp).isoformat()
        else:
            return str(timestamp)

    def _validate_date_range(self, start_date: str, end_date: str) -> tuple[str, str]:
        """Validate and normalize date range.

        Args:
            start_date: Start date string
            end_date: End date string

        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            if start > end:
                raise ValueError("Start date must be before or equal to end date")

            # Limit to reasonable range (max 1 year)
            max_range = timedelta(days=365)
            if end - start > max_range:
                raise ValueError("Date range cannot exceed 1 year")

            return start_date, end_date

        except ValueError as e:
            raise ValueError(f"Invalid date format or range: {e}")


# Import required modules at the end to avoid circular imports
import time
from datetime import datetime, timedelta
