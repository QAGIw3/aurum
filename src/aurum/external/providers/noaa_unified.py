"""NOAA provider collector using the unified collector framework.

This demonstrates how to migrate the existing NOAA provider to use the new
unified collector framework, reducing code duplication and improving maintainability.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from aurum.external.collect.unified_collector import (
    BaseProviderCollector,
    DataTransformer,
    DatasetConfig,
    ProviderConfig,
    create_provider_collector,
)
from aurum.core.settings import get_settings

logger = logging.getLogger(__name__)


class NoaaDataTransformer(DataTransformer):
    """Data transformer for NOAA API responses."""

    def __init__(self, field_mappings: Optional[Dict[str, str]] = None):
        self.field_mappings = field_mappings or {
            "station_id": "station_id",
            "station_name": "name",
            "latitude": "latitude",
            "longitude": "longitude",
            "elevation": "elevation",
            "state": "state",
            "country": "country",
            "date": "date",
            "datatype": "datatype",
            "value": "value",
            "units": "units",
        }

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform NOAA API response into canonical format."""
        if not isinstance(response_data, dict):
            return []

        # NOAA responses have different structures based on endpoint
        if "results" in response_data:
            # Station search response
            return self._transform_stations_response(response_data)
        elif "data" in response_data:
            # Observations response
            return self._transform_observations_response(response_data)

        return []

    def _transform_stations_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform station search response."""
        results = response_data.get("results", [])
        transformed = []

        for station in results:
            record = {
                "station_id": station.get("id"),
                "station_name": station.get("name"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "elevation": station.get("elevation"),
                "state": station.get("state"),
                "country": station.get("country"),
                "mindate": station.get("mindate"),
                "maxdate": station.get("maxdate"),
                "datacoverage": station.get("datacoverage"),
            }
            transformed.append(record)

        return transformed

    def _transform_observations_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform observations response."""
        data = response_data.get("data", [])
        transformed = []

        for obs in data:
            record = {
                "station_id": obs.get("station"),
                "date": obs.get("date"),
                "datatype": obs.get("datatype"),
                "value": obs.get("value"),
                "units": obs.get("units"),
            }
            transformed.append(record)

        return transformed

    def transform_record(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record."""
        transformed = {}

        for field, value in raw_record.items():
            # Apply field mapping if defined
            mapped_field = self.field_mappings.get(field, field)
            transformed[mapped_field] = value

        return transformed


def create_noaa_collector() -> BaseProviderCollector:
    """Create a NOAA collector using the unified framework."""

    # Get settings for configuration
    settings = get_settings()
    api_token = getattr(settings, "noaa_token", None)

    # Configure provider
    provider_config = ProviderConfig(
        name="noaa",
        base_url="https://www.ncei.noaa.gov/cdo-web/api/v2/",
        api_key=api_token,
        rate_limit_requests_per_minute=1000,  # NOAA allows 1000/min
        rate_limit_burst_size=10,
        timeout_seconds=30.0,
        max_retries=3,
        user_agent="Aurum-EnergyTrading/1.0",
    )

    # Configure datasets
    dataset_configs = [
        DatasetConfig(
            dataset_id="stations",
            endpoint_path="stations",
            data_format="json",
            pagination=True,
            pagination_param="offset",
            pagination_size=1000,
            date_field="date",
            id_field="station_id",
        ),
        DatasetConfig(
            dataset_id="observations",
            endpoint_path="data",
            data_format="json",
            pagination=False,
            date_field="date",
            id_field="station_id",
        ),
    ]

    # Create data transformer
    data_transformer = NoaaDataTransformer()

    # Create and return collector
    return create_provider_collector(
        provider_name="noaa",
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
    )


class NoaaUnifiedCollector(BaseProviderCollector):
    """NOAA collector implementation using the unified framework."""

    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute HTTP request using aiohttp for NOAA API."""
        try:
            from aurum.external.collect.base import HttpRequestError
            import aiohttp

            # Add NOAA-specific headers
            headers = request.headers.copy()
            headers["token"] = self.provider_config.api_key or ""

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=request.method,
                    url=request.url,
                    params=request.params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=request.timeout),
                ) as response:
                    response_data = await response.json()

                    return HttpResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        data=response_data,
                    )

        except Exception as e:
            raise HttpRequestError(f"NOAA API request failed: {e}")


# Example usage for migrating existing NOAA collectors
async def migrate_noaa_collector():
    """Example of how to migrate existing NOAA collectors."""

    # Create unified collector
    collector = create_noaa_collector()

    # Collect catalog (stations)
    await collector.collect_catalog()

    # Collect observations for specific stations
    await collector.collect_observations("stations")

    # The unified framework handles:
    # - Rate limiting (1000 requests/min for NOAA)
    # - Authentication with token
    # - Retry logic with exponential backoff
    # - Checkpoint management for incremental updates
    # - Error handling and logging
    # - Data transformation
    # - Kafka emission

    return collector
