"""FRED provider collector using the unified collector framework.

This demonstrates how to migrate the existing FRED provider to use the new
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


class FredDataTransformer(DataTransformer):
    """Data transformer for FRED API responses."""

    def __init__(self, field_mappings: Optional[Dict[str, str]] = None):
        self.field_mappings = field_mappings or {
            "id": "series_id",
            "title": "title",
            "observation_start": "start_date",
            "observation_end": "end_date",
            "frequency": "frequency",
            "units": "units",
            "seasonal_adjustment": "seasonal_adjustment",
        }

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform FRED API response into canonical format."""
        if not isinstance(response_data, dict):
            return []

        # FRED responses have different structures based on endpoint
        if "series" in response_data:
            # Series catalog response
            return self._transform_series_response(response_data)
        elif "observations" in response_data:
            # Observations response
            return self._transform_observations_response(response_data)

        return []

    def _transform_series_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform series catalog response."""
        series = response_data.get("series", [])
        transformed = []

        for item in series:
            record = {
                "series_id": item.get("id"),
                "title": item.get("title"),
                "description": item.get("notes"),
                "frequency": item.get("frequency"),
                "units": item.get("units"),
                "seasonal_adjustment": item.get("seasonal_adjustment"),
                "observation_start": item.get("observation_start"),
                "observation_end": item.get("observation_end"),
                "last_updated": item.get("last_updated"),
            }
            transformed.append(record)

        return transformed

    def _transform_observations_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform observations response."""
        observations = response_data.get("observations", [])
        transformed = []

        for obs in observations:
            record = {
                "series_id": obs.get("series_id"),
                "date": obs.get("date"),
                "value": obs.get("value"),
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


def create_fred_collector() -> BaseProviderCollector:
    """Create a FRED collector using the unified framework."""

    # Get settings for configuration
    settings = get_settings()
    api_key = getattr(settings, "fred_api_key", None)

    # Configure provider
    provider_config = ProviderConfig(
        name="fred",
        base_url="https://api.stlouisfed.org/fred/",
        api_key=api_key,
        rate_limit_requests_per_minute=120,  # FRED allows 120/min
        rate_limit_burst_size=10,
        timeout_seconds=30.0,
        max_retries=3,
        user_agent="Aurum-EnergyTrading/1.0",
    )

    # Configure datasets
    dataset_configs = [
        DatasetConfig(
            dataset_id="economic_series",
            endpoint_path="series/search",
            data_format="json",
            pagination=True,
            pagination_param="offset",
            pagination_size=1000,
            date_field="date",
            id_field="series_id",
        ),
        DatasetConfig(
            dataset_id="observations",
            endpoint_path="series/observations",
            data_format="json",
            pagination=False,
            date_field="date",
            id_field="series_id",
        ),
    ]

    # Create data transformer
    data_transformer = FredDataTransformer()

    # Create and return collector
    return create_provider_collector(
        provider_name="fred",
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
    )


class FredUnifiedCollector(BaseProviderCollector):
    """FRED collector implementation using the unified framework."""

    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute HTTP request using aiohttp for FRED API."""
        try:
            from aurum.external.collect.base import HttpRequestError
            import aiohttp

            # Add FRED-specific parameters
            params = request.params or {}
            if self.provider_config.api_key:
                params["api_key"] = self.provider_config.api_key
            params["file_type"] = "json"

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=request.method,
                    url=request.url,
                    params=params,
                    headers=request.headers,
                    timeout=aiohttp.ClientTimeout(total=request.timeout),
                ) as response:
                    response_data = await response.json()

                    return HttpResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        data=response_data,
                    )

        except Exception as e:
            raise HttpRequestError(f"FRED API request failed: {e}")


# Example usage for migrating existing FRED collectors
async def migrate_fred_collector():
    """Example of how to migrate existing FRED collectors."""

    # Create unified collector
    collector = create_fred_collector()

    # Collect catalog
    await collector.collect_catalog()

    # Collect observations for specific datasets
    await collector.collect_observations("economic_series")

    # The unified framework handles:
    # - Rate limiting (120 requests/min for FRED)
    # - Retry logic with exponential backoff
    # - Checkpoint management
    # - Error handling and logging
    # - Data transformation
    # - Kafka emission

    return collector
