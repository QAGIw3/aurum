"""WorldBank provider collector using the unified collector framework.

This demonstrates how to migrate the existing WorldBank provider to use the new
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


class WorldBankDataTransformer(DataTransformer):
    """Data transformer for WorldBank API responses."""

    def __init__(self, field_mappings: Optional[Dict[str, str]] = None):
        self.field_mappings = field_mappings or {
            "indicator_id": "indicator_id",
            "indicator_name": "indicator_name",
            "country_id": "country_id",
            "country_name": "country_name",
            "date": "date",
            "value": "value",
            "unit": "unit",
            "obs_status": "obs_status",
        }

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform WorldBank API response into canonical format."""
        if not isinstance(response_data, list):
            return []

        # WorldBank API returns array of indicator data
        transformed = []

        for item in response_data:
            if isinstance(item, dict):
                record = {
                    "indicator_id": item.get("indicator", {}).get("id") if item.get("indicator") else None,
                    "indicator_name": item.get("indicator", {}).get("value") if item.get("indicator") else None,
                    "country_id": item.get("country", {}).get("id") if item.get("country") else None,
                    "country_name": item.get("country", {}).get("value") if item.get("country") else None,
                    "date": item.get("date"),
                    "value": item.get("value"),
                    "unit": item.get("unit"),
                    "obs_status": item.get("obs_status"),
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


def create_worldbank_collector() -> BaseProviderCollector:
    """Create a WorldBank collector using the unified framework."""

    # Get settings for configuration (WorldBank typically doesn't require API key)
    settings = get_settings()

    # Configure provider
    provider_config = ProviderConfig(
        name="worldbank",
        base_url="https://api.worldbank.org/v2/",
        api_key=None,  # WorldBank API is open
        rate_limit_requests_per_minute=1000,  # WorldBank allows 1000/min
        rate_limit_burst_size=10,
        timeout_seconds=30.0,
        max_retries=3,
        user_agent="Aurum-EnergyTrading/1.0",
    )

    # Configure datasets
    dataset_configs = [
        DatasetConfig(
            dataset_id="indicators",
            endpoint_path="indicators",
            data_format="json",
            pagination=True,
            pagination_param="page",
            pagination_size=32500,  # WorldBank page size limit
            date_field="date",
            id_field="indicator_id",
        ),
        DatasetConfig(
            dataset_id="country_data",
            endpoint_path="country",
            data_format="json",
            pagination=False,
            date_field="date",
            id_field="country_id",
        ),
    ]

    # Create data transformer
    data_transformer = WorldBankDataTransformer()

    # Create and return collector
    return create_provider_collector(
        provider_name="worldbank",
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
    )


class WorldBankUnifiedCollector(BaseProviderCollector):
    """WorldBank collector implementation using the unified framework."""

    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute HTTP request using aiohttp for WorldBank API."""
        try:
            from aurum.external.collect.base import HttpRequestError
            import aiohttp

            # Add WorldBank-specific parameters
            params = request.params or {}
            params["format"] = "json"
            params["per_page"] = 32500  # WorldBank max page size

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
            raise HttpRequestError(f"WorldBank API request failed: {e}")


# Example usage for migrating existing WorldBank collectors
async def migrate_worldbank_collector():
    """Example of how to migrate existing WorldBank collectors."""

    # Create unified collector
    collector = create_worldbank_collector()

    # Collect catalog (indicators)
    await collector.collect_catalog()

    # Collect country data
    await collector.collect_observations("country_data")

    # The unified framework handles:
    # - Rate limiting (1000 requests/min for WorldBank)
    # - Open API (no authentication required)
    # - Retry logic with exponential backoff
    # - Checkpoint management for incremental updates
    # - Error handling and logging
    # - Data transformation
    # - Kafka emission

    return collector
