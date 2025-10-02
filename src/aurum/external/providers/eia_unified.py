"""EIA provider collector using the unified collector framework.

This is an example of how to migrate an existing provider to use the new
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


class EiaDataTransformer(DataTransformer):
    """Data transformer for EIA API responses."""

    def __init__(self, field_mappings: Optional[Dict[str, str]] = None):
        self.field_mappings = field_mappings or {
            "series_id": "series_id",
            "name": "title",
            "description": "description",
            "units": "unit",
            "frequency": "frequency",
            "start_date": "start_date",
            "end_date": "end_date",
        }

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform EIA API response into canonical format."""
        if not isinstance(response_data, dict):
            return []

        # EIA responses have different structures based on endpoint
        if "series" in response_data:
            # Series catalog response
            return self._transform_series_response(response_data)
        elif "data" in response_data:
            # Observations response
            return self._transform_observations_response(response_data)

        return []

    def _transform_series_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform series catalog response."""
        series = response_data.get("series", [])
        transformed = []

        for item in series:
            record = {
                "series_id": item.get("series_id"),
                "title": item.get("name"),
                "description": item.get("description"),
                "unit": item.get("units"),
                "frequency": item.get("f"),
                "start_date": item.get("start"),
                "end_date": item.get("end"),
                "geography": item.get("geography"),
                "sector": item.get("sector"),
                "source": item.get("source"),
            }
            transformed.append(record)

        return transformed

    def _transform_observations_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform observations response."""
        data = response_data.get("data", [])
        transformed = []

        for item in data:
            if isinstance(item, list) and len(item) >= 2:
                record = {
                    "series_id": item[0],
                    "period": item[1],
                    "value": item[2] if len(item) > 2 else None,
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


def create_eia_collector() -> BaseProviderCollector:
    """Create an EIA collector using the unified framework."""

    # Get settings for configuration
    settings = get_settings()
    api_key = getattr(settings, "eia_api_key", None)

    # Configure provider
    provider_config = ProviderConfig(
        name="eia",
        base_url="https://api.eia.gov/v2/",
        api_key=api_key,
        rate_limit_requests_per_minute=1000,  # EIA allows 1000/min
        rate_limit_burst_size=10,
        timeout_seconds=30.0,
        max_retries=3,
        user_agent="Aurum-EnergyTrading/1.0",
    )

    # Configure datasets
    dataset_configs = [
        DatasetConfig(
            dataset_id="electricity_series",
            endpoint_path="electricity/rto/region-data/data/",
            data_format="json",
            pagination=True,
            pagination_param="page",
            pagination_size=5000,
            date_field="period",
            id_field="series_id",
        ),
        DatasetConfig(
            dataset_id="petroleum_series",
            endpoint_path="petroleum/pri/spt/data/",
            data_format="json",
            pagination=True,
            pagination_param="page",
            pagination_size=5000,
            date_field="period",
            id_field="series_id",
        ),
    ]

    # Create data transformer
    data_transformer = EiaDataTransformer()

    # Create and return collector
    return create_provider_collector(
        provider_name="eia",
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
    )


# Example usage and migration guide
class EiaUnifiedCollector(BaseProviderCollector):
    """Example EIA collector implementation using the unified framework.

    This shows how to implement the abstract _execute_http_request method
    for a specific provider while leveraging all the common functionality
    from BaseProviderCollector.
    """

    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute HTTP request using the resilient HTTP client."""
        try:
            # Import here to avoid circular imports
            from aurum.external.collect.base import HttpRequestError

            # Use the resilient HTTP client
            response = await self._make_resilient_request(request)

            return HttpResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                data=response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
            )

        except Exception as e:
            raise HttpRequestError(f"HTTP request failed: {e}")

    async def _make_resilient_request(self, request: HttpRequest) -> Any:
        """Make resilient HTTP request (implementation would use existing HTTP client)."""
        # This would integrate with the existing resilient HTTP client
        # For now, this is a placeholder showing the pattern
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=request.method,
                url=request.url,
                params=request.params,
                headers=request.headers,
                timeout=aiohttp.ClientTimeout(total=request.timeout),
            ) as response:
                return response


# Migration benefits:
#
# 1. **Reduced Code Duplication**: Common patterns like rate limiting,
#    retry logic, pagination, and error handling are now centralized
#
# 2. **Consistent Configuration**: All providers use the same config structures
#
# 3. **Better Error Handling**: Standardized error handling and logging
#
# 4. **Easier Testing**: Base functionality can be tested independently
#
# 5. **Extensibility**: Easy to add new providers or modify behavior
#
# Migration steps for other providers:
# 1. Create ProviderConfig with provider-specific settings
# 2. Define DatasetConfig for each dataset
# 3. Implement DataTransformer for response format
# 4. Create collector using create_provider_collector()
# 5. Implement _execute_http_request() for HTTP client integration
