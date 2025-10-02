"""Weather data provider using the unified collector framework.

This demonstrates how to add a new weather data provider using the unified framework,
showing patterns for API integration and data transformation.
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


class WeatherDataTransformer(DataTransformer):
    """Data transformer for weather API responses."""

    def __init__(self, field_mappings: Optional[Dict[str, str]] = None):
        self.field_mappings = field_mappings or {
            "station_id": "station_id",
            "timestamp": "timestamp",
            "temperature": "temperature",
            "humidity": "humidity",
            "pressure": "pressure",
            "wind_speed": "wind_speed",
            "wind_direction": "wind_direction",
            "precipitation": "precipitation",
            "visibility": "visibility",
            "cloud_cover": "cloud_cover",
        }

    def transform_response(self, response_data: Any) -> List[Dict[str, Any]]:
        """Transform weather API response into canonical format."""
        if not isinstance(response_data, dict):
            return []

        # Weather APIs have different response structures
        if "features" in response_data:
            # GeoJSON FeatureCollection format
            return self._transform_geojson_response(response_data)
        elif "data" in response_data:
            # Simple data array format
            return self._transform_data_response(response_data)
        elif "observations" in response_data:
            # Observations format
            return self._transform_observations_response(response_data)

        return []

    def _transform_geojson_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform GeoJSON FeatureCollection response."""
        features = response_data.get("features", [])
        transformed = []

        for feature in features:
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})

            record = {
                "station_id": properties.get("station_id"),
                "timestamp": properties.get("timestamp"),
                "temperature": properties.get("temperature"),
                "humidity": properties.get("relativeHumidity"),
                "pressure": properties.get("pressure"),
                "wind_speed": properties.get("windSpeed"),
                "wind_direction": properties.get("windDirection"),
                "precipitation": properties.get("precipitation"),
                "visibility": properties.get("visibility"),
                "cloud_cover": properties.get("cloudCover"),
                "latitude": geometry.get("coordinates", [None, None])[1],
                "longitude": geometry.get("coordinates", [None, None])[0],
            }
            transformed.append(record)

        return transformed

    def _transform_data_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform simple data array response."""
        data = response_data.get("data", [])
        transformed = []

        for item in data:
            record = {
                "station_id": item.get("station_id"),
                "timestamp": item.get("timestamp"),
                "temperature": item.get("temperature"),
                "humidity": item.get("humidity"),
                "pressure": item.get("pressure"),
                "wind_speed": item.get("wind_speed"),
                "wind_direction": item.get("wind_direction"),
                "precipitation": item.get("precipitation"),
            }
            transformed.append(record)

        return transformed

    def _transform_observations_response(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform observations response."""
        observations = response_data.get("observations", [])
        transformed = []

        for obs in observations:
            record = {
                "station_id": obs.get("station_id"),
                "timestamp": obs.get("timestamp"),
                "temperature": obs.get("temperature"),
                "humidity": obs.get("humidity"),
                "pressure": obs.get("pressure"),
                "wind_speed": obs.get("wind_speed"),
                "wind_direction": obs.get("wind_direction"),
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


def create_weather_collector() -> BaseProviderCollector:
    """Create a weather data collector using the unified framework."""

    # Get settings for configuration
    settings = get_settings()
    api_key = getattr(settings, "weather_api_key", None)

    # Configure provider
    provider_config = ProviderConfig(
        name="weather",
        base_url="https://api.weather.gov/",
        api_key=api_key,
        rate_limit_requests_per_minute=1000,  # Weather APIs typically allow high rates
        rate_limit_burst_size=10,
        timeout_seconds=30.0,
        max_retries=3,
        user_agent="Aurum-EnergyTrading/1.0",
    )

    # Configure datasets
    dataset_configs = [
        DatasetConfig(
            dataset_id="weather_stations",
            endpoint_path="stations",
            data_format="json",
            pagination=True,
            pagination_param="cursor",
            pagination_size=500,
            date_field="timestamp",
            id_field="station_id",
        ),
        DatasetConfig(
            dataset_id="weather_observations",
            endpoint_path="observations",
            data_format="json",
            pagination=False,
            date_field="timestamp",
            id_field="station_id",
        ),
        DatasetConfig(
            dataset_id="weather_forecasts",
            endpoint_path="forecasts",
            data_format="json",
            pagination=False,
            date_field="timestamp",
            id_field="station_id",
        ),
    ]

    # Create data transformer
    data_transformer = WeatherDataTransformer()

    # Create and return collector
    return create_provider_collector(
        provider_name="weather",
        provider_config=provider_config,
        dataset_configs=dataset_configs,
        data_transformer=data_transformer,
    )


class WeatherUnifiedCollector(BaseProviderCollector):
    """Weather data collector implementation using the unified framework."""

    async def _execute_http_request(self, request: HttpRequest) -> HttpResponse:
        """Execute HTTP request using aiohttp for weather APIs."""
        try:
            from aurum.external.collect.base import HttpRequestError
            import aiohttp

            # Add weather API specific headers
            headers = request.headers.copy()
            if self.provider_config.api_key:
                headers["X-API-Key"] = self.provider_config.api_key

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=request.method,
                    url=request.url,
                    params=request.params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=request.timeout),
                ) as response:
                    # Weather APIs may return different content types
                    content_type = response.headers.get("content-type", "")

                    if "application/json" in content_type:
                        response_data = await response.json()
                    elif "application/geo+json" in content_type:
                        response_data = await response.json()
                    else:
                        response_data = await response.text()

                    return HttpResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        data=response_data,
                    )

        except Exception as e:
            raise HttpRequestError(f"Weather API request failed: {e}")


# Example usage for migrating existing weather collectors
async def migrate_weather_collector():
    """Example of how to migrate existing weather collectors."""

    # Create unified collector
    collector = create_weather_collector()

    # Collect station catalog
    await collector.collect_catalog()

    # Collect weather observations
    await collector.collect_observations("weather_observations")

    # Collect weather forecasts
    await collector.collect_observations("weather_forecasts")

    # The unified framework handles:
    # - Rate limiting (1000 requests/min for weather APIs)
    # - Authentication if required
    # - Retry logic with exponential backoff
    # - Checkpoint management for incremental updates
    # - Error handling and logging
    # - Data transformation for different response formats
    # - Kafka emission for downstream processing

    return collector
