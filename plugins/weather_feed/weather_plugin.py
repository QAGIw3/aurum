"""Weather Feed Plugin Implementation."""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager


class WeatherDataProvider:
    """Weather data provider interface."""

    def __init__(self, api_key: str, base_url: str = "https://api.weatherapi.com/v1"):
        """Initialize weather provider.

        Args:
            api_key: API key for weather service
            base_url: Base URL for weather API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def get_current_weather(self, location: str) -> Dict[str, Any]:
        """Get current weather for location.

        Args:
            location: Location (city, state or coordinates)

        Returns:
            Current weather data
        """
        if not self.session:
            self.session = aiohttp.ClientSession()

        url = f"{self.base_url}/current.json"
        params = {
            "key": self.api_key,
            "q": location
        }

        async with self.session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Weather API error: {response.status}")

    async def get_forecast(self, location: str, days: int = 7) -> Dict[str, Any]:
        """Get weather forecast for location.

        Args:
            location: Location (city, state or coordinates)
            days: Number of forecast days

        Returns:
            Weather forecast data
        """
        if not self.session:
            self.session = aiohttp.ClientSession()

        url = f"{self.base_url}/forecast.json"
        params = {
            "key": self.api_key,
            "q": location,
            "days": days
        }

        async with self.session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Weather API error: {response.status}")


class WeatherFeedPlugin:
    """Weather feed plugin for Aurum platform."""

    def __init__(self, configuration: Dict[str, Any]):
        """Initialize weather feed plugin.

        Args:
            configuration: Plugin configuration
        """
        self.config = configuration
        self.provider = WeatherDataProvider(
            api_key=configuration["api_key"],
            base_url=configuration.get("base_url", "https://api.weatherapi.com/v1")
        )
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()
        self.logger = logging.getLogger("weather_feed_plugin")

        # Plugin state
        self._running = False
        self._polling_task: Optional[asyncio.Task] = None

    async def startup(self, configuration: Dict[str, Any]) -> None:
        """Start the weather feed plugin.

        Args:
            configuration: Runtime configuration
        """
        self.logger.info("Starting weather feed plugin")
        self._running = True

        # Start background polling if configured
        polling_interval = configuration.get("polling_interval_minutes", 60)
        if polling_interval > 0:
            self._polling_task = asyncio.create_task(
                self._polling_loop(polling_interval * 60)
            )

        self.telemetry.info(
            "Weather feed plugin started",
            plugin="weather_feed",
            version="1.0.0"
        )

    async def shutdown(self, configuration: Dict[str, Any]) -> None:
        """Shutdown the weather feed plugin.

        Args:
            configuration: Runtime configuration
        """
        self.logger.info("Shutting down weather feed plugin")
        self._running = False

        # Stop polling task
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

        # Close provider session
        if self.provider.session:
            await self.provider.session.close()

        self.telemetry.info("Weather feed plugin shutdown complete")

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check.

        Returns:
            Health status information
        """
        try:
            # Test API connectivity
            async with self.provider:
                await self.provider.get_current_weather("London")

            return {
                "status": "ok",
                "message": "Weather API connection successful",
                "timestamp": datetime.utcnow().isoformat(),
                "api_available": True
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Health check failed: {str(e)}",
                "timestamp": datetime.utcnow().isoformat(),
                "api_available": False
            }

    async def process_data(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process weather data and return enhanced results.

        Args:
            input_data: Input data containing location and parameters

        Returns:
            Processed weather data with energy market insights
        """
        location = input_data.get("location", "New York")
        forecast_days = input_data.get("forecast_days", 3)
        include_energy_impact = input_data.get("include_energy_impact", True)

        try:
            # Get current weather and forecast
            async with self.provider:
                current_data = await self.provider.get_current_weather(location)
                forecast_data = await self.provider.get_forecast(location, forecast_days)

            # Process and enhance data
            result = {
                "location": location,
                "current": self._process_current_weather(current_data),
                "forecast": self._process_forecast(forecast_data),
                "energy_impact": {} if include_energy_impact else None,
                "metadata": {
                    "plugin_version": "1.0.0",
                    "data_source": "WeatherAPI",
                    "processed_at": datetime.utcnow().isoformat()
                }
            }

            # Calculate energy market impact if requested
            if include_energy_impact:
                result["energy_impact"] = await self._calculate_energy_impact(
                    location, result["current"], result["forecast"]
                )

            # Cache results
            cache_key = f"weather_data:{location}:{datetime.utcnow().strftime('%Y%m%d_%H')}"
            await self.cache_manager.set(
                cache_key,
                result,
                ttl_seconds=self.config.get("cache_ttl_hours", 1) * 3600
            )

            self.telemetry.info(
                "Weather data processed",
                location=location,
                forecast_days=forecast_days
            )

            return result

        except Exception as e:
            self.telemetry.error("Weather data processing failed", error=str(e))
            raise

    def _process_current_weather(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process current weather data."""
        current = data.get("current", {})
        location = data.get("location", {})

        return {
            "temperature_c": current.get("temp_c"),
            "temperature_f": current.get("temp_f"),
            "humidity": current.get("humidity"),
            "wind_speed_mph": current.get("wind_mph"),
            "wind_direction": current.get("wind_dir"),
            "pressure_mb": current.get("pressure_mb"),
            "precipitation_mm": current.get("precip_mm"),
            "condition": current.get("condition", {}).get("text"),
            "location": location.get("name"),
            "region": location.get("region"),
            "country": location.get("country"),
            "timestamp": current.get("last_updated")
        }

    def _process_forecast(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process forecast data."""
        forecast = data.get("forecast", {})
        forecast_days = forecast.get("forecastday", [])

        processed_forecast = []
        for day in forecast_days:
            processed_forecast.append({
                "date": day.get("date"),
                "max_temp_c": day.get("day", {}).get("maxtemp_c"),
                "min_temp_c": day.get("day", {}).get("mintemp_c"),
                "avg_humidity": day.get("day", {}).get("avghumidity"),
                "max_wind_mph": day.get("day", {}).get("maxwind_mph"),
                "total_precip_mm": day.get("day", {}).get("totalprecip_mm"),
                "condition": day.get("day", {}).get("condition", {}).get("text"),
                "chance_of_rain": day.get("day", {}).get("daily_chance_of_rain")
            })

        return processed_forecast

    async def _calculate_energy_impact(
        self,
        location: str,
        current: Dict[str, Any],
        forecast: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate energy market impact of weather conditions.

        Args:
            location: Location name
            current: Current weather data
            forecast: Weather forecast data

        Returns:
            Energy impact analysis
        """
        # Simplified energy impact calculation
        # In a real implementation, this would use more sophisticated models

        current_temp = current.get("temperature_c", 20)
        avg_humidity = current.get("humidity", 50)

        # Temperature impact on load
        temp_factor = 1.0
        if current_temp > 25:  # Hot weather increases cooling load
            temp_factor = 1.0 + (current_temp - 25) * 0.02
        elif current_temp < 15:  # Cold weather increases heating load
            temp_factor = 1.0 + (15 - current_temp) * 0.02

        # Wind impact on renewable generation
        wind_speed = current.get("wind_speed_mph", 10)
        wind_factor = min(wind_speed / 15, 1.5)  # Wind generation increases with speed

        # Precipitation impact on hydro and solar
        precip = current.get("precipitation_mm", 0)
        hydro_factor = min(precip * 0.1, 1.2)  # Rain increases hydro generation
        solar_factor = max(0.3, 1.0 - precip * 0.05)  # Rain decreases solar generation

        return {
            "load_impact_factor": temp_factor,
            "wind_generation_factor": wind_factor,
            "hydro_generation_factor": hydro_factor,
            "solar_generation_factor": solar_factor,
            "estimated_load_change_percent": (temp_factor - 1.0) * 100,
            "estimated_renewable_change_percent": (
                (wind_factor + hydro_factor + solar_factor - 3.0) / 3.0 * 100
            ),
            "risk_level": self._calculate_weather_risk_level(current, forecast)
        }

    def _calculate_weather_risk_level(
        self,
        current: Dict[str, Any],
        forecast: List[Dict[str, Any]]
    ) -> str:
        """Calculate weather risk level for energy markets."""
        # Check for extreme weather conditions
        temp = current.get("temperature_c", 20)
        wind = current.get("wind_speed_mph", 10)
        precip = current.get("precipitation_mm", 0)

        # Check forecast for severe weather
        max_temp = max([day.get("max_temp_c", 20) for day in forecast] + [temp])
        min_temp = min([day.get("min_temp_c", 20) for day in forecast] + [temp])
        max_wind = max([day.get("max_wind_mph", 10) for day in forecast] + [wind])
        max_precip = max([day.get("total_precip_mm", 0) for day in forecast] + [precip])

        if max_temp > 35 or min_temp < -10 or max_wind > 50 or max_precip > 100:
            return "high"
        elif max_temp > 30 or min_temp < 0 or max_wind > 30 or max_precip > 50:
            return "medium"
        else:
            return "low"

    async def _polling_loop(self, interval_seconds: int) -> None:
        """Background polling loop for weather data updates."""
        while self._running:
            try:
                # Poll configured locations
                locations = self.config.get("polling_locations", ["New York", "California"])

                for location in locations:
                    try:
                        # Check if we need fresh data
                        cache_key = f"weather_data:{location}:{datetime.utcnow().strftime('%Y%m%d_%H')}"
                        cached = await self.cache_manager.get(cache_key)

                        if not cached:
                            # Get fresh data
                            await self.process_data({
                                "location": location,
                                "include_energy_impact": True
                            })

                            self.logger.info(f"Polled weather data for {location}")

                    except Exception as e:
                        self.telemetry.error(
                            "Weather polling failed",
                            location=location,
                            error=str(e)
                        )

                # Wait for next polling interval
                await asyncio.sleep(interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Weather polling loop failed", error=str(e))
                await asyncio.sleep(60)  # Wait before retrying

    async def get_metadata(self) -> Dict[str, Any]:
        """Get plugin metadata."""
        return {
            "name": "weather_feed",
            "version": "1.0.0",
            "description": "Weather data integration and forecasting plugin",
            "capabilities": [
                "current_weather",
                "weather_forecast",
                "energy_impact_analysis",
                "weather_risk_assessment"
            ],
            "configuration": self.config,
            "status": "running" if self._running else "stopped"
        }

    async def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """Validate input data for processing."""
        required_fields = ["location"]

        for field in required_fields:
            if field not in input_data:
                raise ValueError(f"Missing required field: {field}")

        # Validate location format
        location = input_data["location"]
        if not isinstance(location, str) or len(location.strip()) == 0:
            raise ValueError("Invalid location format")

        return True

    async def transform_output(self, output_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform output data for consumption."""
        # Add additional metadata and formatting
        output_data["processed_by"] = "weather_feed_plugin"
        output_data["processing_timestamp"] = datetime.utcnow().isoformat()

        # Convert to pandas DataFrame for easier analysis
        if "forecast" in output_data:
            forecast_df = pd.DataFrame(output_data["forecast"])
            output_data["forecast_dataframe"] = forecast_df.to_dict("records")

        return output_data


# Plugin instance factory
def create_weather_feed_plugin(configuration: Dict[str, Any]) -> WeatherFeedPlugin:
    """Create weather feed plugin instance."""
    return WeatherFeedPlugin(configuration)
