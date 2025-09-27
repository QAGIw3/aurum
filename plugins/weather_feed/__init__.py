"""Weather Feed Plugin for Aurum Platform.

This plugin provides weather data integration capabilities including:
- Weather data ingestion from external APIs
- Temperature and precipitation forecasting
- Weather impact analysis on energy markets
- Historical weather pattern analysis
"""

__version__ = "1.0.0"
__author__ = "Aurum Team"

# Plugin contract definition
PLUGIN_CONTRACT = {
    "name": "weather_feed",
    "version": __version__,
    "description": "Weather data integration and forecasting plugin",
    "author": __author__,
    "entry_point": "weather_feed",
    "security_level": "restricted",  # Can access network and filesystem
    "required_permissions": [
        "network:api_calls",
        "filesystem:read_write",
        "cache:read_write"
    ],
    "dependencies": [],
    "configuration_schema": {
        "api_key": {"type": "string", "required": True},
        "base_url": {"type": "string", "default": "https://api.weatherapi.com/v1"},
        "cache_ttl_hours": {"type": "integer", "default": 1},
        "forecast_days": {"type": "integer", "default": 7},
        "polling_interval_minutes": {"type": "integer", "default": 60}
    },
    "lifecycle_hooks": ["startup", "shutdown", "health_check"]
}
