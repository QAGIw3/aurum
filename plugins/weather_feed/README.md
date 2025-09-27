# Weather Feed Plugin

A plugin for the Aurum platform that provides weather data integration and energy market impact analysis.

## Features

- **Current Weather Data**: Retrieve real-time weather conditions for any location
- **Weather Forecasting**: Get multi-day weather forecasts with detailed metrics
- **Energy Impact Analysis**: Calculate how weather conditions affect energy demand and renewable generation
- **Risk Assessment**: Assess weather-related risks to energy markets
- **Background Polling**: Automatic weather data updates for configured locations
- **Caching**: Intelligent caching to reduce API calls and improve performance

## Configuration

The plugin requires the following configuration:

```json
{
  "api_key": "your_weatherapi_key_here",
  "base_url": "https://api.weatherapi.com/v1",
  "cache_ttl_hours": 1,
  "forecast_days": 7,
  "polling_interval_minutes": 60,
  "polling_locations": ["New York", "California", "Texas"]
}
```

### Configuration Parameters

- `api_key`: WeatherAPI.com API key (required)
- `base_url`: Base URL for the weather API (optional, defaults to WeatherAPI)
- `cache_ttl_hours`: How long to cache weather data (optional, default: 1 hour)
- `forecast_days`: Number of forecast days to retrieve (optional, default: 7)
- `polling_interval_minutes`: Background polling interval (optional, default: 60 minutes)
- `polling_locations`: List of locations to poll automatically (optional)

## Usage

### Basic Weather Data

```python
from aurum.api.services.plugin_system_service import get_plugin_system_service

# Get plugin service
plugin_service = get_plugin_system_service()

# Load weather plugin
instance_id = await plugin_service.load_plugin(
    "weather_feed",
    tenant_id="your_tenant",
    configuration={
        "api_key": "your_api_key",
        "polling_locations": ["New York", "Los Angeles"]
    }
)

# Get current weather
weather_data = await plugin_service.execute_plugin_method(
    instance_id,
    "process_data",
    {
        "location": "New York",
        "include_energy_impact": True
    }
)

print(weather_data)
```

### Energy Impact Analysis

```python
# Get weather data with energy impact analysis
result = await plugin_service.execute_plugin_method(
    instance_id,
    "process_data",
    {
        "location": "California",
        "forecast_days": 5,
        "include_energy_impact": True
    }
)

# Access energy impact data
energy_impact = result["energy_impact"]
print(f"Load impact factor: {energy_impact['load_impact_factor']}")
print(f"Estimated load change: {energy_impact['estimated_load_change_percent']}%")
print(f"Weather risk level: {energy_impact['risk_level']}")
```

### Plugin Lifecycle Management

```python
# Check plugin health
health = await plugin_service.get_plugin_health(instance_id)
print(f"Plugin status: {health['status']}")

# Get plugin metadata
metadata = await plugin_service.execute_plugin_method(instance_id, "get_metadata", {})
print(f"Plugin version: {metadata['version']}")

# Unload plugin when done
await plugin_service.unload_plugin(instance_id)
```

## Data Output Format

### Current Weather Data
```json
{
  "location": "New York",
  "current": {
    "temperature_c": 22.5,
    "temperature_f": 72.5,
    "humidity": 65,
    "wind_speed_mph": 12.3,
    "wind_direction": "NW",
    "pressure_mb": 1013,
    "precipitation_mm": 0.0,
    "condition": "Partly cloudy",
    "location": "New York",
    "region": "New York",
    "country": "United States",
    "timestamp": "2024-01-15 14:30"
  },
  "forecast": [...],
  "energy_impact": {
    "load_impact_factor": 1.0,
    "wind_generation_factor": 0.82,
    "hydro_generation_factor": 1.0,
    "solar_generation_factor": 1.0,
    "estimated_load_change_percent": 0.0,
    "estimated_renewable_change_percent": -6.0,
    "risk_level": "low"
  },
  "metadata": {
    "plugin_version": "1.0.0",
    "data_source": "WeatherAPI",
    "processed_at": "2024-01-15T14:30:00Z"
  }
}
```

## Security

This plugin operates at the **restricted** security level, which means:

- Network access to external APIs
- Filesystem read/write access for caching
- Cache read/write access for data storage
- No database access
- No system-level operations

## Installation

1. Install the plugin package:
```bash
pip install aurum-weather-feed-plugin
```

2. The plugin will be automatically discovered by the Aurum plugin system via the `aurum.plugins` entry point group.

## Development

To develop and test the plugin:

1. Clone the repository
2. Install development dependencies:
```bash
pip install -e ".[dev]"
```

3. Run tests:
```bash
pytest tests/
```

4. Install the plugin in development mode:
```bash
pip install -e .
```

## API Reference

### Methods

- `process_data(input_data)`: Process weather data for a location
- `get_metadata()`: Get plugin metadata and capabilities
- `validate_input(input_data)`: Validate input data
- `transform_output(output_data)`: Transform output data
- `health_check()`: Check plugin health status

### Lifecycle Hooks

- `startup(configuration)`: Initialize plugin and start background tasks
- `shutdown(configuration)`: Clean shutdown and resource cleanup
- `health_check()`: Health monitoring and diagnostics

## Troubleshooting

### Common Issues

1. **API Key Issues**: Ensure your WeatherAPI.com API key is valid and has sufficient quota
2. **Network Connectivity**: Check firewall and network access to api.weatherapi.com
3. **Cache Issues**: Clear plugin cache if experiencing stale data

### Debug Logging

Enable debug logging to troubleshoot issues:

```python
import logging
logging.getLogger("weather_feed_plugin").setLevel(logging.DEBUG)
```

## Contributing

Contributions are welcome! Please see the main Aurum platform documentation for plugin development guidelines.

## License

MIT License - see LICENSE file for details.
