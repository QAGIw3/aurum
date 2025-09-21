#!/usr/bin/env python3
"""
Generate canary monitoring configuration from data source catalogs.

This script creates canary monitoring configurations for all registered
data sources by analyzing their catalogs and creating appropriate
canary checks for API health monitoring.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.canary import CanaryConfig


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""

    name: str
    api_base_url: str
    api_key_env_var: str
    default_timeout_ms: int = 30000
    default_limit: int = 1
    response_format: str = "json"
    auth_method: str = "api_key"  # api_key, token, none

    # Canary-specific settings
    canary_enabled: bool = True
    canary_schedule: str = "*/15 * * * *"
    canary_alert_channels: List[str] = field(default_factory=lambda: ["email"])

    # Sample datasets for canary checks
    sample_datasets: List[Dict[str, str]] = field(default_factory=list)

    def __post_init__(self):
        if not self.sample_datasets:
            self.sample_datasets = [
                {
                    "name": f"{self.name}_sample_1",
                    "dataset": f"{self.name}_sample_dataset",
                    "endpoint_pattern": "/sample/endpoint/1",
                    "description": f"Sample canary for {self.name}"
                }
            ]


@dataclass
class GeneratedCanaryConfig:
    """Generated canary configuration."""

    name: str
    source: str
    dataset: str
    api_endpoint: str
    api_params: Dict[str, Any] = field(default_factory=dict)
    expected_response_format: str = "json"
    expected_fields: List[str] = field(default_factory=list)
    timeout_seconds: int = 30
    description: str = ""

    # Generated fields
    canary_topic: str = ""
    canary_schema_subject: str = ""
    canary_dlq_topic: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "api_endpoint": self.api_endpoint,
            "api_params": self.api_params,
            "expected_response_format": self.expected_response_format,
            "expected_fields": self.expected_fields,
            "timeout_seconds": self.timeout_seconds,
            "description": self.description,
            "canary_topic": self.canary_topic,
            "canary_schema_subject": self.canary_schema_subject,
            "canary_dlq_topic": self.canary_dlq_topic
        }


class CanaryConfigGenerator:
    """Generate canary configurations from data source catalogs."""

    def __init__(self):
        """Initialize generator."""
        self.data_sources = self._load_data_source_configs()

    def _load_data_source_configs(self) -> Dict[str, DataSourceConfig]:
        """Load data source configurations."""
        return {
            "eia": DataSourceConfig(
                name="eia",
                api_base_url="https://api.eia.gov/v2",
                api_key_env_var="EIA_API_KEY",
                default_timeout_ms=30000,
                sample_datasets=[
                    {
                        "name": "eia_electricity_canary",
                        "dataset": "electricity_prices",
                        "endpoint_pattern": "/electricity/retail-sales/data/",
                        "description": "EIA electricity prices canary"
                    },
                    {
                        "name": "eia_petroleum_canary",
                        "dataset": "petroleum_prices",
                        "endpoint_pattern": "/petroleum/pri/spt/data/",
                        "description": "EIA petroleum prices canary"
                    }
                ]
            ),
            "fred": DataSourceConfig(
                name="fred",
                api_base_url="https://api.stlouisfed.org/fred",
                api_key_env_var="FRED_API_KEY",
                default_timeout_ms=30000,
                sample_datasets=[
                    {
                        "name": "fred_unemployment_canary",
                        "dataset": "unemployment_rate",
                        "endpoint_pattern": "/series/observations",
                        "description": "FRED unemployment rate canary"
                    },
                    {
                        "name": "fred_gdp_canary",
                        "dataset": "gdp",
                        "endpoint_pattern": "/series/observations",
                        "description": "FRED GDP canary"
                    }
                ]
            ),
            "cpi": DataSourceConfig(
                name="cpi",
                api_base_url="https://api.stlouisfed.org/fred",
                api_key_env_var="FRED_API_KEY",  # CPI uses FRED API
                default_timeout_ms=30000,
                sample_datasets=[
                    {
                        "name": "cpi_all_items_canary",
                        "dataset": "cpi_all_items",
                        "endpoint_pattern": "/series/observations",
                        "description": "CPI All Items canary"
                    }
                ]
            ),
            "noaa": DataSourceConfig(
                name="noaa",
                api_base_url="https://www.ncei.noaa.gov/access/services/data/v1",
                api_key_env_var="NOAA_GHCND_TOKEN",
                default_timeout_ms=45000,  # NOAA can be slower
                auth_method="token",
                sample_datasets=[
                    {
                        "name": "noaa_weather_canary",
                        "dataset": "weather_daily",
                        "endpoint_pattern": "/",
                        "description": "NOAA weather data canary"
                    }
                ]
            ),
            "iso": DataSourceConfig(
                name="iso",
                api_base_url="",  # Varies by ISO
                api_key_env_var="",
                default_timeout_ms=30000,
                auth_method="none",
                sample_datasets=[
                    {
                        "name": "nyiso_lmp_canary",
                        "dataset": "nyiso_lmp",
                        "endpoint_pattern": "http://mis.nyiso.com/public/api/v1/lbmp/current",
                        "description": "NYISO LMP canary"
                    },
                    {
                        "name": "pjm_lmp_canary",
                        "dataset": "pjm_lmp",
                        "endpoint_pattern": "https://api.pjm.com/api/v1/lmp/current",
                        "description": "PJM LMP canary"
                    },
                    {
                        "name": "caiso_oasis_canary",
                        "dataset": "caiso_oasis",
                        "endpoint_pattern": "http://oasis.caiso.com/oasisapi/SingleZip",
                        "description": "CAISO OASIS canary"
                    }
                ]
            )
        }

    def generate_canary_configs(self) -> List[GeneratedCanaryConfig]:
        """Generate canary configurations for all data sources.

        Returns:
            List of generated canary configurations
        """
        configs = []

        for source_name, source_config in self.data_sources.items():
            if not source_config.canary_enabled:
                continue

            for sample_dataset in source_config.sample_datasets:
                config = self._generate_single_canary_config(
                    source_name,
                    source_config,
                    sample_dataset
                )
                configs.append(config)

        return configs

    def _generate_single_canary_config(
        self,
        source_name: str,
        source_config: DataSourceConfig,
        sample_dataset: Dict[str, str]
    ) -> GeneratedCanaryConfig:
        """Generate configuration for a single canary.

        Args:
            source_name: Name of the data source
            source_config: Data source configuration
            sample_dataset: Sample dataset information

        Returns:
            Generated canary configuration
        """
        # Build API endpoint
        if source_name == "iso":
            # ISO uses specific endpoints
            api_endpoint = sample_dataset["endpoint_pattern"]
        else:
            api_endpoint = f"{source_config.api_base_url}{sample_dataset['endpoint_pattern']}"

        # Build API parameters
        api_params = {
            "api_key": f"${{{source_config.api_key_env_var}:-demo_key}}",
            "limit": str(source_config.default_limit),
            "format": "json"
        }

        # Add source-specific parameters
        if source_name == "eia":
            api_params.update({
                "frequency": "monthly",
                "data[0]": "price",
                "facets[stateid][]": "CA",
                "start": "{{ data_interval_start | ds }}",
                "end": "{{ data_interval_start | ds }}"
            })
        elif source_name == "fred":
            api_params.update({
                "series_id": "UNRATE",  # Use unemployment as sample
                "observation_start": "{{ data_interval_start | ds }}",
                "observation_end": "{{ data_interval_start | ds }}"
            })
        elif source_name == "cpi":
            api_params.update({
                "series_id": "CPIAUCSL",  # CPI All Items
                "observation_start": "{{ data_interval_start | ds }}",
                "observation_end": "{{ data_interval_start | ds }}"
            })
        elif source_name == "noaa":
            api_params.update({
                "dataset": "daily-summaries",
                "stations": "USW00094728",
                "startDate": "{{ data_interval_start | ds }}",
                "endDate": "{{ data_interval_start | ds }}"
            })

        # Build expected fields based on source
        expected_fields = ["status", "message", "timestamp", "data"]
        if source_name == "fred":
            expected_fields.extend(["realtime_start", "realtime_end", "observations"])
        elif source_name == "eia":
            expected_fields.extend(["response", "data"])

        # Build topic and schema names
        canary_topic = f"aurum.canary.{source_name}.{sample_dataset['name']}.v1"
        canary_schema_subject = f"{canary_topic}-value"
        canary_dlq_topic = f"aurum.canary.{source_name}.dlq.v1"

        return GeneratedCanaryConfig(
            name=sample_dataset["name"],
            source=source_name,
            dataset=sample_dataset["dataset"],
            api_endpoint=api_endpoint,
            api_params=api_params,
            expected_response_format=source_config.response_format,
            expected_fields=expected_fields,
            timeout_seconds=source_config.default_timeout_ms // 1000,
            description=sample_dataset["description"],
            canary_topic=canary_topic,
            canary_schema_subject=canary_schema_subject,
            canary_dlq_topic=canary_dlq_topic
        )

    def save_canary_configs(self, configs: List[GeneratedCanaryConfig], output_path: Path) -> None:
        """Save canary configurations to file.

        Args:
            configs: Generated canary configurations
            output_path: Output file path
        """
        config_data = {
            "generated_at": str(datetime.now()),
            "total_canaries": len(configs),
            "by_source": {},
            "canaries": [config.to_dict() for config in configs]
        }

        # Count by source
        for config in configs:
            source = config.source
            config_data["by_source"][source] = config_data["by_source"].get(source, 0) + 1

        # Save to file
        output_path.write_text(json.dumps(config_data, indent=2) + "\n", encoding="utf-8")

        print(f"Generated {len(configs)} canary configurations")
        print(f"Saved to {output_path}")

        # Print summary
        print("\nCanary Summary by Source:")
        for source, count in config_data["by_source"].items():
            print(f"  {source}: {count} canaries")


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Generate canary monitoring configurations"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "config" / "canary_configs.json",
        help="Output file path"
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Generate configs for specific source only"
    )
    parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        default="json",
        help="Output format"
    )

    args = parser.parse_args(argv)

    try:
        generator = CanaryConfigGenerator()

        if args.source:
            if args.source not in generator.data_sources:
                print(f"Unknown source: {args.source}")
                return 1
            # Filter configs for specific source
            all_configs = generator.generate_canary_configs()
            configs = [c for c in all_configs if c.source == args.source]
        else:
            configs = generator.generate_canary_configs()

        if args.format == "json":
            generator.save_canary_configs(configs, args.output)
        else:
            print("YAML format not yet implemented")
            return 1

        return 0

    except Exception as e:
        print(f"Error generating canary configs: {e}")
        return 1


if __name__ == "__main__":
    from datetime import datetime
    sys.exit(main())
