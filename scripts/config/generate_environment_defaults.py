#!/usr/bin/env python3
"""Generate environment defaults from data source quotas configuration."""

import json
import argparse
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class EnvironmentConfig:
    """Environment configuration for a data source."""

    source_name: str
    requests_per_second: int
    requests_per_minute: int
    batch_size: int
    max_concurrent_jobs: int
    retry_backoff_seconds: int
    rate_limit_sleep_ms: int
    connection_timeout_seconds: int = 30
    read_timeout_seconds: int = 120
    max_retries: int = 5
    retry_strategy: str = "exponential_backoff"
    parallelism: int = 4

    def to_env_vars(self) -> Dict[str, str]:
        """Convert to environment variables."""
        prefix = f"AURUM_{self.source_name.upper()}"

        return {
            f"{prefix}_REQUESTS_PER_SECOND": str(self.requests_per_second),
            f"{prefix}_REQUESTS_PER_MINUTE": str(self.requests_per_minute),
            f"{prefix}_BATCH_SIZE": str(self.batch_size),
            f"{prefix}_MAX_CONCURRENT_JOBS": str(self.max_concurrent_jobs),
            f"{prefix}_RETRY_BACKOFF_SECONDS": str(self.retry_backoff_seconds),
            f"{prefix}_RATE_LIMIT_SLEEP_MS": str(self.rate_limit_sleep_ms),
            f"{prefix}_CONNECTION_TIMEOUT_SECONDS": str(self.connection_timeout_seconds),
            f"{prefix}_READ_TIMEOUT_SECONDS": str(self.read_timeout_seconds),
            f"{prefix}_MAX_RETRIES": str(self.max_retries),
            f"{prefix}_RETRY_STRATEGY": self.retry_strategy,
            f"{prefix}_PARALLELISM": str(self.parallelism)
        }


class EnvironmentDefaultsGenerator:
    """Generate environment defaults from quotas configuration."""

    def __init__(self, quotas_file: Path):
        """Initialize with quotas configuration file.

        Args:
            quotas_file: Path to quotas JSON file
        """
        self.quotas_file = quotas_file
        self.quotas_data = self._load_quotas()

    def _load_quotas(self) -> Dict[str, Any]:
        """Load quotas configuration from file.

        Returns:
            Quotas configuration dictionary
        """
        with open(self.quotas_file, 'r') as f:
            return json.load(f)

    def get_data_source_config(self, source_name: str, profile: str = "production") -> Optional[EnvironmentConfig]:
        """Get environment configuration for a data source.

        Args:
            source_name: Name of the data source
            profile: Deployment profile (development, staging, production, high_volume)

        Returns:
            Environment configuration or None if source not found
        """
        if source_name not in self.quotas_data["data_sources"]:
            return None

        source_config = self.quotas_data["data_sources"][source_name]
        optimal_settings = source_config["optimal_settings"]

        # Apply deployment profile multipliers
        multipliers = self.quotas_data["deployment_profiles"].get(profile, {}).get("multipliers", {})

        config = EnvironmentConfig(
            source_name=source_name,
            requests_per_second=int(optimal_settings["batch_size"] / max(optimal_settings.get("retry_backoff_seconds", 2), 1) * multipliers.get("requests_per_second", 1.0)),
            requests_per_minute=int(optimal_settings["batch_size"] * 60 / max(optimal_settings.get("retry_backoff_seconds", 2), 1) * multipliers.get("requests_per_minute", 1.0)),
            batch_size=int(optimal_settings["batch_size"] * multipliers.get("batch_size", 1.0)),
            max_concurrent_jobs=int(optimal_settings["max_concurrent_jobs"] * multipliers.get("max_concurrent_jobs", 1.0)),
            retry_backoff_seconds=int(optimal_settings.get("retry_backoff_seconds", 2)),
            rate_limit_sleep_ms=int(optimal_settings.get("rate_limit_sleep_ms", 100)),
            connection_timeout_seconds=optimal_settings.get("connection_timeout_seconds", 30),
            read_timeout_seconds=optimal_settings.get("read_timeout_seconds", 120),
            max_retries=optimal_settings.get("max_retries", 5),
            retry_strategy=optimal_settings.get("retry_strategy", "exponential_backoff"),
            parallelism=int(optimal_settings.get("parallelism", 4))
        )

        return config

    def generate_env_file(self, output_file: Path, sources: list, profile: str = "production") -> None:
        """Generate .env file with environment variables.

        Args:
            output_file: Path to output .env file
            sources: List of data sources to include
            profile: Deployment profile
        """
        env_vars = {}

        # Add global settings
        global_settings = self.quotas_data["global_settings"]
        env_vars.update({
            "AURUM_GLOBAL_DEFAULT_BATCH_SIZE": str(global_settings["default_batch_size"]),
            "AURUM_GLOBAL_DEFAULT_MAX_CONCURRENT_JOBS": str(global_settings["default_max_concurrent_jobs"]),
            "AURUM_GLOBAL_DEFAULT_RETRY_BACKOFF_SECONDS": str(global_settings["default_retry_backoff_seconds"]),
            "AURUM_GLOBAL_DEFAULT_RATE_LIMIT_SLEEP_MS": str(global_settings["default_rate_limit_sleep_ms"]),
            "AURUM_GLOBAL_THROUGHPUT_WARNING_THRESHOLD": str(global_settings["monitoring"]["throughput_warning_threshold"]),
            "AURUM_GLOBAL_ERROR_RATE_WARNING_THRESHOLD": str(global_settings["monitoring"]["error_rate_warning_threshold"]),
            "AURUM_GLOBAL_MEMORY_USAGE_WARNING_MB": str(global_settings["monitoring"]["memory_usage_warning_mb"]),
            "AURUM_GLOBAL_COST_PER_GB_THRESHOLD_USD": str(global_settings["monitoring"]["cost_per_gb_threshold_usd"])
        })

        # Add data source specific settings
        for source in sources:
            config = self.get_data_source_config(source, profile)
            if config:
                env_vars.update(config.to_env_vars())

        # Write .env file
        with open(output_file, 'w') as f:
            f.write("# Aurum Data Ingestion Environment Defaults\n")
            f.write(f"# Generated from quotas configuration: {profile} profile\n")
            f.write(f"# Generated on: {self._get_timestamp()}\n\n")

            for key, value in sorted(env_vars.items()):
                f.write(f"{key}={value}\n")

    def generate_seatunnel_config(self, output_dir: Path, sources: list, profile: str = "production") -> None:
        """Generate SeaTunnel configuration files.

        Args:
            output_dir: Directory to output configuration files
            sources: List of data sources to include
            profile: Deployment profile
        """
        output_dir.mkdir(exist_ok=True)

        for source in sources:
            config = self.get_data_source_config(source, profile)
            if not config:
                continue

            # Generate SeaTunnel job configuration
            seatunnel_config = {
                "env": {
                    "parallelism": config.parallelism,
                    "job.mode": "BATCH",
                    "checkpoint.interval": 60000,
                    "checkpoint.timeout": 60000
                },
                "source": [
                    {
                        "plugin_name": "Http",
                        "result_table_name": f"{source}_data",
                        "url": "${API_BASE_URL}",
                        "http_request_config": {
                            "url": "${API_BASE_URL}",
                            "method": "GET",
                            "headers": {
                                "Authorization": "Bearer ${API_KEY}",
                                "User-Agent": "Aurum/1.0"
                            },
                            "params": {
                                "limit": config.batch_size
                            },
                            "connection_timeout": config.connection_timeout_seconds * 1000,
                            "socket_timeout": config.read_timeout_seconds * 1000,
                            "retry": {
                                "max_retries": config.max_retries,
                                "retry_backoff_multiplier_ms": config.retry_backoff_seconds * 1000,
                                "retry_backoff_max_ms": config.retry_backoff_seconds * 5000,
                                "rate_limit_sleep_ms": config.rate_limit_sleep_ms
                            }
                        },
                        "schema": {
                            "fields": [
                                {"name": "id", "type": "string"},
                                {"name": "value", "type": "double"},
                                {"name": "timestamp", "type": "timestamp"}
                            ]
                        }
                    }
                ],
                "sink": [
                    {
                        "plugin_name": "Kafka",
                        "source_table_name": f"{source}_data",
                        "topic": f"aurum.{source}.data",
                        "bootstrap_servers": "${KAFKA_BOOTSTRAP_SERVERS}",
                        "kafka_config": {
                            "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
                            "value.serializer": "io.confluent.kafka.serializers.KafkaAvroSerializer",
                            "schema.registry.url": "${SCHEMA_REGISTRY_URL}",
                            "acks": "all",
                            "retries": 10,
                            "max.in.flight.requests.per.connection": 1,
                            "enable.idempotence": True,
                            "compression.type": "snappy"
                        },
                        "sink_columns": [
                            {"name": "id", "type": "string"},
                            {"name": "value", "type": "double"},
                            {"name": "timestamp", "type": "timestamp"}
                        ]
                    }
                ]
            }

            # Write configuration file
            config_file = output_dir / f"seatunnel_{source}_config.json"
            with open(config_file, 'w') as f:
                json.dump(seatunnel_config, f, indent=2)

    def generate_airflow_config(self, output_file: Path, sources: list, profile: str = "production") -> None:
        """Generate Airflow DAG configuration.

        Args:
            output_file: Path to output DAG configuration file
            sources: List of data sources to include
            profile: Deployment profile
        """
        airflow_config = {
            "dag_config": {
                "default_args": {
                    "owner": "data-ingestion",
                    "depends_on_past": False,
                    "start_date": "2024-01-01",
                    "email_on_failure": True,
                    "email_on_retry": False,
                    "retries": 2,
                    "retry_delay": 300  # 5 minutes
                },
                "schedule_interval": "0 * * * *",  # Hourly
                "catchup": False,
                "max_active_runs": 3,
                "max_active_tasks": 10
            },
            "data_sources": {}
        }

        for source in sources:
            config = self.get_data_source_config(source, profile)
            if config:
                airflow_config["data_sources"][source] = {
                    "pool": f"{source}_pool",
                    "pool_slots": config.max_concurrent_jobs,
                    "task_concurrency": config.parallelism,
                    "execution_timeout": 3600,  # 1 hour
                    "sla": 1800,  # 30 minutes
                    "priority_weight": 1,
                    "queue": "default"
                }

        with open(output_file, 'w') as f:
            json.dump(airflow_config, f, indent=2)

    def _get_timestamp(self) -> str:
        """Get current timestamp string."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Generate environment defaults from data source quotas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate environment file for all sources in production
  python generate_environment_defaults.py env --all --profile production

  # Generate SeaTunnel configs for specific sources
  python generate_environment_defaults.py seatunnel --sources eia fred --profile staging

  # Generate Airflow config for EIA only
  python generate_environment_defaults.py airflow --sources eia --output airflow_config.json

  # Generate all configs for development profile
  python generate_environment_defaults.py all --profile development
        """
    )

    parser.add_argument(
        "command",
        choices=["env", "seatunnel", "airflow", "all"],
        help="Type of configuration to generate"
    )

    parser.add_argument(
        "--quotas-file",
        default="config/data_source_quotas.json",
        help="Path to quotas configuration file"
    )

    parser.add_argument(
        "--sources",
        nargs="*",
        help="Specific data sources (default: all)"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Include all data sources"
    )

    parser.add_argument(
        "--profile",
        choices=["development", "staging", "production", "high_volume"],
        default="production",
        help="Deployment profile"
    )

    parser.add_argument(
        "--output", "-o",
        help="Output file or directory"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.all and not args.sources:
        print("Error: Must specify --all or --sources")
        return 1

    # Load quotas
    quotas_file = Path(args.quotas_file)
    if not quotas_file.exists():
        print(f"Error: Quotas file not found: {quotas_file}")
        return 1

    generator = EnvironmentDefaultsGenerator(quotas_file)

    # Determine sources
    all_sources = list(generator.quotas_data["data_sources"].keys())
    sources = args.sources or (all_sources if args.all else all_sources)

    try:
        if args.command == "env":
            output_file = args.output or f".env.{args.profile}"
            generator.generate_env_file(Path(output_file), sources, args.profile)
            print(f"Generated environment file: {output_file}")

        elif args.command == "seatunnel":
            output_dir = Path(args.output or f"config/seatunnel/{args.profile}")
            generator.generate_seatunnel_config(output_dir, sources, args.profile)
            print(f"Generated SeaTunnel configs in: {output_dir}")

        elif args.command == "airflow":
            output_file = args.output or f"config/airflow/dag_config_{args.profile}.json"
            generator.generate_airflow_config(Path(output_file), sources, args.profile)
            print(f"Generated Airflow config: {output_file}")

        elif args.command == "all":
            # Generate all configurations
            base_dir = Path(args.output or f"config/generated/{args.profile}")
            base_dir.mkdir(parents=True, exist_ok=True)

            env_file = base_dir / f".env.{args.profile}"
            generator.generate_env_file(env_file, sources, args.profile)
            print(f"Generated environment file: {env_file}")

            seatunnel_dir = base_dir / "seatunnel"
            generator.generate_seatunnel_config(seatunnel_dir, sources, args.profile)
            print(f"Generated SeaTunnel configs in: {seatunnel_dir}")

            airflow_file = base_dir / f"dag_config_{args.profile}.json"
            generator.generate_airflow_config(airflow_file, sources, args.profile)
            print(f"Generated Airflow config: {airflow_file}")

            print(f"\nAll configurations generated in: {base_dir}")

    except Exception as e:
        print(f"Error generating configuration: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
