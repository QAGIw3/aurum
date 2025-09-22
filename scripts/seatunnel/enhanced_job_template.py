"""Enhanced SeaTunnel job templating with dynamic partitioning and parameterization."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from jinja2 import Environment, FileSystemLoader, Template
from pydantic import BaseModel, Field

from ..data_contracts.schemas import get_schema_for_topic


class SeaTunnelJobConfig(BaseModel):
    """Configuration for SeaTunnel job generation."""

    # Basic job configuration
    job_name: str = Field(..., description="Name of the SeaTunnel job")
    source_type: str = Field(..., description="Type of source (http, kafka, database, etc.)")
    sink_type: str = Field(..., description="Type of sink (kafka, database, iceberg, etc.)")

    # Data source configuration
    source_config: Dict[str, Any] = Field(default_factory=dict, description="Source-specific configuration")
    sink_config: Dict[str, Any] = Field(default_factory=dict, description="Sink-specific configuration")

    # Partitioning configuration
    partitioning: Dict[str, Any] = Field(default_factory=dict, description="Partitioning strategy")
    partition_columns: List[str] = Field(default_factory=list, description="Columns to partition by")

    # Performance configuration
    parallelism: int = Field(default=1, description="Job parallelism")
    batch_size: int = Field(default=1000, description="Batch processing size")
    checkpoint_interval: int = Field(default=300000, description="Checkpoint interval in ms")

    # Schema configuration
    schema_subject: str = Field(..., description="Schema Registry subject")
    schema_version: str = Field(default="latest", description="Schema version")

    # Environment configuration
    environment_vars: Dict[str, str] = Field(default_factory=dict, description="Environment variables")
    secrets: List[str] = Field(default_factory=list, description="Secret keys to inject")

    # Transformation configuration
    transformations: List[Dict[str, Any]] = Field(default_factory=list, description="SQL transformations")


class EnhancedSeaTunnelTemplate:
    """Enhanced SeaTunnel job template with dynamic partitioning."""

    def __init__(self, template_dir: Optional[str] = None):
        """Initialize template engine."""
        self.template_dir = Path(template_dir or "/aurum/seatunnel/jobs/templates")
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
            autoescape=False
        )

        # Add custom filters
        self.env.filters['json_dumps'] = json.dumps
        self.env.filters['format_timestamp'] = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S')
        self.env.filters['partition_path'] = self._generate_partition_path

    def _generate_partition_path(self, partition_values: Dict[str, Any]) -> str:
        """Generate partition path from partition values."""
        path_parts = []
        for key, value in partition_values.items():
            if isinstance(value, datetime):
                if key.lower() in ['year', 'date']:
                    path_parts.append(f"{key}={value.strftime('%Y')}")
                elif key.lower() in ['month']:
                    path_parts.append(f"{key}={value.strftime('%Y-%m')}")
                elif key.lower() in ['day', 'date']:
                    path_parts.append(f"{key}={value.strftime('%Y-%m-%d')}")
                else:
                    path_parts.append(f"{key}={value}")
            else:
                path_parts.append(f"{key}={value}")
        return "/".join(path_parts)

    def _get_base_template(self, source_type: str, sink_type: str) -> str:
        """Get the base template name for source/sink combination."""
        template_map = {
            ("http", "kafka"): "http_to_kafka.conf.tmpl",
            ("kafka", "iceberg"): "kafka_to_iceberg.conf.tmpl",
            ("kafka", "database"): "kafka_to_database.conf.tmpl",
            ("database", "kafka"): "database_to_kafka.conf.tmpl",
            ("http", "iceberg"): "http_to_iceberg.conf.tmpl",
            ("multi", "kafka"): "multi_source_to_kafka.conf.tmpl",
        }

        return template_map.get((source_type, sink_type), "generic_job.conf.tmpl")

    def generate_partitioned_job(
        self,
        config: SeaTunnelJobConfig,
        partition_values: Dict[str, Any]
    ) -> str:
        """Generate a SeaTunnel job configuration with dynamic partitioning."""

        # Get base template
        template_name = self._get_base_template(config.source_type, config.sink_type)
        template = self.env.get_template(template_name)

        # Prepare template variables
        template_vars = {
            # Basic configuration
            "job_name": config.job_name,
            "job_mode": "BATCH",
            "parallelism": config.parallelism,

            # Source configuration
            "source_type": config.source_type,
            "source_config": config.source_config,

            # Sink configuration
            "sink_type": config.sink_type,
            "sink_config": config.sink_config,

            # Partitioning
            "partition_columns": config.partition_columns,
            "partition_values": partition_values,
            "partition_path": self._generate_partition_path(partition_values),

            # Performance
            "batch_size": config.batch_size,
            "checkpoint_interval": config.checkpoint_interval,

            # Schema
            "schema_subject": config.schema_subject,
            "schema_version": config.schema_version,

            # Environment
            "environment_vars": config.environment_vars,
            "secrets": config.secrets,

            # Transformations
            "transformations": config.transformations,

            # Generated metadata
            "generated_at": datetime.now(),
            "partition_metadata": {
                "partition_values": partition_values,
                "partition_columns": config.partition_columns,
                "total_partitions": len(config.partition_columns)
            }
        }

        return template.render(**template_vars)

    def generate_incremental_job(
        self,
        config: SeaTunnelJobConfig,
        watermark_value: Union[str, datetime],
        lookback_hours: int = 24
    ) -> str:
        """Generate an incremental SeaTunnel job with watermark-based filtering."""

        # Calculate time window
        if isinstance(watermark_value, datetime):
            start_time = watermark_value
            end_time = datetime.now()
        else:
            # For string-based watermarks, use provided lookback
            start_time = datetime.now() - timedelta(hours=lookback_hours)
            end_time = datetime.now()

        # Add watermark filter to source config
        enhanced_source_config = config.source_config.copy()
        enhanced_source_config.update({
            "watermark_column": "updated_at",  # Default column name
            "watermark_value": watermark_value.isoformat() if isinstance(watermark_value, datetime) else watermark_value,
            "lookback_hours": lookback_hours,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
        })

        # Create updated config
        incremental_config = config.copy()
        incremental_config.source_config = enhanced_source_config

        return self.generate_partitioned_job(
            incremental_config,
            partition_values={"incremental": "true"}
        )

    def generate_backfill_job(
        self,
        config: SeaTunnelJobConfig,
        start_date: datetime,
        end_date: datetime,
        chunk_size_days: int = 30
    ) -> List[str]:
        """Generate multiple SeaTunnel jobs for backfill processing."""

        jobs = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_size_days), end_date)

            # Generate partition values for this chunk
            partition_values = {
                "backfill": "true",
                "start_date": current_start.strftime('%Y-%m-%d'),
                "end_date": current_end.strftime('%Y-%m-%d'),
                "chunk_id": f"{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}"
            }

            # Add date-based partitioning if configured
            if "date" in config.partition_columns:
                partition_values["date"] = current_start

            # Generate job for this chunk
            job_config = self.generate_partitioned_job(config, partition_values)
            jobs.append(job_config)

            current_start = current_end + timedelta(days=1)

        return jobs

    def generate_multi_source_job(
        self,
        configs: List[SeaTunnelJobConfig],
        join_config: Dict[str, Any]
    ) -> str:
        """Generate a multi-source SeaTunnel job with joins."""

        # Use multi-source template
        template = self.env.get_template("multi_source_to_kafka.conf.tmpl")

        template_vars = {
            "job_name": "multi_source_aggregation",
            "sources": [
                {
                    "name": f"source_{i}",
                    "config": config.source_config,
                    "schema": config.schema_subject
                }
                for i, config in enumerate(configs)
            ],
            "join_config": join_config,
            "sink_config": configs[0].sink_config,  # Use first config's sink
            "parallelism": max(config.parallelism for config in configs),
            "generated_at": datetime.now()
        }

        return template.render(**template_vars)


class SeaTunnelJobGenerator:
    """High-level job generator for common data ingestion patterns."""

    def __init__(self, template_dir: Optional[str] = None):
        self.template = EnhancedSeaTunnelTemplate(template_dir)

    def generate_eia_series_job(
        self,
        series_id: str,
        topic: str,
        api_key_secret: str = "EIA_API_KEY",
        **kwargs
    ) -> str:
        """Generate SeaTunnel job for EIA time series data."""

        config = SeaTunnelJobConfig(
            job_name=f"eia_series_{series_id}_to_kafka",
            source_type="http",
            sink_type="kafka",
            source_config={
                "url": "https://api.eia.gov/v2/series",
                "method": "GET",
                "params": {
                    "api_key": "${EIA_API_KEY}",
                    "series_id": series_id,
                    "length": 5000,  # EIA default max 5,000 rows for JSON
                },
                "rate_limit_sleep_ms": 100,
                "retry": 5,
                "connection_timeout_ms": 15000,
            },
            sink_config={
                "bootstrap.servers": "${AURUM_KAFKA_BOOTSTRAP_SERVERS}",
                "topic": topic,
                "semantic": "AT_LEAST_ONCE",
                "format": "avro",
                "producer": {
                    "acks": "all",
                    "linger.ms": 500,
                    "batch.size": 32768,
                    "compression.type": "snappy"
                }
            },
            schema_subject=f"{topic}-value",
            environment_vars={
                "EIA_API_KEY": f"${{{api_key_secret}}}",
            },
            secrets=[api_key_secret],
            **kwargs
        )

        return self.template.generate_partitioned_job(config, {"series": series_id})

    def generate_fred_series_job(
        self,
        series_id: str,
        topic: str,
        api_key_secret: str = "FRED_API_KEY",
        **kwargs
    ) -> str:
        """Generate SeaTunnel job for FRED time series data."""

        config = SeaTunnelJobConfig(
            job_name=f"fred_series_{series_id}_to_kafka",
            source_type="http",
            sink_type="kafka",
            source_config={
                "url": "https://api.stlouisfed.org/fred/series/observations",
                "method": "GET",
                "params": {
                    "api_key": "${FRED_API_KEY}",
                    "series_id": series_id,
                    "file_type": "json",
                },
                "rate_limit_sleep_ms": 500,  # ~120 requests per minute community reported limit
                "retry": 3,
                "connection_timeout_ms": 30000,
            },
            sink_config={
                "bootstrap.servers": "${AURUM_KAFKA_BOOTSTRAP_SERVERS}",
                "topic": topic,
                "semantic": "AT_LEAST_ONCE",
                "format": "avro",
                "producer": {
                    "acks": "all",
                    "linger.ms": 300,
                    "batch.size": 16384,
                    "compression.type": "lz4"
                }
            },
            schema_subject=f"{topic}-value",
            environment_vars={
                "FRED_API_KEY": f"${{{api_key_secret}}}",
            },
            secrets=[api_key_secret],
            **kwargs
        )

        return self.template.generate_partitioned_job(config, {"series": series_id})

    def generate_noaa_weather_job(
        self,
        station_id: str,
        topic: str,
        api_token_secret: str = "NOAA_API_TOKEN",
        **kwargs
    ) -> str:
        """Generate SeaTunnel job for NOAA weather station data."""

        config = SeaTunnelJobConfig(
            job_name=f"noaa_station_{station_id}_to_kafka",
            source_type="http",
            sink_type="kafka",
            source_config={
                "url": "https://api.weather.gov/stations/{station_id}/observations",
                "method": "GET",
                "params": {
                    "token": "${NOAA_API_TOKEN}",
                },
                "rate_limit_sleep_ms": 1000,  # NOAA has strict rate limits
                "retry": 3,
                "connection_timeout_ms": 30000,
                "headers": {
                    "User-Agent": "Aurum-Data-Platform/1.0",
                    "Accept": "application/ld+json"
                }
            },
            sink_config={
                "bootstrap.servers": "${AURUM_KAFKA_BOOTSTRAP_SERVERS}",
                "topic": topic,
                "semantic": "AT_LEAST_ONCE",
                "format": "avro",
                "producer": {
                    "acks": "all",
                    "linger.ms": 1000,
                    "batch.size": 8192,
                    "compression.type": "gzip"
                }
            },
            schema_subject=f"{topic}-value",
            environment_vars={
                "NOAA_API_TOKEN": f"${{{api_token_secret}}}",
            },
            secrets=[api_token_secret],
            **kwargs
        )

        return self.template.generate_partitioned_job(config, {"station": station_id})

    def generate_world_bank_job(
        self,
        indicator: str,
        topic: str,
        **kwargs
    ) -> str:
        """Generate SeaTunnel job for World Bank data."""

        config = SeaTunnelJobConfig(
            job_name=f"world_bank_{indicator}_to_kafka",
            source_type="http",
            sink_type="kafka",
            source_config={
                "url": "https://api.worldbank.org/v2/country/all/indicator/{indicator}",
                "method": "GET",
                "params": {
                    "format": "json",
                    "per_page": 15000,  # Max 15,000 data points per call (including nulls)
                    "date": "2010:2024",  # Adjust as needed
                    "MRV": 1,  # Most recent value only to reduce data points
                },
                "rate_limit_sleep_ms": 500,
                "retry": 5,
                "connection_timeout_ms": 30000,
            },
            sink_config={
                "bootstrap.servers": "${AURUM_KAFKA_BOOTSTRAP_SERVERS}",
                "topic": topic,
                "semantic": "AT_LEAST_ONCE",
                "format": "avro",
                "producer": {
                    "acks": "all",
                    "linger.ms": 200,
                    "batch.size": 65536,
                    "compression.type": "snappy"
                }
            },
            schema_subject=f"{topic}-value",
            **kwargs
        )

        return self.template.generate_partitioned_job(config, {"indicator": indicator})

    def generate_incremental_job(
        self,
        config: SeaTunnelJobConfig,
        watermark_value: Union[str, datetime]
    ) -> str:
        """Generate incremental job with watermark filtering."""
        return self.template.generate_incremental_job(config, watermark_value)

    def generate_backfill_jobs(
        self,
        config: SeaTunnelJobConfig,
        start_date: datetime,
        end_date: datetime,
        chunk_size_days: int = 30
    ) -> List[str]:
        """Generate backfill jobs for historical data."""
        return self.template.generate_backfill_job(
            config, start_date, end_date, chunk_size_days
        )
