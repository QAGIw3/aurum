"""Configuration management for performance optimization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any

from ..core import AurumSettings


@dataclass
class OptimizedIngestionSettings:
    """Settings for optimized data ingestion."""

    # Batching configuration
    batch_size: int = 1000
    batch_timeout: float = 5.0
    batch_flush_interval: float = 1.0

    # Caching configuration
    enable_caching: bool = True
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000

    # Connection pooling
    enable_connection_pooling: bool = True
    min_connections: int = 5
    max_connections: int = 20

    # Partitioning
    enable_partitioning: bool = True
    partition_interval: str = "month"
    retention_days: int = 365

    # Performance monitoring
    enable_metrics: bool = True
    metrics_interval: int = 60

    # Resource limits
    max_concurrent_batches: int = 10
    max_memory_usage_mb: int = 2048

    @classmethod
    def from_aurum_settings(cls, settings: AurumSettings) -> OptimizedIngestionSettings:
        """Create settings from AurumSettings."""
        return cls(
            batch_size=settings.api.get("batch_size", 1000),
            batch_timeout=settings.api.get("batch_timeout", 5.0),
            batch_flush_interval=settings.api.get("batch_flush_interval", 1.0),
            enable_caching=settings.redis.mode != "disabled" if hasattr(settings, "redis") else True,
            cache_ttl_seconds=settings.redis.ttl_seconds if hasattr(settings, "redis") else 300,
            enable_connection_pooling=settings.database is not None,
            min_connections=settings.api.get("min_connections", 5),
            max_connections=settings.api.get("max_connections", 20),
            enable_partitioning=settings.api.get("enable_partitioning", True),
            partition_interval=settings.api.get("partition_interval", "month"),
            retention_days=settings.api.get("retention_days", 365),
            enable_metrics=settings.api.get("enable_metrics", True),
            metrics_interval=settings.api.get("metrics_interval", 60),
            max_concurrent_batches=settings.api.get("max_concurrent_batches", 10),
            max_memory_usage_mb=settings.api.get("max_memory_usage_mb", 2048),
        )


@dataclass
class DatabaseOptimizationSettings:
    """Settings for database performance optimization."""

    # TimescaleDB settings
    timescale_compression_enabled: bool = True
    timescale_compression_delay: int = 7  # days
    timescale_retention_policy: str = "30 days"

    # ClickHouse settings
    clickhouse_merge_enabled: bool = True
    clickhouse_merge_interval: int = 3600  # seconds
    clickhouse_max_parts_per_partition: int = 10

    # Trino/Iceberg settings
    iceberg_target_file_size_mb: int = 128
    iceberg_min_rows_per_file: int = 10000

    # Connection settings
    connection_timeout: float = 30.0
    query_timeout: float = 300.0

    # Resource settings
    max_parallel_queries: int = 5
    memory_limit_mb: int = 4096

    @classmethod
    def from_aurum_settings(cls, settings: AurumSettings) -> DatabaseOptimizationSettings:
        """Create settings from AurumSettings."""
        return cls(
            timescale_compression_enabled=settings.api.get("timescale_compression", True),
            timescale_compression_delay=settings.api.get("timescale_compression_delay", 7),
            timescale_retention_policy=settings.api.get("timescale_retention", "30 days"),
            clickhouse_merge_enabled=settings.api.get("clickhouse_merge", True),
            clickhouse_merge_interval=settings.api.get("clickhouse_merge_interval", 3600),
            clickhouse_max_parts_per_partition=settings.api.get("clickhouse_max_parts", 10),
            iceberg_target_file_size_mb=settings.api.get("iceberg_file_size", 128),
            iceberg_min_rows_per_file=settings.api.get("iceberg_min_rows", 10000),
            connection_timeout=settings.api.get("connection_timeout", 30.0),
            query_timeout=settings.api.get("query_timeout", 300.0),
            max_parallel_queries=settings.api.get("max_parallel_queries", 5),
            memory_limit_mb=settings.api.get("memory_limit_mb", 4096),
        )


@dataclass
class PerformanceMonitoringSettings:
    """Settings for performance monitoring and alerting."""

    # Monitoring intervals
    health_check_interval: int = 30  # seconds
    metrics_collection_interval: int = 60  # seconds
    alert_check_interval: int = 60  # seconds

    # Thresholds
    high_latency_threshold: float = 5.0  # seconds
    low_cache_hit_rate: float = 0.8  # 80%
    high_error_rate: float = 0.05  # 5%
    high_memory_usage: float = 0.9  # 90%

    # Alerting
    enable_alerting: bool = True
    alert_webhook_url: str | None = None
    alert_email_recipients: list[str] = field(default_factory=list)

    # Metrics retention
    metrics_retention_days: int = 30

    @classmethod
    def from_aurum_settings(cls, settings: AurumSettings) -> PerformanceMonitoringSettings:
        """Create settings from AurumSettings."""
        return cls(
            health_check_interval=settings.api.get("health_check_interval", 30),
            metrics_collection_interval=settings.api.get("metrics_interval", 60),
            alert_check_interval=settings.api.get("alert_interval", 60),
            high_latency_threshold=settings.api.get("latency_threshold", 5.0),
            low_cache_hit_rate=settings.api.get("cache_hit_rate_threshold", 0.8),
            high_error_rate=settings.api.get("error_rate_threshold", 0.05),
            high_memory_usage=settings.api.get("memory_usage_threshold", 0.9),
            enable_alerting=settings.api.get("enable_alerting", True),
            alert_webhook_url=settings.api.get("alert_webhook_url"),
            alert_email_recipients=settings.api.get("alert_emails", []),
            metrics_retention_days=settings.api.get("metrics_retention", 30),
        )


@dataclass
class GlobalPerformanceSettings:
    """Global settings for all performance optimizations."""

    ingestion: OptimizedIngestionSettings
    database: DatabaseOptimizationSettings
    monitoring: PerformanceMonitoringSettings

    @classmethod
    def from_aurum_settings(cls, settings: AurumSettings) -> GlobalPerformanceSettings:
        """Create settings from AurumSettings."""
        return cls(
            ingestion=OptimizedIngestionSettings.from_aurum_settings(settings),
            database=DatabaseOptimizationSettings.from_aurum_settings(settings),
            monitoring=PerformanceMonitoringSettings.from_aurum_settings(settings),
        )

    def to_environment_variables(self) -> Dict[str, str]:
        """Convert settings to environment variables."""
        env_vars = {}

        # Ingestion settings
        env_vars.update({
            "AURUM_BATCH_SIZE": str(self.ingestion.batch_size),
            "AURUM_BATCH_TIMEOUT": str(self.ingestion.batch_timeout),
            "AURUM_BATCH_FLUSH_INTERVAL": str(self.ingestion.batch_flush_interval),
            "AURUM_ENABLE_CACHING": str(self.ingestion.enable_caching),
            "AURUM_CACHE_TTL_SECONDS": str(self.ingestion.cache_ttl_seconds),
            "AURUM_ENABLE_CONNECTION_POOLING": str(self.ingestion.enable_connection_pooling),
            "AURUM_MIN_CONNECTIONS": str(self.ingestion.min_connections),
            "AURUM_MAX_CONNECTIONS": str(self.ingestion.max_connections),
            "AURUM_ENABLE_PARTITIONING": str(self.ingestion.enable_partitioning),
            "AURUM_PARTITION_INTERVAL": self.ingestion.partition_interval,
            "AURUM_RETENTION_DAYS": str(self.ingestion.retention_days),
            "AURUM_MAX_CONCURRENT_BATCHES": str(self.ingestion.max_concurrent_batches),
            "AURUM_MAX_MEMORY_USAGE_MB": str(self.ingestion.max_memory_usage_mb),
        })

        # Database settings
        env_vars.update({
            "AURUM_TIMESCALE_COMPRESSION": str(self.database.timescale_compression_enabled),
            "AURUM_TIMESCALE_COMPRESSION_DELAY": str(self.database.timescale_compression_delay),
            "AURUM_TIMESCALE_RETENTION": self.database.timescale_retention_policy,
            "AURUM_CLICKHOUSE_MERGE": str(self.database.clickhouse_merge_enabled),
            "AURUM_CLICKHOUSE_MERGE_INTERVAL": str(self.database.clickhouse_merge_interval),
            "AURUM_ICEBERG_FILE_SIZE_MB": str(self.database.iceberg_target_file_size_mb),
            "AURUM_ICEBERG_MIN_ROWS": str(self.database.iceberg_min_rows_per_file),
            "AURUM_CONNECTION_TIMEOUT": str(self.database.connection_timeout),
            "AURUM_QUERY_TIMEOUT": str(self.database.query_timeout),
            "AURUM_MAX_PARALLEL_QUERIES": str(self.database.max_parallel_queries),
            "AURUM_MEMORY_LIMIT_MB": str(self.database.memory_limit_mb),
        })

        # Monitoring settings
        env_vars.update({
            "AURUM_HEALTH_CHECK_INTERVAL": str(self.monitoring.health_check_interval),
            "AURUM_METRICS_INTERVAL": str(self.monitoring.metrics_collection_interval),
            "AURUM_ALERT_INTERVAL": str(self.monitoring.alert_check_interval),
            "AURUM_LATENCY_THRESHOLD": str(self.monitoring.high_latency_threshold),
            "AURUM_CACHE_HIT_RATE_THRESHOLD": str(self.monitoring.low_cache_hit_rate),
            "AURUM_ERROR_RATE_THRESHOLD": str(self.monitoring.high_error_rate),
            "AURUM_MEMORY_USAGE_THRESHOLD": str(self.monitoring.high_memory_usage),
            "AURUM_ENABLE_ALERTING": str(self.monitoring.enable_alerting),
            "AURUM_METRICS_RETENTION_DAYS": str(self.monitoring.metrics_retention_days),
        })

        if self.monitoring.alert_webhook_url:
            env_vars["AURUM_ALERT_WEBHOOK_URL"] = self.monitoring.alert_webhook_url

        if self.monitoring.alert_email_recipients:
            env_vars["AURUM_ALERT_EMAILS"] = ",".join(self.monitoring.alert_email_recipients)

        return env_vars


def create_performance_config_from_env() -> GlobalPerformanceSettings:
    """Create performance configuration from environment variables."""
    ingestion = OptimizedIngestionSettings(
        batch_size=int(os.getenv("AURUM_BATCH_SIZE", "1000")),
        batch_timeout=float(os.getenv("AURUM_BATCH_TIMEOUT", "5.0")),
        batch_flush_interval=float(os.getenv("AURUM_BATCH_FLUSH_INTERVAL", "1.0")),
        enable_caching=os.getenv("AURUM_ENABLE_CACHING", "true").lower() == "true",
        cache_ttl_seconds=int(os.getenv("AURUM_CACHE_TTL_SECONDS", "300")),
        enable_connection_pooling=os.getenv("AURUM_ENABLE_CONNECTION_POOLING", "true").lower() == "true",
        min_connections=int(os.getenv("AURUM_MIN_CONNECTIONS", "5")),
        max_connections=int(os.getenv("AURUM_MAX_CONNECTIONS", "20")),
        enable_partitioning=os.getenv("AURUM_ENABLE_PARTITIONING", "true").lower() == "true",
        partition_interval=os.getenv("AURUM_PARTITION_INTERVAL", "month"),
        retention_days=int(os.getenv("AURUM_RETENTION_DAYS", "365")),
        max_concurrent_batches=int(os.getenv("AURUM_MAX_CONCURRENT_BATCHES", "10")),
        max_memory_usage_mb=int(os.getenv("AURUM_MAX_MEMORY_USAGE_MB", "2048")),
    )

    database = DatabaseOptimizationSettings(
        timescale_compression_enabled=os.getenv("AURUM_TIMESCALE_COMPRESSION", "true").lower() == "true",
        timescale_compression_delay=int(os.getenv("AURUM_TIMESCALE_COMPRESSION_DELAY", "7")),
        timescale_retention_policy=os.getenv("AURUM_TIMESCALE_RETENTION", "30 days"),
        clickhouse_merge_enabled=os.getenv("AURUM_CLICKHOUSE_MERGE", "true").lower() == "true",
        clickhouse_merge_interval=int(os.getenv("AURUM_CLICKHOUSE_MERGE_INTERVAL", "3600")),
        iceberg_target_file_size_mb=int(os.getenv("AURUM_ICEBERG_FILE_SIZE_MB", "128")),
        iceberg_min_rows_per_file=int(os.getenv("AURUM_ICEBERG_MIN_ROWS", "10000")),
        connection_timeout=float(os.getenv("AURUM_CONNECTION_TIMEOUT", "30.0")),
        query_timeout=float(os.getenv("AURUM_QUERY_TIMEOUT", "300.0")),
        max_parallel_queries=int(os.getenv("AURUM_MAX_PARALLEL_QUERIES", "5")),
        memory_limit_mb=int(os.getenv("AURUM_MEMORY_LIMIT_MB", "4096")),
    )

    monitoring = PerformanceMonitoringSettings(
        health_check_interval=int(os.getenv("AURUM_HEALTH_CHECK_INTERVAL", "30")),
        metrics_collection_interval=int(os.getenv("AURUM_METRICS_INTERVAL", "60")),
        alert_check_interval=int(os.getenv("AURUM_ALERT_INTERVAL", "60")),
        high_latency_threshold=float(os.getenv("AURUM_LATENCY_THRESHOLD", "5.0")),
        low_cache_hit_rate=float(os.getenv("AURUM_CACHE_HIT_RATE_THRESHOLD", "0.8")),
        high_error_rate=float(os.getenv("AURUM_ERROR_RATE_THRESHOLD", "0.05")),
        high_memory_usage=float(os.getenv("AURUM_MEMORY_USAGE_THRESHOLD", "0.9")),
        enable_alerting=os.getenv("AURUM_ENABLE_ALERTING", "true").lower() == "true",
        alert_webhook_url=os.getenv("AURUM_ALERT_WEBHOOK_URL"),
        alert_email_recipients=os.getenv("AURUM_ALERT_EMAILS", "").split(",") if os.getenv("AURUM_ALERT_EMAILS") else [],
        metrics_retention_days=int(os.getenv("AURUM_METRICS_RETENTION_DAYS", "30")),
    )

    return GlobalPerformanceSettings(
        ingestion=ingestion,
        database=database,
        monitoring=monitoring,
    )
