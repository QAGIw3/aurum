"""Unified configuration system using pydantic-settings.

Supports layered sources: env > .env > defaults
Frozen at process start for consistency.
"""
from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisMode(str, Enum):
    """Redis deployment modes."""
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


class DataBackendType(str, Enum):
    """Supported data backend engines."""
    TRINO = "trino"
    CLICKHOUSE = "clickhouse"
    TIMESCALE = "timescale"


class DatabaseSettings(BaseSettings):
    """Database connection settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="AURUM_",
        env_file=".env",
        case_sensitive=False,
    )
    
    # Timescale/PostgreSQL
    timescale_host: str = Field(default="localhost", description="Timescale host")
    timescale_port: int = Field(default=5432, description="Timescale port")
    timescale_user: str = Field(default="aurum", description="Timescale user")
    timescale_password: str = Field(default="aurum", description="Timescale password")
    timescale_database: str = Field(default="timeseries", description="Timescale database")
    
    # Postgres (metadata)
    postgres_host: str = Field(default="localhost", description="Postgres host")
    postgres_port: int = Field(default=5432, description="Postgres port")
    postgres_user: str = Field(default="aurum", description="Postgres user")
    postgres_password: str = Field(default="aurum", description="Postgres password")
    postgres_database: str = Field(default="aurum", description="Postgres database")
    
    # Trino
    trino_host: str = Field(default="localhost", description="Trino host")
    trino_port: int = Field(default=8080, description="Trino port")
    trino_user: str = Field(default="aurum", description="Trino user")
    trino_catalog: str = Field(default="iceberg", description="Trino catalog")
    trino_schema: str = Field(default="market", description="Trino schema")
    
    # ClickHouse (optional)
    clickhouse_host: str = Field(default="localhost", description="ClickHouse host")
    clickhouse_port: int = Field(default=8123, description="ClickHouse port")
    clickhouse_user: str = Field(default="default", description="ClickHouse user")
    clickhouse_password: str = Field(default="", description="ClickHouse password")
    clickhouse_database: str = Field(default="default", description="ClickHouse database")
    
    # Connection pooling
    pool_size: int = Field(default=20, description="Database connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    pool_timeout: int = Field(default=30, description="Connection pool timeout")
    pool_recycle: int = Field(default=3600, description="Connection recycle time")
    
    @property
    def timescale_dsn(self) -> str:
        """Async PostgreSQL DSN for Timescale."""
        return f"postgresql+asyncpg://{self.timescale_user}:{self.timescale_password}@{self.timescale_host}:{self.timescale_port}/{self.timescale_database}"
    
    @property
    def postgres_dsn(self) -> str:
        """Async PostgreSQL DSN for metadata."""
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"


class RedisSettings(BaseSettings):
    """Redis cache settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="AURUM_REDIS_",
        env_file=".env",
        case_sensitive=False,
    )
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database")
    password: Optional[str] = Field(default=None, description="Redis password")
    mode: RedisMode = Field(default=RedisMode.STANDALONE, description="Redis mode")
    
    # Connection pooling
    max_connections: int = Field(default=50, description="Max Redis connections")
    socket_timeout: int = Field(default=5, description="Socket timeout")
    socket_connect_timeout: int = Field(default=5, description="Socket connect timeout")
    
    # Sentinel settings (if mode is sentinel)
    sentinel_hosts: List[str] = Field(default_factory=list, description="Sentinel hosts")
    sentinel_service: str = Field(default="mymaster", description="Sentinel service name")
    
    @property
    def redis_url(self) -> str:
        """Redis connection URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class CacheSettings(BaseSettings):
    """Cache TTL settings by data category."""
    
    # Base TTL levels
    high_frequency_ttl: int = Field(default=60, description="High frequency cache TTL (seconds)")
    medium_frequency_ttl: int = Field(default=300, description="Medium frequency cache TTL")
    low_frequency_ttl: int = Field(default=3600, description="Low frequency cache TTL")
    static_ttl: int = Field(default=7200, description="Static data cache TTL")
    
    # Specific data types
    curve_data_ttl: int = Field(default=300, description="Curve data cache TTL")
    metadata_ttl: int = Field(default=7200, description="Metadata cache TTL")
    external_data_ttl: int = Field(default=7200, description="External data cache TTL")
    scenario_data_ttl: int = Field(default=300, description="Scenario data cache TTL")
    
    # Golden query list with longer TTL
    golden_query_ttl: int = Field(default=14400, description="Golden query cache TTL")
    negative_cache_ttl: int = Field(default=60, description="Negative cache TTL for 404s")


class APISettings(BaseSettings):
    """FastAPI application settings."""
    
    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")
    title: str = Field(default="Aurum API", description="API title")
    version: str = Field(default="2.0.0", description="API version")
    
    # CORS
    cors_origins: List[str] = Field(default=["*"], description="CORS origins")
    
    # Request handling
    request_timeout: int = Field(default=30, description="Request timeout seconds")
    max_request_size: int = Field(default=16777216, description="Max request size bytes")
    
    # Workers
    worker_count: Optional[int] = Field(default=None, description="Uvicorn worker count")
    
    # Middleware
    gzip_min_bytes: int = Field(default=500, description="Minimum bytes for gzip")
    enable_etags: bool = Field(default=True, description="Enable ETag headers")
    enable_304_responses: bool = Field(default=True, description="Enable 304 Not Modified")
    
    @field_validator('cors_origins', mode='before')
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v


class ObservabilitySettings(BaseSettings):
    """Observability and monitoring settings."""
    
    service_name: str = Field(default="aurum-api", description="Service name for tracing")
    
    # OpenTelemetry
    enable_tracing: bool = Field(default=True, description="Enable OpenTelemetry tracing")
    enable_metrics: bool = Field(default=True, description="Enable OpenTelemetry metrics")
    otel_endpoint: Optional[str] = Field(default=None, description="OTEL collector endpoint")
    
    # Logging
    log_level: str = Field(default="INFO", description="Log level")
    log_format: str = Field(default="json", description="Log format: json or text")
    
    # Metrics
    metrics_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    metrics_path: str = Field(default="/metrics", description="Metrics endpoint path")


class SecuritySettings(BaseSettings):
    """Security and authentication settings."""
    
    auth_enabled: bool = Field(default=False, description="Enable authentication")
    jwt_secret: Optional[str] = Field(default=None, description="JWT signing secret")
    jwt_algorithm: str = Field(default="HS256", description="JWT algorithm")
    jwt_expiry_hours: int = Field(default=24, description="JWT expiry hours")
    
    # Rate limiting
    rate_limit_enabled: bool = Field(default=True, description="Enable rate limiting")
    rate_limit_requests: int = Field(default=1000, description="Requests per minute")
    rate_limit_window: int = Field(default=60, description="Rate limit window seconds")


class WorkerSettings(BaseSettings):
    """Background worker settings."""
    
    # Celery
    broker_url: str = Field(default="redis://localhost:6379/1", description="Celery broker URL")
    result_backend: str = Field(default="redis://localhost:6379/1", description="Celery result backend")
    default_queue: str = Field(default="default", description="Default task queue")
    
    # Task routing
    enable_async_offload: bool = Field(default=False, description="Enable async task offload")
    task_timeout: int = Field(default=3600, description="Task timeout seconds")


class AurumSettings(BaseSettings):
    """Main Aurum configuration with all subsystems."""
    
    model_config = SettingsConfigDict(
        env_prefix="AURUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True,  # Freeze settings at startup
    )
    
    # Environment
    environment: str = Field(default="development", description="Environment name")
    debug: bool = Field(default=False, description="Debug mode")
    
    # Subsystem settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    api: APISettings = Field(default_factory=APISettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    workers: WorkerSettings = Field(default_factory=WorkerSettings)
    
    # Pagination defaults
    pagination_default_size: int = Field(default=100, description="Default pagination size")
    pagination_max_size: int = Field(default=1000, description="Max pagination size")
    
    # Feature flags
    enable_v2_only: bool = Field(default=False, description="Enable v2 API only")
    enable_timescale_caggs: bool = Field(default=True, description="Enable continuous aggregates")
    enable_iceberg_time_travel: bool = Field(default=False, description="Enable Iceberg time travel")


# Global settings instance
_settings: Optional[AurumSettings] = None


def get_settings() -> AurumSettings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = AurumSettings()
    return _settings


def configure_settings(settings: AurumSettings) -> None:
    """Configure the global settings instance (for testing)."""
    global _settings
    _settings = settings


def reset_settings() -> None:
    """Reset settings (for testing)."""
    global _settings
    _settings = None