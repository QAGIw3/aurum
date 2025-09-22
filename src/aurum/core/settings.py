"""Centralised configuration for Aurum services."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
import os
from enum import Enum
from typing import Any, Dict, Iterable, List

from pydantic import AliasChoices, Field, field_validator

try:  # pragma: no cover - fallback when pydantic-settings is unavailable
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ModuleNotFoundError:  # pragma: no cover - lightweight compatibility shim
    from pydantic import BaseModel as _BaseSettings

    class SettingsConfigDict(dict):
        pass

    class BaseSettings(_BaseSettings):  # type: ignore[misc]
        model_config = {}

        def model_copy(self, update=None, deep=False):  # type: ignore[override]
            return super().model_copy(update=update, deep=deep)

from .models import AurumBaseModel


class RedisMode(str, Enum):
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"
    DISABLED = "disabled"


class DataBackendType(str, Enum):
    TRINO = "trino"
    CLICKHOUSE = "clickhouse"
    TIMESCALE = "timescale"


class DataBackendSettings(AurumBaseModel):
    """Configuration for pluggable data backends."""
    backend_type: DataBackendType = Field(
        default=DataBackendType.TRINO,
        validation_alias=AliasChoices("AURUM_API_BACKEND")
    )

    # Trino-specific settings
    trino_host: str = Field(default="localhost", validation_alias=AliasChoices("API_TRINO_HOST"))
    trino_port: int = Field(default=8080, ge=0, validation_alias=AliasChoices("API_TRINO_PORT"))
    trino_user: str = Field(default="aurum", validation_alias=AliasChoices("API_TRINO_USER"))
    trino_catalog: str = Field(default="iceberg", validation_alias=AliasChoices("API_TRINO_CATALOG"))
    trino_schema: str = Field(default="market", validation_alias=AliasChoices("API_TRINO_SCHEMA"))
    trino_password: str | None = Field(default=None, validation_alias=AliasChoices("API_TRINO_PASSWORD"))
    trino_http_scheme: str = Field(default="http", validation_alias=AliasChoices("API_TRINO_SCHEME"))

    # ClickHouse-specific settings
    clickhouse_host: str = Field(default="localhost", validation_alias=AliasChoices("API_CLICKHOUSE_HOST"))
    clickhouse_port: int = Field(default=9000, ge=0, validation_alias=AliasChoices("API_CLICKHOUSE_PORT"))
    clickhouse_database: str = Field(default="aurum", validation_alias=AliasChoices("API_CLICKHOUSE_DATABASE"))
    clickhouse_user: str = Field(default="aurum", validation_alias=AliasChoices("API_CLICKHOUSE_USER"))
    clickhouse_password: str | None = Field(default=None, validation_alias=AliasChoices("API_CLICKHOUSE_PASSWORD"))

    # Timescale-specific settings
    timescale_host: str = Field(default="localhost", validation_alias=AliasChoices("API_TIMESCALE_HOST"))
    timescale_port: int = Field(default=5432, ge=0, validation_alias=AliasChoices("API_TIMESCALE_PORT"))
    timescale_database: str = Field(default="aurum", validation_alias=AliasChoices("API_TIMESCALE_DATABASE"))
    timescale_user: str = Field(default="aurum", validation_alias=AliasChoices("API_TIMESCALE_USER"))
    timescale_password: str | None = Field(default=None, validation_alias=AliasChoices("API_TIMESCALE_PASSWORD"))

    # Connection pool settings (shared across all backends)
    connection_pool_min_size: int = Field(default=5, ge=0, validation_alias=AliasChoices("API_CONNECTION_POOL_MIN_SIZE"))
    connection_pool_max_size: int = Field(default=20, ge=1, validation_alias=AliasChoices("API_CONNECTION_POOL_MAX_SIZE"))
    connection_pool_max_idle_time: int = Field(default=300, ge=0, validation_alias=AliasChoices("API_CONNECTION_POOL_MAX_IDLE"))
    connection_pool_timeout_seconds: float = Field(default=30.0, gt=0.0, validation_alias=AliasChoices("API_CONNECTION_POOL_TIMEOUT"))
    connection_pool_acquire_timeout_seconds: float = Field(default=10.0, gt=0.0, validation_alias=AliasChoices("API_CONNECTION_POOL_ACQUIRE_TIMEOUT"))

    @field_validator("backend_type", mode="before")
    @classmethod
    def parse_backend_type(cls, v: str | DataBackendType) -> DataBackendType:
        if isinstance(v, str):
            v = v.lower()
            if v in ["trino", "clickhouse", "timescale"]:
                return DataBackendType(v)
        return v


class TrinoSettings(AurumBaseModel):
    host: str = Field(default="localhost", validation_alias=AliasChoices("API_TRINO_HOST"))
    port: int = Field(default=8080, ge=0, validation_alias=AliasChoices("API_TRINO_PORT"))
    user: str = Field(default="aurum", validation_alias=AliasChoices("API_TRINO_USER"))
    http_scheme: str = Field(default="http", validation_alias=AliasChoices("API_TRINO_SCHEME"))
    catalog: str = Field(default="iceberg", validation_alias=AliasChoices("API_TRINO_CATALOG"))
    database_schema: str = Field(default="market", validation_alias=AliasChoices("API_TRINO_SCHEMA"))
    password: str | None = Field(default=None, validation_alias=AliasChoices("API_TRINO_PASSWORD"))


class RedisSettings(AurumBaseModel):
    mode: RedisMode = Field(default=RedisMode.STANDALONE, validation_alias=AliasChoices("API_REDIS_MODE"))
    url: str | None = Field(default=None, validation_alias=AliasChoices("API_REDIS_URL"))
    ttl_seconds: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_CACHE_TTL"))
    namespace: str = Field(default="aurum", validation_alias=AliasChoices("API_CACHE_NAMESPACE"))
    db: int = Field(default=0, ge=0, validation_alias=AliasChoices("API_REDIS_DB"))
    sentinel_master: str | None = Field(default=None, validation_alias=AliasChoices("API_REDIS_SENTINEL_SERVICE"))
    sentinel_endpoints: tuple[str, ...] = Field(default_factory=tuple, validation_alias=AliasChoices("API_REDIS_SENTINELS"))
    cluster_nodes: tuple[str, ...] = Field(default_factory=tuple, validation_alias=AliasChoices("API_REDIS_CLUSTER_NODES"))
    username: str | None = Field(default=None, validation_alias=AliasChoices("API_REDIS_USERNAME"))
    password: str | None = Field(default=None, validation_alias=AliasChoices("API_REDIS_PASSWORD"))
    socket_timeout: float = Field(default=1.5, ge=0.0, validation_alias=AliasChoices("API_REDIS_SOCKET_TIMEOUT"))
    connect_timeout: float = Field(default=1.5, ge=0.0, validation_alias=AliasChoices("API_REDIS_CONNECT_TIMEOUT"))

    @field_validator("sentinel_endpoints", mode="before")
    @classmethod
    def _split_sentinels(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            parts = [item.strip() for item in value.split(",") if item.strip()]
            return tuple(parts)
        if isinstance(value, Iterable):  # pragma: no cover - passthrough
            return tuple(str(item) for item in value)
        raise TypeError("Invalid sentinel list")

    @field_validator("cluster_nodes", mode="before")
    @classmethod
    def _split_cluster(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            parts = [item.strip() for item in value.split(",") if item.strip()]
            return tuple(parts)
        if isinstance(value, Iterable):  # pragma: no cover - passthrough
            return tuple(str(item) for item in value)
        raise TypeError("Invalid cluster node list")


class RateLimitSettings(AurumBaseModel):
    enabled: bool = Field(default=True, validation_alias=AliasChoices("API_RATE_LIMIT_ENABLED"))
    requests_per_second: int = Field(default=10, ge=0, validation_alias=AliasChoices("API_RATE_LIMIT_RPS"))
    burst: int = Field(default=20, ge=0, validation_alias=AliasChoices("API_RATE_LIMIT_BURST"))
    identifier_header: str | None = Field(default=None, validation_alias=AliasChoices("API_RATE_LIMIT_HEADER"))
    overrides: dict[str, tuple[int, int]] = Field(default_factory=dict, validation_alias=AliasChoices("API_RATE_LIMIT_OVERRIDES"))
    tenant_overrides: dict[str, dict[str, tuple[int, int]]] = Field(default_factory=dict, validation_alias=AliasChoices("API_RATE_LIMIT_TENANT_OVERRIDES"))
    whitelist: tuple[str, ...] = Field(default_factory=tuple, validation_alias=AliasChoices("API_RATE_LIMIT_WHITELIST"))

    @field_validator("overrides", mode="before")
    @classmethod
    def _parse_overrides(cls, value: Any) -> dict[str, tuple[int, int]]:
        if value in (None, "", {}):
            return {}
        if isinstance(value, str):
            overrides: dict[str, tuple[int, int]] = {}
            for item in value.split(","):
                item = item.strip()
                if not item:
                    continue
                if "=" not in item:
                    continue
                path_part, limits_part = item.split("=", 1)
                limit_tokens = limits_part.split(":")
                if len(limit_tokens) != 2:
                    continue
                try:
                    overrides[path_part.strip()] = (int(limit_tokens[0]), int(limit_tokens[1]))
                except ValueError:
                    continue
            return overrides
        if isinstance(value, Mapping):
            parsed: dict[str, tuple[int, int]] = {}
            for key, limits in value.items():
                if isinstance(limits, (tuple, list)) and len(limits) == 2:
                    try:
                        parsed[str(key)] = (int(limits[0]), int(limits[1]))
                    except (TypeError, ValueError):
                        continue
            return parsed
        raise TypeError("Invalid rate limit overrides definition")

    @field_validator("whitelist", mode="before")
    @classmethod
    def _parse_whitelist(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            return tuple(item.strip() for item in value.split(",") if item.strip())
        if isinstance(value, Iterable):  # pragma: no cover - passthrough
            return tuple(str(item).strip() for item in value if str(item).strip())
        raise TypeError("Invalid rate limit whitelist definition")

    @field_validator("tenant_overrides", mode="before")
    @classmethod
    def _parse_tenant_overrides(cls, value: Any) -> dict[str, dict[str, tuple[int, int]]]:
        if value in (None, "", {}):
            return {}
        if isinstance(value, str):
            tenant_overrides: dict[str, dict[str, tuple[int, int]]] = {}
            for item in value.split(","):
                item = item.strip()
                if not item:
                    continue
                if "=" not in item:
                    continue
                tenant_path_part, limits_part = item.split("=", 1)

                # Parse tenant:path=rps:burst format
                if ":" not in tenant_path_part:
                    continue
                tenant_part, path_part = tenant_path_part.split(":", 1)

                limit_tokens = limits_part.split(":")
                if len(limit_tokens) != 2:
                    continue

                try:
                    if tenant_part not in tenant_overrides:
                        tenant_overrides[tenant_part] = {}
                    tenant_overrides[tenant_part][path_part.strip()] = (int(limit_tokens[0]), int(limit_tokens[1]))
                except ValueError:
                    continue
            return tenant_overrides
        if isinstance(value, Mapping):
            parsed: dict[str, dict[str, tuple[int, int]]] = {}
            for tenant_key, tenant_limits in value.items():
                if isinstance(tenant_limits, Mapping):
                    tenant_parsed: dict[str, tuple[int, int]] = {}
                    for path_key, limits in tenant_limits.items():
                        if isinstance(limits, (tuple, list)) and len(limits) == 2:
                            try:
                                tenant_parsed[str(path_key)] = (int(limits[0]), int(limits[1]))
                            except (TypeError, ValueError):
                                continue
                    if tenant_parsed:
                        parsed[str(tenant_key)] = tenant_parsed
            return parsed
        raise TypeError("Invalid rate limit tenant overrides definition")


class AuthSettings(AurumBaseModel):
    disabled: bool = Field(default=False, validation_alias=AliasChoices("API_AUTH_DISABLED"))
    admin_groups: tuple[str, ...] = Field(default_factory=lambda: ("aurum-admins",), validation_alias=AliasChoices("API_ADMIN_GROUP"))
    oidc_issuer: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_ISSUER"))
    oidc_audience: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_AUDIENCE"))
    oidc_jwks_url: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_JWKS_URL"))
    client_id: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_CLIENT_ID"))
    client_secret: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_CLIENT_SECRET"))
    jwt_secret: str | None = Field(default=None, validation_alias=AliasChoices("API_JWT_SECRET"))
    jwt_leeway_seconds: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_JWT_LEEWAY"))
    forward_auth_header: str | None = Field(default=None, validation_alias=AliasChoices("API_FORWARD_AUTH_HEADER"))
    forward_auth_claims_header: str | None = Field(default=None, validation_alias=AliasChoices("API_FORWARD_AUTH_CLAIMS_HEADER"))

    @field_validator("admin_groups", mode="before")
    @classmethod
    def _parse_groups(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            groups = tuple(item.strip().lower() for item in value.split(",") if item.strip())
            return groups
        if isinstance(value, Iterable):  # pragma: no cover - passthrough
            return tuple(str(item).lower() for item in value if str(item).strip())
        raise TypeError("Invalid admin group definition")


class ConcurrencyControlSettings(AurumBaseModel):
    """Settings for API concurrency controls and timeouts."""
    max_concurrent_trino_connections: int = Field(default=10, ge=1, validation_alias=AliasChoices("API_TRINO_MAX_CONNECTIONS"))
    trino_connection_timeout_seconds: float = Field(default=5.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_CONNECT_TIMEOUT"))
    trino_query_timeout_seconds: float = Field(default=30.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_QUERY_TIMEOUT"))
    trino_max_retries: int = Field(default=3, ge=0, validation_alias=AliasChoices("API_TRINO_MAX_RETRIES"))
    trino_retry_delay_seconds: float = Field(default=1.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_RETRY_DELAY"))
    trino_max_retry_delay_seconds: float = Field(default=10.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_MAX_RETRY_DELAY"))
    trino_circuit_breaker_failure_threshold: int = Field(default=5, ge=1, validation_alias=AliasChoices("API_TRINO_CIRCUIT_BREAKER_THRESHOLD"))
    trino_circuit_breaker_timeout_seconds: float = Field(default=60.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_CIRCUIT_BREAKER_TIMEOUT"))
    trino_connection_pool_min_size: int = Field(default=2, ge=0, validation_alias=AliasChoices("API_TRINO_POOL_MIN_SIZE"))
    trino_connection_pool_max_size: int = Field(default=10, ge=1, validation_alias=AliasChoices("API_TRINO_POOL_MAX_SIZE"))
    trino_connection_pool_max_idle: int = Field(default=5, ge=0, validation_alias=AliasChoices("API_TRINO_POOL_MAX_IDLE"))
    trino_connection_pool_idle_timeout_seconds: float = Field(default=300.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_POOL_IDLE_TIMEOUT"))
    trino_connection_pool_wait_timeout_seconds: float = Field(default=10.0, gt=0.0, validation_alias=AliasChoices("API_TRINO_POOL_WAIT_TIMEOUT"))

    @field_validator("trino_connection_pool_max_size")
    @classmethod
    def validate_pool_max_size(cls, v):
        """Ensure pool max size is reasonable."""
        if v > 100:  # Reasonable upper bound
            raise ValueError("Trino connection pool max size cannot exceed 100")
        return v

    @field_validator("trino_max_retries")
    @classmethod
    def validate_retry_attempts(cls, v):
        """Ensure retry attempts are reasonable."""
        if v > 10:  # Reasonable upper bound
            raise ValueError("Trino max retries cannot exceed 10")
        return v


class ApiCacheSettings(AurumBaseModel):
    in_memory_ttl: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_INMEMORY_TTL"))
    iso_lmp_cache_ttl: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_ISO_LMP_CACHE_TTL"))
    iso_lmp_inmemory_ttl: int = Field(default=30, ge=0, validation_alias=AliasChoices("API_ISO_LMP_INMEM_TTL"))
    metadata_ttl: int = Field(default=300, ge=0, validation_alias=AliasChoices("API_METADATA_REDIS_TTL"))
    scenario_output_ttl: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_SCENARIO_OUTPUT_TTL"))
    scenario_metric_ttl: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_SCENARIO_METRICS_TTL"))
    curve_ttl: int = Field(default=120, ge=0, validation_alias=AliasChoices("API_CURVE_TTL"))
    curve_diff_ttl: int = Field(default=120, ge=0, validation_alias=AliasChoices("API_CURVE_DIFF_TTL"))
    curve_strip_ttl: int = Field(default=120, ge=0, validation_alias=AliasChoices("API_CURVE_STRIP_TTL"))
    eia_series_ttl: int = Field(default=120, ge=0, validation_alias=AliasChoices("API_EIA_SERIES_TTL"))
    eia_series_dimensions_ttl: int = Field(default=300, ge=0, validation_alias=AliasChoices("API_EIA_SERIES_DIMENSIONS_TTL"))

    # Per-dimension TTL settings for fine-grained caching
    cache_ttl_high_frequency: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_HIGH_FREQUENCY"))
    cache_ttl_medium_frequency: int = Field(default=300, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_MEDIUM_FREQUENCY"))
    cache_ttl_low_frequency: int = Field(default=3600, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_LOW_FREQUENCY"))
    cache_ttl_static: int = Field(default=7200, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_STATIC"))

    # Dimension-specific TTL overrides
    cache_ttl_curve_data: int = Field(default=120, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_CURVE_DATA"))
    cache_ttl_metadata: int = Field(default=300, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_METADATA"))
    cache_ttl_external_data: int = Field(default=1800, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_EXTERNAL_DATA"))
    cache_ttl_scenario_data: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_CACHE_TTL_SCENARIO_DATA"))


class PaginationLimits(AurumBaseModel):
    curves_max_limit: int = Field(default=500, ge=1, validation_alias=AliasChoices("API_CURVE_MAX_LIMIT"))
    scenario_output_max_limit: int = Field(default=500, ge=1, validation_alias=AliasChoices("API_SCENARIO_OUTPUT_MAX_LIMIT"))
    scenario_metric_max_limit: int = Field(default=500, ge=1, validation_alias=AliasChoices("API_SCENARIO_METRIC_MAX_LIMIT"))
    eia_series_max_limit: int = Field(default=1000, ge=1, validation_alias=AliasChoices("API_EIA_SERIES_MAX_LIMIT"))


class MetricsSettings(AurumBaseModel):
    enabled: bool = Field(default=True, validation_alias=AliasChoices("API_METRICS_ENABLED"))
    path: str = Field(default="/metrics", validation_alias=AliasChoices("API_METRICS_PATH"))


class ApiSettings(AurumBaseModel):
    title: str = Field(default="Aurum API")
    version: str = Field(default="0.1.0")
    cors_allow_origins: tuple[str, ...] = Field(default_factory=tuple, validation_alias=AliasChoices("API_CORS_ORIGINS"))
    cors_allow_credentials: bool = Field(default=False, validation_alias=AliasChoices("API_CORS_ALLOW_CREDENTIALS"))
    gzip_min_bytes: int = Field(default=500, ge=0, validation_alias=AliasChoices("API_GZIP_MIN_BYTES"))
    request_timeout_seconds: float = Field(default=30.0, gt=0.0, validation_alias=AliasChoices("API_REQUEST_TIMEOUT"))
    max_request_body_size: int = Field(default=10485760, ge=1024, validation_alias=AliasChoices("API_MAX_REQUEST_BODY_SIZE"))  # 10MB default
    max_response_body_size: int = Field(default=104857600, ge=1024, validation_alias=AliasChoices("API_MAX_RESPONSE_BODY_SIZE"))  # 100MB default
    max_concurrent_requests: int = Field(default=1000, ge=10, validation_alias=AliasChoices("API_MAX_CONCURRENT_REQUESTS"))
    scenario_outputs_enabled: bool = Field(default=True, validation_alias=AliasChoices("API_SCENARIO_OUTPUTS_ENABLED"))
    ppa_write_enabled: bool = Field(default=True, validation_alias=AliasChoices("API_PPA_WRITE_ENABLED"))
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    cache: ApiCacheSettings = Field(default_factory=ApiCacheSettings)
    concurrency: ConcurrencyControlSettings = Field(default_factory=ConcurrencyControlSettings)
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def _split_cors(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            origins = tuple(origin.strip() for origin in value.split(",") if origin.strip())
            return origins
        if isinstance(value, Iterable):  # pragma: no cover - passthrough
            return tuple(str(item) for item in value)
        raise TypeError("Invalid CORS origin definition")


class DatabaseSettings(AurumBaseModel):
    timescale_dsn: str = Field(
        default="postgresql://timescale:timescale@timescale:5432/timeseries",
        validation_alias=AliasChoices("TIMESCALE_DSN"),
    )
    postgres_dsn: str | None = Field(default=None, validation_alias=AliasChoices("POSTGRES_DSN"))
    eia_series_base_table: str = Field(
        default="mart.mart_eia_series_latest",
        validation_alias=AliasChoices("API_EIA_SERIES_TABLE"),
    )


class MessagingSettings(AurumBaseModel):
    bootstrap_servers: str | None = Field(default=None, validation_alias=AliasChoices("KAFKA_BOOTSTRAP_SERVERS"))
    schema_registry_url: str | None = Field(default=None, validation_alias=AliasChoices("KAFKA_SCHEMA_REGISTRY_URL"))
    default_topic_prefix: str = Field(default="aurum.", validation_alias=AliasChoices("KAFKA_TOPIC_PREFIX"))
    client_id: str = Field(default="aurum-client", validation_alias=AliasChoices("KAFKA_CLIENT_ID"))
    sasl_username: str | None = Field(default=None, validation_alias=AliasChoices("KAFKA_SASL_USERNAME"))
    sasl_password: str | None = Field(default=None, validation_alias=AliasChoices("KAFKA_SASL_PASSWORD"))
    security_protocol: str = Field(default="SASL_SSL", validation_alias=AliasChoices("KAFKA_SECURITY_PROTOCOL"))
    sasl_mechanism: str = Field(default="PLAIN", validation_alias=AliasChoices("KAFKA_SASL_MECHANISM"))


class TelemetrySettings(AurumBaseModel):
    service_name: str = Field(default="aurum", validation_alias=AliasChoices("TELEMETRY_SERVICE_NAME"))
    otlp_endpoint: str | None = Field(default=None, validation_alias=AliasChoices("TELEMETRY_OTLP_ENDPOINT"))
    enabled: bool = Field(default=True, validation_alias=AliasChoices("TELEMETRY_ENABLED"))


class AurumSettings(BaseSettings):
    """Primary settings object aggregating configuration for all services."""

    env: str = Field(default="local", validation_alias=AliasChoices("ENV"))
    debug: bool = Field(default=False, validation_alias=AliasChoices("DEBUG"))
    api: ApiSettings = Field(default_factory=ApiSettings)
    data_backend: DataBackendSettings = Field(default_factory=DataBackendSettings)
    trino: TrinoSettings = Field(default_factory=TrinoSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    messaging: MessagingSettings = Field(default_factory=MessagingSettings)
    telemetry: TelemetrySettings = Field(default_factory=TelemetrySettings)
    pagination: PaginationLimits = Field(default_factory=PaginationLimits)

    model_config = SettingsConfigDict(
        env_prefix="AURUM_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    @classmethod
    def from_env(
        cls,
        *,
        vault_mapping: Mapping[str, Any] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> "AurumSettings":
        """Instantiate settings from environment with optional secret overrides."""
        settings = cls(**(overrides or {}))
        env = os.environ

        def update_auth(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"auth": settings.auth.model_copy(update=kwargs)},
                deep=True,
            )

        def update_api(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"api": settings.api.model_copy(update=kwargs)},
                deep=True,
            )

        def update_rate_limit(**kwargs: Any) -> None:
            rate_limit = settings.api.rate_limit.model_copy(update=kwargs)
            update_api(rate_limit=rate_limit)

        def update_cache(**kwargs: Any) -> None:
            cache = settings.api.cache.model_copy(update=kwargs)
            update_api(cache=cache)

        def update_metrics(**kwargs: Any) -> None:
            metrics = settings.api.metrics.model_copy(update=kwargs)
            update_api(metrics=metrics)

        def update_pagination(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"pagination": settings.pagination.model_copy(update=kwargs)},
                deep=True,
            )

        def update_trino(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"trino": settings.trino.model_copy(update=kwargs)},
                deep=True,
            )

        def update_redis(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"redis": settings.redis.model_copy(update=kwargs)},
                deep=True,
            )

        def update_database(**kwargs: Any) -> None:
            nonlocal settings
            settings = settings.model_copy(
                update={"database": settings.database.model_copy(update=kwargs)},
                deep=True,
            )

        if (value := env.get("AURUM_API_ADMIN_GROUP")) is not None:
            groups = tuple(item.strip().lower() for item in value.split(",") if item.strip())
            update_auth(admin_groups=groups)
        if (value := env.get("AURUM_API_AUTH_DISABLED")) is not None:
            update_auth(disabled=cls._truthy(value))
        if (value := env.get("AURUM_API_OIDC_ISSUER")) is not None:
            update_auth(oidc_issuer=value.strip())
        if (value := env.get("AURUM_API_OIDC_AUDIENCE")) is not None:
            update_auth(oidc_audience=value.strip())
        if (value := env.get("AURUM_API_OIDC_JWKS_URL")) is not None:
            update_auth(oidc_jwks_url=value.strip())
        if (value := env.get("AURUM_API_OIDC_CLIENT_ID")) is not None:
            update_auth(client_id=value.strip())
        if (value := env.get("AURUM_API_OIDC_CLIENT_SECRET")) is not None:
            update_auth(client_secret=value.strip())
        if (value := env.get("AURUM_API_JWT_SECRET")) is not None:
            update_auth(jwt_secret=value.strip())
        if (value := env.get("AURUM_API_JWT_LEEWAY")) is not None:
            update_auth(jwt_leeway_seconds=int(value))

        if (value := env.get("AURUM_API_CORS_ORIGINS")) is not None:
            origins = tuple(item.strip() for item in value.split(",") if item.strip())
            update_api(cors_allow_origins=origins)
        if (value := env.get("AURUM_API_CORS_ALLOW_CREDENTIALS")) is not None:
            update_api(cors_allow_credentials=cls._truthy(value))
        if (value := env.get("AURUM_API_GZIP_MIN_BYTES")) is not None:
            update_api(gzip_min_bytes=int(value))
        if (value := env.get("AURUM_API_REQUEST_TIMEOUT")) is not None:
            update_api(request_timeout_seconds=float(value))
        if (value := env.get("AURUM_API_MAX_REQUEST_BODY_SIZE")) is not None:
            update_api(max_request_body_size=int(value))
        if (value := env.get("AURUM_API_MAX_RESPONSE_BODY_SIZE")) is not None:
            update_api(max_response_body_size=int(value))
        if (value := env.get("AURUM_API_MAX_CONCURRENT_REQUESTS")) is not None:
            update_api(max_concurrent_requests=int(value))
        if (value := env.get("AURUM_API_SCENARIO_OUTPUTS_ENABLED")) is not None:
            update_api(scenario_outputs_enabled=cls._truthy(value))
        if (value := env.get("AURUM_API_PPA_WRITE_ENABLED")) is not None:
            update_api(ppa_write_enabled=cls._truthy(value))

        if (value := env.get("AURUM_API_RATE_LIMIT_ENABLED")) is not None:
            update_rate_limit(enabled=cls._truthy(value))
        if (value := env.get("AURUM_API_RATE_LIMIT_RPS")) is not None:
            update_rate_limit(requests_per_second=int(value))
        if (value := env.get("AURUM_API_RATE_LIMIT_BURST")) is not None:
            update_rate_limit(burst=int(value))
        if (value := env.get("AURUM_API_RATE_LIMIT_HEADER")) is not None:
            update_rate_limit(identifier_header=value.strip())
        if (value := env.get("AURUM_API_RATE_LIMIT_OVERRIDES")) is not None:
            overrides: dict[str, tuple[int, int]] = {}
            for entry in value.split(","):
                entry = entry.strip()
                if not entry or "=" not in entry:
                    continue
                path_part, limits_part = entry.split("=", 1)
                tokens = limits_part.split(":")
                if len(tokens) != 2:
                    continue
                try:
                    overrides[path_part.strip()] = (int(tokens[0]), int(tokens[1]))
                except ValueError:
                    continue
            if overrides:
                update_rate_limit(overrides=overrides)

        if (value := env.get("AURUM_API_RATE_LIMIT_TENANT_OVERRIDES")) is not None:
            tenant_overrides: dict[str, dict[str, tuple[int, int]]] = {}
            for entry in value.split(","):
                entry = entry.strip()
                if not entry or "=" not in entry:
                    continue
                tenant_path_part, limits_part = entry.split("=", 1)

                # Parse tenant:path=rps:burst format
                if ":" not in tenant_path_part:
                    continue
                tenant_part, path_part = tenant_path_part.split(":", 1)

                limit_tokens = limits_part.split(":")
                if len(limit_tokens) != 2:
                    continue

                try:
                    if tenant_part not in tenant_overrides:
                        tenant_overrides[tenant_part] = {}
                    tenant_overrides[tenant_part][path_part.strip()] = (int(limit_tokens[0]), int(limit_tokens[1]))
                except ValueError:
                    continue
            if tenant_overrides:
                update_rate_limit(tenant_overrides=tenant_overrides)

        if (value := env.get("AURUM_API_METRICS_ENABLED")) is not None:
            update_metrics(enabled=cls._truthy(value))
        if (value := env.get("AURUM_API_METRICS_PATH")) is not None:
            update_metrics(path=value.strip())

        if (value := env.get("AURUM_API_INMEMORY_TTL")) is not None:
            update_cache(in_memory_ttl=int(value))
        if (value := env.get("AURUM_API_METADATA_REDIS_TTL")) is not None:
            update_cache(metadata_ttl=int(value))
        if (value := env.get("AURUM_API_SCENARIO_OUTPUT_TTL")) is not None:
            update_cache(scenario_output_ttl=int(value))
        if (value := env.get("AURUM_API_SCENARIO_METRICS_TTL")) is not None:
            update_cache(scenario_metric_ttl=int(value))
        if (value := env.get("AURUM_API_CURVE_TTL")) is not None:
            update_cache(curve_ttl=int(value))
        if (value := env.get("AURUM_API_CURVE_DIFF_TTL")) is not None:
            update_cache(curve_diff_ttl=int(value))
        if (value := env.get("AURUM_API_CURVE_STRIP_TTL")) is not None:
            update_cache(curve_strip_ttl=int(value))
        if (value := env.get("AURUM_API_EIA_SERIES_TTL")) is not None:
            update_cache(eia_series_ttl=int(value))
        if (value := env.get("AURUM_API_EIA_SERIES_DIMENSIONS_TTL")) is not None:
            update_cache(eia_series_dimensions_ttl=int(value))

        # Per-dimension cache TTL settings
        if (value := env.get("AURUM_API_CACHE_TTL_HIGH_FREQUENCY")) is not None:
            update_cache(cache_ttl_high_frequency=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_MEDIUM_FREQUENCY")) is not None:
            update_cache(cache_ttl_medium_frequency=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_LOW_FREQUENCY")) is not None:
            update_cache(cache_ttl_low_frequency=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_STATIC")) is not None:
            update_cache(cache_ttl_static=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_CURVE_DATA")) is not None:
            update_cache(cache_ttl_curve_data=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_METADATA")) is not None:
            update_cache(cache_ttl_metadata=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_EXTERNAL_DATA")) is not None:
            update_cache(cache_ttl_external_data=int(value))
        if (value := env.get("AURUM_API_CACHE_TTL_SCENARIO_DATA")) is not None:
            update_cache(cache_ttl_scenario_data=int(value))

        if (value := env.get("AURUM_API_CURVE_MAX_LIMIT")) is not None:
            update_pagination(curves_max_limit=int(value))
        if (value := env.get("AURUM_API_SCENARIO_OUTPUT_MAX_LIMIT")) is not None:
            update_pagination(scenario_output_max_limit=int(value))
        if (value := env.get("AURUM_API_SCENARIO_METRIC_MAX_LIMIT")) is not None:
            update_pagination(scenario_metric_max_limit=int(value))
        if (value := env.get("AURUM_API_EIA_SERIES_MAX_LIMIT")) is not None:
            update_pagination(eia_series_max_limit=int(value))

        if (value := env.get("AURUM_API_TRINO_HOST")) is not None:
            update_trino(host=value.strip())
        if (value := env.get("AURUM_API_TRINO_PORT")) is not None:
            update_trino(port=int(value))
        if (value := env.get("AURUM_API_TRINO_USER")) is not None:
            update_trino(user=value.strip())
        if (value := env.get("AURUM_API_TRINO_SCHEME")) is not None:
            update_trino(http_scheme=value.strip())

        if (value := env.get("AURUM_API_REDIS_URL")) is not None:
            update_redis(url=value.strip())
        if (value := env.get("AURUM_API_REDIS_MODE")) is not None:
            update_redis(mode=value.strip())
        if (value := env.get("AURUM_API_REDIS_DB")) is not None:
            update_redis(db=int(value))
        if (value := env.get("AURUM_API_REDIS_USERNAME")) is not None:
            update_redis(username=value.strip())
        if (value := env.get("AURUM_API_REDIS_PASSWORD")) is not None:
            update_redis(password=value.strip())
        if (value := env.get("AURUM_API_CACHE_TTL")) is not None:
            update_redis(ttl_seconds=int(value))

        if (value := env.get("AURUM_TIMESCALE_DSN")) is not None:
            update_database(timescale_dsn=value.strip())
        if (value := env.get("AURUM_API_EIA_SERIES_TABLE")) is not None:
            update_database(eia_series_base_table=value.strip())

        if vault_mapping:
            settings = settings._apply_vault_mapping(vault_mapping)
        return settings

    @staticmethod
    def _truthy(value: str) -> bool:
        return value.strip().lower() in {"1", "true", "yes", "on"}


    def _apply_vault_mapping(self, mapping: Mapping[str, Any]) -> "AurumSettings":
        """Return a copy with values overridden from a Vault-style mapping.

        Keys use ``__`` as a nested delimiter (matching the settings env structure).
        For example ``{"api__auth__client_secret": "..."}``.
        """

        updates: Dict[str, Any] = {}
        for key, value in mapping.items():
            pointer: MutableMapping[str, Any] = updates
            parts: List[str] = [part for part in key.split("__") if part]
            if not parts:
                continue
            for part in parts[:-1]:
                child = pointer.get(part)
                if not isinstance(child, MutableMapping):
                    child = {}
                    pointer[part] = child
                pointer = child
            pointer[parts[-1]] = value
        if not updates:
            return self
        return self.model_copy(update=updates, deep=True)


__all__ = [
    "AurumSettings",
    "TrinoSettings",
    "RedisSettings",
    "RateLimitSettings",
    "AuthSettings",
    "ApiSettings",
    "DatabaseSettings",
    "MessagingSettings",
    "TelemetrySettings",
    "ConcurrencyControlSettings",
    "RedisMode",
]
