"""Centralised configuration for Aurum services."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from enum import Enum
from typing import Any, Dict, Iterable, List

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import AurumBaseModel


class RedisMode(str, Enum):
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"
    DISABLED = "disabled"


class TrinoSettings(AurumBaseModel):
    host: str = Field(default="localhost", validation_alias=AliasChoices("API_TRINO_HOST"))
    port: int = Field(default=8080, ge=0, validation_alias=AliasChoices("API_TRINO_PORT"))
    user: str = Field(default="aurum", validation_alias=AliasChoices("API_TRINO_USER"))
    http_scheme: str = Field(default="http", validation_alias=AliasChoices("API_TRINO_SCHEME"))
    catalog: str = Field(default="iceberg", validation_alias=AliasChoices("API_TRINO_CATALOG"))
    schema: str = Field(default="market", validation_alias=AliasChoices("API_TRINO_SCHEMA"))
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


class AuthSettings(AurumBaseModel):
    disabled: bool = Field(default=True, validation_alias=AliasChoices("API_AUTH_DISABLED"))
    admin_groups: tuple[str, ...] = Field(default_factory=lambda: ("aurum-admins",), validation_alias=AliasChoices("API_ADMIN_GROUP"))
    oidc_issuer: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_ISSUER"))
    oidc_audience: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_AUDIENCE"))
    oidc_jwks_url: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_JWKS_URL"))
    client_id: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_CLIENT_ID"))
    client_secret: str | None = Field(default=None, validation_alias=AliasChoices("API_OIDC_CLIENT_SECRET"))
    jwt_secret: str | None = Field(default=None, validation_alias=AliasChoices("API_JWT_SECRET"))
    jwt_leeway_seconds: int = Field(default=60, ge=0, validation_alias=AliasChoices("API_JWT_LEEWAY"))

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
    scenario_outputs_enabled: bool = Field(default=True, validation_alias=AliasChoices("API_SCENARIO_OUTPUTS_ENABLED"))
    ppa_write_enabled: bool = Field(default=True, validation_alias=AliasChoices("API_PPA_WRITE_ENABLED"))
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    cache: ApiCacheSettings = Field(default_factory=ApiCacheSettings)
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
        if vault_mapping:
            settings = settings._apply_vault_mapping(vault_mapping)
        return settings

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
    "RedisMode",
]
