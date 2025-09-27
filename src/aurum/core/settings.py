"""Feature-flagged configuration system for Aurum services with gradual migration support."""
from __future__ import annotations

import os
import json
import logging
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Mapping, Optional

print(f"DEBUG: Loading settings.py - AURUM_ENABLE_MIGRATION_MONITORING = {os.getenv('AURUM_ENABLE_MIGRATION_MONITORING', 'NOT_SET')}")

# Setup logging
logger = logging.getLogger(__name__)


# Redis configuration modes (for backward compatibility)
class RedisMode(str, Enum):
    """Redis deployment modes."""
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


class DataBackendType(str, Enum):
    """Supported data backend engines for the API layer."""

    TRINO = "trino"
    CLICKHOUSE = "clickhouse"
    TIMESCALE = "timescale"

# Feature flags for migration (uppercase primary with lowercase fallback)
FEATURE_FLAGS = {
    "USE_SIMPLIFIED_SETTINGS": "AURUM_USE_SIMPLIFIED_SETTINGS",
    "ENABLE_MIGRATION_MONITORING": "AURUM_ENABLE_MIGRATION_MONITORING",
    "SETTINGS_MIGRATION_PHASE": "AURUM_SETTINGS_MIGRATION_PHASE",  # "legacy", "hybrid", "simplified"
}


def get_flag_env(flag_name: str, *, default: str = "") -> str:
    """Return environment value for ``flag_name`` with lowercase fallback.

    Emits a deprecation warning when the lowercase variant is used.
    """

    value = os.getenv(flag_name)
    if value is None:
        lower_key = flag_name.lower()
        value = os.getenv(lower_key)
        if value is not None:
            logger.warning(
                "Using deprecated lowercase environment variable %s; prefer %s",
                lower_key,
                flag_name,
            )
    return value if value is not None else default


def _set_flag_env(flag_name: str, value: str) -> None:
    """Set environment flags for both uppercase and legacy lowercase names."""

    os.environ[flag_name] = value
    os.environ[flag_name.lower()] = value


class MigrationMetrics:
    """Track migration metrics and health."""

    def __init__(self):
        # Check if migration monitoring is enabled at the very beginning
        if not is_feature_enabled(FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"]):
            self._metrics = self._default_metrics()
            self.metrics_file = None
            return

        try:
            metrics_dir = os.getenv("AURUM_METRICS_DIR", str(Path.home() / ".aurum"))
            self.metrics_file = Path(metrics_dir) / "migration_metrics.json"

            # Try to create the directory and handle read-only filesystems
            try:
                self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                # If we can't create the directory (read-only filesystem), create a dummy metrics object
                logger.debug(f"Cannot create metrics directory {self.metrics_file.parent}: {e}")
                self._metrics = self._default_metrics()
                self.metrics_file = None
                return

            try:
                self._load_metrics()
            except OSError as e:
                # If we can't load metrics (read-only filesystem), use default metrics
                logger.debug(f"Cannot load metrics from {self.metrics_file}: {e}")
                self._metrics = self._default_metrics()
        except Exception as e:
            # If anything fails during initialization, use default metrics
            logger.debug(f"MigrationMetrics initialization failed: {e}")
            self._metrics = self._default_metrics()
            self.metrics_file = None

    def _load_metrics(self):
        """Load metrics from file."""
        if self.metrics_file.exists():
            try:
                with open(self.metrics_file, 'r') as f:
                    self._metrics = json.load(f)
            except Exception:
                self._metrics = self._default_metrics()
        else:
            self._metrics = self._default_metrics()

    def _default_metrics(self):
        """Get default metrics structure."""
        return {
            "settings_migration": {
                "legacy_calls": 0,
                "simplified_calls": 0,
                "errors": 0,
                "performance_ms": [],
                "feature_flag_enabled": False,
                "migration_phase": "legacy"
            },
            "database_migration": {
                "legacy_calls": 0,
                "simplified_calls": 0,
                "errors": 0,
                "performance_ms": [],
                "feature_flag_enabled": False
            }
        }

    def _save_metrics(self):
        """Save metrics to file."""
        try:
            with open(self.metrics_file, 'w') as f:
                json.dump(self._metrics, f, indent=2)
        except OSError:
            # If we can't write to the file (read-only filesystem), just log and continue
            logger.debug("Cannot save migration metrics due to read-only filesystem")
        except Exception as e:
            logger.warning(f"Failed to save migration metrics: {e}")

    def record_settings_call(self, system: str, duration_ms: float, error: bool = False):
        """Record a settings system call."""
        if not self.is_monitoring_enabled():
            return

        metrics = self._metrics["settings_migration"]
        if system == "simplified":
            metrics["simplified_calls"] += 1
        else:
            metrics["legacy_calls"] += 1

        if error:
            metrics["errors"] += 1

        metrics["performance_ms"].append(duration_ms)
        # Keep only last 1000 measurements
        if len(metrics["performance_ms"]) > 1000:
            metrics["performance_ms"] = metrics["performance_ms"][-1000:]

        self._save_metrics()

    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status."""
        if "settings_migration" not in self._metrics:
            return {
                "settings_phase": "disabled",
                "settings_simplified_ratio": 0.0,
                "database_phase": "disabled",
                "monitoring_enabled": False,
            }
        return {
            "settings_phase": self._metrics["settings_migration"]["migration_phase"],
            "settings_simplified_ratio": self._get_simplified_ratio("settings_migration"),
            "database_phase": self._metrics["database_migration"].get("migration_phase", "legacy"),
            "monitoring_enabled": self.is_monitoring_enabled(),
        }

    def _get_simplified_ratio(self, component: str) -> float:
        """Calculate ratio of simplified system usage."""
        metrics = self._metrics[component]
        total = metrics.get("simplified_calls", 0) + metrics.get("legacy_calls", 0)
        if total == 0:
            return 0.0
        return metrics.get("simplified_calls", 0) / total

    def is_monitoring_enabled(self) -> bool:
        """Check if migration monitoring is enabled."""
        value = get_flag_env(
            FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"],
            default="false",
        )
        return value.lower() in ("true", "1", "yes")

    def set_migration_phase(self, component: str, phase: str):
        """Set migration phase for a component."""
        if component in self._metrics:
            self._metrics[component]["migration_phase"] = phase
            self._save_metrics()


# Global migration metrics instance - initialized lazily
_migration_metrics: Optional[MigrationMetrics] = None


def _init_migration_metrics():
    """Initialize migration metrics lazily."""
    global _migration_metrics
    if _migration_metrics is None:
        # Check if migration monitoring is enabled before initializing
        if not is_feature_enabled(FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"]):
            _migration_metrics = None
            return

        try:
            _migration_metrics = MigrationMetrics()
        except Exception:
            # If we can't create the metrics file (any error), keep it None
            _migration_metrics = None


def is_feature_enabled(flag: str) -> bool:
    """Check if a feature flag is enabled.

    Defaults to enabled for the simplified settings flag to complete migration.
    """
    # Prefer default enabled for simplified settings roll-out
    default_value = "true" if flag == FEATURE_FLAGS.get("USE_SIMPLIFIED_SETTINGS") else "false"
    value = get_flag_env(flag, default=default_value).lower()
    return value in ("true", "1", "yes")


def get_migration_phase(component: str = "settings") -> str:
    """Get current migration phase for a component."""
    phase = get_flag_env(
        FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"],
        default="legacy",
    )
    # Only set migration phase if metrics are already initialized to avoid triggering initialization during module import
    if _migration_metrics is not None:
        _migration_metrics.set_migration_phase("settings_migration", phase)
    return phase


class DataBackendConfig:
    """Configuration container for pluggable data backends."""

    def __init__(
        self,
        *,
        backend_type: str | DataBackendType = DataBackendType.TRINO,
        trino_host: str = "localhost",
        trino_port: int = 8080,
        trino_catalog: str = "iceberg",
        trino_database_schema: str = "market",
        trino_user: str = "aurum",
        trino_password: str | None = None,
        clickhouse_host: str = "localhost",
        clickhouse_port: int = 9000,
        clickhouse_database: str = "aurum",
        clickhouse_user: str = "aurum",
        clickhouse_password: str | None = None,
        timescale_host: str = "localhost",
        timescale_port: int = 5432,
        timescale_database: str = "aurum",
        timescale_user: str = "aurum",
        timescale_password: str | None = None,
        connection_pool_min_size: int = 5,
        connection_pool_max_size: int = 20,
        connection_pool_max_idle_time: int = 300,
        connection_pool_timeout_seconds: float = 30.0,
        connection_pool_acquire_timeout_seconds: float = 10.0,
    ) -> None:
        self.backend_type = backend_type
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.trino_catalog = trino_catalog
        self.trino_database_schema = trino_database_schema
        self.trino_user = trino_user
        self.trino_password = trino_password
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_database = clickhouse_database
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.timescale_host = timescale_host
        self.timescale_port = timescale_port
        self.timescale_database = timescale_database
        self.timescale_user = timescale_user
        self.timescale_password = timescale_password
        self.connection_pool_min_size = connection_pool_min_size
        self.connection_pool_max_size = connection_pool_max_size
        self.connection_pool_max_idle_time = connection_pool_max_idle_time
        self.connection_pool_timeout_seconds = connection_pool_timeout_seconds
        self.connection_pool_acquire_timeout_seconds = connection_pool_acquire_timeout_seconds

    def _coerce_backend_type(self, value: str | DataBackendType) -> DataBackendType:
        if isinstance(value, DataBackendType):
            return value
        if isinstance(value, str):
            try:
                return DataBackendType(value.lower())
            except ValueError as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"Unsupported data backend type: {value}") from exc
        raise TypeError(f"backend_type must be a string or DataBackendType, got {type(value)!r}")

    @property
    def backend_type(self) -> DataBackendType:
        return self._backend_type

    @backend_type.setter
    def backend_type(self, value: str | DataBackendType) -> None:
        self._backend_type = self._coerce_backend_type(value)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"DataBackendConfig(backend_type={self.backend_type.value!r})"


class SimplifiedSettings:
    """Simplified, focused settings system."""

    def __init__(self, env_prefix: str = "AURUM_"):
        self.env_prefix = env_prefix
        self._cache: Dict[str, Any] = {}
        start_time = self._current_time_ms()
        self._load_from_env()
        duration = self._current_time_ms() - start_time
        if get_migration_metrics() is not None:
            _migration_metrics.record_settings_call("simplified", duration)

    def _current_time_ms(self) -> float:
        """Get current time in milliseconds."""
        return self._get_time_ms()

    @staticmethod
    def _get_time_ms() -> float:
        """Get current wall time in milliseconds."""
        try:
            import time
            return time.perf_counter() * 1000.0
        except Exception:
            return 0.0

    @staticmethod
    def _get_int_from_env(env: Mapping[str, str], keys: Iterable[str], default: int) -> int:
        """Read the first valid integer from the provided environment keys."""
        for key in keys:
            raw = env.get(key)
            if raw is None:
                continue
            try:
                return int(raw)
            except ValueError:
                logger.warning("Invalid integer for %s=%r; using default %s", key, raw, default)
        return default

    @staticmethod
    def _get_float_from_env(env: Mapping[str, str], keys: Iterable[str], default: float) -> float:
        """Read the first valid float from the provided environment keys."""
        for key in keys:
            raw = env.get(key)
            if raw is None:
                continue
            try:
                return float(raw)
            except ValueError:
                logger.warning("Invalid float for %s=%r; using default %s", key, raw, default)
        return default

    @staticmethod
    def _get_bool_from_env(env: Mapping[str, str], keys: Iterable[str], default: bool) -> bool:
        """Read the first boolean-like value from the provided environment keys."""

        truthy = {"true", "1", "yes", "on"}
        falsy = {"false", "0", "no", "off"}
        for key in keys:
            raw = env.get(key)
            if raw is None:
                continue
            lowered = raw.strip().lower()
            if lowered in truthy:
                return True
            if lowered in falsy:
                return False
            logger.warning("Invalid boolean for %s=%r; using default %s", key, raw, default)
        return default

    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        env = os.environ

        # Core settings
        self.environment = env.get(f"{self.env_prefix}ENV", "development")
        self.debug = env.get(f"{self.env_prefix}DEBUG", "false").lower() in ("true", "1", "yes")
        self.log_level = env.get(f"{self.env_prefix}LOG_LEVEL", "INFO")

        # API settings
        self.api_host = env.get(f"{self.env_prefix}API_HOST", "localhost")
        self.api_port = int(env.get(f"{self.env_prefix}API_PORT", "8000"))
        self.api_cors_origins = self._split_env_list(env.get(f"{self.env_prefix}API_CORS_ORIGINS", "*"))
        self.api_title = env.get(f"{self.env_prefix}API_TITLE", "Aurum API")
        self.api_version = env.get(f"{self.env_prefix}API_VERSION", "1.0.0")
        self.api_request_timeout_seconds = int(env.get(f"{self.env_prefix}API_REQUEST_TIMEOUT_SECONDS", "30"))
        # GZip settings (compat: allow unprefixed var used in tests)
        try:
            self.gzip_min_bytes = int(env.get("API_GZIP_MIN_BYTES", env.get(f"{self.env_prefix}API_GZIP_MIN_BYTES", "500")))
        except ValueError:
            self.gzip_min_bytes = 500

        cache_ttl_high_frequency = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_HIGH_FREQUENCY",
                f"{self.env_prefix}API_CACHE_HIGH_TTL",
            ],
            60,
        )
        cache_ttl_medium_frequency = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_MEDIUM_FREQUENCY",
                f"{self.env_prefix}API_CACHE_MEDIUM_TTL",
            ],
            300,
        )
        cache_ttl_low_frequency = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_LOW_FREQUENCY",
                f"{self.env_prefix}API_CACHE_LOW_TTL",
            ],
            3600,
        )
        cache_ttl_static = self._get_int_from_env(
            env,
            [f"{self.env_prefix}API_CACHE_TTL_STATIC"],
            7200,
        )
        cache_ttl_curve_data = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_CURVE_DATA",
                f"{self.env_prefix}API_CACHE_CURVE_TTL",
            ],
            cache_ttl_medium_frequency,
        )
        cache_ttl_metadata = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_METADATA",
                f"{self.env_prefix}API_CACHE_METADATA_TTL",
            ],
            cache_ttl_static,
        )
        cache_ttl_external_data = self._get_int_from_env(
            env,
            [f"{self.env_prefix}API_CACHE_TTL_EXTERNAL_DATA"],
            cache_ttl_static,
        )
        cache_ttl_scenario_data = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_SCENARIO_DATA",
                f"{self.env_prefix}API_CACHE_SCENARIO_TTL",
            ],
            cache_ttl_medium_frequency,
        )
        cache_ttl_curve_diff = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_CURVE_DIFF",
                f"{self.env_prefix}API_CACHE_CURVE_DIFF_TTL",
            ],
            cache_ttl_medium_frequency,
        )
        cache_ttl_curve_strips = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}API_CACHE_TTL_CURVE_STRIPS",
                f"{self.env_prefix}API_CACHE_CURVE_STRIP_TTL",
            ],
            cache_ttl_medium_frequency,
        )
        eia_series_ttl = self._get_int_from_env(
            env,
            [f"{self.env_prefix}API_CACHE_EIA_SERIES_TTL"],
            cache_ttl_medium_frequency,
        )
        eia_series_dim_ttl = self._get_int_from_env(
            env,
            [f"{self.env_prefix}API_CACHE_EIA_SERIES_DIM_TTL"],
            cache_ttl_low_frequency,
        )

        # API cache configuration defaults
        api_cache_defaults = {
            "in_memory_ttl": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CACHE_IN_MEMORY_TTL"],
                60,
            ),
            "metadata_ttl": cache_ttl_metadata,
            "curve_ttl": cache_ttl_curve_data,
            "curve_diff_ttl": cache_ttl_curve_diff,
            "curve_strip_ttl": cache_ttl_curve_strips,
            "eia_series_ttl": eia_series_ttl,
            "eia_series_dimensions_ttl": eia_series_dim_ttl,
            "cache_ttl_high_frequency": cache_ttl_high_frequency,
            "cache_ttl_medium_frequency": cache_ttl_medium_frequency,
            "cache_ttl_low_frequency": cache_ttl_low_frequency,
            "cache_ttl_static": cache_ttl_static,
            "cache_ttl_curve_data": cache_ttl_curve_data,
            "cache_ttl_metadata": cache_ttl_metadata,
            "cache_ttl_external_data": cache_ttl_external_data,
            "cache_ttl_scenario_data": cache_ttl_scenario_data,
        }

        api_metrics_enabled = env.get(f"{self.env_prefix}API_METRICS_ENABLED", "false").lower() in ("true", "1", "yes")
        api_metrics_path = env.get(f"{self.env_prefix}API_METRICS_PATH", "/metrics")

        # Concurrency controls
        concurrency_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_ENABLED"],
            True,
        )
        concurrency_defaults = {
            "max_concurrent_requests": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_MAX_CONCURRENT_REQUESTS"],
                100,
            ),
            "max_requests_per_second": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_MAX_REQUESTS_PER_SECOND"],
                10.0,
            ),
            "max_request_duration_seconds": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_MAX_REQUEST_DURATION_SECONDS"],
                float(self.api_request_timeout_seconds),
            ),
            "max_requests_per_tenant": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_MAX_REQUESTS_PER_TENANT"],
                20,
            ),
            "tenant_burst_limit": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_TENANT_BURST_LIMIT"],
                50,
            ),
            "tenant_queue_limit": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_TENANT_QUEUE_LIMIT"],
                64,
            ),
            "queue_timeout_seconds": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_QUEUE_TIMEOUT_SECONDS"],
                2.0,
            ),
            "burst_refill_per_second": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_BURST_REFILL_PER_SECOND"],
                0.5,
            ),
            "slow_start_initial_limit": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_SLOW_START_INITIAL_LIMIT"],
                2,
            ),
            "slow_start_step_seconds": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_SLOW_START_STEP_SECONDS"],
                3.0,
            ),
            "slow_start_step_size": self._get_int_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_SLOW_START_STEP_SIZE"],
                1,
            ),
            "slow_start_cooldown_seconds": self._get_float_from_env(
                env,
                [f"{self.env_prefix}API_CONCURRENCY_SLOW_START_COOLDOWN_SECONDS"],
                30.0,
            ),
            "offload_routes": [],
        }

        concurrency_field_casts = {
            "max_requests_per_tenant": int,
            "tenant_queue_limit": int,
            "tenant_burst_limit": int,
            "queue_timeout_seconds": float,
            "burst_refill_per_second": float,
            "slow_start_initial_limit": int,
            "slow_start_step_seconds": float,
            "slow_start_step_size": int,
            "slow_start_cooldown_seconds": float,
            "max_request_duration_seconds": float,
            "max_requests_per_second": float,
        }

        tenant_overrides_raw = env.get(
            f"{self.env_prefix}API_CONCURRENCY_TENANT_OVERRIDES",
            "",
        )
        tenant_overrides: Dict[str, Dict[str, Any]] = {}
        if tenant_overrides_raw:
            try:
                overrides_payload = json.loads(tenant_overrides_raw)
            except json.JSONDecodeError:
                logger.warning(
                    "Invalid JSON for %sAPI_CONCURRENCY_TENANT_OVERRIDES; ignoring overrides",
                    self.env_prefix,
                )
                overrides_payload = {}

            if isinstance(overrides_payload, dict):
                for tenant_key, override in overrides_payload.items():
                    if not isinstance(override, dict):
                        logger.warning(
                            "Tenant override for %s must be an object; skipping",
                            tenant_key,
                        )
                        continue
                    normalized: Dict[str, Any] = {}
                    for field_name, value in override.items():
                        caster = concurrency_field_casts.get(field_name)
                        if caster is None:
                            continue
                        try:
                            normalized[field_name] = caster(value)
                        except (TypeError, ValueError):
                            logger.warning(
                                "Invalid override value %r for %s on tenant %s; skipping",
                                value,
                                field_name,
                                tenant_key,
                            )
                    if normalized:
                        tenant_overrides[str(tenant_key)] = normalized
            elif tenant_overrides_raw:
                logger.warning(
                    "Tenant overrides must decode to a JSON object; received %r",
                    overrides_payload,
                )

        redis_url_default = env.get(f"{self.env_prefix}REDIS_URL", "redis://localhost:6379")
        redis_namespace_default = env.get(f"{self.env_prefix}REDIS_NAMESPACE", "aurum")
        offload_routes_raw = env.get(f"{self.env_prefix}API_CONCURRENCY_OFFLOAD_ROUTES", "")
        if offload_routes_raw:
            try:
                routes = json.loads(offload_routes_raw)
                if isinstance(routes, list):
                    concurrency_defaults["offload_routes"] = routes
                else:
                    logger.warning("Offload routes must decode to a JSON array; ignoring configuration")
            except json.JSONDecodeError:
                logger.warning("Invalid JSON for %sAPI_CONCURRENCY_OFFLOAD_ROUTES; ignoring", self.env_prefix)

        concurrency_redis_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_REDIS_ENABLED"],
            False,
        )
        concurrency_redis_url = env.get(
            f"{self.env_prefix}API_CONCURRENCY_REDIS_URL",
            redis_url_default,
        )
        concurrency_redis_namespace = env.get(
            f"{self.env_prefix}API_CONCURRENCY_REDIS_NAMESPACE",
            f"{redis_namespace_default}:api:concurrency",
        )
        concurrency_redis_poll = self._get_float_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_REDIS_POLL_INTERVAL_SECONDS"],
            0.05,
        )
        concurrency_redis_stale = self._get_float_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_REDIS_QUEUE_STALE_SECONDS"],
            concurrency_defaults["queue_timeout_seconds"],
        )
        concurrency_redis_ttl = self._get_float_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_REDIS_QUEUE_TTL_SECONDS"],
            max(concurrency_redis_stale * 10.0, 30.0),
        )
        concurrency_backpressure_threshold = self._get_float_from_env(
            env,
            [f"{self.env_prefix}API_CONCURRENCY_BACKPRESSURE_RATIO_THRESHOLD"],
            0.0,
        )
        concurrency_backpressure_header = env.get(
            f"{self.env_prefix}API_CONCURRENCY_BACKPRESSURE_HEADER",
            "X-Backpressure",
        )
        concurrency_redis_username = env.get(
            f"{self.env_prefix}API_CONCURRENCY_REDIS_USERNAME",
            env.get(f"{self.env_prefix}REDIS_USERNAME"),
        )
        concurrency_redis_password = env.get(
            f"{self.env_prefix}API_CONCURRENCY_REDIS_PASSWORD",
            env.get(f"{self.env_prefix}REDIS_PASSWORD"),
        )

        concurrency_namespace = SimpleNamespace(
            enabled=concurrency_enabled,
            tenant_overrides=tenant_overrides,
            redis_enabled=concurrency_redis_enabled,
            redis_url=concurrency_redis_url,
            redis_namespace=concurrency_redis_namespace,
            redis_poll_interval_seconds=concurrency_redis_poll,
            redis_queue_stale_seconds=concurrency_redis_stale,
            redis_queue_ttl_seconds=concurrency_redis_ttl,
            redis_username=concurrency_redis_username,
            redis_password=concurrency_redis_password,
            backpressure_ratio_threshold=concurrency_backpressure_threshold,
            backpressure_header=concurrency_backpressure_header,
            **concurrency_defaults,
        )

        # Database settings
        self.database_url = env.get(f"{self.env_prefix}DATABASE_URL", "postgresql://localhost/aurum")
        self.trino_host = env.get(f"{self.env_prefix}TRINO_HOST", "localhost")
        self.trino_port = int(env.get(f"{self.env_prefix}TRINO_PORT", "8080"))
        self.redis_url = env.get(f"{self.env_prefix}REDIS_URL", "redis://localhost:6379")
        self.trino_user = env.get(f"{self.env_prefix}TRINO_USER", "aurum")
        self.trino_password = env.get(f"{self.env_prefix}TRINO_PASSWORD", "")
        self.trino_catalog = env.get(f"{self.env_prefix}TRINO_CATALOG", "iceberg")
        self.trino_schema = env.get(f"{self.env_prefix}TRINO_SCHEMA", "market")
        self.trino_http_scheme = env.get(f"{self.env_prefix}TRINO_HTTP_SCHEME", "http")
        self.trino_hot_cache_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}TRINO_HOT_CACHE_ENABLED"],
            True,
        )
        self.trino_prepared_cache_metrics_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}TRINO_PREPARED_CACHE_METRICS_ENABLED"],
            True,
        )
        self.trino_trace_tagging_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}TRINO_TRACE_TAGGING_ENABLED"],
            True,
        )
        self.trino_metrics_label = env.get(f"{self.env_prefix}TRINO_METRICS_LABEL", "default")
        default_cache_ttl = int(env.get(f"{self.env_prefix}CACHE_TTL", "300"))
        self.database_timescale_dsn = env.get(f"{self.env_prefix}TIMESCALE_DSN", self.database_url)
        self.database_eia_series_base_table = env.get(f"{self.env_prefix}EIA_SERIES_BASE_TABLE", "iceberg.market.eia_series")

        # ClickHouse defaults
        self.clickhouse_host = env.get(f"{self.env_prefix}CLICKHOUSE_HOST", "localhost")
        self.clickhouse_port = self._get_int_from_env(
            env,
            [f"{self.env_prefix}CLICKHOUSE_PORT"],
            9000,
        )
        self.clickhouse_user = env.get(f"{self.env_prefix}CLICKHOUSE_USER", "aurum")
        self.clickhouse_password = env.get(f"{self.env_prefix}CLICKHOUSE_PASSWORD", "")
        self.clickhouse_database = env.get(f"{self.env_prefix}CLICKHOUSE_DATABASE", "aurum")

        # Timescale defaults
        self.timescale_host = env.get(f"{self.env_prefix}TIMESCALE_HOST", "localhost")
        self.timescale_port = self._get_int_from_env(
            env,
            [f"{self.env_prefix}TIMESCALE_PORT"],
            5432,
        )
        self.timescale_user = env.get(f"{self.env_prefix}TIMESCALE_USER", "aurum")
        self.timescale_password = env.get(f"{self.env_prefix}TIMESCALE_PASSWORD", "")
        self.timescale_database = env.get(f"{self.env_prefix}TIMESCALE_DATABASE", "aurum")

        # Redis tuning
        redis_mode_value = env.get(f"{self.env_prefix}REDIS_MODE", RedisMode.STANDALONE.value).lower()
        try:
            self.redis_mode = RedisMode(redis_mode_value)
        except ValueError:
            self.redis_mode = redis_mode_value
        self.redis_sentinel_endpoints = self._split_env_list(env.get(f"{self.env_prefix}REDIS_SENTINELS", ""))
        self.redis_sentinel_master = env.get(f"{self.env_prefix}REDIS_SENTINEL_MASTER")
        self.redis_cluster_nodes = self._split_env_list(env.get(f"{self.env_prefix}REDIS_CLUSTER_NODES", ""))
        self.redis_username = env.get(f"{self.env_prefix}REDIS_USERNAME")
        self.redis_password = env.get(f"{self.env_prefix}REDIS_PASSWORD")
        self.redis_namespace = env.get(f"{self.env_prefix}REDIS_NAMESPACE", "aurum")
        self.redis_db = int(env.get(f"{self.env_prefix}REDIS_DB", "0"))
        self.redis_socket_timeout = float(env.get(f"{self.env_prefix}REDIS_SOCKET_TIMEOUT", "1.5"))
        self.redis_connect_timeout = float(env.get(f"{self.env_prefix}REDIS_CONNECT_TIMEOUT", "1.5"))

        # Performance settings
        self.max_workers = int(env.get(f"{self.env_prefix}MAX_WORKERS", "4"))
        self.request_timeout = float(env.get(f"{self.env_prefix}REQUEST_TIMEOUT", "30.0"))
        self.cache_ttl = default_cache_ttl
        self.redis_ttl_seconds = int(env.get(f"{self.env_prefix}REDIS_TTL_SECONDS", str(default_cache_ttl)))
        # Security settings
        self.auth_enabled = env.get(f"{self.env_prefix}AUTH_ENABLED", "false").lower() in ("true", "1", "yes")
        self.jwt_secret = env.get(f"{self.env_prefix}JWT_SECRET", "")
        self.admin_emails = self._split_env_list(env.get(f"{self.env_prefix}ADMIN_EMAILS", ""))

        # Telemetry settings
        self.service_name = env.get(f"{self.env_prefix}OTEL_SERVICE_NAME", "aurum")

        # Structured API settings for simplified mode
        self.api = SimpleNamespace(
            api_title=self.api_title,
            title=self.api_title,
            version=self.api_version,
            request_timeout_seconds=self.api_request_timeout_seconds,
            host=self.api_host,
            port=self.api_port,
            cors_origins=self.api_cors_origins,
            gzip_min_bytes=self.gzip_min_bytes,
            cache=SimpleNamespace(**api_cache_defaults),
            metrics=SimpleNamespace(enabled=api_metrics_enabled, path=api_metrics_path),
            concurrency=concurrency_namespace,
            rate_limit=SimpleNamespace(enabled=False, tenant_overrides={}),
            admin_guard_enabled=self._get_bool_from_env(
                env,
                [f"{self.env_prefix}API_ADMIN_GUARD_ENABLED"],
                False,
            ),
            dimensions_table_trino=env.get(
                f"{self.env_prefix}API_DIMENSIONS_TABLE_TRINO",
                "iceberg.market.curve_observation",
            ),
            dimensions_table_clickhouse=env.get(
                f"{self.env_prefix}API_DIMENSIONS_TABLE_CLICKHOUSE",
                "aurum.curve_observation",
            ),
            dimensions_table_timescale=env.get(
                f"{self.env_prefix}API_DIMENSIONS_TABLE_TIMESCALE",
                "market.curve_observation",
            ),
        )

        if isinstance(self.api.cache, SimpleNamespace):
            # Provide additional aliases expected in legacy code paths
            cache_ns = self.api.cache
            cache_ns.ttl_seconds = cache_ns.curve_ttl

        self.redis = SimpleNamespace(
            url=self.redis_url,
            ttl_seconds=self.redis_ttl_seconds,
            mode=self.redis_mode,
            sentinel_endpoints=self.redis_sentinel_endpoints,
            sentinel_master=self.redis_sentinel_master,
            cluster_nodes=self.redis_cluster_nodes,
            username=self.redis_username,
            password=self.redis_password,
            namespace=self.redis_namespace,
            db=self.redis_db,
            socket_timeout=self.redis_socket_timeout,
            connect_timeout=self.redis_connect_timeout,
        )

        self.database = SimpleNamespace(
            url=self.database_url,
            timescale_dsn=self.database_timescale_dsn,
            eia_series_base_table=self.database_eia_series_base_table,
        )

        self.trino = SimpleNamespace(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            password=self.trino_password or None,
            http_scheme=self.trino_http_scheme,
            catalog=self.trino_catalog,
            database_schema=self.trino_schema,
            hot_cache_enabled=self.trino_hot_cache_enabled,
            prepared_cache_metrics_enabled=self.trino_prepared_cache_metrics_enabled,
            trace_tagging_enabled=self.trino_trace_tagging_enabled,
            metrics_label=self.trino_metrics_label,
        )

        backend_type_value = env.get(f"{self.env_prefix}API_BACKEND", DataBackendType.TRINO.value)
        pool_min_size = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}DATA_BACKEND_POOL_MIN_SIZE",
                f"{self.env_prefix}DATA_BACKEND_CONNECTION_POOL_MIN_SIZE",
                f"{self.env_prefix}CONNECTION_POOL_MIN_SIZE",
            ],
            5,
        )
        pool_max_size = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}DATA_BACKEND_POOL_MAX_SIZE",
                f"{self.env_prefix}DATA_BACKEND_CONNECTION_POOL_MAX_SIZE",
                f"{self.env_prefix}CONNECTION_POOL_MAX_SIZE",
            ],
            20,
        )
        pool_max_idle_time = self._get_int_from_env(
            env,
            [
                f"{self.env_prefix}DATA_BACKEND_POOL_MAX_IDLE_TIME",
                f"{self.env_prefix}DATA_BACKEND_CONNECTION_POOL_MAX_IDLE_TIME",
            ],
            300,
        )
        pool_timeout = self._get_float_from_env(
            env,
            [
                f"{self.env_prefix}DATA_BACKEND_POOL_TIMEOUT_SECONDS",
                f"{self.env_prefix}DATA_BACKEND_CONNECTION_TIMEOUT_SECONDS",
            ],
            30.0,
        )
        pool_acquire_timeout = self._get_float_from_env(
            env,
            [
                f"{self.env_prefix}DATA_BACKEND_POOL_ACQUIRE_TIMEOUT_SECONDS",
                f"{self.env_prefix}DATA_BACKEND_CONNECTION_ACQUIRE_TIMEOUT_SECONDS",
            ],
            10.0,
        )

        self.data_backend = DataBackendConfig(
            backend_type=backend_type_value,
            trino_host=self.trino_host,
            trino_port=self.trino_port,
            trino_catalog=self.trino_catalog,
            trino_database_schema=self.trino_schema,
            trino_user=self.trino_user,
            trino_password=self.trino_password or None,
            clickhouse_host=self.clickhouse_host,
            clickhouse_port=self.clickhouse_port,
            clickhouse_database=self.clickhouse_database,
            clickhouse_user=self.clickhouse_user,
            clickhouse_password=self.clickhouse_password or None,
            timescale_host=self.timescale_host,
            timescale_port=self.timescale_port,
            timescale_database=self.timescale_database,
            timescale_user=self.timescale_user,
            timescale_password=self.timescale_password or None,
            connection_pool_min_size=pool_min_size,
            connection_pool_max_size=pool_max_size,
            connection_pool_max_idle_time=pool_max_idle_time,
            connection_pool_timeout_seconds=pool_timeout,
            connection_pool_acquire_timeout_seconds=pool_acquire_timeout,
        )

        offload_enabled = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}API_OFFLOAD_ENABLED"],
            False,
        )
        default_stub = self.environment.lower() in {"development", "dev", "local"}
        offload_use_stub = self._get_bool_from_env(
            env,
            [f"{self.env_prefix}API_OFFLOAD_USE_STUB"],
            default_stub or not offload_enabled,
        )
        offload_broker = env.get(
            f"{self.env_prefix}API_OFFLOAD_CELERY_BROKER_URL",
            env.get("AURUM_CELERY_BROKER_URL", "redis://localhost:6379/0"),
        )
        offload_backend = env.get(
            f"{self.env_prefix}API_OFFLOAD_CELERY_RESULT_BACKEND",
            env.get("AURUM_CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
        )
        offload_default_queue = env.get(
            f"{self.env_prefix}API_OFFLOAD_DEFAULT_QUEUE",
            env.get("AURUM_CELERY_TASK_DEFAULT_QUEUE", "default"),
        )

        self.telemetry = SimpleNamespace(service_name=self.service_name)
        self.auth = SimpleNamespace(enabled=self.auth_enabled)
        self.pagination = SimpleNamespace(default_page_size=100, max_page_size=1000)
        self.messaging = SimpleNamespace(enabled=False)
        self.async_offload = SimpleNamespace(
            enabled=offload_enabled,
            use_stub=offload_use_stub,
            broker_url=offload_broker,
            result_backend=offload_backend,
            default_queue=offload_default_queue,
        )

    def _split_env_list(self, value: str, separator: str = ",") -> List[str]:
        """Split environment variable list."""
        if not value or value == "*":
            return []
        return [item.strip() for item in value.split(separator) if item.strip()]

    def get(self, key: str, default: Any = None) -> Any:
        """Get setting value with optional default."""
        return getattr(self, key, default)

    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment.lower() in ("development", "dev", "local")

    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment.lower() in ("production", "prod")

    @classmethod
    def from_env(cls, env_prefix: str = "AURUM_") -> "SimplifiedSettings":
        """Create settings from environment."""
        return cls(env_prefix=env_prefix)


class HybridAurumSettings:
    """Hybrid settings system that can use either legacy or simplified based on feature flags."""

    def __init__(self, env_prefix: str = "AURUM_"):
        self.env_prefix = env_prefix
        self._migration_phase = get_migration_phase()
        self._use_simplified = is_feature_enabled(FEATURE_FLAGS["USE_SIMPLIFIED_SETTINGS"])

        # Initialize both systems for hybrid mode
        self._legacy_settings = None
        self._simplified_settings = None

        start_time = self._current_time_ms()
        self._initialize_settings()
        duration = self._current_time_ms() - start_time

        # Record metrics
        if get_migration_metrics() is not None:
            _migration_metrics.record_settings_call(
                "simplified" if self._use_simplified else "legacy",
                duration
            )

    def _current_time_ms(self) -> float:
        """Get current time in milliseconds."""
        return SimplifiedSettings._get_time_ms()

    def _initialize_settings(self):
        """Initialize the appropriate settings system."""
        if self._use_simplified or self._migration_phase in ("simplified", "hybrid"):
            self._simplified_settings = SimplifiedSettings(self.env_prefix)
            self._delegate_to_simplified()
        else:
            # Legacy mode - would import and use complex settings
            self._delegate_to_legacy()

    def __getattr__(self, name):
        """Dynamic attribute delegation to the active settings system."""
        if self._use_simplified and self._simplified_settings:
            value = getattr(self._simplified_settings, name)
            logger.debug(f"Simplified fallback for '{name}': {value}")
            return value
        else:
            # Fallback to legacy behavior with sensible defaults
            if name == "api_title":
                value = "Aurum API"
                logger.debug(f"Legacy fallback for '{name}': {value}")
                return value
            elif name == "version":
                value = "1.0.0"
                logger.debug(f"Legacy fallback for '{name}': {value}")
                return value
            elif name == "request_timeout_seconds":
                value = 30
                logger.debug(f"Legacy fallback for '{name}': {value}")
                return value
            elif name == "gzip_min_bytes":
                try:
                    value = int(os.environ.get("API_GZIP_MIN_BYTES", os.environ.get(f"{self.env_prefix}API_GZIP_MIN_BYTES", "500")))
                except ValueError:
                    value = 500
                logger.debug(f"Legacy fallback for '{name}': {value}")
                return value
            elif name == "service_name":
                value = "aurum"
                logger.debug(f"Legacy fallback for '{name}': {value}")
                return value
            else:
                logger.debug(f"No fallback defined for '{name}', returning None")
                return None

    def _delegate_to_simplified(self):
        """Set up delegation to simplified settings."""
        if self._simplified_settings:
            for attr in (
                "api",
                "data_backend",
                "trino",
                "redis",
                "database",
                "auth",
                "messaging",
                "telemetry",
                "pagination",
                "async_offload",
            ):
                setattr(self, attr, getattr(self._simplified_settings, attr, None))

    def _delegate_to_legacy(self):
        """Delegate to legacy settings system."""
        # This would import and use the complex settings system
        # For now, create a minimal stub
        logger.warning("Using legacy settings system - consider migrating to simplified version")

        simplified = SimplifiedSettings(self.env_prefix)
        self._simplified_settings = simplified
        self._delegate_to_simplified()

        # Set basic attributes that the rest of the system expects using simplified defaults
        self.environment = simplified.environment
        self.debug = simplified.debug
        self.database_url = simplified.database_url
        self.gzip_min_bytes = getattr(simplified.api, "gzip_min_bytes", 500)

    def get_migration_status(self) -> Dict[str, Any]:
        """Get migration status for this settings instance."""
        return {
            "using_simplified": self._use_simplified,
            "migration_phase": self._migration_phase,
            "has_simplified": self._simplified_settings is not None,
            "has_legacy": self._legacy_settings is not None,
        }

    def switch_to_simplified(self) -> bool:
        """Switch to simplified settings system."""
        if self._simplified_settings:
            self._use_simplified = True
            self._delegate_to_simplified()
            logger.info("Switched to simplified settings system")
            return True
        return False

    def switch_to_legacy(self) -> bool:
        """Switch to legacy settings system."""
        if self._legacy_settings or self._simplified_settings:
            self._use_simplified = False
            self._delegate_to_legacy()
            logger.info("Switched to legacy settings system")
            return True
        return False

    @classmethod
    def from_env(
        cls,
        *,
        vault_mapping: Mapping[str, Any] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> "HybridAurumSettings":
        """Factory method with vault and override support."""
        instance = cls()

        # Apply vault mappings if provided
        if vault_mapping:
            for key, value in vault_mapping.items():
                setattr(instance, key.lower(), value)

        # Apply overrides
        if overrides:
            for key, value in overrides.items():
                setattr(instance, key.lower(), value)

        return instance


# Legacy compatibility - use hybrid system
class AurumSettings(HybridAurumSettings):
    """Legacy settings class for backward compatibility - now uses hybrid system."""

    def __init__(self, env_prefix: str = "AURUM_"):
        super().__init__(env_prefix)


# Global settings instance
_settings_instance: AurumSettings | None = None


def get_settings() -> AurumSettings:
    """Return the configured settings instance, raising if unavailable."""
    global _settings_instance
    if _settings_instance is not None:
        return _settings_instance

    # Allow opt-in lazy defaulting in tests/dev only
    if os.getenv("AURUM_TEST_DEFAULT_SETTINGS", "0").lower() in ("1", "true", "yes"):
        _settings_instance = AurumSettings()
        return _settings_instance

    raise RuntimeError("AurumSettings have not been configured")


def configure_settings(settings: AurumSettings) -> None:
    """Configure the global settings instance."""
    global _settings_instance
    _settings_instance = settings


def get_migration_metrics() -> Optional[MigrationMetrics]:
    """Get the global migration metrics instance."""
    # Only initialize migration metrics if not during module import
    if _migration_metrics is None:
        _init_migration_metrics()
    return _migration_metrics


def log_migration_status() -> None:
    """Log current migration status."""
    if get_migration_metrics() is not None:
        status = _migration_metrics.get_migration_status()
        logger.info("Migration Status", extra={"migration": status})

    if is_feature_enabled(FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"]):
        logger.info("Migration monitoring enabled")
    else:
        logger.info("Migration monitoring disabled")


def validate_migration_health() -> Dict[str, Any]:
    """Validate migration health and return status."""
    migration_metrics = get_migration_metrics()
    if migration_metrics is None:
        return {"healthy": True, "issues": [], "monitoring_disabled": True}
    metrics = migration_metrics._metrics
    health = {"healthy": True, "issues": []}

    # Check settings migration
    settings_metrics = metrics["settings_migration"]
    if settings_metrics["errors"] > 10:
        health["issues"].append("High error rate in settings migration")
        health["healthy"] = False

    # Check performance
    if settings_metrics["performance_ms"]:
        avg_performance = sum(settings_metrics["performance_ms"]) / len(settings_metrics["performance_ms"])
        if avg_performance > 100:  # 100ms threshold
            health["issues"].append(f"Settings performance degraded: {avg_performance:.2f}ms avg")
            health["healthy"] = False

    return health


# Add migration management functions
def advance_migration_phase(component: str = "settings", phase: str = "hybrid") -> bool:
    """Advance migration phase for a component."""
    if component == "settings":
        _set_flag_env(FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"], phase)
        if get_migration_metrics() is not None:
            _migration_metrics.set_migration_phase("settings_migration", phase)
        logger.info(f"Advanced settings migration to phase: {phase}")
        return True
    return False


def rollback_migration_phase(component: str = "settings") -> bool:
    """Rollback migration phase for a component."""
    current_phase = get_migration_phase(component)
    if current_phase == "simplified":
        advance_migration_phase(component, "hybrid")
        logger.warning(f"Rolled back settings migration from simplified to hybrid")
        return True
    elif current_phase == "hybrid":
        advance_migration_phase(component, "legacy")
        logger.warning(f"Rolled back settings migration from hybrid to legacy")
        return True
    return False


# Audit settings classes
class AuditSinkType:
    """Types of audit sinks supported."""
    KAFKA = "kafka"
    CLICKHOUSE = "clickhouse"
    FILE = "file"
    STDOUT = "stdout"


class AuditKafkaSettings:
    """Settings for Kafka audit sink."""
    def __init__(self):
        self.bootstrap_servers = os.getenv("AURUM_AUDIT_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("AURUM_AUDIT_KAFKA_TOPIC", "aurum-audit")
        self.client_id = os.getenv("AURUM_AUDIT_KAFKA_CLIENT_ID", "aurum-audit-producer")


class AuditClickHouseSettings:
    """Settings for ClickHouse audit sink."""
    def __init__(self):
        self.host = os.getenv("AURUM_AUDIT_CLICKHOUSE_HOST", "localhost")
        self.port = int(os.getenv("AURUM_AUDIT_CLICKHOUSE_PORT", "8123"))
        self.database = os.getenv("AURUM_AUDIT_CLICKHOUSE_DATABASE", "aurum_audit")
        self.table = os.getenv("AURUM_AUDIT_CLICKHOUSE_TABLE", "audit_events")
        self.username = os.getenv("AURUM_AUDIT_CLICKHOUSE_USERNAME", "aurum")
        self.password = os.getenv("AURUM_AUDIT_CLICKHOUSE_PASSWORD", "")


class ExternalAuditSettings:
    """Settings for external audit logging with override support."""

    def __init__(
        self,
        *,
        enabled: Optional[bool] = None,
        sink_type: Optional[str] = None,
        log_dir: Optional[str] = None,
        sinks: Optional[Iterable[str]] = None,
        kafka_settings: Optional[AuditKafkaSettings] = None,
        clickhouse_settings: Optional[AuditClickHouseSettings] = None,
    ) -> None:
        env_enabled = os.getenv("AURUM_EXTERNAL_AUDIT_ENABLED", "true")
        self.enabled = enabled if enabled is not None else env_enabled.lower() in ("true", "1", "yes")

        env_sink_type = os.getenv("AURUM_EXTERNAL_AUDIT_SINK_TYPE", AuditSinkType.FILE)
        self.sink_type = self._normalize_sink(sink_type or env_sink_type)

        self.log_dir = log_dir or os.getenv("AURUM_EXTERNAL_AUDIT_LOG_DIR", "/var/log/aurum/audit")

        if sinks is None:
            sinks_str = os.getenv("AURUM_EXTERNAL_AUDIT_SINKS", "file,stdout")
            parsed = self._parse_sinks(sinks_str)
        else:
            parsed = [self._normalize_sink(value) for value in sinks]
        self.sinks = tuple(parsed)

        self.kafka_settings = kafka_settings or AuditKafkaSettings()
        self.clickhouse_settings = clickhouse_settings or AuditClickHouseSettings()

    @staticmethod
    def _normalize_sink(value: str) -> str:
        """Normalise sink identifiers to lowercase strings."""
        return str(value).strip().lower()

    def _parse_sinks(self, sinks_str: str) -> List[str]:
        """Parse sink configuration string into a list of sink identifiers."""
        return [self._normalize_sink(sink) for sink in sinks_str.split(",") if sink.strip()]


__all__ = [
    "AurumSettings",
    "HybridAurumSettings",
    "SimplifiedSettings",
    "MigrationMetrics",
    "get_settings",
    "configure_settings",
    "get_migration_metrics",
    "log_migration_status",
    "validate_migration_health",
    "advance_migration_phase",
    "rollback_migration_phase",
    "is_feature_enabled",
    "get_migration_phase",
    "FEATURE_FLAGS",
    "get_flag_env",
    "ExternalAuditSettings",
    "AuditSinkType",
    "AuditKafkaSettings",
    "AuditClickHouseSettings",
    "DataBackendType",
    "DataBackendConfig",
]
