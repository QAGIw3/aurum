"""Feature-flagged configuration system for Aurum services with gradual migration support."""
from __future__ import annotations

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

# Setup logging
logger = logging.getLogger(__name__)

# Feature flags for migration
FEATURE_FLAGS = {
    "USE_SIMPLIFIED_SETTINGS": "aurum_use_simplified_settings",
    "ENABLE_MIGRATION_MONITORING": "aurum_enable_migration_monitoring",
    "SETTINGS_MIGRATION_PHASE": "aurum_settings_migration_phase",  # "legacy", "hybrid", "simplified"
}


class MigrationMetrics:
    """Track migration metrics and health."""

    def __init__(self):
        self.metrics_file = Path.home() / ".aurum" / "migration_metrics.json"
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_metrics()

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
        return os.getenv(FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"], "false").lower() in ("true", "1", "yes")

    def set_migration_phase(self, component: str, phase: str):
        """Set migration phase for a component."""
        if component in self._metrics:
            self._metrics[component]["migration_phase"] = phase
            self._save_metrics()


# Global migration metrics instance
_migration_metrics = MigrationMetrics()


def is_feature_enabled(flag: str) -> bool:
    """Check if a feature flag is enabled."""
    value = os.getenv(flag, "false").lower()
    return value in ("true", "1", "yes")


def get_migration_phase(component: str = "settings") -> str:
    """Get current migration phase for a component."""
    phase = os.getenv(FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"], "legacy")
    _migration_metrics.set_migration_phase("settings_migration", phase)
    return phase


class SimplifiedSettings:
    """Simplified, focused settings system."""

    def __init__(self, env_prefix: str = "AURUM_"):
        self.env_prefix = env_prefix
        self._cache: Dict[str, Any] = {}
        start_time = self._current_time_ms()
        self._load_from_env()
        duration = self._current_time_ms() - start_time
        _migration_metrics.record_settings_call("simplified", duration)

    def _current_time_ms(self) -> float:
        """Get current time in milliseconds."""
        return self._get_time_ms()

    @staticmethod
    def _get_time_ms() -> float:
        """Get current time in milliseconds."""
        return float(os.times()[4] * 1000) if hasattr(os, 'times') else 0.0

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

        # Database settings
        self.database_url = env.get(f"{self.env_prefix}DATABASE_URL", "postgresql://localhost/aurum")
        self.trino_host = env.get(f"{self.env_prefix}TRINO_HOST", "localhost")
        self.trino_port = int(env.get(f"{self.env_prefix}TRINO_PORT", "8080"))
        self.redis_url = env.get(f"{self.env_prefix}REDIS_URL", "redis://localhost:6379")

        # Security settings
        self.auth_enabled = env.get(f"{self.env_prefix}AUTH_ENABLED", "false").lower() in ("true", "1", "yes")
        self.jwt_secret = env.get(f"{self.env_prefix}JWT_SECRET", "")
        self.admin_emails = self._split_env_list(env.get(f"{self.env_prefix}ADMIN_EMAILS", ""))

        # Performance settings
        self.max_workers = int(env.get(f"{self.env_prefix}MAX_WORKERS", "4"))
        self.request_timeout = float(env.get(f"{self.env_prefix}REQUEST_TIMEOUT", "30.0"))
        self.cache_ttl = int(env.get(f"{self.env_prefix}CACHE_TTL", "300"))

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
            return getattr(self._simplified_settings, name)
        else:
            # Fallback to legacy behavior
            return getattr(self, f'_legacy_{name}', None)

    def _delegate_to_simplified(self):
        """Set up delegation to simplified settings."""
        if self._simplified_settings:
            # Add nested structure for backward compatibility
            self.api = self
            self.data_backend = self
            self.trino = self
            self.redis = self
            self.database = self
            self.auth = self
            self.messaging = self
            self.telemetry = self
            self.pagination = self

    def _delegate_to_legacy(self):
        """Delegate to legacy settings system."""
        # This would import and use the complex settings system
        # For now, create a minimal stub
        logger.warning("Using legacy settings system - consider migrating to simplified version")

        # Set basic attributes that the rest of the system expects
        env = os.environ
        self.environment = env.get(f"{self.env_prefix}ENV", "development")
        self.debug = env.get(f"{self.env_prefix}DEBUG", "false").lower() in ("true", "1", "yes")
        self.database_url = env.get(f"{self.env_prefix}DATABASE_URL", "postgresql://localhost/aurum")

        # Add nested structure
        self.api = self
        self.data_backend = self
        self.trino = self
        self.redis = self
        self.database = self
        self.auth = self
        self.messaging = self
        self.telemetry = self
        self.pagination = self

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
    if _settings_instance is None:  # pragma: no cover - defensive guard
        raise RuntimeError("AurumSettings have not been configured")
    return _settings_instance


def configure_settings(settings: AurumSettings) -> None:
    """Configure the global settings instance."""
    global _settings_instance
    _settings_instance = settings


def get_migration_metrics() -> MigrationMetrics:
    """Get the global migration metrics instance."""
    return _migration_metrics


def log_migration_status() -> None:
    """Log current migration status."""
    status = _migration_metrics.get_migration_status()
    logger.info("Migration Status", extra={"migration": status})

    if is_feature_enabled(FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"]):
        logger.info("Migration monitoring enabled")
    else:
        logger.info("Migration monitoring disabled")


def validate_migration_health() -> Dict[str, Any]:
    """Validate migration health and return status."""
    metrics = _migration_metrics._metrics
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
        os.environ[FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"]] = phase
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
]
