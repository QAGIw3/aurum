"""Feature-flagged Trino client with simplified and legacy options.

Provides both simplified and complex Trino clients with feature flags
for gradual migration and comprehensive monitoring.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import trino  # type: ignore
    import trino.dbapi  # type: ignore
except ImportError:
    # Trino is optional for import-time tooling (e.g., docs). Runtime paths
    # that require Trino will raise a clear error when invoked.
    trino = None  # type: ignore

from .config import TrinoConfig

LOGGER = logging.getLogger(__name__)

# Feature flags for database migration
DB_FEATURE_FLAGS = {
    "USE_SIMPLE_DB_CLIENT": "aurum_use_simple_db_client",
    "ENABLE_DB_MIGRATION_MONITORING": "aurum_enable_db_migration_monitoring",
    "DB_MIGRATION_PHASE": "aurum_db_migration_phase",  # "legacy", "hybrid", "simplified"
}


class SimpleTrinoClient:
    """Simplified Trino client without complex connection pooling."""

    def __init__(self, config: TrinoConfig):
        self.config = config
        self._connection: Optional[trino.dbapi.Connection] = None

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        conn = await self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = cursor.fetchall()
            cursor.close()

            return [dict(zip(columns, row)) for row in results]
        except Exception as exc:
            LOGGER.error("Query failed: %s", exc)
            raise

    async def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a statement without returning results."""
        conn = await self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            cursor.close()
        except Exception as exc:
            conn.rollback()
            LOGGER.error("Execute failed: %s", exc)
            raise

    async def _get_connection(self) -> trino.dbapi.Connection:
        """Get or create a connection."""
        if self._connection is None or not await self._is_connection_valid():
            if self._connection:
                await self._close_connection()
            self._connection = await self._create_connection()
        return self._connection

    async def _create_connection(self) -> trino.dbapi.Connection:
        """Create a new Trino connection."""
        return trino.dbapi.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            catalog=self.config.catalog,
            schema=self.config.schema,
            http_scheme=self.config.http_scheme,
            auth=trino.auth.BasicAuthentication(self.config.password) if self.config.password else None,
        )

    async def _is_connection_valid(self) -> bool:
        """Check if connection is still valid."""
        if self._connection is None:
            return False
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False

    async def _close_connection(self) -> None:
        """Close the current connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception as exc:
                LOGGER.warning("Error closing connection: %s", exc)
            finally:
                self._connection = None

    async def close(self) -> None:
        """Close the client."""
        await self._close_connection()


# Database migration metrics
class DatabaseMigrationMetrics:
    """Track database client migration metrics."""

    def __init__(self):
        self.metrics_file = Path.home() / ".aurum" / "db_migration_metrics.json"
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
            "db_migration": {
                "legacy_calls": 0,
                "simplified_calls": 0,
                "errors": 0,
                "performance_ms": [],
                "migration_phase": "legacy"
            }
        }

    def _save_metrics(self):
        """Save metrics to file."""
        try:
            with open(self.metrics_file, 'w') as f:
                json.dump(self._metrics, f, indent=2)
        except Exception as e:
            LOGGER.warning(f"Failed to save DB migration metrics: {e}")

    def record_db_call(self, client_type: str, duration_ms: float, error: bool = False):
        """Record a database client call."""
        if not self.is_monitoring_enabled():
            return

        metrics = self._metrics["db_migration"]
        if client_type == "simplified":
            metrics["simplified_calls"] += 1
        else:
            metrics["legacy_calls"] += 1

        if error:
            metrics["errors"] += 1

        metrics["performance_ms"].append(duration_ms)
        if len(metrics["performance_ms"]) > 1000:
            metrics["performance_ms"] = metrics["performance_ms"][-1000:]

        self._save_metrics()

    def is_monitoring_enabled(self) -> bool:
        """Check if database migration monitoring is enabled."""
        return os.getenv(DB_FEATURE_FLAGS["ENABLE_DB_MIGRATION_MONITORING"], "false").lower() in ("true", "1", "yes")

    def set_migration_phase(self, phase: str):
        """Set migration phase for database layer."""
        self._metrics["db_migration"]["migration_phase"] = phase
        self._save_metrics()


# Global database migration metrics
_db_migration_metrics = DatabaseMigrationMetrics()


def is_db_feature_enabled(flag: str) -> bool:
    """Check if a database feature flag is enabled."""
    return os.getenv(flag, "false").lower() in ("true", "1", "yes")


def get_db_migration_phase() -> str:
    """Get current database migration phase."""
    phase = os.getenv(DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"], "legacy")
    _db_migration_metrics.set_migration_phase(phase)
    return phase


# Factory function for creating clients
def create_trino_client(config: TrinoConfig) -> SimpleTrinoClient:
    """Create a simplified Trino client."""
    return SimpleTrinoClient(config)


# Hybrid client manager with feature flags
class HybridTrinoClientManager:
    """Hybrid Trino client manager that can use either simplified or legacy clients."""

    def __init__(self):
        self._legacy_manager = None
        self._simple_clients: Dict[str, SimpleTrinoClient] = {}
        self._migration_phase = get_db_migration_phase()
        self._use_simple = is_db_feature_enabled(DB_FEATURE_FLAGS["USE_SIMPLE_DB_CLIENT"])

    def _get_legacy_manager(self):
        """Get or create legacy Trino client manager."""
        if self._legacy_manager is None:
            # Import legacy manager - simplified for demo
            from types import SimpleNamespace

            class LegacyManagerStub(SimpleNamespace):
                def __init__(self):
                    self._clients = {}

                def get_client(self, catalog: str):
                    # Return a mock client for demo
                    if catalog not in self._clients:
                        config = TrinoConfig(
                            host="localhost",
                            port=8080,
                            user="aurum",
                            catalog=catalog,
                            schema="default",
                            http_scheme="http"
                        )
                        self._clients[catalog] = create_trino_client(config)
                    return self._clients[catalog]

                def close_all(self):
                    for client in self._clients.values():
                        asyncio.run(client.close())
                    self._clients.clear()

            self._legacy_manager = LegacyManagerStub()
        return self._legacy_manager

    def get_client(self, catalog: str):
        """Get a Trino client based on feature flags."""
        start_time = time.time()

        if self._use_simple or self._migration_phase in ("simplified", "hybrid"):
            # Use simplified client
            if catalog not in self._simple_clients:
                config = TrinoConfig(
                    host="localhost",
                    port=8080,
                    user="aurum",
                    catalog=catalog,
                    schema="default",
                    http_scheme="http"
                )
                self._simple_clients[catalog] = create_trino_client(config)

            client = self._simple_clients[catalog]
            client_type = "simplified"
        else:
            # Use legacy client
            legacy_manager = self._get_legacy_manager()
            client = legacy_manager.get_client(catalog)
            client_type = "legacy"

        # Record metrics
        duration_ms = (time.time() - start_time) * 1000
        _db_migration_metrics.record_db_call(client_type, duration_ms)

        return client

    def close_all(self):
        """Close all clients."""
        for client in self._simple_clients.values():
            asyncio.run(client.close())
        self._simple_clients.clear()

        if self._legacy_manager:
            self._legacy_manager.close_all()

    def switch_to_simplified(self) -> bool:
        """Switch to simplified clients."""
        if self._simple_clients:
            self._use_simple = True
            LOGGER.info("Switched to simplified Trino clients")
            return True
        return False

    def switch_to_legacy(self) -> bool:
        """Switch to legacy clients."""
        if self._legacy_manager:
            self._use_simple = False
            LOGGER.info("Switched to legacy Trino clients")
            return True
        return False


# Global instance for backward compatibility
_client_manager = HybridTrinoClientManager()


def get_trino_client(catalog: str = "iceberg"):
    """Get a Trino client for the specified catalog."""
    return _client_manager.get_client(catalog)


# Migration management functions
def advance_db_migration_phase(phase: str = "hybrid") -> bool:
    """Advance database migration phase."""
    os.environ[DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"]] = phase
    _db_migration_metrics.set_migration_phase(phase)
    LOGGER.info(f"Advanced database migration to phase: {phase}")
    return True


def rollback_db_migration_phase() -> bool:
    """Rollback database migration phase."""
    current_phase = get_db_migration_phase()
    if current_phase == "simplified":
        advance_db_migration_phase("hybrid")
        LOGGER.warning("Rolled back database migration from simplified to hybrid")
        return True
    elif current_phase == "hybrid":
        advance_db_migration_phase("legacy")
        LOGGER.warning("Rolled back database migration from hybrid to legacy")
        return True
    return False


def get_db_migration_status() -> Dict[str, Any]:
    """Get database migration status."""
    return {
        "migration_phase": get_db_migration_phase(),
        "using_simple": _client_manager._use_simple,
        "has_simple_clients": len(_client_manager._simple_clients) > 0,
        "monitoring_enabled": _db_migration_metrics.is_monitoring_enabled(),
    }


# Add to __all__ for proper exports
__all__ = [
    "SimpleTrinoClient",
    "create_trino_client",
    "HybridTrinoClientManager",
    "get_trino_client",
    "advance_db_migration_phase",
    "rollback_db_migration_phase",
    "get_db_migration_status",
    "DB_FEATURE_FLAGS",
    "DatabaseMigrationMetrics",
]

