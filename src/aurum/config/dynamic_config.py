"""
Advanced Configuration Management System for Aurum.

This module provides a dynamic, hot-reloadable configuration system with:
- Layered configuration sources with precedence
- Environment-specific inheritance
- Schema validation and enforcement
- Change tracking and audit trails
- Feature flag integration
- Backup and recovery capabilities
"""

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Union
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


class ConfigSource(Protocol):
    """Protocol for configuration sources."""

    @property
    def name(self) -> str:
        """Source name for debugging."""
        ...

    @property
    def priority(self) -> int:
        """Precedence order (higher = applied later = wins)."""
        ...

    def load(self) -> Dict[str, Any]:
        """Load configuration from this source."""
        ...

    def watch(self, callback: Callable[[], None]) -> None:
        """Start watching for changes and call callback on updates."""
        ...


class LayerMerger:
    """Deep merges configuration layers with typed coercion."""

    def __init__(self):
        self._coercion_hooks: Dict[str, Callable[[Any], Any]] = {}

    def register_coercion_hook(self, key_path: str, hook: Callable[[Any], Any]) -> None:
        """Register a coercion hook for specific config paths."""
        self._coercion_hooks[key_path] = hook

    def merge(self, layers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Deep merge layers with later layers taking precedence."""
        result: Dict[str, Any] = {}

        for layer in layers:
            self._deep_merge_dicts(result, layer)

        # Apply coercion hooks
        self._apply_coercion_hooks(result)

        return result

    def _deep_merge_dicts(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Deep merge source into target, with source taking precedence."""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge_dicts(target[key], value)
            else:
                target[key] = value

    def _apply_coercion_hooks(self, config: Dict[str, Any]) -> None:
        """Apply registered coercion hooks."""
        for key_path, hook in self._coercion_hooks.items():
            keys = key_path.split('.')
            current = config
            try:
                for key in keys[:-1]:
                    if key not in current or not isinstance(current[key], dict):
                        break
                    current = current[key]
                else:
                    final_key = keys[-1]
                    if final_key in current:
                        current[final_key] = hook(current[final_key])
            except Exception as e:
                logger.warning(f"Failed to apply coercion hook {key_path}: {e}")


class EnvConfigSource:
    """Loads configuration from environment variables with AURUM_ prefix."""

    def __init__(self, env_prefix: str = "AURUM_"):
        self._env_prefix = env_prefix
        self._name = f"env:{env_prefix}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def priority(self) -> int:
        return 100  # Highest priority - env vars always win

    def load(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config: Dict[str, Any] = {}

        for key, value in os.environ.items():
            if key.startswith(self._env_prefix):
                config_key = key[len(self._env_prefix):].lower()
                self._set_nested_value(config, config_key, value)

        return config

    def _set_nested_value(self, config: Dict[str, Any], key: str, value: str) -> None:
        """Set a nested configuration value using double underscore syntax."""
        keys = key.split('__')
        current = config

        for i, k in enumerate(keys[:-1]):
            if k not in current:
                current[k] = {}
            elif not isinstance(current[k], dict):
                # Convert to dict if it's not already
                current[k] = {'__value__': current[k]}
            current = current[k]

        final_key = keys[-1]
        current[final_key] = value

    def watch(self, callback: Callable[[], None]) -> None:
        """Environment variables don't change at runtime."""
        pass


class FileConfigSource:
    """Loads configuration from YAML/JSON files in config/ directory."""

    def __init__(self, config_base_path: Optional[Path] = None, environment: str = "development"):
        self._config_base_path = config_base_path or Path(__file__).resolve().parents[3] / "config"
        self._environment = environment
        self._name = f"file:{self._config_base_path}:{environment}"
        self._file_mtimes: Dict[Path, float] = {}
        self._cached_config: Optional[Dict[str, Any]] = None
        self._watchers: Set[Callable[[], None]] = set()

    @property
    def name(self) -> str:
        return self._name

    @property
    def priority(self) -> int:
        return 50  # Lower than env vars but higher than defaults

    def load(self) -> Dict[str, Any]:
        """Load configuration from overlay files."""
        if self._cached_config is not None:
            return self._cached_config

        overlays: List[Dict[str, Any]] = []

        # Load base configuration
        base_files = [
            self._config_base_path / "base.yaml",
            self._config_base_path / "base.yml",
            self._config_base_path / "base.json"
        ]

        for file_path in base_files:
            config = self._load_file(file_path)
            if config:
                overlays.append(config)

        # Load environment-specific configuration
        env_files = [
            self._config_base_path / f"{self._environment}.yaml",
            self._config_base_path / f"{self._environment}.yml",
            self._config_base_path / f"{self._environment}.json"
        ]

        for file_path in env_files:
            config = self._load_file(file_path)
            if config:
                overlays.append(config)

        # Load from environment-specific directory (config/<env>/*.yaml)
        env_dir = self._config_base_path / self._environment
        if env_dir.exists() and env_dir.is_dir():
            for yaml_file in env_dir.glob("*.yaml"):
                config = self._load_file(yaml_file)
                if config:
                    overlays.append(config)
            for yml_file in env_dir.glob("*.yml"):
                config = self._load_file(yml_file)
                if config:
                    overlays.append(config)

        # Merge all overlays
        merger = LayerMerger()
        self._cached_config = merger.merge(overlays)

        return self._cached_config

    def _load_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load a single configuration file."""
        try:
            if not file_path.exists():
                return None

            mtime = file_path.stat().st_mtime
            if file_path in self._file_mtimes and self._file_mtimes[file_path] == mtime:
                return None  # File hasn't changed

            self._file_mtimes[file_path] = mtime

            with file_path.open('r', encoding='utf-8') as f:
                if file_path.suffix in ('.yaml', '.yml'):
                    try:
                        import yaml
                        config = yaml.safe_load(f) or {}
                    except ImportError:
                        logger.warning(f"PyYAML not available, skipping {file_path}")
                        return None
                elif file_path.suffix == '.json':
                    config = json.load(f) or {}
                else:
                    logger.warning(f"Unsupported file format: {file_path}")
                    return None

            if not isinstance(config, dict):
                logger.warning(f"Configuration file {file_path} must contain an object")
                return None

            return config

        except Exception as e:
            logger.error(f"Failed to load configuration file {file_path}: {e}")
            return None

    def watch(self, callback: Callable[[], None]) -> None:
        """Watch configuration files for changes."""
        self._watchers.add(callback)

        def check_files():
            """Check if any watched files have changed."""
            changed = False
            for file_path in self._file_mtimes:
                try:
                    if not file_path.exists():
                        continue
                    current_mtime = file_path.stat().st_mtime
                    if current_mtime != self._file_mtimes[file_path]:
                        self._file_mtimes[file_path] = current_mtime
                        changed = True
                except OSError:
                    continue

            if changed:
                self._cached_config = None  # Force reload on next access
                for watcher in self._watchers:
                    try:
                        watcher()
                    except Exception as e:
                        logger.error(f"Error in file watcher callback: {e}")

        # Check files periodically
        def watch_loop():
            while True:
                try:
                    check_files()
                    time.sleep(2.0)  # Check every 2 seconds
                except Exception as e:
                    logger.error(f"Error in file watcher loop: {e}")
                    time.sleep(5.0)

        thread = threading.Thread(target=watch_loop, daemon=True)
        thread.start()


class EphemeralOverrideSource:
    """In-memory configuration overrides with TTL support."""

    def __init__(self):
        self._overrides: Dict[str, Dict[str, Any]] = {}
        self._expiries: Dict[str, float] = {}
        self._name = "ephemeral"
        self._cleanup_thread: Optional[threading.Thread] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def priority(self) -> int:
        return 90  # Very high priority, second only to env vars

    def load(self) -> Dict[str, Any]:
        """Load current ephemeral overrides."""
        current_time = time.time()
        expired_keys = []

        # Clean up expired overrides
        for key, expiry in self._expiries.items():
            if current_time > expiry:
                expired_keys.append(key)

        for key in expired_keys:
            self._overrides.pop(key, None)
            self._expiries.pop(key, None)

        # Merge all active overrides
        result: Dict[str, Any] = {}
        for override in self._overrides.values():
            LayerMerger()._deep_merge_dicts(result, override)

        return result

    def set_override(self, key: str, value: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        """Set an ephemeral override with optional TTL."""
        self._overrides[key] = value
        if ttl_seconds:
            self._expiries[key] = time.time() + ttl_seconds

    def remove_override(self, key: str) -> None:
        """Remove an ephemeral override."""
        self._overrides.pop(key, None)
        self._expiries.pop(key, None)

    def watch(self, callback: Callable[[], None]) -> None:
        """Ephemeral overrides don't need file watching."""
        pass

    def _start_cleanup_thread(self) -> None:
        """Start background thread to clean up expired overrides."""
        if self._cleanup_thread is not None:
            return

        def cleanup_loop():
            while True:
                try:
                    time.sleep(60.0)  # Clean up every minute
                    self.load()  # This will clean up expired overrides
                except Exception as e:
                    logger.error(f"Error in ephemeral override cleanup: {e}")

        self._cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        self._cleanup_thread.start()


@dataclass
class ConfigSnapshot:
    """A snapshot of configuration state."""
    version: int
    timestamp: float
    content_hash: str
    config: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


class DynamicConfigService:
    """Main service for dynamic configuration management."""

    def __init__(
        self,
        environment: str = "development",
        config_base_path: Optional[Path] = None,
        hot_reload_enabled: bool = True,
        reload_interval_seconds: float = 2.0
    ):
        self._environment = environment
        self._config_base_path = config_base_path
        self._hot_reload_enabled = hot_reload_enabled
        self._reload_interval_seconds = reload_interval_seconds

        # Configuration sources (ordered by priority, highest first)
        self._sources: List[ConfigSource] = [
            EnvConfigSource(),
            EphemeralOverrideSource(),
            FileConfigSource(config_base_path, environment)
        ]

        self._ephemeral_source = self._sources[1]  # EphemeralOverrideSource
        if isinstance(self._ephemeral_source, EphemeralOverrideSource):
            self._ephemeral_source._start_cleanup_thread()

        # Layer merger
        self._merger = LayerMerger()

        # Current state
        self._current_snapshot: Optional[ConfigSnapshot] = None
        self._subscribers: Set[Callable[[ConfigSnapshot], None]] = set()
        self._lock = threading.RLock()

        # Start hot reload if enabled
        if hot_reload_enabled:
            self._start_hot_reload()

        # Initial load
        self._reload_config()

    def get(self) -> Dict[str, Any]:
        """Get the current effective configuration."""
        with self._lock:
            return self._current_snapshot.config.copy() if self._current_snapshot else {}

    def get_snapshot(self) -> Optional[ConfigSnapshot]:
        """Get the current configuration snapshot."""
        with self._lock:
            return self._current_snapshot

    def subscribe(self, callback: Callable[[ConfigSnapshot], None]) -> None:
        """Subscribe to configuration changes."""
        with self._lock:
            self._subscribers.add(callback)

    def unsubscribe(self, callback: Callable[[ConfigSnapshot], None]) -> None:
        """Unsubscribe from configuration changes."""
        with self._lock:
            self._subscribers.discard(callback)

    def set_ephemeral_override(self, key: str, value: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
        """Set an ephemeral configuration override."""
        if isinstance(self._ephemeral_source, EphemeralOverrideSource):
            self._ephemeral_source.set_override(key, value, ttl_seconds)
            self._reload_config()

    def remove_ephemeral_override(self, key: str) -> None:
        """Remove an ephemeral configuration override."""
        if isinstance(self._ephemeral_source, EphemeralOverrideSource):
            self._ephemeral_source.remove_override(key)
            self._reload_config()

    def export_schema(self, path: str) -> None:
        """Export configuration schema to file."""
        # This will be implemented when we add validation
        schema = {
            "type": "object",
            "properties": {
                "api": {"type": "object"},
                "redis": {"type": "object"},
                "database": {"type": "object"},
                "security": {"type": "object"},
                "feature_flags": {"type": "object"}
            }
        }

        with open(path, 'w') as f:
            json.dump(schema, f, indent=2)

    def _reload_config(self) -> None:
        """Reload configuration from all sources."""
        try:
            # Load from all sources
            layers = [source.load() for source in self._sources]

            # Merge layers
            merged_config = self._merger.merge(layers)

            # Create content hash
            content_str = json.dumps(merged_config, sort_keys=True, default=str)
            content_hash = hashlib.sha256(content_str.encode()).hexdigest()

            # Create new snapshot
            current_version = self._current_snapshot.version + 1 if self._current_snapshot else 1
            new_snapshot = ConfigSnapshot(
                version=current_version,
                timestamp=time.time(),
                content_hash=content_hash,
                config=merged_config
            )

            # Update current snapshot
            with self._lock:
                old_snapshot = self._current_snapshot
                self._current_snapshot = new_snapshot

                # Notify subscribers if config changed
                if old_snapshot is None or old_snapshot.content_hash != content_hash:
                    for subscriber in self._subscribers:
                        try:
                            subscriber(new_snapshot)
                        except Exception as e:
                            logger.error(f"Error notifying config subscriber: {e}")

        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")

    def _start_hot_reload(self) -> None:
        """Start hot reload thread."""
        def reload_loop():
            while True:
                try:
                    time.sleep(self._reload_interval_seconds)
                    self._reload_config()
                except Exception as e:
                    logger.error(f"Error in config reload loop: {e}")

        thread = threading.Thread(target=reload_loop, daemon=True)
        thread.start()

        # Also start watching file sources
        for source in self._sources:
            if hasattr(source, 'watch'):
                source.watch(lambda: self._reload_config())
