"""
Configuration change tracking and audit for the Advanced Configuration Management System.

This module provides:
- Versioned configuration snapshots
- Change diffing and comparison
- Audit trails with actor and reason tracking
- Kafka event publishing for configuration changes
- Backup and recovery capabilities
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import deepdiff
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of configuration changes."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    RESTORED = "restored"


class ChangeSource(Enum):
    """Sources of configuration changes."""
    API = "api"
    CLI = "cli"
    CI_CD = "ci_cd"
    FILE_WATCHER = "file_watcher"
    SYSTEM = "system"


@dataclass
class ConfigChange:
    """Represents a configuration change."""
    change_id: str
    timestamp: float
    change_type: ChangeType
    source: ChangeSource
    actor: str  # User ID, service name, etc.
    namespace: Optional[str] = None
    reason: str = ""
    correlation_id: Optional[str] = None
    old_config: Optional[Dict[str, Any]] = None
    new_config: Optional[Dict[str, Any]] = None
    diff: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConfigVersion:
    """Represents a versioned configuration snapshot."""
    version: int
    timestamp: float
    content_hash: str
    config: Dict[str, Any]
    change_id: str
    compressed_size: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ChangeTracker:
    """Tracks configuration changes and maintains audit trail."""

    def __init__(
        self,
        db_connection=None,
        kafka_producer=None,
        backup_storage_path: Optional[str] = None,
        retention_days: int = 30
    ):
        self._db_connection = db_connection
        self._kafka_producer = kafka_producer
        self._backup_storage_path = Path(backup_storage_path) if backup_storage_path else None
        self._retention_days = retention_days
        self._changes: List[ConfigChange] = []
        self._versions: Dict[int, ConfigVersion] = {}
        self._current_version = 0
        self._lock = asyncio.Lock()

    async def record_change(
        self,
        change_type: ChangeType,
        source: ChangeSource,
        actor: str,
        namespace: Optional[str] = None,
        reason: str = "",
        correlation_id: Optional[str] = None,
        old_config: Optional[Dict[str, Any]] = None,
        new_config: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Record a configuration change."""
        async with self._lock:
            change_id = str(uuid.uuid4())
            timestamp = time.time()

            # Calculate diff if both old and new config are provided
            diff = None
            if old_config is not None and new_config is not None:
                diff = self._calculate_diff(old_config, new_config)

            change = ConfigChange(
                change_id=change_id,
                timestamp=timestamp,
                change_type=change_type,
                source=source,
                actor=actor,
                namespace=namespace,
                reason=reason,
                correlation_id=correlation_id,
                old_config=old_config,
                new_config=new_config,
                diff=diff,
                metadata=metadata or {}
            )

            self._changes.append(change)

            # Store in database if available
            if self._db_connection:
                await self._store_change_in_db(change)

            # Publish to Kafka if available
            if self._kafka_producer:
                await self._publish_to_kafka(change)

            # Cleanup old changes
            await self._cleanup_old_changes()

            logger.info(f"Recorded config change {change_id}: {change_type.value} by {actor}")
            return change_id

    async def create_version(
        self,
        config: Dict[str, Any],
        change_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Create a new versioned snapshot of the configuration."""
        async with self._lock:
            self._current_version += 1
            timestamp = time.time()

            # Calculate content hash
            content_str = json.dumps(config, sort_keys=True, default=str)
            content_hash = hashlib.sha256(content_str.encode()).hexdigest()

            # Compress and calculate size
            compressed_config = self._compress_config(config)
            compressed_size = len(compressed_config)

            version = ConfigVersion(
                version=self._current_version,
                timestamp=timestamp,
                content_hash=content_hash,
                config=config,
                change_id=change_id,
                compressed_size=compressed_size,
                metadata=metadata or {}
            )

            self._versions[self._current_version] = version

            # Store in database if available
            if self._db_connection:
                await self._store_version_in_db(version)

            # Backup to storage if configured
            if self._backup_storage_path:
                await self._backup_version(version)

            logger.info(f"Created config version {self._current_version} with hash {content_hash[:8]}")
            return self._current_version

    def get_change_history(
        self,
        limit: int = 100,
        namespace: Optional[str] = None,
        actor: Optional[str] = None,
        since_timestamp: Optional[float] = None
    ) -> List[ConfigChange]:
        """Get configuration change history with optional filtering."""
        changes = self._changes.copy()

        # Apply filters
        if namespace is not None:
            changes = [c for c in changes if c.namespace == namespace]

        if actor is not None:
            changes = [c for c in changes if c.actor == actor]

        if since_timestamp is not None:
            changes = [c for c in changes if c.timestamp >= since_timestamp]

        # Sort by timestamp descending and limit
        changes.sort(key=lambda c: c.timestamp, reverse=True)
        return changes[:limit]

    def get_version(self, version: int) -> Optional[ConfigVersion]:
        """Get a specific configuration version."""
        return self._versions.get(version)

    def get_latest_version(self) -> Optional[ConfigVersion]:
        """Get the latest configuration version."""
        return self._versions.get(self._current_version)

    def compare_versions(self, from_version: int, to_version: int) -> Dict[str, Any]:
        """Compare two configuration versions and return a diff."""
        from_config = self._versions.get(from_version)
        to_config = self._versions.get(to_version)

        if not from_config:
            raise ValueError(f"Version {from_version} not found")
        if not to_config:
            raise ValueError(f"Version {to_version} not found")

        return self._calculate_diff(from_config.config, to_config.config)

    def list_versions(self, limit: int = 50) -> List[ConfigVersion]:
        """List recent configuration versions."""
        versions = list(self._versions.values())
        versions.sort(key=lambda v: v.timestamp, reverse=True)
        return versions[:limit]

    def get_changes_for_version(self, version: int) -> List[ConfigChange]:
        """Get all changes that led to a specific version."""
        target_version = self._versions.get(version)
        if not target_version:
            return []

        # Find all changes that occurred before this version
        changes = []
        for change in self._changes:
            if change.timestamp <= target_version.timestamp:
                changes.append(change)
            else:
                break

        return changes

    async def backup_current_config(self, config: Dict[str, Any], reason: str = "manual") -> str:
        """Backup the current configuration."""
        change_id = await self.record_change(
            change_type=ChangeType.UPDATED,
            source=ChangeSource.SYSTEM,
            actor="system",
            reason=f"Backup: {reason}",
            new_config=config
        )

        await self.create_version(config, change_id, {"backup_reason": reason})

        return change_id

    async def restore_version(self, version: int, actor: str, reason: str = "") -> str:
        """Restore configuration to a specific version."""
        target_version = self.get_version(version)
        if not target_version:
            raise ValueError(f"Version {version} not found")

        change_id = await self.record_change(
            change_type=ChangeType.RESTORED,
            source=ChangeSource.API,
            actor=actor,
            reason=reason,
            old_config=self.get_latest_version().config if self.get_latest_version() else None,
            new_config=target_version.config,
            metadata={"restored_version": version}
        )

        # Create a new version for the restored config
        await self.create_version(target_version.config, change_id, {"restored_from": version})

        return change_id

    def _calculate_diff(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate the difference between two configurations."""
        try:
            diff = deepdiff.DeepDiff(old_config, new_config, ignore_order=True)
            return diff.to_dict()
        except Exception as e:
            logger.error(f"Failed to calculate diff: {e}")
            return {"error": str(e)}

    def _compress_config(self, config: Dict[str, Any]) -> bytes:
        """Compress configuration for storage."""
        try:
            import lz4.frame
            config_json = json.dumps(config, default=str).encode('utf-8')
            return lz4.frame.compress(config_json)
        except ImportError:
            # Fallback to uncompressed if lz4 not available
            return json.dumps(config, default=str).encode('utf-8')

    async def _store_change_in_db(self, change: ConfigChange) -> None:
        """Store a change record in the database."""
        try:
            # This would use SQLAlchemy or similar ORM
            # For now, just log that it would be stored
            logger.debug(f"Would store change {change.change_id} in database")
        except Exception as e:
            logger.error(f"Failed to store change in database: {e}")

    async def _store_version_in_db(self, version: ConfigVersion) -> None:
        """Store a version record in the database."""
        try:
            # This would use SQLAlchemy or similar ORM
            # For now, just log that it would be stored
            logger.debug(f"Would store version {version.version} in database")
        except Exception as e:
            logger.error(f"Failed to store version in database: {e}")

    async def _publish_to_kafka(self, change: ConfigChange) -> None:
        """Publish change event to Kafka."""
        try:
            event = {
                "event_type": "config_change",
                "change_id": change.change_id,
                "timestamp": change.timestamp,
                "change_type": change.change_type.value,
                "source": change.source.value,
                "actor": change.actor,
                "namespace": change.namespace,
                "reason": change.reason,
                "correlation_id": change.correlation_id,
                "metadata": change.metadata
            }

            # Only include diff if it's not too large
            if change.diff and len(str(change.diff)) < 10000:
                event["diff"] = change.diff

            # This would use Kafka producer
            # await self._kafka_producer.send("config.events", event)
            logger.debug(f"Would publish config change event to Kafka: {change.change_id}")

        except Exception as e:
            logger.error(f"Failed to publish change to Kafka: {e}")

    async def _backup_version(self, version: ConfigVersion) -> None:
        """Backup a version to storage."""
        if not self._backup_storage_path:
            return

        try:
            backup_dir = self._backup_storage_path / f"version_{version.version}"
            backup_dir.mkdir(exist_ok=True)

            # Save full config
            config_file = backup_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(version.config, f, indent=2, default=str)

            # Save metadata
            metadata_file = backup_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump({
                    "version": version.version,
                    "timestamp": version.timestamp,
                    "content_hash": version.content_hash,
                    "change_id": version.change_id,
                    "metadata": version.metadata
                }, f, indent=2)

            logger.info(f"Backed up version {version.version} to {backup_dir}")

        except Exception as e:
            logger.error(f"Failed to backup version {version.version}: {e}")

    async def _cleanup_old_changes(self) -> None:
        """Clean up old changes beyond retention period."""
        cutoff_time = time.time() - (self._retention_days * 24 * 60 * 60)

        original_count = len(self._changes)
        self._changes = [c for c in self._changes if c.timestamp > cutoff_time]

        if len(self._changes) < original_count:
            logger.info(f"Cleaned up {original_count - len(self._changes)} old change records")


# Global change tracker instance
_change_tracker: Optional[ChangeTracker] = None


def get_change_tracker() -> ChangeTracker:
    """Get the global change tracker instance."""
    global _change_tracker
    if _change_tracker is None:
        _change_tracker = ChangeTracker()
    return _change_tracker


def initialize_change_tracker(
    db_connection=None,
    kafka_producer=None,
    backup_storage_path: Optional[str] = None,
    retention_days: int = 30
) -> ChangeTracker:
    """Initialize the global change tracker."""
    global _change_tracker
    _change_tracker = ChangeTracker(
        db_connection=db_connection,
        kafka_producer=kafka_producer,
        backup_storage_path=backup_storage_path,
        retention_days=retention_days
    )
    return _change_tracker


async def record_config_change(
    change_type: ChangeType,
    source: ChangeSource,
    actor: str,
    namespace: Optional[str] = None,
    reason: str = "",
    correlation_id: Optional[str] = None,
    old_config: Optional[Dict[str, Any]] = None,
    new_config: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """Convenience function to record a configuration change."""
    tracker = get_change_tracker()
    return await tracker.record_change(
        change_type=change_type,
        source=source,
        actor=actor,
        namespace=namespace,
        reason=reason,
        correlation_id=correlation_id,
        old_config=old_config,
        new_config=new_config,
        metadata=metadata
    )


async def create_config_version(
    config: Dict[str, Any],
    change_id: str,
    metadata: Optional[Dict[str, Any]] = None
) -> int:
    """Convenience function to create a configuration version."""
    tracker = get_change_tracker()
    return await tracker.create_version(config, change_id, metadata)
