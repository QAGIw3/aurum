"""Index lifecycle management for Elasticsearch search indices.

Provides automated index rollover, cleanup, and optimization
based on size, age, and performance metrics.
"""

import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .elasticsearch_engine import ElasticsearchEngine


logger = logging.getLogger(__name__)


class IndexLifecycleAction(Enum):
    """Index lifecycle management actions."""
    CREATE = "create"
    ROLLOVER = "rollover"
    SHRINK = "shrink"
    DELETE = "delete"
    OPTIMIZE = "optimize"


@dataclass
class IndexLifecyclePolicy:
    """Configuration for index lifecycle management."""
    max_age_days: int = 30  # Maximum age before rollover
    max_size_gb: float = 50.0  # Maximum size before rollover
    max_docs: int = 100000000  # Maximum documents before rollover
    min_docs_for_shrink: int = 10000  # Minimum docs for shrink operation
    shrink_ratio: float = 0.5  # Target size reduction for shrink
    retention_days: int = 90  # Days to retain old indices
    optimization_interval_hours: int = 24  # Hours between optimizations
    enable_auto_rollover: bool = True
    enable_auto_cleanup: bool = True
    enable_auto_optimization: bool = True


@dataclass
class IndexMetrics:
    """Index performance and size metrics."""
    index_name: str
    creation_date: datetime
    size_in_bytes: int
    total_docs: int
    max_doc: int
    deleted_docs: int
    segments_count: int
    segments_memory_mb: float
    search_rate: float
    indexing_rate: float
    query_time_ms: float
    fetch_time_ms: float


class IndexLifecycleManager:
    """Manages Elasticsearch index lifecycle operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize index lifecycle manager.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.policy = IndexLifecyclePolicy()
        self._engine: Optional[ElasticsearchEngine] = None
        self._last_optimization: Optional[datetime] = None

    async def initialize(self, engine: ElasticsearchEngine):
        """Initialize with Elasticsearch engine.

        Args:
            engine: Elasticsearch engine instance
        """
        self._engine = engine
        logger.info("Index lifecycle manager initialized")

    async def check_and_execute_policies(self) -> Dict[str, Any]:
        """Check and execute lifecycle policies.

        Returns:
            Dictionary with actions taken
        """
        if not self._engine:
            raise RuntimeError("Index lifecycle manager not initialized")

        actions_taken = {
            "rollovers": [],
            "deletions": [],
            "optimizations": [],
            "errors": []
        }

        try:
            # Get current index metrics
            metrics = await self._get_index_metrics()

            # Check rollover policy
            if self.policy.enable_auto_rollover:
                rollover_actions = await self._check_rollover_policy(metrics)
                actions_taken["rollovers"].extend(rollover_actions)

            # Check cleanup policy
            if self.policy.enable_auto_cleanup:
                deletion_actions = await self._check_cleanup_policy(metrics)
                actions_taken["deletions"].extend(deletion_actions)

            # Check optimization policy
            if (self.policy.enable_auto_optimization and
                await self._should_optimize()):
                optimization_actions = await self._optimize_indices(metrics)
                actions_taken["optimizations"].extend(optimization_actions)
                self._last_optimization = datetime.now()

        except Exception as e:
            logger.error(f"Error executing lifecycle policies: {e}")
            actions_taken["errors"].append(str(e))

        return actions_taken

    async def _get_index_metrics(self) -> List[IndexMetrics]:
        """Get metrics for all search indices.

        Returns:
            List of index metrics
        """
        if not self._engine:
            return []

        try:
            client = await self._engine._get_client()

            # Get index stats
            stats_response = await client.indices.stats(
                index=f"{self.settings.search_index_prefix}*",
                metric="store,docs,search,indexing"
            )

            # Get index settings for creation dates
            settings_response = await client.indices.get_settings(
                index=f"{self.settings.search_index_prefix}*",
                name="index.creation_date"
            )

            metrics = []
            indices = stats_response.get('indices', {})

            for index_name, stats in indices.items():
                if not index_name.startswith(self.settings.search_index_prefix):
                    continue

                # Parse creation date
                settings = settings_response.get(index_name, {}).get('settings', {})
                creation_date_str = settings.get('index', {}).get('creation_date')
                creation_date = None

                if creation_date_str:
                    creation_date = datetime.fromtimestamp(
                        int(creation_date_str) / 1000
                    )

                # Extract metrics
                total = stats.get('total', {})
                primaries = stats.get('primaries', {})

                metrics.append(IndexMetrics(
                    index_name=index_name,
                    creation_date=creation_date,
                    size_in_bytes=total.get('store', {}).get('size_in_bytes', 0),
                    total_docs=total.get('docs', {}).get('count', 0),
                    max_doc=total.get('docs', {}).get('max_doc', 0),
                    deleted_docs=total.get('docs', {}).get('deleted', 0),
                    segments_count=primaries.get('segments', {}).get('count', 0),
                    segments_memory_mb=primaries.get('segments', {}).get('memory_in_bytes', 0) / (1024 * 1024),
                    search_rate=primaries.get('search', {}).get('query_total', 0),
                    indexing_rate=primaries.get('indexing', {}).get('index_total', 0),
                    query_time_ms=primaries.get('search', {}).get('query_time_in_millis', 0),
                    fetch_time_ms=primaries.get('search', {}).get('fetch_time_in_millis', 0)
                ))

            return metrics

        except Exception as e:
            logger.error(f"Failed to get index metrics: {e}")
            return []

    async def _check_rollover_policy(self, metrics: List[IndexMetrics]) -> List[str]:
        """Check and execute rollover policy.

        Args:
            metrics: Current index metrics

        Returns:
            List of rollover actions taken
        """
        actions = []

        for metric in metrics:
            should_rollover = False
            reason = ""

            # Check age-based rollover
            if metric.creation_date:
                age_days = (datetime.now() - metric.creation_date).days
                if age_days >= self.policy.max_age_days:
                    should_rollover = True
                    reason = f"Age: {age_days} days"

            # Check size-based rollover
            size_gb = metric.size_in_bytes / (1024 * 1024 * 1024)
            if size_gb >= self.policy.max_size_gb:
                should_rollover = True
                reason = f"Size: {size_gb".1f"} GB"

            # Check document count-based rollover
            if metric.total_docs >= self.policy.max_docs:
                should_rollover = True
                reason = f"Documents: {metric.total_docs","}"

            if should_rollover:
                try:
                    await self._rollover_index(metric.index_name)
                    actions.append(f"Rolled over {metric.index_name}: {reason}")
                    logger.info(f"Rolled over index {metric.index_name}: {reason}")
                except Exception as e:
                    logger.error(f"Failed to rollover {metric.index_name}: {e}")
                    actions.append(f"Failed to rollover {metric.index_name}: {e}")

        return actions

    async def _rollover_index(self, index_name: str):
        """Rollover an index to a new one.

        Args:
            index_name: Name of index to rollover
        """
        if not self._engine:
            return

        client = await self._engine._get_client()

        # Create rollover request
        rollover_body = {
            "conditions": {
                "max_age": f"{self.policy.max_age_days}d",
                "max_size": f"{int(self.policy.max_size_gb)}gb",
                "max_docs": self.policy.max_docs
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }

        try:
            response = await client.indices.rollover(
                alias=f"{self.settings.search_index_prefix}-search",
                body=rollover_body
            )
            logger.info(f"Successfully rolled over index: {response}")
        except Exception as e:
            logger.error(f"Failed to rollover index {index_name}: {e}")
            raise

    async def _check_cleanup_policy(self, metrics: List[IndexMetrics]) -> List[str]:
        """Check and execute cleanup policy.

        Args:
            metrics: Current index metrics

        Returns:
            List of cleanup actions taken
        """
        actions = []

        for metric in metrics:
            if not metric.creation_date:
                continue

            age_days = (datetime.now() - metric.creation_date).days

            # Check if index should be deleted
            if age_days > self.policy.retention_days:
                try:
                    await self._delete_index(metric.index_name)
                    actions.append(f"Deleted {metric.index_name}: {age_days} days old")
                    logger.info(f"Deleted old index {metric.index_name}: {age_days} days")
                except Exception as e:
                    logger.error(f"Failed to delete {metric.index_name}: {e}")
                    actions.append(f"Failed to delete {metric.index_name}: {e}")

        return actions

    async def _delete_index(self, index_name: str):
        """Delete an index.

        Args:
            index_name: Name of index to delete
        """
        if not self._engine:
            return

        client = await self._engine._get_client()

        try:
            await client.indices.delete(index=index_name)
            logger.info(f"Successfully deleted index: {index_name}")
        except Exception as e:
            logger.error(f"Failed to delete index {index_name}: {e}")
            raise

    async def _should_optimize(self) -> bool:
        """Check if optimization should be performed.

        Returns:
            True if optimization should be performed
        """
        if not self._last_optimization:
            return True

        hours_since_last = (datetime.now() - self._last_optimization).total_seconds() / 3600
        return hours_since_last >= self.policy.optimization_interval_hours

    async def _optimize_indices(self, metrics: List[IndexMetrics]) -> List[str]:
        """Optimize indices for better performance.

        Args:
            metrics: Current index metrics

        Returns:
            List of optimization actions taken
        """
        actions = []

        for metric in metrics:
            # Check if index needs optimization
            needs_optimization = (
                metric.segments_count > 20 or  # Too many segments
                metric.segments_memory_mb > 1000 or  # High memory usage
                metric.deleted_docs > metric.total_docs * 0.1  # Too many deleted docs
            )

            if needs_optimization:
                try:
                    await self._optimize_index(metric.index_name)
                    actions.append(f"Optimized {metric.index_name}")
                    logger.info(f"Optimized index {metric.index_name}")
                except Exception as e:
                    logger.error(f"Failed to optimize {metric.index_name}: {e}")
                    actions.append(f"Failed to optimize {metric.index_name}: {e}")

        return actions

    async def _optimize_index(self, index_name: str):
        """Optimize a single index.

        Args:
            index_name: Name of index to optimize
        """
        if not self._engine:
            return

        client = await self._engine._get_client()

        try:
            # Force merge to reduce segments
            await client.indices.forcemerge(
                index=index_name,
                max_num_segments=1,
                only_expunge_deletes=True
            )
            logger.info(f"Force merged index: {index_name}")
        except Exception as e:
            logger.error(f"Failed to optimize index {index_name}: {e}")
            raise

    async def create_snapshot(self, snapshot_name: str, indices: Optional[List[str]] = None) -> bool:
        """Create a snapshot of search indices.

        Args:
            snapshot_name: Name for the snapshot
            indices: Specific indices to snapshot (None for all search indices)

        Returns:
            True if snapshot created successfully
        """
        if not self._engine:
            return False

        try:
            client = await self._engine._get_client()

            if indices is None:
                indices = [f"{self.settings.search_index_prefix}*"]

            # Create snapshot
            response = await client.snapshot.create(
                repository="aurum-search-snapshots",  # Assumes repository exists
                snapshot=snapshot_name,
                body={
                    "indices": indices,
                    "include_global_state": False,
                    "metadata": {
                        "created_by": "search-lifecycle-manager",
                        "created_at": datetime.now().isoformat()
                    }
                }
            )

            logger.info(f"Created snapshot {snapshot_name}: {response}")
            return True

        except Exception as e:
            logger.error(f"Failed to create snapshot {snapshot_name}: {e}")
            return False

    async def get_lifecycle_status(self) -> Dict[str, Any]:
        """Get current lifecycle management status.

        Returns:
            Dictionary with lifecycle status information
        """
        metrics = await self._get_index_metrics()

        return {
            "total_indices": len(metrics),
            "indices": [
                {
                    "name": m.index_name,
                    "size_gb": m.size_in_bytes / (1024 * 1024 * 1024),
                    "docs": m.total_docs,
                    "age_days": (datetime.now() - m.creation_date).days if m.creation_date else None,
                    "segments": m.segments_count,
                    "memory_mb": m.segments_memory_mb
                }
                for m in metrics
            ],
            "last_optimization": self._last_optimization.isoformat() if self._last_optimization else None,
            "policy": {
                "max_age_days": self.policy.max_age_days,
                "max_size_gb": self.policy.max_size_gb,
                "max_docs": self.policy.max_docs,
                "retention_days": self.policy.retention_days,
                "auto_rollover": self.policy.enable_auto_rollover,
                "auto_cleanup": self.policy.enable_auto_cleanup,
                "auto_optimization": self.policy.enable_auto_optimization
            }
        }


class SearchIndexManager:
    """High-level manager for search index operations."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize search index manager.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.lifecycle_manager = IndexLifecycleManager(settings)
        self._engine: Optional[ElasticsearchEngine] = None

    async def initialize(self, engine: ElasticsearchEngine):
        """Initialize with Elasticsearch engine.

        Args:
            engine: Elasticsearch engine instance
        """
        self._engine = engine
        await self.lifecycle_manager.initialize(engine)
        logger.info("Search index manager initialized")

    async def perform_maintenance(self) -> Dict[str, Any]:
        """Perform comprehensive index maintenance.

        Returns:
            Dictionary with maintenance results
        """
        if not self._engine:
            raise RuntimeError("Search index manager not initialized")

        results = {
            "lifecycle_actions": await self.lifecycle_manager.check_and_execute_policies(),
            "status": await self.lifecycle_manager.get_lifecycle_status()
        }

        return results

    async def create_backup(self, backup_name: Optional[str] = None) -> bool:
        """Create a backup of all search indices.

        Args:
            backup_name: Optional name for backup (defaults to timestamp)

        Returns:
            True if backup created successfully
        """
        if not backup_name:
            backup_name = f"search-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        return await self.lifecycle_manager.create_snapshot(backup_name)

    async def get_index_health(self) -> Dict[str, Any]:
        """Get comprehensive index health information.

        Returns:
            Dictionary with health metrics
        """
        if not self._engine:
            return {"status": "not_initialized"}

        try:
            client = await self._engine._get_client()

            # Get cluster health
            cluster_health = await client.cluster.health()

            # Get index-specific health
            indices_health = await client.cat.indices(
                index=f"{self.settings.search_index_prefix}*",
                format="json",
                h="index,health,status,docs.count,store.size,pri,rep"
            )

            return {
                "cluster_health": cluster_health['status'],
                "total_indices": len(indices_health),
                "indices": indices_health,
                "lifecycle_status": await self.lifecycle_manager.get_lifecycle_status()
            }

        except Exception as e:
            logger.error(f"Failed to get index health: {e}")
            return {"status": "error", "error": str(e)}


# Global index manager
_index_manager: Optional[SearchIndexManager] = None


def get_search_index_manager(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> SearchIndexManager:
    """Get or create global search index manager.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings

    Returns:
        Search index manager instance
    """
    global _index_manager
    if _index_manager is None:
        _index_manager = SearchIndexManager(settings)
    return _index_manager


async def initialize_search_index_manager(
    engine: ElasticsearchEngine,
    settings: Optional[AurumSettings] = None
) -> None:
    """Initialize search index manager globally.

    Args:
        engine: Elasticsearch engine instance
        settings: Application settings
    """
    manager = get_search_index_manager(engine, settings)
    await manager.initialize(engine)
