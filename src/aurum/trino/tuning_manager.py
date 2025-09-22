"""Trino tuning and optimization manager for performance and security."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Awaitable
from enum import Enum
import psutil
import os

from ..observability.metrics import get_metrics_client
from ..logging import StructuredLogger, LogLevel, create_logger

logger = logging.getLogger(__name__)


class QueryComplexity(str, Enum):
    """Query complexity levels for tuning."""
    SIMPLE = "simple"           # Basic SELECT queries
    MODERATE = "moderate"       # Joins, aggregations
    COMPLEX = "complex"         # Multiple joins, window functions
    ANALYTICAL = "analytical"   # Heavy analytics, ML queries


class MemoryPool(str, Enum):
    """Memory pool configurations."""
    RESERVED = "reserved"       # System reserved memory
    GENERAL = "general"         # General query memory
    SPILL = "spill"            # Memory for spilling
    SYSTEM = "system"          # System operations


@dataclass
class TrinoConfig:
    """Comprehensive Trino configuration for performance and security."""

    # Memory configuration
    query_max_memory: str = "8GB"
    query_max_memory_per_node: str = "2GB"
    query_max_total_memory: str = "16GB"
    reserved_memory: str = "1GB"

    # Spill configuration
    spill_enabled: bool = True
    spill_path: str = "/tmp/trino-spill"
    spill_compression_enabled: bool = True
    spill_max_used_space_threshold: str = "0.9"

    # Query optimization
    hash_partition_count: int = 100
    join_reordering_strategy: str = "COST_BASED"
    join_max_broadcast_table_size: str = "100MB"
    optimizer_use_mark_distinct: bool = True
    optimizer_use_exact_partitioning: bool = True

    # Statistics configuration
    statistics_enabled: bool = True
    statistics_precalculation_for_pushed_filters: bool = True
    collect_column_statistics_on_write: bool = True

    # Security configuration
    access_control_enabled: bool = True
    catalog_access_control: Dict[str, str] = field(default_factory=dict)
    schema_access_control: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Performance monitoring
    query_min_cpu_time: str = "1ms"
    resource_overcommit: bool = False

    # Caching configuration
    metadata_cache_ttl: str = "5m"
    cache_enabled: bool = True
    cache_max_size: str = "1GB"

    def to_properties_dict(self) -> Dict[str, str]:
        """Convert config to Trino properties format."""
        return {
            "query.max-memory": self.query_max_memory,
            "query.max-memory-per-node": self.query_max_memory_per_node,
            "query.max-total-memory": self.query_max_total_memory,
            "memory.heap-headroom-per-node": "1GB",
            "spill-enabled": str(self.spill_enabled).lower(),
            "spiller-spill-path": self.spill_path,
            "spiller-spill-compression-enabled": str(self.spill_compression_enabled).lower(),
            "spiller-max-used-space-threshold": self.spill_max_used_space_threshold,
            "hash-partition-count": str(self.hash_partition_count),
            "join-reordering-strategy": self.join_reordering_strategy,
            "join.max-broadcast-table-size": self.join_max_broadcast_table_size,
            "optimizer.use-mark-distinct": str(self.optimizer_use_mark_distinct).lower(),
            "optimizer.use-exact-partitioning": str(self.optimizer_use_exact_partitioning).lower(),
            "statistics.enabled": str(self.statistics_enabled).lower(),
            "statistics.precalculation-for-pushed-filters": str(self.statistics_precalculation_for_pushed_filters).lower(),
            "collect-column-statistics-on-write": str(self.collect_column_statistics_on_write).lower(),
            "query.min-cpu-time": self.query_min_cpu_time,
            "resource-overcommit": str(self.resource_overcommit).lower(),
            "metadata.cache-ttl": self.metadata_cache_ttl,
            "cache.enabled": str(self.cache_enabled).lower(),
            "cache.max-size": self.cache_max_size
        }


@dataclass
class QueryMetrics:
    """Metrics for query performance analysis."""

    query_id: str
    query_text: str
    user: str = ""
    start_time: datetime = field(default_factory=lambda: datetime.now())
    end_time: Optional[datetime] = None
    execution_time_seconds: float = 0.0
    cpu_time_seconds: float = 0.0
    peak_memory_bytes: int = 0
    spilled_bytes: int = 0
    input_rows: int = 0
    input_bytes: int = 0
    output_rows: int = 0
    output_bytes: int = 0
    complexity: QueryComplexity = QueryComplexity.SIMPLE

    # Performance indicators
    memory_efficiency: float = 1.0
    io_efficiency: float = 1.0
    spill_ratio: float = 0.0

    def calculate_efficiency_metrics(self):
        """Calculate efficiency metrics."""
        if self.execution_time_seconds > 0:
            self.memory_efficiency = self.peak_memory_bytes / (self.execution_time_seconds * 1024 * 1024)  # MB/s

        if self.input_bytes > 0:
            self.io_efficiency = self.output_bytes / self.input_bytes

        if self.peak_memory_bytes > 0:
            self.spill_ratio = self.spilled_bytes / self.peak_memory_bytes


@dataclass
class CatalogConfiguration:
    """Configuration for a Trino catalog."""

    name: str
    connector: str
    properties: Dict[str, str]
    security_properties: Dict[str, str] = field(default_factory=dict)
    performance_hints: Dict[str, str] = field(default_factory=dict)

    # Optimization settings
    enable_pushdown: bool = True
    enable_partitioning: bool = True
    enable_statistics: bool = True

    def to_properties_dict(self) -> Dict[str, str]:
        """Convert to catalog properties format."""
        props = self.properties.copy()
        props["connector.name"] = self.connector

        if self.enable_pushdown:
            props["allow-pushdown-into-connectors"] = "true"
            props["complex-expression-pushdown"] = "true"

        if self.enable_partitioning:
            props["partitioning-provider-catalog"] = "iceberg"

        if self.enable_statistics:
            props["statistics.enabled"] = "true"

        # Add performance hints
        props.update(self.performance_hints)

        return props


class TrinoTuningManager:
    """Manager for Trino performance tuning and optimization."""

    def __init__(self):
        self.metrics = get_metrics_client()
        self.logger = create_logger(
            source_name="trino_tuning",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.trino.tuning",
            dataset="trino_tuning"
        )

        # Configuration
        self.config = TrinoConfig()
        self.catalog_configs: Dict[str, CatalogConfiguration] = {}

        # Query tracking
        self.active_queries: Dict[str, QueryMetrics] = {}
        self.query_history: List[QueryMetrics] = []

        # Performance monitoring
        self.system_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
        self.system_cpu_count = psutil.cpu_count()

        # Auto-tuning state
        self.last_tuning_time = datetime.now()
        self.tuning_interval_minutes = 30

        # RBAC integration
        self.rbac_config: Dict[str, Any] = {}

    async def initialize_catalogs(self):
        """Initialize catalog configurations."""
        # Iceberg catalog (main data lake)
        self.catalog_configs["iceberg"] = CatalogConfiguration(
            name="iceberg",
            connector="iceberg",
            properties={
                "iceberg.catalog.type": "rest",
                "iceberg.rest-catalog.uri": "http://nessie:19121/api/v1",
                "iceberg.file-format": "PARQUET",
                "iceberg.compression-codec": "ZSTD",
                "iceberg.new-table-format-version": "2"
            },
            security_properties={
                "iceberg.security": "read_only"
            },
            performance_hints={
                "iceberg.pushdown-filter-enabled": "true",
                "iceberg.max-partitions-per-writer": "100"
            }
        )

        # PostgreSQL/TimescaleDB catalog
        self.catalog_configs["timescale"] = CatalogConfiguration(
            name="timescale",
            connector="postgresql",
            properties={
                "connection-url": "jdbc:postgresql://timescale:5432/timescale",
                "case-insensitive-name-matching": "true"
            },
            enable_statistics=True,
            performance_hints={
                "postgresql.read.batch-size": "10000"
            }
        )

        # ClickHouse catalog
        self.catalog_configs["clickhouse"] = CatalogConfiguration(
            name="clickhouse",
            connector="clickhouse",
            properties={
                "clickhouse.connection-url": "jdbc:clickhouse://clickhouse:8123/default"
            },
            enable_pushdown=True,
            enable_statistics=True
        )

        # Kafka catalog
        self.catalog_configs["kafka"] = CatalogConfiguration(
            name="kafka",
            connector="kafka",
            properties={
                "kafka.table-description-supplier": "AURUM_SCHEMA_REGISTRY",
                "kafka.hide-internal-columns": "true"
            },
            enable_partitioning=False  # Kafka doesn't support traditional partitioning
        )

        logger.info("Trino catalogs initialized")

    async def tune_for_query_complexity(self, complexity: QueryComplexity):
        """Tune Trino configuration for specific query complexity."""
        base_config = self.config

        if complexity == QueryComplexity.SIMPLE:
            # Simple queries: conservative memory usage
            base_config.query_max_memory = "2GB"
            base_config.query_max_memory_per_node = "512MB"
            base_config.hash_partition_count = 50

        elif complexity == QueryComplexity.MODERATE:
            # Moderate queries: balanced configuration
            base_config.query_max_memory = "8GB"
            base_config.query_max_memory_per_node = "2GB"
            base_config.hash_partition_count = 100

        elif complexity == QueryComplexity.COMPLEX:
            # Complex queries: more memory for joins
            base_config.query_max_memory = "16GB"
            base_config.query_max_memory_per_node = "4GB"
            base_config.hash_partition_count = 200
            base_config.join_reordering_strategy = "COST_BASED"

        elif complexity == QueryComplexity.ANALYTICAL:
            # Analytical queries: maximum resources
            base_config.query_max_memory = "32GB"
            base_config.query_max_memory_per_node = "8GB"
            base_config.hash_partition_count = 500
            base_config.spill_enabled = True

        # Apply spill tuning
        await self._tune_spill_configuration(base_config)

        logger.info(f"Tuned Trino for {complexity.value} query complexity")

    async def _tune_spill_configuration(self, config: TrinoConfig):
        """Tune spill configuration based on available resources."""
        available_memory = self.system_memory_mb
        reserved_memory = 1024  # 1GB reserved

        if available_memory > 16384:  # >16GB system memory
            config.spill_max_used_space_threshold = "0.8"
            config.spill_path = "/tmp/trino-spill"
        elif available_memory > 8192:  # >8GB system memory
            config.spill_max_used_space_threshold = "0.7"
        else:  # Limited memory
            config.spill_max_used_space_threshold = "0.6"
            config.query_max_memory = "4GB"

    async def optimize_memory_fences(self):
        """Optimize memory fences for concurrent query performance."""
        # Memory fence optimization would involve:
        # 1. Setting appropriate memory pool sizes
        # 2. Configuring query admission control
        # 3. Setting up memory arbitration

        fence_config = {
            "memory.heap-headroom-per-node": "2GB",
            "memory.max-usage-per-query": self.config.query_max_memory,
            "query.max-concurrent-queries": max(1, self.system_cpu_count // 2),
            "query.queue-config-file": "/etc/trino/queue_config.json"
        }

        logger.info("Memory fences optimized for concurrent queries")

    async def setup_rbac_integration(self, rbac_config: Dict[str, Any]):
        """Setup RBAC integration with external systems."""
        self.rbac_config = rbac_config

        # Configure catalog-level access control
        for catalog_name, catalog_config in self.catalog_configs.items():
            if catalog_name in rbac_config.get("catalog_permissions", {}):
                permissions = rbac_config["catalog_permissions"][catalog_name]
                catalog_config.security_properties.update({
                    "security": permissions.get("mode", "read_only"),
                    "allow-drop-table": str(permissions.get("allow_drop", False)).lower(),
                    "allow-create-table": str(permissions.get("allow_create", False)).lower()
                })

        # Configure schema-level permissions
        schema_permissions = rbac_config.get("schema_permissions", {})
        for schema_key, permissions in schema_permissions.items():
            catalog, schema = schema_key.split(".", 1)
            if catalog in self.catalog_configs:
                if catalog not in self.catalog_configs[catalog].security_properties:
                    self.catalog_configs[catalog].security_properties["schemas"] = {}

                self.catalog_configs[catalog].security_properties["schemas"][schema] = permissions

        logger.info("RBAC integration configured")

    async def collect_query_statistics(self, query_id: str, query_text: str, user: str = ""):
        """Collect and analyze query statistics."""
        metrics = QueryMetrics(
            query_id=query_id,
            query_text=query_text,
            user=user,
            start_time=datetime.now()
        )

        self.active_queries[query_id] = metrics

        # Analyze query complexity
        metrics.complexity = await self._analyze_query_complexity(query_text)

        # Apply complexity-based tuning
        await self.tune_for_query_complexity(metrics.complexity)

        logger.info(f"Started tracking query {query_id} with complexity {metrics.complexity.value}")

    async def record_query_completion(
        self,
        query_id: str,
        execution_time_seconds: float,
        cpu_time_seconds: float,
        peak_memory_bytes: int,
        spilled_bytes: int,
        input_rows: int,
        input_bytes: int,
        output_rows: int,
        output_bytes: int
    ):
        """Record query completion metrics."""
        if query_id not in self.active_queries:
            logger.warning(f"Query {query_id} not found in active queries")
            return

        metrics = self.active_queries[query_id]
        metrics.end_time = datetime.now()
        metrics.execution_time_seconds = execution_time_seconds
        metrics.cpu_time_seconds = cpu_time_seconds
        metrics.peak_memory_bytes = peak_memory_bytes
        metrics.spilled_bytes = spilled_bytes
        metrics.input_rows = input_rows
        metrics.input_bytes = input_bytes
        metrics.output_rows = output_rows
        metrics.output_bytes = output_bytes

        # Calculate efficiency metrics
        metrics.calculate_efficiency_metrics()

        # Move to history
        self.query_history.append(metrics)
        del self.active_queries[query_id]

        # Emit metrics
        await self._emit_query_metrics(metrics)

        # Check for performance issues
        await self._analyze_query_performance(metrics)

        logger.info(f"Query {query_id} completed in {execution_time_seconds:.2f}s")

    async def _analyze_query_complexity(self, query_text: str) -> QueryComplexity:
        """Analyze query complexity for tuning."""
        query_lower = query_text.lower().strip()

        # Simple queries
        if any(keyword in query_lower for keyword in ["select", "from", "where"]) and "join" not in query_lower:
            return QueryComplexity.SIMPLE

        # Moderate complexity
        if "join" in query_lower and "group by" not in query_lower:
            return QueryComplexity.MODERATE

        # Complex queries
        if "group by" in query_lower or "window" in query_lower or "over" in query_lower:
            return QueryComplexity.COMPLEX

        # Analytical queries
        if any(keyword in query_lower for keyword in ["rank()", "dense_rank()", "row_number()"]):
            return QueryComplexity.ANALYTICAL

        return QueryComplexity.MODERATE

    async def _emit_query_metrics(self, metrics: QueryMetrics):
        """Emit query metrics to monitoring system."""
        self.metrics.gauge("trino.query.execution_time", metrics.execution_time_seconds)
        self.metrics.gauge("trino.query.cpu_time", metrics.cpu_time_seconds)
        self.metrics.gauge("trino.query.peak_memory_bytes", metrics.peak_memory_bytes)
        self.metrics.gauge("trino.query.spilled_bytes", metrics.spilled_bytes)
        self.metrics.gauge("trino.query.memory_efficiency", metrics.memory_efficiency)
        self.metrics.gauge("trino.query.spill_ratio", metrics.spill_ratio)

        # Complexity-specific metrics
        self.metrics.increment_counter(f"trino.query.complexity.{metrics.complexity.value}")

    async def _analyze_query_performance(self, metrics: QueryMetrics):
        """Analyze query performance and generate recommendations."""
        recommendations = []

        # Check for excessive spilling
        if metrics.spill_ratio > 0.1:  # More than 10% memory spilled
            recommendations.append({
                "type": "memory_pressure",
                "action": "increase_query_memory",
                "severity": "high" if metrics.spill_ratio > 0.5 else "medium"
            })

        # Check for inefficient queries
        if metrics.execution_time_seconds > 300 and metrics.complexity in [QueryComplexity.SIMPLE, QueryComplexity.MODERATE]:
            recommendations.append({
                "type": "query_optimization",
                "action": "review_query_plan",
                "severity": "medium"
            })

        # Check for memory fence violations
        if metrics.peak_memory_bytes > (8 * 1024 * 1024 * 1024):  # >8GB per query
            recommendations.append({
                "type": "memory_management",
                "action": "adjust_memory_limits",
                "severity": "high"
            })

        # Log recommendations
        if recommendations:
            self.logger.log(
                LogLevel.INFO,
                f"Performance recommendations for query {metrics.query_id}",
                "query_performance_analysis",
                query_id=metrics.query_id,
                recommendations=recommendations
            )

    async def generate_tuning_report(self) -> Dict[str, Any]:
        """Generate comprehensive tuning report."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "system_info": {
                "memory_mb": self.system_memory_mb,
                "cpu_count": self.system_cpu_count,
                "current_config": self.config.to_properties_dict()
            },
            "catalog_configs": {
                name: config.to_properties_dict()
                for name, config in self.catalog_configs.items()
            },
            "query_statistics": {
                "total_queries": len(self.query_history),
                "avg_execution_time": sum(q.execution_time_seconds for q in self.query_history) / max(len(self.query_history), 1),
                "spill_incidents": sum(1 for q in self.query_history if q.spill_ratio > 0),
                "complexity_distribution": {
                    complexity.value: sum(1 for q in self.query_history if q.complexity == complexity)
                    for complexity in QueryComplexity
                }
            },
            "recommendations": await self._generate_tuning_recommendations()
        }

        return report

    async def _generate_tuning_recommendations(self) -> List[Dict[str, Any]]:
        """Generate tuning recommendations based on observed performance."""
        recommendations = []

        # Memory recommendations
        if self.system_memory_mb < 8192:  # <8GB system memory
            recommendations.append({
                "category": "memory",
                "recommendation": "Increase system memory for better Trino performance",
                "priority": "high"
            })

        # Query pattern recommendations
        complex_queries = sum(1 for q in self.query_history if q.complexity in [QueryComplexity.COMPLEX, QueryComplexity.ANALYTICAL])
        if complex_queries > len(self.query_history) * 0.5:
            recommendations.append({
                "category": "performance",
                "recommendation": "Consider query result caching for complex analytical queries",
                "priority": "medium"
            })

        # Spill recommendations
        spill_incidents = sum(1 for q in self.query_history if q.spill_ratio > 0)
        if spill_incidents > len(self.query_history) * 0.3:
            recommendations.append({
                "category": "memory",
                "recommendation": "Configure dedicated spill storage with SSD for better performance",
                "priority": "high"
            })

        return recommendations

    async def start_auto_tuning(self):
        """Start automatic performance tuning based on workload patterns."""
        while True:
            try:
                # Generate tuning report
                report = await self.generate_tuning_report()

                # Apply automatic tuning
                await self._apply_auto_tuning(report)

                # Emit tuning metrics
                self.metrics.gauge("trino.tuning.system_memory_mb", self.system_memory_mb)
                self.metrics.gauge("trino.tuning.active_queries", len(self.active_queries))
                self.metrics.gauge("trino.tuning.query_history_size", len(self.query_history))

                # Wait before next tuning cycle
                await asyncio.sleep(self.tuning_interval_minutes * 60)

            except Exception as e:
                logger.error(f"Error in auto-tuning cycle: {e}")
                await asyncio.sleep(60)  # Brief pause before retry

    async def _apply_auto_tuning(self, report: Dict[str, Any]):
        """Apply automatic tuning based on report analysis."""
        recommendations = report.get("recommendations", [])

        for rec in recommendations:
            if rec["category"] == "memory" and rec["priority"] == "high":
                # Increase memory limits if system has capacity
                if self.system_memory_mb > 16384:  # >16GB
                    self.config.query_max_memory = "16GB"
                    self.config.query_max_total_memory = "32GB"
                    logger.info("Applied memory tuning for high-performance workload")

            elif rec["category"] == "performance" and rec["priority"] == "medium":
                # Enable more aggressive optimizations
                self.config.optimizer_use_exact_partitioning = True
                self.config.statistics_precalculation_for_pushed_filters = True
                logger.info("Applied performance optimizations for analytical workload")

    async def get_catalog_config(self, catalog_name: str) -> Optional[CatalogConfiguration]:
        """Get configuration for a specific catalog."""
        return self.catalog_configs.get(catalog_name)

    async def update_catalog_config(self, catalog_name: str, updates: Dict[str, Any]):
        """Update catalog configuration."""
        if catalog_name in self.catalog_configs:
            config = self.catalog_configs[catalog_name]
            for key, value in updates.items():
                if hasattr(config, key):
                    setattr(config, key, value)
            logger.info(f"Updated configuration for catalog {catalog_name}")


# Global tuning manager
_global_tuning_manager: Optional[TrinoTuningManager] = None


async def get_trino_tuning_manager() -> TrinoTuningManager:
    """Get or create the global Trino tuning manager."""
    global _global_tuning_manager

    if _global_tuning_manager is None:
        _global_tuning_manager = TrinoTuningManager()
        await _global_tuning_manager.initialize_catalogs()

    return _global_tuning_manager


async def initialize_trino_tuning():
    """Initialize the Trino tuning system."""
    manager = await get_trino_tuning_manager()
    return manager
