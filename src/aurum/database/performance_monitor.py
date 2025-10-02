"""Performance monitoring and auto-scaling for database connection pools.

This module provides intelligent monitoring of database performance and automatic
scaling of connection pools based on load patterns and performance metrics.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from aurum.database import get_connection_manager_registry, PoolMetrics, ConnectionManagerRegistry
from aurum.observability import get_application_metrics

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for a database pool."""

    pool_name: str
    timestamp: datetime

    # Connection metrics
    active_connections: int
    idle_connections: int
    total_connections: int
    max_connections: int
    pool_utilization: float

    # Performance metrics
    avg_acquire_time_ms: float
    p95_acquire_time_ms: float
    p99_acquire_time_ms: float

    # Query metrics
    avg_query_time_ms: float
    p95_query_time_ms: float
    p99_query_time_ms: float
    queries_per_second: float

    # Error metrics
    connection_errors: int
    query_errors: int
    error_rate: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "pool_name": self.pool_name,
            "timestamp": self.timestamp.isoformat(),
            "active_connections": self.active_connections,
            "idle_connections": self.idle_connections,
            "total_connections": self.total_connections,
            "max_connections": self.max_connections,
            "pool_utilization": self.pool_utilization,
            "avg_acquire_time_ms": self.avg_acquire_time_ms,
            "p95_acquire_time_ms": self.p95_acquire_time_ms,
            "p99_acquire_time_ms": self.p99_acquire_time_ms,
            "avg_query_time_ms": self.avg_query_time_ms,
            "p95_query_time_ms": self.p95_query_time_ms,
            "p99_query_time_ms": self.p99_query_time_ms,
            "queries_per_second": self.queries_per_second,
            "connection_errors": self.connection_errors,
            "query_errors": self.query_errors,
            "error_rate": self.error_rate,
        }


@dataclass
class ScalingConfig:
    """Configuration for auto-scaling behavior."""

    enabled: bool = True

    # Pool size limits
    min_pool_size: int = 2
    max_pool_size: int = 50
    target_utilization: float = 0.7  # Target 70% utilization

    # Scaling parameters
    scale_up_threshold: float = 0.8  # Scale up when utilization > 80%
    scale_down_threshold: float = 0.3  # Scale down when utilization < 30%
    scale_increment: int = 2  # Number of connections to add/remove
    scale_interval_minutes: int = 5  # Minimum time between scaling operations

    # Performance thresholds
    max_acquire_time_ms: float = 1000.0  # Max acceptable acquire time
    max_query_time_ms: float = 5000.0  # Max acceptable query time
    max_error_rate: float = 0.05  # Max acceptable error rate

    # Monitoring windows
    metrics_window_minutes: int = 10  # Window for calculating metrics
    scaling_cooldown_minutes: int = 15  # Cooldown between scaling operations


@dataclass
class ScalingRecommendation:
    """Recommendation for scaling a connection pool."""

    pool_name: str
    current_size: int
    recommended_size: int
    reason: str
    confidence: float  # 0.0 to 1.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


class PerformanceAnalyzer:
    """Analyzes database performance and generates scaling recommendations."""

    def __init__(self, config: Optional[ScalingConfig] = None):
        self.config = config or ScalingConfig()
        self.metrics_history: Dict[str, List[PerformanceMetrics]] = {}
        self.last_scaling_times: Dict[str, datetime] = {}
        self.app_metrics = get_application_metrics()

    async def analyze_pool_performance(
        self,
        pool_name: str,
        current_metrics: PoolMetrics
    ) -> PerformanceMetrics:
        """Analyze current pool performance and return enriched metrics."""
        # Create performance metrics object
        perf_metrics = PerformanceMetrics(
            pool_name=pool_name,
            timestamp=datetime.utcnow(),
            active_connections=current_metrics.active_connections,
            idle_connections=current_metrics.idle_connections,
            total_connections=current_metrics.total_connections,
            max_connections=current_metrics.max_connections,
            pool_utilization=current_metrics.pool_utilization,
            avg_acquire_time_ms=current_metrics.acquire_timeout_seconds * 1000,
            p95_acquire_time_ms=0.0,  # Would need percentile calculation
            p99_acquire_time_ms=0.0,  # Would need percentile calculation
            avg_query_time_ms=0.0,  # Would need query timing data
            p95_query_time_ms=0.0,
            p99_query_time_ms=0.0,
            queries_per_second=0.0,  # Would need query count data
            connection_errors=0,  # Would need error tracking
            query_errors=0,
            error_rate=0.0,
        )

        # Store in history for trend analysis
        if pool_name not in self.metrics_history:
            self.metrics_history[pool_name] = []

        self.metrics_history[pool_name].append(perf_metrics)

        # Keep only recent metrics
        cutoff_time = datetime.utcnow() - timedelta(minutes=self.config.metrics_window_minutes)
        self.metrics_history[pool_name] = [
            m for m in self.metrics_history[pool_name]
            if m.timestamp > cutoff_time
        ]

        return perf_metrics

    def generate_scaling_recommendation(
        self,
        pool_name: str,
        current_metrics: PoolMetrics
    ) -> Optional[ScalingRecommendation]:
        """Generate scaling recommendation based on current metrics."""

        # Check if we're in cooldown period
        last_scaling = self.last_scaling_times.get(pool_name)
        if last_scaling:
            cooldown_end = last_scaling + timedelta(minutes=self.config.scaling_cooldown_minutes)
            if datetime.utcnow() < cooldown_end:
                return None  # Still in cooldown

        # Analyze current performance
        if current_metrics.pool_utilization > self.config.scale_up_threshold:
            # Scale up recommendation
            new_size = min(
                current_metrics.max_connections + self.config.scale_increment,
                self.config.max_pool_size
            )

            if new_size > current_metrics.max_connections:
                return ScalingRecommendation(
                    pool_name=pool_name,
                    current_size=current_metrics.max_connections,
                    recommended_size=new_size,
                    reason=f"High pool utilization: {current_metrics.pool_utilization:.1%} > {self.config.scale_up_threshold:.1%}",
                    confidence=0.8,
                )

        elif current_metrics.pool_utilization < self.config.scale_down_threshold:
            # Scale down recommendation
            new_size = max(
                current_metrics.max_connections - self.config.scale_increment,
                self.config.min_pool_size
            )

            if new_size < current_metrics.max_connections:
                return ScalingRecommendation(
                    pool_name=pool_name,
                    current_size=current_metrics.max_connections,
                    recommended_size=new_size,
                    reason=f"Low pool utilization: {current_metrics.pool_utilization:.1%} < {self.config.scale_down_threshold:.1%}",
                    confidence=0.6,
                )

        return None

    def get_performance_trends(self, pool_name: str) -> Dict[str, Any]:
        """Get performance trends for a pool."""
        if pool_name not in self.metrics_history:
            return {"error": "No metrics history available"}

        metrics_list = self.metrics_history[pool_name]
        if len(metrics_list) < 2:
            return {"error": "Insufficient data for trend analysis"}

        # Calculate trends
        recent_metrics = metrics_list[-10:]  # Last 10 data points

        utilizations = [m.pool_utilization for m in recent_metrics]
        acquire_times = [m.avg_acquire_time_ms for m in recent_metrics]

        trends = {
            "utilization_trend": "increasing" if utilizations[-1] > utilizations[0] else "decreasing",
            "avg_utilization": statistics.mean(utilizations),
            "max_utilization": max(utilizations),
            "min_utilization": min(utilizations),
            "utilization_volatility": statistics.stdev(utilizations) if len(utilizations) > 1 else 0.0,
            "avg_acquire_time": statistics.mean(acquire_times),
            "acquire_time_trend": "increasing" if acquire_times[-1] > acquire_times[0] else "decreasing",
        }

        return trends


class AutoScalingManager:
    """Manages automatic scaling of database connection pools."""

    def __init__(
        self,
        config: Optional[ScalingConfig] = None,
        registry: Optional[ConnectionManagerRegistry] = None
    ):
        self.config = config or ScalingConfig()
        self.registry = registry or get_connection_manager_registry()
        self.analyzer = PerformanceAnalyzer(self.config)
        self.scaling_task: Optional[asyncio.Task] = None
        self.is_scaling = False

    async def start_auto_scaling(self) -> None:
        """Start automatic scaling monitoring."""
        if self.is_scaling:
            return

        self.is_scaling = True
        self.scaling_task = asyncio.create_task(self._scaling_loop())
        logger.info("Started database auto-scaling")

    async def stop_auto_scaling(self) -> None:
        """Stop automatic scaling."""
        if not self.is_scaling:
            return

        self.is_scaling = False
        if self.scaling_task:
            self.scaling_task.cancel()
            try:
                await self.scaling_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped database auto-scaling")

    async def _scaling_loop(self) -> None:
        """Main scaling monitoring loop."""
        while self.is_scaling:
            try:
                await self._perform_scaling_cycle()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in scaling loop: {e}")
                await asyncio.sleep(60)

    async def _perform_scaling_cycle(self) -> None:
        """Perform one cycle of scaling analysis and execution."""
        pools = await self.registry.get_all_pools()
        scaling_recommendations = []

        for pool_name, pool in pools.items():
            try:
                # Get current metrics
                metrics = await pool.get_pool_metrics()

                # Analyze performance
                perf_metrics = await self.analyzer.analyze_pool_performance(pool_name, metrics)

                # Generate scaling recommendation
                recommendation = self.analyzer.generate_scaling_recommendation(pool_name, metrics)
                if recommendation:
                    scaling_recommendations.append(recommendation)

            except Exception as e:
                logger.error(f"Error analyzing pool {pool_name}: {e}")

        # Execute scaling recommendations
        for recommendation in scaling_recommendations:
            try:
                await self._execute_scaling(recommendation)
            except Exception as e:
                logger.error(f"Error executing scaling for {recommendation.pool_name}: {e}")

    async def _execute_scaling(self, recommendation: ScalingRecommendation) -> None:
        """Execute a scaling recommendation."""
        try:
            # Get the pool manager
            pool = await self.registry.get_pool(recommendation.pool_name)
            if not pool:
                logger.error(f"Pool not found: {recommendation.pool_name}")
                return

            # Update pool configuration (this would need to be implemented in pool managers)
            # For now, just log the recommendation
            logger.info(
                f"Scaling recommendation for {recommendation.pool_name}: "
                f"{recommendation.current_size} -> {recommendation.recommended_size} "
                f"({recommendation.reason})"
            )

            # Update last scaling time
            self.analyzer.last_scaling_times[recommendation.pool_name] = datetime.utcnow()

        except Exception as e:
            logger.error(f"Error executing scaling: {e}")

    async def get_scaling_status(self) -> Dict[str, Any]:
        """Get current auto-scaling status."""
        pools = await self.registry.get_all_pools()
        status = {}

        for pool_name, pool in pools.items():
            try:
                metrics = await pool.get_pool_metrics()
                perf_metrics = await self.analyzer.analyze_pool_performance(pool_name, metrics)
                trends = self.analyzer.get_performance_trends(pool_name)

                recommendation = self.analyzer.generate_scaling_recommendation(pool_name, metrics)

                status[pool_name] = {
                    "current_size": metrics.max_connections,
                    "utilization": metrics.pool_utilization,
                    "performance_metrics": perf_metrics.to_dict(),
                    "trends": trends,
                    "last_scaling": self.analyzer.last_scaling_times.get(pool_name),
                    "recommendation": recommendation.to_dict() if recommendation else None,
                }

            except Exception as e:
                logger.error(f"Error getting status for {pool_name}: {e}")
                status[pool_name] = {"error": str(e)}

        return {
            "auto_scaling_enabled": self.is_scaling,
            "pools_monitored": len(pools),
            "config": self.config.__dict__,
            "pool_status": status,
        }


# Global auto-scaling manager instance
_auto_scaling_manager: Optional[AutoScalingManager] = None


def get_auto_scaling_manager(config: Optional[ScalingConfig] = None) -> AutoScalingManager:
    """Get the global auto-scaling manager."""
    global _auto_scaling_manager
    if _auto_scaling_manager is None:
        _auto_scaling_manager = AutoScalingManager(config)
    return _auto_scaling_manager


async def start_auto_scaling(config: Optional[ScalingConfig] = None) -> None:
    """Start automatic scaling of database connection pools."""
    manager = get_auto_scaling_manager(config)
    await manager.start_auto_scaling()


async def stop_auto_scaling() -> None:
    """Stop automatic scaling."""
    manager = get_auto_scaling_manager()
    await manager.stop_auto_scaling()


async def get_scaling_status() -> Dict[str, Any]:
    """Get current auto-scaling status."""
    manager = get_auto_scaling_manager()
    return await manager.get_scaling_status()


def create_production_scaling_config() -> ScalingConfig:
    """Create production-optimized scaling configuration."""
    return ScalingConfig(
        enabled=True,
        min_pool_size=5,
        max_pool_size=100,
        target_utilization=0.75,
        scale_up_threshold=0.85,
        scale_down_threshold=0.25,
        scale_increment=5,
        scale_interval_minutes=10,
        max_acquire_time_ms=2000.0,
        max_query_time_ms=10000.0,
        max_error_rate=0.02,
        metrics_window_minutes=15,
        scaling_cooldown_minutes=20,
    )
