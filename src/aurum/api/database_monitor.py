"""Database performance monitoring, optimization suggestions, and slow query detection."""

from __future__ import annotations

import asyncio
import time
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

from ..telemetry.context import get_request_id
from .cache import AsyncCache, CacheManager


class QueryPerformanceLevel(Enum):
    """Performance levels for queries."""
    EXCELLENT = "excellent"   # < 100ms
    GOOD = "good"            # 100ms - 500ms
    SLOW = "slow"            # 500ms - 2s
    VERY_SLOW = "very_slow"  # > 2s
    CRITICAL = "critical"    # > 10s


class OptimizationType(Enum):
    """Types of optimization suggestions."""
    INDEX = "index"
    QUERY_REWRITE = "query_rewrite"
    PARTITIONING = "partitioning"
    MATERIALIZED_VIEW = "materialized_view"
    CONNECTION_POOL = "connection_pool"
    CONFIGURATION = "configuration"


@dataclass
class QueryMetrics:
    """Metrics for a single query execution."""
    query_hash: str
    query_text: str
    execution_time: float
    timestamp: datetime
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    endpoint: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    result_count: int = 0
    error_message: Optional[str] = None
    database_engine: str = "trino"

    @property
    def performance_level(self) -> QueryPerformanceLevel:
        """Get performance level for this query."""
        if self.execution_time < 0.1:
            return QueryPerformanceLevel.EXCELLENT
        elif self.execution_time < 0.5:
            return QueryPerformanceLevel.GOOD
        elif self.execution_time < 2.0:
            return QueryPerformanceLevel.SLOW
        elif self.execution_time < 10.0:
            return QueryPerformanceLevel.VERY_SLOW
        else:
            return QueryPerformanceLevel.CRITICAL


@dataclass
class QueryPattern:
    """Represents a pattern of similar queries."""
    query_hash: str
    pattern_text: str  # Normalized query text
    total_executions: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    avg_time: float = 0.0
    error_count: int = 0
    last_seen: datetime = datetime.utcnow()
    first_seen: datetime = datetime.utcnow()
    endpoints: List[str] = field(default_factory=list)
    optimization_suggestions: List[str] = field(default_factory=list)

    def add_execution(self, metrics: QueryMetrics) -> None:
        """Add a query execution to this pattern."""
        self.total_executions += 1
        self.total_time += metrics.execution_time
        self.min_time = min(self.min_time, metrics.execution_time)
        self.max_time = max(self.max_time, metrics.execution_time)
        self.avg_time = self.total_time / self.total_executions
        self.last_seen = metrics.timestamp

        if metrics.endpoint not in self.endpoints:
            self.endpoints.append(metrics.endpoint)

        if metrics.error_message:
            self.error_count += 1

    def get_performance_score(self) -> float:
        """Get a performance score for this query pattern (0-100)."""
        if self.total_executions == 0:
            return 100.0

        # Base score from average execution time
        if self.avg_time < 0.1:
            score = 100.0
        elif self.avg_time < 0.5:
            score = 85.0
        elif self.avg_time < 2.0:
            score = 60.0
        elif self.avg_time < 10.0:
            score = 30.0
        else:
            score = 10.0

        # Adjust for error rate
        error_rate = self.error_count / self.total_executions
        if error_rate > 0.1:  # 10% error rate
            score *= 0.5
        elif error_rate > 0.05:  # 5% error rate
            score *= 0.8

        # Adjust for frequency (very frequent queries get higher priority)
        if self.total_executions > 1000:
            score *= 1.2
        elif self.total_executions > 100:
            score *= 1.1

        return max(0.0, min(100.0, score))


@dataclass
class OptimizationSuggestion:
    """A suggestion for optimizing a query or database."""
    query_hash: str
    suggestion_type: OptimizationType
    title: str
    description: str
    impact: float  # Estimated performance improvement (0-1)
    effort: float  # Estimated implementation effort (0-1)
    confidence: float  # Confidence in the suggestion (0-1)
    created_at: datetime = datetime.utcnow()
    implemented: bool = False

    def get_priority_score(self) -> float:
        """Get priority score for this suggestion."""
        return (self.impact * self.confidence) / (self.effort + 0.1)  # Add small value to avoid division by zero


class DatabaseMonitor:
    """Monitors database query performance and provides optimization suggestions."""

    def __init__(
        self,
        slow_query_threshold: float = 2.0,
        very_slow_query_threshold: float = 10.0,
        cache_manager: Optional[CacheManager] = None
    ):
        self.slow_query_threshold = slow_query_threshold
        self.very_slow_query_threshold = very_slow_query_threshold
        self.cache_manager = cache_manager

        # Query tracking
        self.query_patterns: Dict[str, QueryPattern] = {}
        self.recent_queries: deque = deque(maxlen=10000)  # Keep last 10k queries
        self.slow_queries: deque = deque(maxlen=1000)     # Keep last 1k slow queries

        # Optimization suggestions
        self.optimization_suggestions: Dict[str, List[OptimizationSuggestion]] = defaultdict(list)

        # Monitoring configuration
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.analysis_task: Optional[asyncio.Task] = None

        # Performance thresholds
        self.performance_thresholds = {
            "excellent": 0.1,    # 100ms
            "good": 0.5,        # 500ms
            "slow": 2.0,        # 2s
            "very_slow": 10.0,  # 10s
            "critical": 30.0    # 30s
        }

    async def start_monitoring(self) -> None:
        """Start database monitoring."""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.analysis_task = asyncio.create_task(self._analysis_loop())

    async def stop_monitoring(self) -> None:
        """Stop database monitoring."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False

        if self.monitoring_task:
            self.monitoring_task.cancel()
        if self.analysis_task:
            self.analysis_task.cancel()

    async def record_query(self, metrics: QueryMetrics) -> None:
        """Record a query execution for monitoring."""
        # Add to recent queries
        self.recent_queries.append(metrics)

        # Check if it's a slow query
        if metrics.execution_time >= self.slow_query_threshold:
            self.slow_queries.append(metrics)

            # Generate query hash for pattern tracking
            query_hash = self._generate_query_hash(metrics.query_text)

            # Update or create query pattern
            if query_hash not in self.query_patterns:
                self.query_patterns[query_hash] = QueryPattern(
                    query_hash=query_hash,
                    pattern_text=self._normalize_query(metrics.query_text)
                )

            self.query_patterns[query_hash].add_execution(metrics)

            # Cache slow query for analysis
            if self.cache_manager:
                await self.cache_manager.cache_metadata(
                    metrics.dict(),
                    "slow_query",
                    query_hash=query_hash,
                    timestamp=metrics.timestamp.isoformat()
                )

    async def get_query_patterns(
        self,
        limit: int = 50,
        min_executions: int = 5,
        sort_by: str = "performance_score"
    ) -> List[QueryPattern]:
        """Get query patterns sorted by specified criteria."""
        patterns = [
            pattern for pattern in self.query_patterns.values()
            if pattern.total_executions >= min_executions
        ]

        if sort_by == "performance_score":
            patterns.sort(key=lambda p: p.get_performance_score())
        elif sort_by == "total_time":
            patterns.sort(key=lambda p: p.total_time, reverse=True)
        elif sort_by == "execution_count":
            patterns.sort(key=lambda p: p.total_executions, reverse=True)
        elif sort_by == "avg_time":
            patterns.sort(key=lambda p: p.avg_time, reverse=True)

        return patterns[:limit]

    async def get_slow_queries(
        self,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[QueryMetrics]:
        """Get slow queries within a time range."""
        queries = list(self.slow_queries)

        if since:
            queries = [q for q in queries if q.timestamp >= since]

        # Sort by execution time (slowest first)
        queries.sort(key=lambda q: q.execution_time, reverse=True)

        return queries[:limit]

    async def generate_optimization_suggestions(self, query_hash: str) -> List[OptimizationSuggestion]:
        """Generate optimization suggestions for a query."""
        if query_hash not in self.query_patterns:
            return []

        pattern = self.query_patterns[query_hash]
        suggestions = []

        # Analyze query patterns and suggest optimizations
        normalized_query = pattern.pattern_text.lower()

        # Index suggestions
        if "where" in normalized_query and "select" in normalized_query:
            if self._count_table_joins(normalized_query) > 0:
                suggestions.append(OptimizationSuggestion(
                    query_hash=query_hash,
                    suggestion_type=OptimizationType.INDEX,
                    title="Add Missing Indexes",
                    description=f"Query accesses multiple tables without proper indexing. Consider adding indexes on frequently filtered columns.",
                    impact=0.8,
                    effort=0.6,
                    confidence=0.7
                ))

        # Query rewrite suggestions
        if "select *" in normalized_query:
            suggestions.append(OptimizationSuggestion(
                query_hash=query_hash,
                suggestion_type=OptimizationType.QUERY_REWRITE,
                title="Avoid SELECT *",
                description="Using SELECT * prevents query optimization. Specify only the columns you need.",
                impact=0.3,
                effort=0.1,
                confidence=0.9
            ))

        # Partitioning suggestions for large tables
        if "order by" in normalized_query and pattern.total_executions > 1000:
            suggestions.append(OptimizationSuggestion(
                query_hash=query_hash,
                suggestion_type=OptimizationType.PARTITIONING,
                title="Consider Table Partitioning",
                description="High-frequency queries with ORDER BY may benefit from table partitioning for better performance.",
                impact=0.7,
                effort=0.8,
                confidence=0.5
            ))

        # Materialized view for complex aggregations
        if self._has_aggregations(normalized_query) and pattern.total_executions > 100:
            suggestions.append(OptimizationSuggestion(
                query_hash=query_hash,
                suggestion_type=OptimizationType.MATERIALIZED_VIEW,
                title="Consider Materialized View",
                description="Complex aggregation queries run frequently could benefit from materialized views.",
                impact=0.9,
                effort=0.7,
                confidence=0.6
            ))

        return suggestions

    async def get_optimization_suggestions(
        self,
        limit: int = 20,
        min_impact: float = 0.5
    ) -> List[OptimizationSuggestion]:
        """Get optimization suggestions sorted by priority."""
        all_suggestions = []

        for pattern in self.query_patterns.values():
            suggestions = await self.generate_optimization_suggestions(pattern.query_hash)
            all_suggestions.extend(suggestions)

        # Sort by priority score
        all_suggestions.sort(key=lambda s: s.get_priority_score(), reverse=True)

        # Filter by minimum impact
        return [s for s in all_suggestions if s.impact >= min_impact][:limit]

    async def get_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of database performance."""
        total_queries = len(self.recent_queries)
        slow_queries = len(self.slow_queries)

        if total_queries == 0:
            return {"message": "No queries recorded yet"}

        slow_query_percentage = (slow_queries / total_queries) * 100

        # Calculate average performance
        total_time = sum(q.execution_time for q in self.recent_queries)
        avg_time = total_time / total_queries

        # Get performance distribution
        performance_levels = {
            level.value: 0 for level in QueryPerformanceLevel
        }

        for query in self.recent_queries:
            level = query.performance_level.value
            performance_levels[level] += 1

        # Get top slow queries
        top_slow_queries = await self.get_slow_queries(limit=5)

        return {
            "total_queries": total_queries,
            "slow_queries": slow_queries,
            "slow_query_percentage": round(slow_query_percentage, 2),
            "average_query_time": round(avg_time, 3),
            "performance_distribution": performance_levels,
            "top_slow_queries": [
                {
                    "query_hash": q.query_hash,
                    "execution_time": q.execution_time,
                    "endpoint": q.endpoint,
                    "timestamp": q.timestamp.isoformat()
                }
                for q in top_slow_queries
            ],
            "total_patterns": len(self.query_patterns),
            "request_id": get_request_id(),
        }

    def _generate_query_hash(self, query_text: str) -> str:
        """Generate a hash for a query to identify patterns."""
        # Normalize query by removing extra whitespace and parameter values
        normalized = self._normalize_query(query_text)
        return hashlib.md5(normalized.encode()).hexdigest()

    def _normalize_query(self, query_text: str) -> str:
        """Normalize a query for pattern matching."""
        # Remove extra whitespace and normalize
        normalized = ' '.join(query_text.split())
        # Remove parameter values (basic implementation)
        import re
        normalized = re.sub(r'\d+', '?', normalized)
        normalized = re.sub(r"'[^']*'", '?', normalized)
        return normalized

    def _count_table_joins(self, query: str) -> int:
        """Count the number of table joins in a query."""
        # Simple heuristic - count JOIN keywords
        return query.count(' join ') + query.count('JOIN ')

    def _has_aggregations(self, query: str) -> bool:
        """Check if query contains aggregations."""
        aggregation_functions = ['count(', 'sum(', 'avg(', 'min(', 'max(', 'group by']
        return any(func in query for func in aggregation_functions)

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                # Clean up old data periodically
                await self._cleanup_old_data()

                # Check for slow queries to alert on
                await self._check_slow_queries()

                await asyncio.sleep(60)  # Run every minute

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log error but continue
                print(f"Monitoring loop error: {exc}")
                await asyncio.sleep(10)

    async def _analysis_loop(self) -> None:
        """Analysis loop for generating optimization suggestions."""
        while self.monitoring_active:
            try:
                # Generate optimization suggestions for active patterns
                for query_hash in list(self.query_patterns.keys()):
                    if query_hash not in self.optimization_suggestions:
                        suggestions = await self.generate_optimization_suggestions(query_hash)
                        if suggestions:
                            self.optimization_suggestions[query_hash] = suggestions

                await asyncio.sleep(300)  # Run every 5 minutes

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log error but continue
                print(f"Analysis loop error: {exc}")
                await asyncio.sleep(60)

    async def _cleanup_old_data(self) -> None:
        """Clean up old monitoring data."""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)

        # Clean up old recent queries
        while self.recent_queries and self.recent_queries[0].timestamp < cutoff_time:
            self.recent_queries.popleft()

        # Clean up old slow queries
        while self.slow_queries and self.slow_queries[0].timestamp < cutoff_time:
            self.slow_queries.popleft()

        # Clean up old query patterns (keep only recent ones)
        to_remove = []
        for query_hash, pattern in self.query_patterns.items():
            if pattern.last_seen < cutoff_time:
                to_remove.append(query_hash)

        for query_hash in to_remove:
            del self.query_patterns[query_hash]
            if query_hash in self.optimization_suggestions:
                del self.optimization_suggestions[query_hash]

    async def _check_slow_queries(self) -> None:
        """Check for slow queries and generate alerts if needed."""
        if not self.slow_queries:
            return

        recent_slow_queries = [
            q for q in self.slow_queries
            if q.timestamp > datetime.utcnow() - timedelta(minutes=5)
        ]

        if len(recent_slow_queries) > 10:  # Alert threshold
            # In a real implementation, this would send alerts
            print(f"ALERT: {len(recent_slow_queries)} slow queries detected in last 5 minutes")

            # Group by endpoint
            endpoint_counts = defaultdict(int)
            for query in recent_slow_queries:
                endpoint_counts[query.endpoint] += 1

            worst_endpoint = max(endpoint_counts.items(), key=lambda x: x[1])
            print(f"Worst performing endpoint: {worst_endpoint[0]} ({worst_endpoint[1]} slow queries)")

    async def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get monitoring system statistics."""
        return {
            "monitoring_active": self.monitoring_active,
            "recent_queries_count": len(self.recent_queries),
            "slow_queries_count": len(self.slow_queries),
            "query_patterns_count": len(self.query_patterns),
            "optimization_suggestions_count": sum(len(s) for s in self.optimization_suggestions.values()),
            "cache_manager_available": self.cache_manager is not None,
            "slow_query_threshold": self.slow_query_threshold,
            "very_slow_query_threshold": self.very_slow_query_threshold,
            "performance_thresholds": self.performance_thresholds,
            "request_id": get_request_id(),
        }


# Global database monitor instance
_database_monitor: Optional[DatabaseMonitor] = None


def get_database_monitor() -> DatabaseMonitor:
    """Get the global database monitor."""
    global _database_monitor
    if _database_monitor is None:
        _database_monitor = DatabaseMonitor()
    return _database_monitor


async def initialize_database_monitoring(
    slow_query_threshold: float = 2.0,
    cache_manager: Optional[CacheManager] = None
) -> DatabaseMonitor:
    """Initialize the database monitoring system."""
    global _database_monitor

    if _database_monitor is None:
        _database_monitor = DatabaseMonitor(
            slow_query_threshold=slow_query_threshold,
            cache_manager=cache_manager
        )

        await _database_monitor.start_monitoring()

    return _database_monitor
