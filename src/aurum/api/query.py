from __future__ import annotations

"""SQL query builders for Aurum data services."""

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional, Union, Tuple, List, Mapping, Sequence
import hashlib
import json

from .http.pagination import MAX_PAGE_SIZE


ORDER_COLUMNS = [
    "curve_key",
    "tenor_label",
    "contract_month",
    "asof_date",
    "price_type",
]

DIFF_ORDER_COLUMNS = [
    "curve_key",
    "tenor_label",
    "contract_month",
]


class QueryOptimizer:
    """Query optimization utilities for improved data access patterns."""

    def __init__(self):
        self.query_cache: Dict[str, str] = {}
        self.performance_metrics: Dict[str, Any] = {}

    def optimize_query_structure(
        self,
        base_table: str,
        filters: Dict[str, Optional[str]],
        select_columns: List[str],
        order_columns: List[str],
        limit: int,
        offset: int = 0
    ) -> str:
        """Optimize query structure for better performance."""
        # Analyze filter selectivity
        filter_analysis = self._analyze_filter_selectivity(filters)

        # Optimize column selection
        optimized_columns = self._optimize_column_selection(select_columns, filters)

        # Build optimized query
        query_parts = []

        # SELECT clause with optimized columns
        query_parts.append(f"SELECT {', '.join(optimized_columns)}")

        # FROM clause
        query_parts.append(f"FROM {base_table}")

        # WHERE clause with optimized filters
        where_clause = self._build_optimized_where_clause(filters, filter_analysis)
        if where_clause:
            query_parts.append(where_clause)

        # ORDER BY clause
        if order_columns:
            order_clause = f"ORDER BY {', '.join(order_columns)}"
            query_parts.append(order_clause)

        # LIMIT clause
        if limit > 0:
            query_parts.append(f"LIMIT {limit}")
            if offset > 0:
                query_parts.append(f"OFFSET {offset}")

        return " ".join(query_parts)

    def _analyze_filter_selectivity(self, filters: Dict[str, Optional[str]]) -> Dict[str, float]:
        """Analyze filter selectivity to optimize query execution."""
        selectivity_scores = {}

        for column, value in filters.items():
            if value is None:
                continue

            # Estimate selectivity based on column type and value characteristics
            if column in ["iso", "location", "market"]:
                # Geographic filters typically have medium selectivity
                selectivity_scores[column] = 0.3
            elif column in ["asset_class", "product"]:
                # Category filters typically have low selectivity
                selectivity_scores[column] = 0.1
            elif column in ["asof_date", "contract_month"]:
                # Date filters typically have high selectivity
                selectivity_scores[column] = 0.8
            elif column in ["curve_key", "tenor_type"]:
                # Specific identifier filters have very high selectivity
                selectivity_scores[column] = 0.95
            else:
                # Default selectivity estimate
                selectivity_scores[column] = 0.5

        return selectivity_scores

    def _optimize_column_selection(
        self,
        requested_columns: List[str],
        filters: Dict[str, Optional[str]]
    ) -> List[str]:
        """Optimize column selection based on filters and usage patterns."""
        # For filtered queries, we might need additional columns for ordering
        needed_columns = set(requested_columns)

        # Add columns needed for efficient filtering
        for column in filters.keys():
            if column not in needed_columns:
                needed_columns.add(column)

        # Optimize column order for better cache locality
        # Primary key columns first, then frequently accessed columns
        priority_columns = ["curve_key", "asof_date", "contract_month"]
        ordered_columns = []

        for col in priority_columns:
            if col in needed_columns:
                ordered_columns.append(col)
                needed_columns.remove(col)

        ordered_columns.extend(sorted(needed_columns))
        return ordered_columns

    def _build_optimized_where_clause(
        self,
        filters: Dict[str, Optional[str]],
        selectivity_scores: Dict[str, float]
    ) -> str:
        """Build optimized WHERE clause with filters ordered by selectivity."""
        if not filters:
            return ""

        # Order filters by selectivity (high selectivity first)
        sorted_filters = sorted(
            [(col, val) for col, val in filters.items() if val is not None],
            key=lambda x: selectivity_scores.get(x[0], 0.5),
            reverse=True
        )

        clauses = [f"{col} = '{_safe_literal(val)}'" for col, val in sorted_filters]
        return " WHERE " + " AND ".join(clauses)

    def get_query_hash(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate a hash for query caching and optimization."""
        cache_key_data = {
            "query": query,
            "params": params or {}
        }
        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        return hashlib.md5(cache_key_str.encode()).hexdigest()

    def cache_query_plan(self, query_hash: str, optimized_query: str) -> None:
        """Cache optimized query for reuse."""
        self.query_cache[query_hash] = optimized_query

    def get_cached_query(self, query_hash: str) -> Optional[str]:
        """Get cached optimized query."""
        return self.query_cache.get(query_hash)


# Global query optimizer instance
_query_optimizer = QueryOptimizer()


def get_query_optimizer() -> QueryOptimizer:
    """Get the global query optimizer instance."""
    return _query_optimizer


class DataAccessPatternOptimizer:
    """Optimize data access patterns for better performance and resource usage."""

    def __init__(self):
        self.access_patterns: Dict[str, Any] = {}
        self.query_frequency: Dict[str, int] = {}
        self.cache_hit_patterns: Dict[str, float] = {}

    def record_data_access(
        self,
        table: str,
        operation: str,
        columns: List[str],
        filters: Dict[str, Any],
        execution_time: float,
        result_count: int
    ) -> None:
        """Record data access pattern for optimization analysis."""
        pattern_key = f"{table}:{operation}:{len(columns)}:{len(filters)}"

        if pattern_key not in self.access_patterns:
            self.access_patterns[pattern_key] = {
                "count": 0,
                "total_time": 0.0,
                "total_results": 0,
                "columns": set(),
                "filters": set(),
            }

        pattern = self.access_patterns[pattern_key]
        pattern["count"] += 1
        pattern["total_time"] += execution_time
        pattern["total_results"] += result_count
        pattern["columns"].update(columns)
        pattern["filters"].update(filters.keys())

        # Track query frequency
        self.query_frequency[pattern_key] = pattern["count"]

    def suggest_optimizations(self) -> List[Dict[str, Any]]:
        """Suggest optimizations based on observed access patterns."""
        suggestions = []

        # Analyze slow patterns
        slow_threshold = 1000  # ms
        for pattern_key, pattern in self.access_patterns.items():
            avg_time = pattern["total_time"] / pattern["count"]
            if avg_time > slow_threshold:
                suggestions.append({
                    "type": "slow_query_pattern",
                    "pattern": pattern_key,
                    "avg_time": avg_time,
                    "frequency": pattern["count"],
                    "suggestion": "Consider adding indexes or optimizing query structure",
                    "columns": list(pattern["columns"]),
                    "filters": list(pattern["filters"]),
                })

        # Analyze frequently accessed patterns
        frequent_threshold = 10
        for pattern_key, frequency in self.query_frequency.items():
            if frequency > frequent_threshold:
                pattern = self.access_patterns[pattern_key]
                suggestions.append({
                    "type": "frequent_access_pattern",
                    "pattern": pattern_key,
                    "frequency": frequency,
                    "suggestion": "Consider caching this query pattern",
                    "columns": list(pattern["columns"]),
                    "filters": list(pattern["filters"]),
                })

        return suggestions

    def optimize_access_pattern(
        self,
        table: str,
        columns: List[str],
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Suggest optimized access pattern for given parameters."""
        optimization = {
            "suggested_indexes": [],
            "cache_strategy": "none",
            "query_structure": "standard",
            "estimated_performance": "unknown"
        }

        # Suggest indexes based on filter patterns
        if filters:
            filter_columns = list(filters.keys())
            optimization["suggested_indexes"] = [
                f"CREATE INDEX IF NOT EXISTS idx_{table}_{col} ON {table} ({col})"
                for col in filter_columns
            ]

        # Suggest caching for frequently accessed patterns
        pattern_key = f"{table}:select:{len(columns)}:{len(filters)}"
        if self.query_frequency.get(pattern_key, 0) > 5:
            optimization["cache_strategy"] = "aggressive"
            optimization["estimated_performance"] = "improved"

        return optimization


# Global data access optimizer instance
_data_access_optimizer = DataAccessPatternOptimizer()


def get_data_access_optimizer() -> DataAccessPatternOptimizer:
    """Get the global data access optimizer instance."""
    return _data_access_optimizer


class QueryPerformanceMonitor:
    """Monitor and analyze query performance for optimization."""

    def __init__(self):
        self.query_metrics: Dict[str, Any] = {}
        self.slow_query_threshold = 1000  # ms
        self.frequent_query_threshold = 10

    def record_query_performance(
        self,
        query_hash: str,
        query: str,
        execution_time: float,
        result_count: int,
        cache_hit: bool = False,
        table: Optional[str] = None
    ) -> None:
        """Record query performance metrics."""
        if query_hash not in self.query_metrics:
            self.query_metrics[query_hash] = {
                "query": query,
                "table": table,
                "executions": 0,
                "total_time": 0.0,
                "min_time": float('inf'),
                "max_time": 0.0,
                "total_results": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "first_seen": None,
                "last_seen": None,
            }

        metrics = self.query_metrics[query_hash]
        metrics["executions"] += 1
        metrics["total_time"] += execution_time
        metrics["min_time"] = min(metrics["min_time"], execution_time)
        metrics["max_time"] = max(metrics["max_time"], execution_time)
        metrics["total_results"] += result_count

        if cache_hit:
            metrics["cache_hits"] += 1
        else:
            metrics["cache_misses"] += 1

        if metrics["first_seen"] is None:
            metrics["first_seen"] = time.time()
        metrics["last_seen"] = time.time()

    def get_slow_queries(self) -> List[Dict[str, Any]]:
        """Get queries that exceed performance thresholds."""
        slow_queries = []

        for query_hash, metrics in self.query_metrics.items():
            avg_time = metrics["total_time"] / metrics["executions"]

            if avg_time > self.slow_query_threshold:
                slow_queries.append({
                    "query_hash": query_hash,
                    "query": metrics["query"][:200] + "..." if len(metrics["query"]) > 200 else metrics["query"],
                    "avg_time": avg_time,
                    "max_time": metrics["max_time"],
                    "executions": metrics["executions"],
                    "table": metrics["table"],
                    "cache_hit_rate": metrics["cache_hits"] / (metrics["cache_hits"] + metrics["cache_misses"])
                    if (metrics["cache_hits"] + metrics["cache_misses"]) > 0 else 0,
                })

        return sorted(slow_queries, key=lambda x: x["avg_time"], reverse=True)

    def get_frequently_executed_queries(self) -> List[Dict[str, Any]]:
        """Get queries that are executed frequently."""
        frequent_queries = []

        for query_hash, metrics in self.query_metrics.items():
            if metrics["executions"] >= self.frequent_query_threshold:
                frequent_queries.append({
                    "query_hash": query_hash,
                    "query": metrics["query"][:200] + "..." if len(metrics["query"]) > 200 else metrics["query"],
                    "executions": metrics["executions"],
                    "avg_time": metrics["total_time"] / metrics["executions"],
                    "table": metrics["table"],
                })

        return sorted(frequent_queries, key=lambda x: x["executions"], reverse=True)

    def suggest_query_optimizations(self) -> List[Dict[str, Any]]:
        """Suggest query optimizations based on performance data."""
        suggestions = []

        for query_hash, metrics in self.query_metrics.items():
            avg_time = metrics["total_time"] / metrics["executions"]

            if avg_time > self.slow_query_threshold:
                suggestions.append({
                    "type": "slow_query",
                    "query_hash": query_hash,
                    "avg_time": avg_time,
                    "executions": metrics["executions"],
                    "suggestion": "Consider query optimization, indexing, or caching",
                    "table": metrics["table"],
                })

            # Suggest caching for frequently executed queries
            if metrics["executions"] >= self.frequent_query_threshold and avg_time < 100:
                suggestions.append({
                    "type": "cache_candidate",
                    "query_hash": query_hash,
                    "executions": metrics["executions"],
                    "avg_time": avg_time,
                    "suggestion": "Consider caching this frequently executed query",
                    "table": metrics["table"],
                })

        return suggestions


# Global query performance monitor instance
_query_performance_monitor = QueryPerformanceMonitor()


def get_query_performance_monitor() -> QueryPerformanceMonitor:
    """Get the global query performance monitor instance."""
    return _query_performance_monitor


class OptimizedQueryBuilder:
    """Query builder with optimization features."""

    def __init__(self):
        self.query_optimizer = get_query_optimizer()
        self.performance_monitor = get_query_performance_monitor()
        self.data_access_optimizer = get_data_access_optimizer()

    def build_optimized_curve_query(
        self,
        *,
        asof: Optional[date],
        curve_key: Optional[str],
        asset_class: Optional[str],
        iso: Optional[str],
        location: Optional[str],
        market: Optional[str],
        product: Optional[str],
        block: Optional[str],
        tenor_type: Optional[str],
        limit: int,
        offset: int = 0,
        cursor_after: Optional[Dict[str, Any]] = None,
        cursor_before: Optional[Dict[str, Any]] = None,
        descending: bool = False,
    ) -> str:
        """Build an optimized curve query with performance monitoring."""

        # Prepare parameters for optimization
        base_table = "iceberg.market.curve_observation"
        filters = {
            k: v for k, v in {
                "curve_key": curve_key,
                "asset_class": asset_class,
                "iso": iso,
                "location": location,
                "market": market,
                "product": product,
                "block": block,
                "tenor_type": tenor_type,
            }.items() if v is not None
        }

        select_columns = [
            "curve_key", "tenor_label", "tenor_type", "contract_month",
            "asof_date", "mid", "bid", "ask", "price_type"
        ]

        order_columns = ORDER_COLUMNS

        # Get optimization suggestions
        access_optimization = self.data_access_optimizer.optimize_access_pattern(
            base_table, select_columns, filters
        )

        # Build optimized query
        optimized_query = self.query_optimizer.optimize_query_structure(
            base_table=base_table,
            filters=filters,
            select_columns=select_columns,
            order_columns=order_columns,
            limit=limit,
            offset=offset
        )

        # Add cursor-based pagination if needed
        if cursor_after or cursor_before:
            cursor_clause = self._build_cursor_clause(
                cursor_after, cursor_before, descending
            )
            if cursor_clause:
                optimized_query += cursor_clause

        # Add asof date filter if specified
        if asof:
            asof_clause = f" AND asof_date = DATE '{asof.isoformat()}'"
            optimized_query += asof_clause

        # Generate query hash for caching and monitoring
        query_hash = self.query_optimizer.get_query_hash(optimized_query, {
            "filters": filters,
            "limit": limit,
            "offset": offset,
            "descending": descending,
        })

        # Cache the optimized query
        self.query_optimizer.cache_query_plan(query_hash, optimized_query)

        return optimized_query

    def _build_cursor_clause(
        self,
        cursor_after: Optional[Dict[str, Any]],
        cursor_before: Optional[Dict[str, Any]],
        descending: bool
    ) -> str:
        """Build cursor-based pagination clause."""
        if cursor_after:
            comparison = ">"
            cursor = cursor_after
            offset = 0
        elif cursor_before:
            comparison = "<"
            cursor = cursor_before
            offset = 0
        else:
            return ""

        # Build cursor conditions
        conditions = []
        for col in ORDER_COLUMNS:
            if col in cursor:
                value = cursor[col]
                if col in {"contract_month", "asof_date"}:
                    conditions.append(f"{col} {comparison} DATE '{value}'")
                elif col in {"period_start", "period_end", "ingest_ts"}:
                    conditions.append(f"{col} {comparison} TIMESTAMP '{value}'")
                else:
                    conditions.append(f"{col} {comparison} '{value}'")

        if conditions:
            return f" AND ({' AND '.join(conditions)})"

        return ""

    def execute_with_monitoring(
        self,
        query: str,
        table: str,
        cache_manager = None
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Execute query with performance monitoring."""
        import time

        query_hash = self.query_optimizer.get_query_hash(query)

        start_time = time.time()

        try:
            # Check cache first
            cache_hit = False
            if cache_manager:
                cached_result = cache_manager.get(query_hash)
                if cached_result:
                    cache_hit = True
                    execution_time = (time.time() - start_time) * 1000
                    self.performance_monitor.record_query_performance(
                        query_hash, query, execution_time, len(cached_result), cache_hit=True, table=table
                    )
                    return cached_result, execution_time

            # Execute query
            # This would normally execute the actual query
            # For now, return empty result to maintain structure
            result = []

            execution_time = (time.time() - start_time) * 1000

            # Record performance metrics
            self.performance_monitor.record_query_performance(
                query_hash, query, execution_time, len(result), cache_hit=False, table=table
            )

            # Cache result if cache manager is available
            if cache_manager and result:
                cache_manager.set(query_hash, result, ttl_seconds=300)

            return result, execution_time

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            self.performance_monitor.record_query_performance(
                query_hash, query, execution_time, 0, cache_hit=False, table=table
            )
            raise


# Global optimized query builder instance
_optimized_query_builder = OptimizedQueryBuilder()


def get_optimized_query_builder() -> OptimizedQueryBuilder:
    """Get the global optimized query builder instance."""
    return _optimized_query_builder


def _safe_literal(value: str) -> str:
    return value.replace("'", "''")


def build_filter_clause(filters: Dict[str, Optional[str]]) -> str:
    clauses = [f"{col} = '{_safe_literal(val)}'" for col, val in filters.items() if val is not None]
    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _order_expression(column: str, *, alias: str = "") -> str:
    qualified = f"{alias}{column}" if alias else column
    if column in {"contract_month", "asof_date"}:
        return f"coalesce(cast({qualified} as date), DATE '0001-01-01')"
    if column in {"period_start", "period_end", "ingest_ts"}:
        return f"coalesce(cast({qualified} as timestamp), TIMESTAMP '0001-01-01 00:00:00')"
    if column in {"tenor_label", "price_type"}:
        return f"coalesce({qualified}, '')"
    return qualified


def _literal_for_column(column: str, value: Any) -> str:
    if column in {"asof_date", "contract_month"}:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return f"DATE '{_safe_literal(value.isoformat())}'"
        return "DATE '0001-01-01'"
    if column in {"period_start", "period_end", "ingest_ts"}:
        if isinstance(value, datetime):
            aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            iso = aware.astimezone(timezone.utc).isoformat(sep=" ", timespec="microseconds")
            return f"TIMESTAMP '{_safe_literal(iso)}'"
        if value:
            return f"TIMESTAMP '{_safe_literal(str(value))}'"
        return "TIMESTAMP '0001-01-01 00:00:00'"
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        iso = aware.astimezone(timezone.utc).isoformat(sep=" ", timespec="microseconds")
        return f"TIMESTAMP '{_safe_literal(iso)}'"
    safe_val = _safe_literal(str(value or ""))
    return f"'{safe_val}'"


def _escape_like_pattern(value: str) -> str:
    """Escape user supplied text for safe LIKE pattern usage."""

    return value.replace("\\", "\\\\").replace("%", "%%").replace("_", "__")


def _timestamp_literal(value: Any) -> str:
    """Serialise a Python value into a Trino timestamp literal."""

    if isinstance(value, datetime):
        iso = value.isoformat(sep=" ", timespec="microseconds")
    elif isinstance(value, date):
        iso = datetime(value.year, value.month, value.day).isoformat(sep=" ")
    else:
        iso = str(value)
    return f"TIMESTAMP '{_safe_literal(iso)}'"


def build_keyset_clause(
    cursor: Optional[Dict[str, Any]],
    *,
    alias: str = "",
    order_columns: Iterable[str] = ORDER_COLUMNS,
    comparison: str = ">",
) -> str:
    if not cursor:
        return ""
    alias_prefix = alias
    if alias_prefix and not alias_prefix.endswith("."):
        alias_prefix = f"{alias_prefix}."
    columns = list(order_columns)
    clauses = []
    for idx, column in enumerate(columns):
        if column not in cursor:
            continue
        literal = _literal_for_column(column, cursor.get(column))
        expr = _order_expression(column, alias=alias_prefix)
        base_condition = f"{expr} {comparison} {literal}"
        if idx == 0:
            clauses.append(base_condition)
            continue
        equals_chain = []
        for prev in columns[:idx]:
            prev_literal = _literal_for_column(prev, cursor.get(prev))
            prev_expr = _order_expression(prev, alias=alias_prefix)
            equals_chain.append(f"{prev_expr} = {prev_literal}")
        chain = " AND ".join(equals_chain + [base_condition])
        clauses.append(f"({chain})")
    if not clauses:
        return ""
    return " AND (" + " OR ".join(clauses) + ")"


def build_curve_query(
    *,
    asof: Optional[date],
    curve_key: Optional[str],
    asset_class: Optional[str],
    iso: Optional[str],
    location: Optional[str],
    market: Optional[str],
    product: Optional[str],
    block: Optional[str],
    tenor_type: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> str:
    limit = max(1, min(limit, MAX_PAGE_SIZE))
    base_latest = "iceberg.market.curves_latest"
    base_asof = "iceberg.market.curves_asof"
    filters: Dict[str, Optional[str]] = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where = build_filter_clause(filters)
    select_cols = (
        "curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "cast(asof_date as date) as asof_date, mid, bid, ask, price_type"
    )
    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in ORDER_COLUMNS)

    comparison_cursor = cursor_after
    comparison = ">"
    effective_offset = offset
    if cursor_before:
        comparison_cursor = cursor_before
        comparison = "<"
        effective_offset = 0

    if asof:
        asof_clause = f"asof_date = DATE '{asof.isoformat()}'"
        where_final = where + (" AND " if where else " WHERE ") + asof_clause
        where_final += build_keyset_clause(
            comparison_cursor,
            alias="",
            order_columns=ORDER_COLUMNS,
            comparison=comparison,
        )
        if comparison_cursor:
            effective_offset = 0
        return (
            f"SELECT {select_cols} FROM {base_asof}{where_final}{order_clause} "
            f"LIMIT {limit} OFFSET {effective_offset}"
        )

    keyset_clause = build_keyset_clause(
        comparison_cursor,
        alias="",
        order_columns=ORDER_COLUMNS,
        comparison=comparison,
    )
    if comparison_cursor:
        effective_offset = 0
    return (
        f"SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
        f"FROM {base_latest}{where}{keyset_clause}{order_clause} "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )


def build_curve_export_query(
    *,
    asof: Optional[str],
    iso: Optional[str],
    market: Optional[str],
    location: Optional[str],
    product: Optional[str],
    block: Optional[str],
    curve_key: Optional[str] = None,
    asset_class: Optional[str] = None,
    tenor_type: Optional[str] = None,
    descending: bool = False,
) -> str:
    """Build an export-oriented query without pagination limits."""

    filters: Dict[str, Optional[str]] = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where_clause = build_filter_clause(filters)
    if asof:
        asof_literal = _safe_literal(asof)
        clause = f"asof_date = DATE '{asof_literal}'"
        if where_clause:
            where_clause += f" AND {clause}"
        else:
            where_clause = f" WHERE {clause}"

    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in ORDER_COLUMNS)

    select_cols = (
        "curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "cast(asof_date as date) as asof_date, mid, bid, ask, price_type"
    )

    return f"SELECT {select_cols} FROM iceberg.market.curve_observation{where_clause}{order_clause}"


def build_curve_diff_query(
    *,
    asof_a: date,
    asof_b: date,
    curve_key: Optional[str],
    asset_class: Optional[str],
    iso: Optional[str],
    location: Optional[str],
    market: Optional[str],
    product: Optional[str],
    block: Optional[str],
    tenor_type: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]] = None,
) -> str:
    limit = max(1, min(limit, MAX_PAGE_SIZE))
    base = "iceberg.market.curve_observation"
    filters = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where = build_filter_clause(filters)
    asof_in = (
        f"(DATE '{asof_a.isoformat()}', DATE '{asof_b.isoformat()}')"
    )
    where_final = where + f" AND asof_date IN {asof_in}" if where else f" WHERE asof_date IN {asof_in}"
    cte = (
        "WITH base AS ("
        " SELECT curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "        cast(asof_date as date) as asof_date, mid"
        f" FROM {base}{where_final}"
        ")"
    )
    keyset_clause = build_keyset_clause(cursor_after, alias="a", order_columns=DIFF_ORDER_COLUMNS)
    effective_offset = 0 if cursor_after else offset
    return (
        f"{cte} "
        "SELECT a.curve_key, a.tenor_label, a.tenor_type, a.contract_month, "
        "a.asof_date as asof_a, a.mid as mid_a, "
        "b.asof_date as asof_b, b.mid as mid_b, "
        "(b.mid - a.mid) as diff_abs, "
        "CASE WHEN a.mid IS NOT NULL AND a.mid <> 0 THEN (b.mid - a.mid) / a.mid ELSE NULL END as diff_pct "
        "FROM base a JOIN base b ON a.curve_key = b.curve_key AND a.tenor_label = b.tenor_label "
        f"WHERE a.asof_date = DATE '{asof_a.isoformat()}' AND b.asof_date = DATE '{asof_b.isoformat()}' "
        f"{keyset_clause} "
        "ORDER BY a.curve_key, a.tenor_label, a.contract_month "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )


_CATALOG_SELECT_COLUMNS: tuple[str, ...] = (
    "tenant_id",
    "provider",
    "series_id",
    "dataset_code",
    "title",
    "description",
    "unit_code",
    "frequency_code",
    "provider_geo_code",
    "canonical_region_id",
    "canonical_region_name",
    "geography_type",
    "mapping_status",
    "status",
    "category",
    "source_url",
    "notes",
    "start_ts",
    "end_ts",
    "last_observation_ts",
    "asof_date",
    "tags",
    "iso_code",
    "iso_market",
    "iso_product",
    "iso_location_type",
    "iso_location_name",
    "iso_location_id",
    "iso_timezone",
    "iso_interval_minutes",
    "iso_unit",
    "iso_subject",
    "iso_curve_role",
)

_CATALOG_EQUAL_FILTERS: dict[str, str] = {
    "provider": "provider",
    "dataset_code": "dataset_code",
    "status": "status",
    "iso_code": "iso_code",
    "iso_market": "iso_market",
    "iso_product": "iso_product",
    "iso_location_type": "iso_location_type",
    "iso_location_id": "iso_location_id",
    "canonical_region_id": "canonical_region_id",
    "geography_type": "geography_type",
    "category": "category",
}

_CATALOG_ILIKE_FILTERS: dict[str, str] = {
    "title": "title",
    "description": "description",
    "iso_location_name": "iso_location_name",
}

_CATALOG_TIME_FILTERS: dict[str, tuple[str, str]] = {
    "start_ts_from": ("start_ts", ">="),
    "start_ts_to": ("start_ts", "<="),
    "end_ts_from": ("end_ts", ">="),
    "end_ts_to": ("end_ts", "<="),
    "last_obs_from": ("last_observation_ts", ">="),
    "last_obs_to": ("last_observation_ts", "<="),
}


def build_series_catalog_query(
    *,
    tenant_id: str,
    filters: Mapping[str, Any] | None,
    limit: int,
    offset: int,
    order_by: Sequence[str] | None = None,
) -> str:
    """Build a paginated query against the external series catalog view."""

    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    safe_tenant = _safe_literal(tenant_id)
    clauses = [f"tenant_id = '{safe_tenant}'"]
    filters = filters or {}

    for key, column in _CATALOG_EQUAL_FILTERS.items():
        value = filters.get(key)
        if value is None:
            continue
        clauses.append(f"{column} = '{_safe_literal(str(value))}'")

    for key, column in _CATALOG_ILIKE_FILTERS.items():
        value = filters.get(key)
        if not value:
            continue
        pattern = _escape_like_pattern(str(value))
        clauses.append(f"lower({column}) LIKE lower('%{pattern}%')")

    tags = filters.get("tags")
    if tags:
        for raw_tag in tags:
            if not raw_tag:
                continue
            clauses.append(f"contains(tags, '{_safe_literal(str(raw_tag))}')")

    for key, (column, operator) in _CATALOG_TIME_FILTERS.items():
        value = filters.get(key)
        if value is None:
            continue
        clauses.append(f"{column} {operator} {_timestamp_literal(value)}")

    where_clause = " WHERE " + " AND ".join(clauses)
    order_columns = list(order_by) if order_by else ["provider", "series_id"]
    select_list = ", ".join(_CATALOG_SELECT_COLUMNS)
    order_clause = ", ".join(order_columns)

    return (
        f"SELECT {select_list} "
        "FROM iceberg.market.external_series_catalog "
        f"{where_clause} "
        f"ORDER BY {order_clause} "
        f"LIMIT {limit} OFFSET {offset}"
    )


_SEARCH_ALLOWED_FACETS: set[str] = {
    "doc_type",
    "provider",
    "iso_code",
    "iso_market",
    "iso_product",
    "category",
    "status",
}

_SEARCH_EQUAL_FILTERS: dict[str, str] = {
    "provider": "provider",
    "iso_code": "iso_code",
    "iso_market": "iso_market",
    "iso_product": "iso_product",
    "category": "category",
    "status": "status",
}


def _build_search_where_clauses(
    *,
    tenant_id: str,
    tokens: Sequence[str],
    filters: Mapping[str, Any] | None,
) -> tuple[list[str], list[str]]:
    clauses = [f"tenant_id = '{_safe_literal(tenant_id)}'"]
    filters = filters or {}

    doc_type_filter = filters.get("doc_type")
    doc_type_clauses: list[str] = []
    if doc_type_filter:
        values = doc_type_filter if isinstance(doc_type_filter, (list, tuple, set)) else [doc_type_filter]
        sanitized = [v for v in values if str(v).lower() in {"series", "curve"}]
        if sanitized:
            joined = ", ".join(f"'{_safe_literal(str(v).lower())}'" for v in sanitized)
            doc_type_clauses.append(f"doc_type IN ({joined})")

    for key, column in _SEARCH_EQUAL_FILTERS.items():
        value = filters.get(key)
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            cleaned = [val for val in value if val is not None]
            if not cleaned:
                continue
            joined = ", ".join(f"'{_safe_literal(str(val))}'" for val in cleaned)
            clauses.append(f"{column} IN ({joined})")
        else:
            clauses.append(f"{column} = '{_safe_literal(str(value))}'")

    if doc_type_clauses:
        clauses.extend(doc_type_clauses)

    search_columns = (
        "coalesce(title, '')",
        "coalesce(description, '')",
        "coalesce(name, '')",
        "coalesce(id, '')",
        "coalesce(dataset_code, '')",
        "coalesce(iso_code, '')",
        "coalesce(iso_market, '')",
        "coalesce(iso_product, '')",
        "coalesce(provider, '')",
    )
    token_clauses: list[str] = []
    for token in tokens:
        if not token:
            continue
        pattern = _escape_like_pattern(token)
        or_parts = [f"lower({col}) LIKE lower('%{pattern}%')" for col in search_columns]
        token_clauses.append("(" + " OR ".join(or_parts) + ")")

    if token_clauses:
        clauses.extend(token_clauses)

    return clauses, token_clauses


def build_search_union_queries(
    *,
    tenant_id: str,
    tokens: Sequence[str],
    filters: Mapping[str, Any] | None,
    limit: int,
    offset: int,
    facets: Sequence[str] | None = None,
    facet_bucket_limit: int = 10,
) -> tuple[str, dict[str, str], str]:
    """Build SQL for coarse search results and optional facet aggregations."""

    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    clauses, token_clauses = _build_search_where_clauses(
        tenant_id=tenant_id,
        tokens=tokens,
        filters=filters,
    )
    where_clause = " WHERE " + " AND ".join(clauses)

    scoring_terms = []
    for token in token_clauses:
        scoring_terms.append(f"(CASE WHEN {token} THEN 1 ELSE 0 END)")
    score_expression = " + ".join(scoring_terms) if scoring_terms else "0"

    select_columns = (
        "tenant_id",
        "doc_type",
        "id",
        "title",
        "name",
        "description",
        "tags",
        "provider",
        "dataset_code",
        "iso_code",
        "iso_market",
        "iso_product",
        "category",
        "status",
    )
    select_list = ", ".join(select_columns)
    base_cte = (
        "WITH series_docs AS ("
        " SELECT tenant_id, series_id AS id, 'series' AS doc_type, title, title AS name,"
        " description, tags, provider, dataset_code, iso_code, iso_market, iso_product,"
        " category, status"
        " FROM iceberg.market.external_series_catalog"
        f" WHERE tenant_id = '{_safe_literal(tenant_id)}'"
        ")"
        ", curve_docs AS ("
        " SELECT tenant_id, curve_key AS id, 'curve' AS doc_type, curve_key AS title,"
        " max(tenor_label) AS name, CAST(NULL AS varchar) AS description,"
        " CAST(ARRAY[] AS array(varchar)) AS tags, NULL AS provider, NULL AS dataset_code,"
        " NULL AS iso_code, NULL AS iso_market, NULL AS iso_product, NULL AS category, NULL AS status"
        " FROM iceberg.market.curves_latest"
        f" WHERE tenant_id = '{_safe_literal(tenant_id)}'"
        " GROUP BY tenant_id, curve_key"
        ")"
        ", combined AS ("
        " SELECT * FROM series_docs"
        " UNION ALL"
        " SELECT * FROM curve_docs"
        ")"
        ", filtered AS ("
        " SELECT *, "
        f" {score_expression} AS score"
        " FROM combined"
        f"{where_clause}"
        ")"
    )

    main_query = (
        f"{base_cte} "
        f"SELECT {select_list}, score FROM filtered "
        "ORDER BY score DESC, doc_type, id "
        f"LIMIT {limit} OFFSET {offset}"
    )

    facet_queries: dict[str, str] = {}
    if facets:
        for facet_name in facets:
            if facet_name not in _SEARCH_ALLOWED_FACETS:
                continue
            facet_sql = (
                f"{base_cte} "
                "SELECT {facet} AS value, COUNT(*) AS count "
                "FROM filtered "
                "WHERE {facet} IS NOT NULL "
                "GROUP BY {facet} "
                "ORDER BY count DESC, value "
                f"LIMIT {facet_bucket_limit}"
            ).format(facet=facet_name)
            facet_queries[facet_name] = facet_sql

    count_query = f"{base_cte} SELECT COUNT(*) AS total FROM filtered"

    return main_query, facet_queries, count_query
