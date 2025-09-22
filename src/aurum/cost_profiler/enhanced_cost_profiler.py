"""Enhanced cost profiler with per-query cost hints and budget management."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable, Awaitable
import logging

from ..logging import StructuredLogger, LogLevel, create_logger
from ..observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class CostCategory(str, Enum):
    """Categories of costs in data processing."""
    COMPUTE = "compute"              # CPU, memory usage
    STORAGE = "storage"              # Data storage costs
    NETWORK = "network"              # Network I/O costs
    API_CALLS = "api_calls"          # External API calls
    DATA_TRANSFER = "data_transfer"  # Data transfer costs
    PROCESSING = "processing"        # Data processing costs


class QueryComplexity(str, Enum):
    """Query complexity levels for cost estimation."""
    LOW = "low"          # Simple queries, small datasets
    MEDIUM = "medium"    # Moderate complexity, medium datasets
    HIGH = "high"        # Complex queries, large datasets
    CRITICAL = "critical"  # Very complex or resource-intensive


class BudgetStatus(str, Enum):
    """Budget status indicators."""
    UNDER_BUDGET = "under_budget"
    APPROACHING_LIMIT = "approaching_limit"
    OVER_BUDGET = "over_budget"
    EXCEEDED_LIMIT = "exceeded_limit"


@dataclass
class QueryCostHint:
    """Cost hints for individual queries."""

    query_id: str
    query_type: str  # SELECT, INSERT, UPDATE, etc.
    dataset_name: str
    estimated_complexity: QueryComplexity
    estimated_cost_usd: float = 0.0
    actual_cost_usd: float = 0.0
    budget_allocated: float = 0.0
    budget_remaining: float = 0.0

    # Performance metrics
    estimated_rows: int = 0
    actual_rows: int = 0
    estimated_runtime_seconds: float = 0.0
    actual_runtime_seconds: float = 0.0

    # Cost breakdown
    compute_cost: float = 0.0
    storage_cost: float = 0.0
    network_cost: float = 0.0

    # Metadata
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    status: str = "pending"  # pending, running, completed, failed

    # Optimization recommendations
    recommendations: List[str] = field(default_factory=list)

    def get_cost_efficiency(self) -> float:
        """Calculate cost efficiency (rows per dollar)."""
        if self.actual_cost_usd == 0:
            return 0.0
        return self.actual_rows / self.actual_cost_usd

    def get_performance_ratio(self) -> float:
        """Calculate performance ratio (actual vs estimated)."""
        if self.estimated_runtime_seconds == 0:
            return 1.0
        return self.actual_runtime_seconds / self.estimated_runtime_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "query_id": self.query_id,
            "query_type": self.query_type,
            "dataset_name": self.dataset_name,
            "estimated_complexity": self.estimated_complexity.value,
            "estimated_cost_usd": self.estimated_cost_usd,
            "actual_cost_usd": self.actual_cost_usd,
            "budget_allocated": self.budget_allocated,
            "budget_remaining": self.budget_remaining,
            "estimated_rows": self.estimated_rows,
            "actual_rows": self.actual_rows,
            "estimated_runtime_seconds": self.estimated_runtime_seconds,
            "actual_runtime_seconds": self.actual_runtime_seconds,
            "compute_cost": self.compute_cost,
            "storage_cost": self.storage_cost,
            "network_cost": self.network_cost,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "recommendations": self.recommendations,
            "cost_efficiency": self.get_cost_efficiency(),
            "performance_ratio": self.get_performance_ratio()
        }


@dataclass
class BudgetConfig:
    """Budget configuration for cost management."""

    name: str
    budget_type: str  # "daily", "weekly", "monthly", "project"
    total_budget_usd: float
    allocated_budget_usd: float = 0.0
    remaining_budget_usd: float = 0.0

    # Budget categories
    compute_budget: float = 0.0
    storage_budget: float = 0.0
    network_budget: float = 0.0
    api_budget: float = 0.0

    # Alert thresholds
    warning_threshold_percent: float = 80.0  # Alert at 80% usage
    critical_threshold_percent: float = 95.0  # Critical at 95% usage
    hard_limit_percent: float = 100.0  # Hard limit at 100% usage

    # Budget policies
    allow_overages: bool = False
    auto_approve_under_limit: bool = True
    require_approval_over_limit: bool = True

    # Metadata
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())

    def get_status(self, current_usage: float) -> BudgetStatus:
        """Get budget status based on current usage."""
        usage_percent = (current_usage / self.total_budget_usd) * 100

        if usage_percent >= self.hard_limit_percent:
            return BudgetStatus.EXCEEDED_LIMIT
        elif usage_percent >= self.critical_threshold_percent:
            return BudgetStatus.OVER_BUDGET
        elif usage_percent >= self.warning_threshold_percent:
            return BudgetStatus.APPROACHING_LIMIT
        else:
            return BudgetStatus.UNDER_BUDGET

    def can_allocate(self, amount: float, current_usage: float) -> bool:
        """Check if budget can allocate the requested amount."""
        new_usage = current_usage + amount
        usage_percent = (new_usage / self.total_budget_usd) * 100

        if not self.allow_overages and usage_percent > self.hard_limit_percent:
            return False

        return True


@dataclass
class CostOptimizationRecommendation:
    """Recommendation for cost optimization."""

    query_id: str
    recommendation_type: str  # "query_rewrite", "indexing", "caching", "partitioning"
    description: str
    estimated_savings_usd: float = 0.0
    implementation_effort: str = "medium"  # low, medium, high
    risk_level: str = "low"  # low, medium, high

    # Before/after metrics
    current_cost: float = 0.0
    projected_cost: float = 0.0
    confidence_score: float = 0.0  # 0.0 to 1.0

    # Implementation details
    sql_changes: Optional[str] = None
    config_changes: Dict[str, Any] = field(default_factory=dict)
    estimated_implementation_time_minutes: int = 30

    def get_savings_percentage(self) -> float:
        """Calculate percentage savings."""
        if self.current_cost == 0:
            return 0.0
        return (self.estimated_savings_usd / self.current_cost) * 100


class EnhancedCostProfiler:
    """Enhanced cost profiler with per-query cost tracking and budget management."""

    def __init__(self):
        self.logger = create_logger(
            source_name="enhanced_cost_profiler",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.cost.profiler",
            dataset="cost_profiling"
        )

        self.metrics = get_metrics_client()

        # Cost tracking
        self.query_costs: Dict[str, QueryCostHint] = {}
        self.budgets: Dict[str, BudgetConfig] = {}
        self.recommendations: List[CostOptimizationRecommendation] = []

        # Cost estimation models
        self.cost_models: Dict[str, Callable] = {}

        # Configuration
        self.enable_real_time_tracking: bool = True
        self.enable_budget_enforcement: bool = True
        self.enable_auto_recommendations: bool = True

        logger.info("Enhanced Cost Profiler initialized")

    async def track_query_cost(
        self,
        query_id: str,
        query_type: str,
        dataset_name: str,
        estimated_complexity: QueryComplexity = QueryComplexity.MEDIUM,
        estimated_cost_usd: float = 0.0,
        budget_allocated: float = 0.0,
        **kwargs
    ) -> QueryCostHint:
        """Track cost for a query with budget allocation."""
        cost_hint = QueryCostHint(
            query_id=query_id,
            query_type=query_type,
            dataset_name=dataset_name,
            estimated_complexity=estimated_complexity,
            estimated_cost_usd=estimated_cost_usd,
            budget_allocated=budget_allocated,
            **kwargs
        )

        self.query_costs[query_id] = cost_hint

        # Update budget
        if budget_allocated > 0:
            await self._allocate_budget(cost_hint)

        # Emit metrics
        self.metrics.increment_counter("cost_profiler.queries_tracked")
        self.metrics.gauge("cost_profiler.active_queries", len(self.query_costs))

        logger.info(
            f"Tracking query {query_id} with estimated cost ${estimated_cost_usd:.".2f"",
            "query_cost_tracking_started",
            query_id=query_id,
            dataset_name=dataset_name,
            estimated_cost_usd=estimated_cost_usd,
            budget_allocated=budget_allocated
        )

        return cost_hint

    async def update_query_actuals(
        self,
        query_id: str,
        actual_cost_usd: float,
        actual_rows: int,
        actual_runtime_seconds: float,
        **kwargs
    ) -> Optional[QueryCostHint]:
        """Update actual costs and performance for a completed query."""
        if query_id not in self.query_costs:
            logger.warning(f"Query {query_id} not found in cost tracking")
            return None

        cost_hint = self.query_costs[query_id]

        # Update actual values
        cost_hint.actual_cost_usd = actual_cost_usd
        cost_hint.actual_rows = actual_rows
        cost_hint.actual_runtime_seconds = actual_runtime_seconds
        cost_hint.completed_at = datetime.now().isoformat()
        cost_hint.status = "completed"

        # Update budget remaining
        if cost_hint.budget_allocated > 0:
            cost_hint.budget_remaining = max(0, cost_hint.budget_allocated - actual_cost_usd)
            await self._update_budget_usage(cost_hint.dataset_name, actual_cost_usd)

        # Generate recommendations if needed
        if self.enable_auto_recommendations:
            await self._generate_recommendations(cost_hint)

        # Emit metrics
        self.metrics.increment_counter("cost_profiler.queries_completed")
        self.metrics.histogram("cost_profiler.query_cost_usd", actual_cost_usd)
        self.metrics.histogram("cost_profiler.query_runtime_seconds", actual_runtime_seconds)

        logger.info(
            f"Query {query_id} completed - Cost: ${actual_cost_usd:.".2f" Runtime: {actual_runtime_seconds:.2f}s",
            "query_cost_tracking_completed",
            query_id=query_id,
            actual_cost_usd=actual_cost_usd,
            actual_runtime_seconds=actual_runtime_seconds,
            efficiency=cost_hint.get_cost_efficiency()
        )

        return cost_hint

    async def estimate_query_cost(
        self,
        query_type: str,
        dataset_name: str,
        estimated_rows: int,
        complexity: QueryComplexity = QueryComplexity.MEDIUM,
        **context
    ) -> float:
        """Estimate cost for a query based on type, size, and complexity."""
        base_costs = {
            QueryComplexity.LOW: 0.01,      # $0.01 base cost
            QueryComplexity.MEDIUM: 0.05,   # $0.05 base cost
            QueryComplexity.HIGH: 0.25,     # $0.25 base cost
            QueryComplexity.CRITICAL: 1.00  # $1.00 base cost
        }

        # Adjust for query type
        type_multipliers = {
            "SELECT": 1.0,
            "INSERT": 1.2,
            "UPDATE": 1.5,
            "DELETE": 1.3,
            "CREATE": 0.8,
            "ALTER": 0.6
        }

        # Adjust for dataset size
        size_multiplier = min(1.0 + (estimated_rows / 1000000), 10.0)  # Cap at 10x

        # Apply context adjustments
        context_multiplier = 1.0
        if context.get('joins', 0) > 3:
            context_multiplier *= 1.5
        if context.get('aggregations', 0) > 5:
            context_multiplier *= 1.3
        if context.get('window_functions', 0) > 0:
            context_multiplier *= 1.4

        base_cost = base_costs[complexity]
        estimated_cost = base_cost * type_multipliers.get(query_type, 1.0) * size_multiplier * context_multiplier

        # Store for tracking
        query_id = context.get('query_id', f"estimate_{datetime.now().timestamp()}")
        cost_hint = QueryCostHint(
            query_id=query_id,
            query_type=query_type,
            dataset_name=dataset_name,
            estimated_complexity=complexity,
            estimated_cost_usd=estimated_cost,
            estimated_rows=estimated_rows
        )

        self.query_costs[query_id] = cost_hint

        return estimated_cost

    async def check_budget_availability(
        self,
        dataset_name: str,
        requested_amount: float
    ) -> Dict[str, Any]:
        """Check if budget is available for the requested amount."""
        result = {
            "available": True,
            "budget_status": BudgetStatus.UNDER_BUDGET.value,
            "remaining_budget": 0.0,
            "recommendation": "proceed",
            "message": "Budget available"
        }

        # Find relevant budget
        budget = await self._get_budget_for_dataset(dataset_name)
        if not budget:
            result["available"] = False
            result["message"] = f"No budget configured for {dataset_name}"
            return result

        # Check current usage
        current_usage = await self._get_current_budget_usage(dataset_name)
        can_allocate = budget.can_allocate(requested_amount, current_usage)

        if not can_allocate:
            result["available"] = False
            result["budget_status"] = budget.get_status(current_usage + requested_amount).value
            result["remaining_budget"] = max(0, budget.total_budget_usd - current_usage)
            result["recommendation"] = "deny"
            result["message"] = f"Budget allocation denied: would exceed limit"
        else:
            new_usage = current_usage + requested_amount
            result["budget_status"] = budget.get_status(new_usage).value
            result["remaining_budget"] = budget.total_budget_usd - new_usage

            if result["budget_status"] == BudgetStatus.APPROACHING_LIMIT.value:
                result["recommendation"] = "proceed_with_warning"
                result["message"] = "Budget approaching limit - monitor usage"
            elif result["budget_status"] == BudgetStatus.OVER_BUDGET.value:
                result["recommendation"] = "proceed_with_approval"
                result["message"] = "Budget over limit - requires approval"

        return result

    async def _allocate_budget(self, cost_hint: QueryCostHint) -> bool:
        """Allocate budget for a query cost hint."""
        try:
            # Find or create budget for dataset
            budget = await self._get_or_create_budget_for_dataset(cost_hint.dataset_name)

            # Check if allocation is possible
            budget_check = await self.check_budget_availability(
                cost_hint.dataset_name,
                cost_hint.budget_allocated
            )

            if not budget_check["available"]:
                logger.warning(
                    f"Budget allocation failed for {cost_hint.query_id}: {budget_check['message']}"
                )
                return False

            # Allocate the budget
            budget.allocated_budget_usd += cost_hint.budget_allocated
            budget.remaining_budget_usd -= cost_hint.budget_allocated
            budget.last_updated = datetime.now().isoformat()

            logger.info(
                f"Allocated ${cost_hint.budget_allocated:.".2f"budget for query {cost_hint.query_id}"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to allocate budget for {cost_hint.query_id}: {e}")
            return False

    async def _update_budget_usage(self, dataset_name: str, actual_cost: float) -> None:
        """Update budget usage with actual costs."""
        try:
            budget = await self._get_budget_for_dataset(dataset_name)
            if budget:
                budget.allocated_budget_usd += actual_cost
                budget.remaining_budget_usd = max(0, budget.total_budget_usd - budget.allocated_budget_usd)
                budget.last_updated = datetime.now().isoformat()

                # Check budget status and alert if needed
                await self._check_budget_alerts(budget)

        except Exception as e:
            logger.error(f"Failed to update budget usage for {dataset_name}: {e}")

    async def _check_budget_alerts(self, budget: BudgetConfig) -> None:
        """Check budget status and generate alerts if needed."""
        current_usage = budget.allocated_budget_usd
        usage_percent = (current_usage / budget.total_budget_usd) * 100

        if usage_percent >= budget.hard_limit_percent:
            await self._send_budget_alert(
                budget.name,
                "Budget exceeded hard limit",
                "critical",
                {"usage_percent": usage_percent, "current_usage": current_usage}
            )
        elif usage_percent >= budget.critical_threshold_percent:
            await self._send_budget_alert(
                budget.name,
                "Budget exceeded critical threshold",
                "warning",
                {"usage_percent": usage_percent, "current_usage": current_usage}
            )
        elif usage_percent >= budget.warning_threshold_percent:
            await self._send_budget_alert(
                budget.name,
                "Budget approaching limit",
                "info",
                {"usage_percent": usage_percent, "current_usage": current_usage}
            )

    async def _send_budget_alert(
        self,
        budget_name: str,
        message: str,
        severity: str,
        context: Dict[str, Any]
    ) -> None:
        """Send budget alert."""
        alert_data = {
            "budget_name": budget_name,
            "message": message,
            "severity": severity,
            "timestamp": datetime.now().isoformat(),
            **context
        }

        self.logger.log(
            LogLevel.WARNING if severity in ["warning", "critical"] else LogLevel.INFO,
            f"Budget alert: {message}",
            "budget_alert",
            **alert_data
        )

        # Emit metrics
        self.metrics.increment_counter(f"cost_profiler.budget_alerts.{severity}")

    async def _get_budget_for_dataset(self, dataset_name: str) -> Optional[BudgetConfig]:
        """Get budget configuration for a dataset."""
        # Simple mapping for now - could be enhanced with more sophisticated logic
        return self.budgets.get(dataset_name)

    async def _get_or_create_budget_for_dataset(self, dataset_name: str) -> BudgetConfig:
        """Get or create budget for a dataset."""
        if dataset_name in self.budgets:
            return self.budgets[dataset_name]

        # Create default budget for dataset
        budget = BudgetConfig(
            name=f"dataset_{dataset_name}",
            budget_type="project",
            total_budget_usd=1000.0,  # Default $1000 budget
            allocated_budget_usd=0.0,
            remaining_budget_usd=1000.0
        )

        self.budgets[dataset_name] = budget
        return budget

    async def _get_current_budget_usage(self, dataset_name: str) -> float:
        """Get current budget usage for a dataset."""
        budget = await self._get_budget_for_dataset(dataset_name)
        return budget.allocated_budget_usd if budget else 0.0

    async def _generate_recommendations(self, cost_hint: QueryCostHint) -> None:
        """Generate cost optimization recommendations."""
        recommendations = []

        # Performance-based recommendations
        performance_ratio = cost_hint.get_performance_ratio()
        cost_efficiency = cost_hint.get_cost_efficiency()

        if performance_ratio > 1.5:  # Query took 50% longer than estimated
            recommendations.append(CostOptimizationRecommendation(
                query_id=cost_hint.query_id,
                recommendation_type="query_rewrite",
                description="Query performance is significantly worse than estimated",
                estimated_savings_usd=cost_hint.actual_cost_usd * 0.3,
                implementation_effort="medium",
                risk_level="low",
                current_cost=cost_hint.actual_cost_usd,
                projected_cost=cost_hint.actual_cost_usd * 0.7,
                confidence_score=0.8
            ))

        if cost_efficiency < 1000:  # Less than 1000 rows per dollar
            recommendations.append(CostOptimizationRecommendation(
                query_id=cost_hint.query_id,
                recommendation_type="caching",
                description="Consider caching frequently accessed data",
                estimated_savings_usd=cost_hint.actual_cost_usd * 0.5,
                implementation_effort="high",
                risk_level="medium",
                current_cost=cost_hint.actual_cost_usd,
                projected_cost=cost_hint.actual_cost_usd * 0.5,
                confidence_score=0.6
            ))

        # Cost-based recommendations
        if cost_hint.actual_cost_usd > cost_hint.estimated_cost_usd * 1.5:
            recommendations.append(CostOptimizationRecommendation(
                query_id=cost_hint.query_id,
                recommendation_type="budget_adjustment",
                description="Query cost significantly exceeded estimate - review budget allocation",
                estimated_savings_usd=0.0,
                implementation_effort="low",
                risk_level="low",
                current_cost=cost_hint.actual_cost_usd,
                projected_cost=cost_hint.estimated_cost_usd,
                confidence_score=0.9
            ))

        self.recommendations.extend(recommendations)

        if recommendations:
            logger.info(f"Generated {len(recommendations)} recommendations for query {cost_hint.query_id}")

    async def generate_cost_report(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Generate comprehensive cost report."""
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()

        # Filter queries by time range
        report_queries = [
            cost_hint for cost_hint in self.query_costs.values()
            if (cost_hint.created_at and
                start_time <= datetime.fromisoformat(cost_hint.created_at) <= end_time)
        ]

        # Calculate totals
        total_estimated = sum(q.estimated_cost_usd for q in report_queries)
        total_actual = sum(q.actual_cost_usd for q in report_queries if q.status == "completed")
        total_budget_allocated = sum(q.budget_allocated for q in report_queries)

        # Budget analysis
        budgets_in_period = {
            name: budget for name, budget in self.budgets.items()
            if budget.created_at and datetime.fromisoformat(budget.created_at) <= end_time
        }

        report = {
            "report_period": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            },
            "query_summary": {
                "total_queries": len(report_queries),
                "completed_queries": len([q for q in report_queries if q.status == "completed"]),
                "failed_queries": len([q for q in report_queries if q.status == "failed"]),
                "total_estimated_cost": total_estimated,
                "total_actual_cost": total_actual,
                "total_budget_allocated": total_budget_allocated,
                "cost_variance_percent": ((total_actual - total_estimated) / total_estimated * 100) if total_estimated > 0 else 0
            },
            "budget_summary": {
                "total_budgets": len(budgets_in_period),
                "total_budget_allocated": sum(b.allocated_budget_usd for b in budgets_in_period.values()),
                "total_budget_remaining": sum(b.remaining_budget_usd for b in budgets_in_period.values()),
                "budgets_over_limit": len([b for b in budgets_in_period.values() if b.get_status(b.allocated_budget_usd).value == "exceeded_limit"])
            },
            "cost_breakdown": {
                "compute": sum(q.compute_cost for q in report_queries),
                "storage": sum(q.storage_cost for q in report_queries),
                "network": sum(q.network_cost for q in report_queries)
            },
            "recommendations": [
                {
                    "query_id": rec.query_id,
                    "type": rec.recommendation_type,
                    "description": rec.description,
                    "estimated_savings": rec.estimated_savings_usd,
                    "confidence": rec.confidence_score
                }
                for rec in self.recommendations
                if rec.query_id in [q.query_id for q in report_queries]
            ],
            "query_details": [q.to_dict() for q in report_queries[:100]]  # Limit to 100 queries
        }

        return report

    async def export_cost_data(self, format: str = "json", filename: Optional[str] = None) -> str:
        """Export cost data in specified format."""
        if format.lower() == "json":
            data = {
                "queries": [q.to_dict() for q in self.query_costs.values()],
                "budgets": {name: {
                    "total_budget_usd": budget.total_budget_usd,
                    "allocated_budget_usd": budget.allocated_budget_usd,
                    "remaining_budget_usd": budget.remaining_budget_usd,
                    "status": budget.get_status(budget.allocated_budget_usd).value
                } for name, budget in self.budgets.items()},
                "recommendations": [
                    {
                        "query_id": rec.query_id,
                        "type": rec.recommendation_type,
                        "description": rec.description,
                        "estimated_savings_usd": rec.estimated_savings_usd,
                        "confidence_score": rec.confidence_score
                    }
                    for rec in self.recommendations
                ],
                "export_timestamp": datetime.now().isoformat()
            }

            json_data = json.dumps(data, indent=2, default=str)

            if filename:
                with open(filename, 'w') as f:
                    f.write(json_data)

            return json_data

        else:
            raise ValueError(f"Unsupported export format: {format}")


# Global cost profiler instance
_global_cost_profiler: Optional[EnhancedCostProfiler] = None


async def get_enhanced_cost_profiler() -> EnhancedCostProfiler:
    """Get or create the global enhanced cost profiler."""
    global _global_cost_profiler

    if _global_cost_profiler is None:
        _global_cost_profiler = EnhancedCostProfiler()

    return _global_cost_profiler


# Convenience functions for common operations
async def track_query(
    query_id: str,
    query_type: str,
    dataset_name: str,
    estimated_rows: int = 0,
    complexity: QueryComplexity = QueryComplexity.MEDIUM,
    **kwargs
) -> QueryCostHint:
    """Track a query with cost estimation."""
    profiler = await get_enhanced_cost_profiler()
    estimated_cost = await profiler.estimate_query_cost(
        query_type, dataset_name, estimated_rows, complexity, query_id=query_id, **kwargs
    )
    return await profiler.track_query_cost(
        query_id, query_type, dataset_name, complexity, estimated_cost, **kwargs
    )


async def update_query_costs(
    query_id: str,
    actual_cost: float,
    actual_rows: int,
    runtime_seconds: float,
    **kwargs
) -> None:
    """Update actual costs for a completed query."""
    profiler = await get_enhanced_cost_profiler()
    await profiler.update_query_actuals(query_id, actual_cost, actual_rows, runtime_seconds, **kwargs)


async def check_budget(dataset_name: str, amount: float) -> Dict[str, Any]:
    """Check budget availability for a dataset."""
    profiler = await get_enhanced_cost_profiler()
    return await profiler.check_budget_availability(dataset_name, amount)


async def generate_cost_report(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """Generate cost report."""
    profiler = await get_enhanced_cost_profiler()
    return await profiler.generate_cost_report(start_time, end_time)
