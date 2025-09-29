"""Feature flag analytics and monitoring."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict, deque
import json

from ..telemetry.context import get_request_id, get_tenant_id


class FeatureAnalyticsCollector:
    """Collects and aggregates feature flag analytics data."""

    def __init__(self):
        # In-memory storage for analytics (in production, use Redis or time-series DB)
        self._flag_evaluations: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._ab_test_exposures: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._dependency_blocks: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    async def record_flag_evaluation(
        self,
        flag_key: str,
        enabled: bool,
        user_context: Dict[str, Any],
        decision_time: float,
        request_id: str = None,
        tenant_id: str = None,
    ) -> None:
        """Record a feature flag evaluation event."""
        if request_id is None:
            request_id = get_request_id()
        if tenant_id is None:
            tenant_id = get_tenant_id()

        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "flag_key": flag_key,
            "enabled": enabled,
            "decision_time_ms": round(decision_time * 1000, 2),
            "user_id": user_context.get("user_id"),
            "user_segment": user_context.get("user_segment"),
            "request_id": request_id,
            "tenant_id": tenant_id,
        }

        async with self._lock:
            self._flag_evaluations[flag_key].append(event)

    async def record_ab_test_exposure(
        self,
        flag_key: str,
        variant: str,
        user_context: Dict[str, Any],
        request_id: str = None,
        tenant_id: str = None,
    ) -> None:
        """Record an A/B test exposure event."""
        if request_id is None:
            request_id = get_request_id()
        if tenant_id is None:
            tenant_id = get_tenant_id()

        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "flag_key": flag_key,
            "variant": variant,
            "user_id": user_context.get("user_id"),
            "user_segment": user_context.get("user_segment"),
            "request_id": request_id,
            "tenant_id": tenant_id,
        }

        async with self._lock:
            self._ab_test_exposures[flag_key].append(event)

    async def record_dependency_block(
        self,
        flag_key: str,
        blocked_by: List[str],
        user_context: Dict[str, Any],
        request_id: str = None,
        tenant_id: str = None,
    ) -> None:
        """Record when a flag evaluation was blocked by dependencies."""
        if request_id is None:
            request_id = get_request_id()
        if tenant_id is None:
            tenant_id = get_tenant_id()

        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "flag_key": flag_key,
            "blocked_by": blocked_by,
            "user_id": user_context.get("user_id"),
            "user_segment": user_context.get("user_segment"),
            "request_id": request_id,
            "tenant_id": tenant_id,
        }

        async with self._lock:
            self._dependency_blocks[flag_key].append(event)

    async def get_flag_analytics(
        self,
        flag_key: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get analytics for a specific feature flag."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        async with self._lock:
            evaluations = [
                e for e in self._flag_evaluations[flag_key]
                if datetime.fromisoformat(e["timestamp"].replace('Z', '+00:00')) >= cutoff_time
            ]

            ab_exposures = [
                e for e in self._ab_test_exposures[flag_key]
                if datetime.fromisoformat(e["timestamp"].replace('Z', '+00:00')) >= cutoff_time
            ]

            dependency_blocks = [
                e for e in self._dependency_blocks[flag_key]
                if datetime.fromisoformat(e["timestamp"].replace('Z', '+00:00')) >= cutoff_time
            ]

        if not evaluations:
            return {
                "flag_key": flag_key,
                "total_evaluations": 0,
                "enabled_evaluations": 0,
                "disabled_evaluations": 0,
                "enable_rate": 0.0,
                "avg_decision_time_ms": 0.0,
                "ab_test_exposures": 0,
                "dependency_blocks": 0,
            }

        enabled_count = sum(1 for e in evaluations if e["enabled"])
        total_count = len(evaluations)
        decision_times = [e["decision_time_ms"] for e in evaluations if e["decision_time_ms"] > 0]

        return {
            "flag_key": flag_key,
            "total_evaluations": total_count,
            "enabled_evaluations": enabled_count,
            "disabled_evaluations": total_count - enabled_count,
            "enable_rate": enabled_count / total_count if total_count > 0 else 0.0,
            "avg_decision_time_ms": sum(decision_times) / len(decision_times) if decision_times else 0.0,
            "ab_test_exposures": len(ab_exposures),
            "dependency_blocks": len(dependency_blocks),
            "time_range_hours": hours,
        }

    async def get_all_flags_analytics(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get analytics for all feature flags."""
        # Get all flag keys that have analytics data
        async with self._lock:
            all_flag_keys = set(self._flag_evaluations.keys()) | set(self._ab_test_exposures.keys())

        if not all_flag_keys:
            return {
                "total_flags": 0,
                "flags": [],
                "summary": {
                    "total_evaluations": 0,
                    "total_ab_exposures": 0,
                    "total_dependency_blocks": 0,
                }
            }

        # Get analytics for each flag
        flag_analytics = []
        total_evaluations = 0
        total_ab_exposures = 0
        total_dependency_blocks = 0

        for flag_key in all_flag_keys:
            analytics = await self.get_flag_analytics(flag_key, hours)
            flag_analytics.append(analytics)
            total_evaluations += analytics["total_evaluations"]
            total_ab_exposures += analytics["ab_test_exposures"]
            total_dependency_blocks += analytics["dependency_blocks"]

        return {
            "total_flags": len(all_flag_keys),
            "flags": flag_analytics,
            "summary": {
                "total_evaluations": total_evaluations,
                "total_ab_exposures": total_ab_exposures,
                "total_dependency_blocks": total_dependency_blocks,
                "avg_evaluations_per_flag": total_evaluations / len(all_flag_keys) if all_flag_keys else 0,
            },
            "time_range_hours": hours,
        }

    async def get_ab_test_analytics(
        self,
        flag_key: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get A/B test analytics for a specific flag."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        async with self._lock:
            exposures = [
                e for e in self._ab_test_exposures[flag_key]
                if datetime.fromisoformat(e["timestamp"].replace('Z', '+00:00')) >= cutoff_time
            ]

        if not exposures:
            return {
                "flag_key": flag_key,
                "total_exposures": 0,
                "variant_distribution": {},
                "time_range_hours": hours,
            }

        # Count variants
        variant_counts = defaultdict(int)
        for exposure in exposures:
            variant_counts[exposure["variant"]] += 1

        total_exposures = len(exposures)

        return {
            "flag_key": flag_key,
            "total_exposures": total_exposures,
            "variant_distribution": dict(variant_counts),
            "variant_percentages": {
                variant: count / total_exposures * 100 if total_exposures > 0 else 0
                for variant, count in variant_counts.items()
            },
            "time_range_hours": hours,
        }


# Global analytics collector instance
_analytics_collector: Optional[FeatureAnalyticsCollector] = None


def get_analytics_collector() -> FeatureAnalyticsCollector:
    """Get the global analytics collector."""
    global _analytics_collector
    if _analytics_collector is None:
        _analytics_collector = FeatureAnalyticsCollector()
    return _analytics_collector


async def initialize_analytics_collector() -> FeatureAnalyticsCollector:
    """Initialize the analytics collector (for testing)."""
    global _analytics_collector
    if _analytics_collector is None:
        _analytics_collector = FeatureAnalyticsCollector()
    return _analytics_collector
