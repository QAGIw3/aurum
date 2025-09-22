"""Traffic management for canary deployments.

This module provides traffic splitting and routing capabilities for canary deployments,
supporting multiple strategies including weighted routing, header-based routing,
and gradual migration.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Awaitable
from enum import Enum

logger = logging.getLogger(__name__)


class TrafficStrategy(Enum):
    """Traffic routing strategies."""
    WEIGHTED = "weighted"  # Percentage-based traffic split
    HEADER_BASED = "header_based"  # Route based on request headers
    USER_ID_BASED = "user_id_based"  # Route based on user ID hashing
    SESSION_BASED = "session_based"  # Route based on session affinity
    GEOGRAPHIC = "geographic"  # Route based on geographic location


@dataclass
class TrafficEndpoint:
    """Represents a service endpoint for traffic routing."""

    name: str
    url: str
    version: str
    weight: float = 0.0
    healthy: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Performance metrics
    avg_latency_ms: float = 0.0
    error_rate_percent: float = 0.0
    request_count: int = 0
    last_health_check: Optional[datetime] = None


@dataclass
class RoutingRule:
    """A rule for routing traffic to endpoints."""

    name: str
    strategy: TrafficStrategy
    conditions: Dict[str, Any] = field(default_factory=dict)
    baseline_endpoints: List[TrafficEndpoint] = field(default_factory=list)
    canary_endpoints: List[TrafficEndpoint] = field(default_factory=list)
    enabled: bool = True

    # Traffic distribution
    canary_traffic_percent: float = 0.0

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now())


class TrafficRouter(ABC):
    """Abstract base class for traffic routing strategies."""

    @abstractmethod
    async def route_request(
        self,
        request: Dict[str, Any],
        rule: RoutingRule
    ) -> TrafficEndpoint:
        """Route a request to the appropriate endpoint."""
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the strategy name."""
        pass


class WeightedTrafficRouter(TrafficRouter):
    """Weighted traffic router using percentage-based routing."""

    async def route_request(
        self,
        request: Dict[str, Any],
        rule: RoutingRule
    ) -> TrafficEndpoint:
        """Route based on weighted percentages."""
        # Use a deterministic hash of request ID for consistent routing
        request_id = request.get("request_id", str(hash(str(request))))
        request_hash = int(hashlib.md5(request_id.encode()).hexdigest(), 16)

        # Normalize hash to 0-100 range for percentage routing
        percentage = (request_hash % 10000) / 100.0  # 0.00 to 99.99

        if percentage < rule.canary_traffic_percent:
            # Route to canary
            canary_endpoints = [e for e in rule.canary_endpoints if e.healthy]
            if canary_endpoints:
                return self._select_endpoint_by_weight(canary_endpoints)
        else:
            # Route to baseline
            baseline_endpoints = [e for e in rule.baseline_endpoints if e.healthy]
            if baseline_endpoints:
                return self._select_endpoint_by_weight(baseline_endpoints)

        # Fallback: return first healthy endpoint
        all_endpoints = [e for e in rule.baseline_endpoints + rule.canary_endpoints if e.healthy]
        return all_endpoints[0] if all_endpoints else rule.baseline_endpoints[0]

    def _select_endpoint_by_weight(self, endpoints: List[TrafficEndpoint]) -> TrafficEndpoint:
        """Select endpoint based on weights."""
        if not endpoints:
            raise RuntimeError("No healthy endpoints available")

        if len(endpoints) == 1:
            return endpoints[0]

        # Simple round-robin for now
        # In production, this would use actual weights
        return endpoints[0]

    def get_strategy_name(self) -> str:
        return "weighted"


class HeaderBasedTrafficRouter(TrafficRouter):
    """Header-based traffic routing."""

    async def route_request(
        self,
        request: Dict[str, Any],
        rule: RoutingRule
    ) -> TrafficEndpoint:
        """Route based on request headers."""
        headers = request.get("headers", {})

        # Check if canary header is present
        canary_header = headers.get("X-Canary", "").lower()
        force_canary = canary_header in ["true", "1", "yes", "canary"]

        if force_canary:
            canary_endpoints = [e for e in rule.canary_endpoints if e.healthy]
            if canary_endpoints:
                return self._select_endpoint_by_weight(canary_endpoints)

        # Check for other routing conditions
        for condition_field, expected_value in rule.conditions.items():
            if condition_field.startswith("header:"):
                header_name = condition_field[7:]  # Remove "header:" prefix
                header_value = headers.get(header_name, "")

                if isinstance(expected_value, list):
                    if header_value in expected_value:
                        canary_endpoints = [e for e in rule.canary_endpoints if e.healthy]
                        if canary_endpoints:
                            return self._select_endpoint_by_weight(canary_endpoints)
                else:
                    if header_value == expected_value:
                        canary_endpoints = [e for e in rule.canary_endpoints if e.healthy]
                        if canary_endpoints:
                            return self._select_endpoint_by_weight(canary_endpoints)

        # Default to weighted routing
        router = WeightedTrafficRouter()
        return await router.route_request(request, rule)

    def _select_endpoint_by_weight(self, endpoints: List[TrafficEndpoint]) -> TrafficEndpoint:
        """Select endpoint based on weights."""
        if not endpoints:
            raise RuntimeError("No healthy endpoints available")
        return endpoints[0]  # Simple selection for now

    def get_strategy_name(self) -> str:
        return "header_based"


class TrafficManager:
    """Main traffic management system for canary deployments."""

    def __init__(self):
        self.routing_rules: Dict[str, RoutingRule] = {}
        self.endpoints: Dict[str, TrafficEndpoint] = {}
        self.routers: Dict[str, TrafficRouter] = {
            "weighted": WeightedTrafficRouter(),
            "header_based": HeaderBasedTrafficRouter()
        }

        # Traffic statistics
        self.traffic_stats: Dict[str, Dict[str, int]] = {}

        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._stats_collection_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self):
        """Start the traffic manager."""
        if self._is_running:
            return

        self._is_running = True

        # Start background health checks
        self._health_check_task = asyncio.create_task(self._perform_health_checks())
        self._stats_collection_task = asyncio.create_task(self._collect_traffic_stats())

        logger.info("Traffic manager started")

    async def stop(self):
        """Stop the traffic manager."""
        self._is_running = False

        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self._stats_collection_task:
            self._stats_collection_task.cancel()
            try:
                await self._stats_collection_task
            except asyncio.CancelledError:
                pass

        logger.info("Traffic manager stopped")

    async def add_routing_rule(self, rule: RoutingRule) -> None:
        """Add a routing rule."""
        self.routing_rules[rule.name] = rule

        # Initialize traffic statistics
        self.traffic_stats[rule.name] = {
            "total_requests": 0,
            "baseline_requests": 0,
            "canary_requests": 0,
            "errors": 0
        }

        logger.info(f"Added routing rule: {rule.name}")

    async def remove_routing_rule(self, rule_name: str) -> None:
        """Remove a routing rule."""
        if rule_name in self.routing_rules:
            del self.routing_rules[rule_name]

        if rule_name in self.traffic_stats:
            del self.traffic_stats[rule_name]

        logger.info(f"Removed routing rule: {rule_name}")

    async def add_endpoint(self, endpoint: TrafficEndpoint) -> None:
        """Add a traffic endpoint."""
        self.endpoints[endpoint.name] = endpoint
        logger.info(f"Added endpoint: {endpoint.name} ({endpoint.version})")

    async def update_endpoint_health(self, endpoint_name: str, healthy: bool) -> None:
        """Update the health status of an endpoint."""
        if endpoint_name in self.endpoints:
            self.endpoints[endpoint_name].healthy = healthy
            self.endpoints[endpoint_name].last_health_check = datetime.now()

            status = "healthy" if healthy else "unhealthy"
            logger.info(f"Updated endpoint {endpoint_name} health: {status}")

    async def update_traffic_percentage(self, rule_name: str, canary_percent: float) -> None:
        """Update the canary traffic percentage for a rule."""
        if rule_name in self.routing_rules:
            rule = self.routing_rules[rule_name]
            rule.canary_traffic_percent = canary_percent

            logger.info(f"Updated traffic percentage for {rule_name}: {canary_percent}%")

    async def route_request(
        self,
        rule_name: str,
        request: Dict[str, Any]
    ) -> Optional[TrafficEndpoint]:
        """Route a request using the specified rule."""
        if rule_name not in self.routing_rules:
            logger.warning(f"Routing rule not found: {rule_name}")
            return None

        rule = self.routing_rules[rule_name]

        if not rule.enabled:
            logger.warning(f"Routing rule disabled: {rule_name}")
            return None

        # Get appropriate router
        router = self.routers.get(rule.strategy.value)
        if not router:
            logger.error(f"Router not found for strategy: {rule.strategy.value}")
            return None

        try:
            # Update statistics
            self.traffic_stats[rule_name]["total_requests"] += 1

            # Route the request
            endpoint = await router.route_request(request, rule)

            # Update endpoint statistics
            endpoint.request_count += 1

            # Track which version served the request
            if endpoint in rule.canary_endpoints:
                self.traffic_stats[rule_name]["canary_requests"] += 1
            else:
                self.traffic_stats[rule_name]["baseline_requests"] += 1

            logger.debug(f"Routed request to {endpoint.name} ({endpoint.version})")
            return endpoint

        except Exception as e:
            logger.error(f"Error routing request: {e}")
            self.traffic_stats[rule_name]["errors"] += 1
            return None

    async def get_traffic_stats(self, rule_name: str) -> Optional[Dict[str, Any]]:
        """Get traffic statistics for a rule."""
        if rule_name not in self.traffic_stats:
            return None

        stats = self.traffic_stats[rule_name].copy()

        # Calculate percentages
        total = stats["total_requests"]
        if total > 0:
            stats["baseline_percent"] = (stats["baseline_requests"] / total) * 100
            stats["canary_percent"] = (stats["canary_requests"] / total) * 100
            stats["error_percent"] = (stats["errors"] / total) * 100

        return stats

    async def get_all_traffic_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get traffic statistics for all rules."""
        return {
            rule_name: await self.get_traffic_stats(rule_name)
            for rule_name in self.routing_rules.keys()
        }

    async def get_endpoint_status(self, endpoint_name: str) -> Optional[Dict[str, Any]]:
        """Get status of an endpoint."""
        if endpoint_name not in self.endpoints:
            return None

        endpoint = self.endpoints[endpoint_name]
        return {
            "name": endpoint.name,
            "url": endpoint.url,
            "version": endpoint.version,
            "healthy": endpoint.healthy,
            "weight": endpoint.weight,
            "avg_latency_ms": endpoint.avg_latency_ms,
            "error_rate_percent": endpoint.error_rate_percent,
            "request_count": endpoint.request_count,
            "last_health_check": endpoint.last_health_check.isoformat() if endpoint.last_health_check else None,
            "metadata": endpoint.metadata
        }

    async def _perform_health_checks(self):
        """Background task to perform health checks on endpoints."""
        while self._is_running:
            try:
                await asyncio.sleep(30)  # Health check interval

                for endpoint in self.endpoints.values():
                    # Perform health check (mock implementation)
                    healthy = await self._check_endpoint_health(endpoint)

                    # Update health status
                    if healthy != endpoint.healthy:
                        await self.update_endpoint_health(endpoint.name, healthy)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check task: {e}")

    async def _check_endpoint_health(self, endpoint: TrafficEndpoint) -> bool:
        """Check the health of an endpoint."""
        # Mock health check implementation
        # In production, this would make actual HTTP requests
        return endpoint.healthy  # For now, just return current status

    async def _collect_traffic_stats(self):
        """Background task to collect and report traffic statistics."""
        while self._is_running:
            try:
                await asyncio.sleep(60)  # Stats collection interval

                # Collect and log statistics
                for rule_name, stats in self.traffic_stats.items():
                    logger.info(f"Traffic stats for {rule_name}: {stats}")

                    # Emit metrics (mock)
                    total_requests = stats["total_requests"]
                    if total_requests > 0:
                        canary_percent = (stats["canary_requests"] / total_requests) * 100
                        logger.info(f"Canary traffic percentage: {canary_percent".1f"}%")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stats collection task: {e}")


# Global traffic manager instance
_global_traffic_manager: Optional[TrafficManager] = None


async def get_traffic_manager() -> TrafficManager:
    """Get or create the global traffic manager."""
    global _global_traffic_manager

    if _global_traffic_manager is None:
        _global_traffic_manager = TrafficManager()
        await _global_traffic_manager.start()

    return _global_traffic_manager


# Convenience functions for common traffic management operations
async def setup_caiso_canary_traffic(
    baseline_endpoints: List[TrafficEndpoint],
    canary_endpoints: List[TrafficEndpoint],
    initial_canary_percent: float = 5.0
) -> str:
    """Set up traffic management for CAISO canary deployment."""
    manager = await get_traffic_manager()

    rule = RoutingRule(
        name="caiso-canary-traffic",
        strategy=TrafficStrategy.WEIGHTED,
        baseline_endpoints=baseline_endpoints,
        canary_endpoints=canary_endpoints,
        canary_traffic_percent=initial_canary_percent,
        conditions={
            "header:X-Canary": "true"
        }
    )

    await manager.add_routing_rule(rule)
    return rule.name


async def update_canary_traffic_percentage(
    rule_name: str,
    canary_percent: float
) -> bool:
    """Update the canary traffic percentage."""
    manager = await get_traffic_manager()
    await manager.update_traffic_percentage(rule_name, canary_percent)
    return True


async def get_traffic_distribution(rule_name: str) -> Optional[Dict[str, Any]]:
    """Get current traffic distribution for a rule."""
    manager = await get_traffic_manager()
    return await manager.get_traffic_stats(rule_name)
