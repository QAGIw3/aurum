"""Gradual rollout manager for production deployment safety.

This module provides tools for safely rolling out new features and architecture changes
using feature flags, canary deployments, and progressive rollout strategies.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Protocol

from aurum.api.features.migration_flags import is_migration_feature_enabled
from aurum.database import get_connection_manager_registry
from aurum.observability import get_application_metrics

logger = logging.getLogger(__name__)


class RolloutStrategy(Enum):
    """Rollout strategies for feature deployment."""

    IMMEDIATE = "immediate"  # 100% rollout immediately
    PERCENTAGE = "percentage"  # Gradual percentage-based rollout
    CANARY = "canary"  # Deploy to small subset first
    GRADUAL = "gradual"  # Linear increase over time
    TRAFFIC_SPLIT = "traffic_split"  # Split traffic between old and new


@dataclass
class RolloutConfig:
    """Configuration for feature rollout."""

    feature_key: str
    strategy: RolloutStrategy
    initial_percentage: float = 0.0
    target_percentage: float = 100.0
    increment_percentage: float = 10.0
    increment_interval_minutes: int = 60
    canary_size: int = 100  # Number of users for canary
    traffic_split_ratio: float = 0.5  # Old:new traffic ratio
    max_duration_hours: int = 24
    rollback_on_error_rate: float = 0.05  # 5% error rate triggers rollback


@dataclass
class RolloutStatus:
    """Current status of a feature rollout."""

    feature_key: str
    current_percentage: float
    strategy: RolloutStrategy
    started_at: datetime
    last_updated: datetime
    total_users_exposed: int = 0
    successful_interactions: int = 0
    error_rate: float = 0.0
    status: str = "active"  # active, paused, completed, rolled_back


class RolloutDecision(Protocol):
    """Protocol for custom rollout decision logic."""

    async def should_enable_feature(
        self,
        feature_key: str,
        user_context: Dict[str, Any],
        rollout_status: RolloutStatus
    ) -> bool:
        """Determine if feature should be enabled for a user."""
        ...


class TrafficBasedRollout:
    """Traffic-based rollout decision logic."""

    async def should_enable_feature(
        self,
        feature_key: str,
        user_context: Dict[str, Any],
        rollout_status: RolloutStatus
    ) -> bool:
        """Simple traffic-based rollout decision."""
        import hashlib

        # Use user ID for consistent hashing
        user_id = user_context.get("user_id", "anonymous")
        hash_input = f"{feature_key}:{user_id}".encode()
        hash_value = int(hashlib.md5(hash_input).hexdigest(), 16)
        percentage = (hash_value % 10000) / 100.0  # 0-100 with 2 decimal places

        return percentage <= rollout_status.current_percentage


class UserSegmentRollout:
    """User segment-based rollout decision logic."""

    def __init__(self, segment_priorities: Dict[str, List[str]]):
        """Initialize with segment priorities for rollout order."""
        self.segment_priorities = segment_priorities

    async def should_enable_feature(
        self,
        feature_key: str,
        user_context: Dict[str, Any],
        rollout_status: RolloutStatus
    ) -> bool:
        """Check if user segment is eligible for feature."""
        user_segment = user_context.get("user_segment", "default")

        # Find the priority level for this user's segment
        segment_priority = None
        for priority_level, segments in self.segment_priorities.items():
            if user_segment in segments:
                segment_priority = priority_level
                break

        if segment_priority is None:
            return False

        # Check if this priority level is enabled
        enabled_priorities = self.segment_priorities.keys()
        priority_threshold = max(enabled_priorities) if enabled_priorities else 0

        return segment_priority <= priority_threshold


class RolloutManager:
    """Manages gradual rollout of features with monitoring and safety controls."""

    def __init__(self):
        self.rollout_configs: Dict[str, RolloutConfig] = {}
        self.rollout_statuses: Dict[str, RolloutStatus] = {}
        self.rollout_tasks: Dict[str, asyncio.Task] = {}
        self.decision_engines: Dict[str, RolloutDecision] = {}
        self.metrics = get_application_metrics()

        # Default decision engine
        self.decision_engines["default"] = TrafficBasedRollout()

        # User segment priorities for targeted rollouts
        self.segment_priorities = {
            1: ["enterprise_users", "premium_users"],  # Roll out to high-value users first
            2: ["standard_users"],
            3: ["trial_users"],
            4: ["anonymous_users"],
        }

        self.segment_engine = UserSegmentRollout(self.segment_priorities)

    async def start_rollout(self, config: RolloutConfig) -> None:
        """Start a feature rollout."""
        logger.info(f"Starting rollout for feature: {config.feature_key}")

        # Initialize status
        status = RolloutStatus(
            feature_key=config.feature_key,
            current_percentage=config.initial_percentage,
            strategy=config.strategy,
            started_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
        )

        self.rollout_configs[config.feature_key] = config
        self.rollout_statuses[config.feature_key] = status

        # Start background rollout task if needed
        if config.strategy in [RolloutStrategy.GRADUAL, RolloutStrategy.PERCENTAGE]:
            task = asyncio.create_task(self._run_gradual_rollout(config))
            self.rollout_tasks[config.feature_key] = task

        logger.info(f"Rollout started: {config.feature_key} at {config.initial_percentage}%")

    async def stop_rollout(self, feature_key: str) -> None:
        """Stop a feature rollout."""
        if feature_key in self.rollout_tasks:
            self.rollout_tasks[feature_key].cancel()
            del self.rollout_tasks[feature_key]

        logger.info(f"Rollout stopped for feature: {feature_key}")

    async def pause_rollout(self, feature_key: str) -> None:
        """Pause a feature rollout."""
        if feature_key in self.rollout_statuses:
            status = self.rollout_statuses[feature_key]
            status.status = "paused"
            status.last_updated = datetime.utcnow()

        logger.info(f"Rollout paused for feature: {feature_key}")

    async def resume_rollout(self, feature_key: str) -> None:
        """Resume a paused feature rollout."""
        if feature_key in self.rollout_statuses:
            status = self.rollout_statuses[feature_key]
            status.status = "active"
            status.last_updated = datetime.utcnow()

        logger.info(f"Rollout resumed for feature: {feature_key}")

    async def rollback_rollout(self, feature_key: str) -> None:
        """Rollback a feature rollout."""
        if feature_key in self.rollout_statuses:
            status = self.rollout_statuses[feature_key]
            status.status = "rolled_back"
            status.current_percentage = 0.0
            status.last_updated = datetime.utcnow()

        await self.stop_rollout(feature_key)
        logger.warning(f"Rollout rolled back for feature: {feature_key}")

    async def should_enable_feature(
        self,
        feature_key: str,
        user_context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if a feature should be enabled for a user."""
        if not user_context:
            user_context = {}

        # Check if feature is in active rollout
        if feature_key not in self.rollout_statuses:
            # Check base feature flag
            return await is_migration_feature_enabled(feature_key, user_context)

        status = self.rollout_statuses[feature_key]

        if status.status != "active":
            return False

        # Use appropriate decision engine
        if "user_segment" in user_context:
            return await self.segment_engine.should_enable_feature(
                feature_key, user_context, status
            )
        else:
            return await self.decision_engines["default"].should_enable_feature(
                feature_key, user_context, status
            )

    async def record_feature_usage(
        self,
        feature_key: str,
        user_context: Dict[str, Any],
        success: bool,
        error_message: Optional[str] = None
    ) -> None:
        """Record feature usage for monitoring and analytics."""
        if feature_key in self.rollout_statuses:
            status = self.rollout_statuses[feature_key]
            status.total_users_exposed += 1

            if success:
                status.successful_interactions += 1
            else:
                # Calculate error rate
                total_interactions = status.total_users_exposed
                error_count = total_interactions - status.successful_interactions
                status.error_rate = error_count / total_interactions if total_interactions > 0 else 0.0

            # Check for automatic rollback conditions
            if not success and error_message and status.error_rate > 0.05:
                logger.warning(
                    f"High error rate detected for {feature_key}: {status.error_rate:.2%}"
                )

            status.last_updated = datetime.utcnow()

    async def get_rollout_status(self, feature_key: str) -> Optional[RolloutStatus]:
        """Get current rollout status for a feature."""
        return self.rollout_statuses.get(feature_key)

    async def get_all_rollout_statuses(self) -> Dict[str, RolloutStatus]:
        """Get status for all active rollouts."""
        return self.rollout_statuses.copy()

    async def _run_gradual_rollout(self, config: RolloutConfig) -> None:
        """Run gradual rollout process."""
        status = self.rollout_statuses[config.feature_key]
        start_time = time.time()

        try:
            while status.current_percentage < config.target_percentage:
                # Check if rollout should be paused or stopped
                if status.status != "active":
                    break

                # Check for automatic rollback conditions
                if status.error_rate > config.rollback_on_error_rate:
                    logger.error(f"Rolling back {config.feature_key} due to high error rate")
                    await self.rollback_rollout(config.feature_key)
                    break

                # Check maximum duration
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= config.max_duration_hours:
                    logger.warning(f"Rollout timeout for {config.feature_key}")
                    break

                # Increment percentage
                new_percentage = min(
                    status.current_percentage + config.increment_percentage,
                    config.target_percentage
                )

                if new_percentage > status.current_percentage:
                    status.current_percentage = new_percentage
                    status.last_updated = datetime.utcnow()
                    logger.info(
                        f"Rollout progress: {config.feature_key} at {new_percentage}%"
                    )

                # Wait for next increment
                await asyncio.sleep(config.increment_interval_minutes * 60)

            # Mark as completed if target reached
            if status.current_percentage >= config.target_percentage:
                status.status = "completed"
                logger.info(f"Rollout completed for {config.feature_key}")

        except asyncio.CancelledError:
            logger.info(f"Gradual rollout cancelled for {config.feature_key}")
        except Exception as e:
            logger.error(f"Error in gradual rollout for {config.feature_key}: {e}")
            await self.rollback_rollout(config.feature_key)


# Global rollout manager instance
_rollout_manager: Optional[RolloutManager] = None


def get_rollout_manager() -> RolloutManager:
    """Get the global rollout manager."""
    global _rollout_manager
    if _rollout_manager is None:
        _rollout_manager = RolloutManager()
    return _rollout_manager


async def start_feature_rollout(config: RolloutConfig) -> None:
    """Start a feature rollout."""
    manager = get_rollout_manager()
    await manager.start_rollout(config)


async def should_enable_feature_for_user(
    feature_key: str,
    user_context: Optional[Dict[str, Any]] = None
) -> bool:
    """Check if feature should be enabled for a specific user."""
    manager = get_rollout_manager()
    return await manager.should_enable_feature(feature_key, user_context)


async def record_feature_interaction(
    feature_key: str,
    user_context: Dict[str, Any],
    success: bool,
    error_message: Optional[str] = None
) -> None:
    """Record a feature interaction for monitoring."""
    manager = get_rollout_manager()
    await manager.record_feature_usage(feature_key, user_context, success, error_message)


def create_database_rollout_config() -> RolloutConfig:
    """Create rollout config for database connection management."""
    return RolloutConfig(
        feature_key="unified_db_connections",
        strategy=RolloutStrategy.PERCENTAGE,
        initial_percentage=10.0,  # Start with 10% of traffic
        target_percentage=100.0,
        increment_percentage=20.0,  # Increase by 20% every hour
        increment_interval_minutes=60,
        max_duration_hours=12,
    )


def create_collector_rollout_config() -> RolloutConfig:
    """Create rollout config for unified external collectors."""
    return RolloutConfig(
        feature_key="unified_external_collectors",
        strategy=RolloutStrategy.CANARY,
        initial_percentage=5.0,  # Start with canary deployment
        target_percentage=100.0,
        increment_percentage=25.0,
        increment_interval_minutes=120,  # Slower rollout for external systems
        canary_size=50,  # Small canary group
        max_duration_hours=48,
    )


def create_service_rollout_config(service_name: str) -> RolloutConfig:
    """Create rollout config for service layer migration."""
    return RolloutConfig(
        feature_key=f"new_{service_name}_service",
        strategy=RolloutStrategy.GRADUAL,
        initial_percentage=0.0,
        target_percentage=100.0,
        increment_percentage=15.0,
        increment_interval_minutes=90,
        max_duration_hours=24,
    )
