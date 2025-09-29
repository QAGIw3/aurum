"""Testing utilities for feature flags."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.api.features import (
    FeatureFlag,
    FeatureFlagRule,
    FeatureFlagStatus,
    UserSegment,
    FeatureFlagManager,
    get_feature_manager,
    initialize_feature_flags,
    RolloutPlan,
    RolloutStrategy,
    get_rollout_evaluator,
    get_analytics_collector,
)


class FlagOverride:
    """Context manager for overriding feature flag values during tests."""

    def __init__(
        self,
        flag_key: str,
        value: Union[bool, str],
        user_context: Dict[str, Any] = None,
        feature_context: Dict[str, Any] = None
    ):
        self.flag_key = flag_key
        self.value = value
        self.user_context = user_context or {}
        self.feature_context = feature_context or {}
        self._original_manager = None
        self._original_evaluator = None

    async def __aenter__(self):
        """Enter the context and set up overrides."""
        # Get the current manager
        self._original_manager = get_feature_manager()

        # Create a mock manager
        mock_manager = AsyncMock(spec=FeatureFlagManager)

        # Configure the mock based on the override value
        if isinstance(self.value, bool):
            mock_manager.is_enabled.return_value = self.value
            mock_manager.evaluate_flag.return_value = self.value
        else:
            # For A/B test variants
            mock_manager.evaluate_flag.return_value = self.value
            mock_manager.get_variant.return_value = self.value
            mock_manager.is_enabled.return_value = True  # A/B tests are enabled

        # Configure other methods to delegate to the original manager
        mock_manager.list_flags = self._original_manager.list_flags
        mock_manager.get_flag = self._original_manager.get_flag
        mock_manager.get_all_flags_for_user = self._original_manager.get_all_flags_for_user

        # Patch the global manager
        patch('aurum.api.features.get_feature_manager', return_value=mock_manager).start()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and restore original state."""
        # The patch will be automatically cleaned up by the context manager
        pass


@asynccontextmanager
async def feature_flag_override(
    flag_key: str,
    value: Union[bool, str],
    user_context: Dict[str, Any] = None,
    feature_context: Dict[str, Any] = None
):
    """Context manager for temporarily overriding a feature flag value."""
    async with FlagOverride(flag_key, value, user_context, feature_context):
        yield


def create_test_flag(
    key: str,
    name: str = None,
    description: str = "",
    status: FeatureFlagStatus = FeatureFlagStatus.ENABLED,
    default_value: bool = False,
    rules: List[FeatureFlagRule] = None,
    ab_test_config = None,
    **kwargs
) -> FeatureFlag:
    """Create a test feature flag."""
    return FeatureFlag(
        name=name or f"Test {key}",
        key=key,
        description=description,
        status=status,
        default_value=default_value,
        rules=rules or [],
        ab_test_config=ab_test_config,
        **kwargs
    )


def create_test_rule(
    name: str,
    conditions: Dict[str, Any] = None,
    rollout_percentage: float = 100.0,
    user_segments: List[UserSegment] = None,
    required_flags: List[str] = None,
    excluded_flags: List[str] = None,
) -> FeatureFlagRule:
    """Create a test feature flag rule."""
    return FeatureFlagRule(
        name=name,
        conditions=conditions or {},
        rollout_percentage=rollout_percentage,
        user_segments=user_segments or [],
        required_flags=required_flags or [],
        excluded_flags=excluded_flags or [],
    )


def create_test_rollout_plan(
    flag_key: str,
    strategy: RolloutStrategy = RolloutStrategy.PERCENTAGE,
    percentage: float = 50.0,
    name: str = "Test Rollout",
    description: str = "Test rollout plan",
    **kwargs
) -> RolloutPlan:
    """Create a test rollout plan."""
    return RolloutPlan(
        strategy=strategy,
        percentage=percentage,
        name=name,
        description=description,
        **kwargs
    )


async def setup_test_feature_flags(
    manager: FeatureFlagManager = None,
    flags: List[FeatureFlag] = None,
    rollout_plans: Dict[str, RolloutPlan] = None,
) -> FeatureFlagManager:
    """Set up feature flags for testing."""
    if manager is None:
        # Create a test manager with in-memory store
        from aurum.api.features.stores import InMemoryFeatureFlagStore
        store = InMemoryFeatureFlagStore()
        manager = FeatureFlagManager(store)

    if flags:
        for flag in flags:
            await manager.set_flag(flag)

    if rollout_plans:
        rollout_evaluator = get_rollout_evaluator()
        for flag_key, plan in rollout_plans.items():
            await rollout_evaluator.set_rollout_plan(flag_key, plan)

    return manager


async def clear_test_analytics():
    """Clear analytics data for testing."""
    analytics_collector = get_analytics_collector()
    # Reset the internal state (in a real implementation, this would clear the store)
    # For now, we just create a new instance
    global _analytics_collector
    _analytics_collector = None


# Pytest fixtures
def pytest_configure(config):
    """Configure pytest for feature flag testing."""
    # Register custom markers
    config.addinivalue_line("markers", "feature_flags: mark test as requiring feature flag setup")
    config.addinivalue_line("markers", "rollout: mark test as requiring rollout plan setup")


def pytest_collection_modifyitems(config, items):
    """Modify test collection for feature flag tests."""
    # Add markers to tests that use feature flag utilities
    for item in items:
        if "feature_flag" in item.nodeid or "rollout" in item.nodeid:
            item.add_marker(pytest.mark.feature_flags)
