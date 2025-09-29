"""Feature management functionality for the Aurum API."""

from .models import (
    FeatureFlag,
    FeatureFlagRule,
    ABTestConfiguration,
    FeatureFlagStatus,
    UserSegment,
    RolloutStrategy,
    ABTestVariant,
    FeatureFlagStore,
)

from .stores import (
    InMemoryFeatureFlagStore,
    RedisFeatureFlagStore,
)

from .rollouts import (
    RolloutPlan,
    RolloutEvaluator,
    get_rollout_evaluator,
    initialize_rollout_evaluator,
)

from .analytics import (
    FeatureAnalyticsCollector,
    get_analytics_collector,
    initialize_analytics_collector,
)

from .manager import (
    get_feature_manager,
    initialize_feature_flags,
    FeatureFlagManager,
)

from .feature_management import router as feature_management_router

# Public API helper functions
async def is_enabled(
    flag_key: str,
    user_context: dict = None,
    feature_context: dict = None
) -> bool:
    """
    Check if a feature flag is enabled for a user.

    This is the main public API for feature flag evaluation.

    Args:
        flag_key: The unique key of the feature flag
        user_context: Dictionary containing user information (e.g., {"user_id": "123", "user_segment": "premium"})
        feature_context: Dictionary containing feature-specific context

    Returns:
        bool: True if the feature is enabled, False otherwise

    Example:
        >>> from aurum.api.features import is_enabled
        >>>
        >>> user_context = {"user_id": "user123", "user_segment": "premium_users"}
        >>> enabled = await is_enabled("new_dashboard_widget", user_context)
        >>> if enabled:
        ...     show_dashboard_widget()
    """
    if user_context is None:
        user_context = {}
    if feature_context is None:
        feature_context = {}

    manager = get_feature_manager()
    return await manager.is_enabled(flag_key, user_context, feature_context)


async def get_variant(
    flag_key: str,
    user_context: dict = None,
    feature_context: dict = None
) -> str:
    """
    Get the A/B test variant for a user.

    Args:
        flag_key: The unique key of the feature flag
        user_context: Dictionary containing user information
        feature_context: Dictionary containing feature-specific context

    Returns:
        str: The variant name (e.g., "control", "variant_a", "variant_b")

    Returns None if the flag doesn't have A/B test configuration.
    """
    if user_context is None:
        user_context = {}
    if feature_context is None:
        feature_context = {}

    manager = get_feature_manager()
    return await manager.get_variant(flag_key, user_context, feature_context)


async def evaluate_flag(
    flag_key: str,
    user_context: dict = None,
    feature_context: dict = None
) -> bool | str:
    """
    Evaluate a feature flag for a user (returns bool or variant string).

    Args:
        flag_key: The unique key of the feature flag
        user_context: Dictionary containing user information
        feature_context: Dictionary containing feature-specific context

    Returns:
        bool | str: True/False for boolean flags, or variant name for A/B tests
    """
    if user_context is None:
        user_context = {}
    if feature_context is None:
        feature_context = {}

    manager = get_feature_manager()
    return await manager.evaluate_flag(flag_key, user_context, feature_context)


__all__ = [
    # Core Models
    "FeatureFlag",
    "FeatureFlagRule",
    "ABTestConfiguration",
    "FeatureFlagStatus",
    "UserSegment",
    "RolloutStrategy",
    "ABTestVariant",
    "FeatureFlagStore",

    # Storage Implementations
    "InMemoryFeatureFlagStore",
    "RedisFeatureFlagStore",

    # Rollout Management
    "RolloutPlan",
    "RolloutEvaluator",
    "get_rollout_evaluator",
    "initialize_rollout_evaluator",

    # Analytics and Monitoring
    "FeatureAnalyticsCollector",
    "get_analytics_collector",
    "initialize_analytics_collector",

    # Manager and Utilities
    "get_feature_manager",
    "initialize_feature_flags",
    "FeatureFlagManager",
    "is_enabled",
    "get_variant",
    "evaluate_flag",

    # Routers
    "feature_management_router",
]
