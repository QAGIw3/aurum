"""Feature management functionality for the Aurum API."""

from .feature_flags import (
    get_feature_manager,
    initialize_feature_flags,
    FeatureFlagManager,
    FeatureFlag,
    FeatureFlagRule,
    ABTestConfiguration,
    FeatureFlagStatus,
    UserSegment,
    RolloutStrategy,
    ABTestVariant,
    FeatureFlagStore,
    InMemoryFeatureFlagStore,
    RedisFeatureFlagStore,
)

from .feature_management import router as feature_management_router

__all__ = [
    # Feature Flag Management
    "get_feature_manager",
    "initialize_feature_flags",
    "FeatureFlagManager",
    "FeatureFlag",
    "FeatureFlagRule",
    "ABTestConfiguration",
    "FeatureFlagStatus",
    "UserSegment",
    "RolloutStrategy",
    "ABTestVariant",
    "FeatureFlagStore",
    "InMemoryFeatureFlagStore",
    "RedisFeatureFlagStore",
    # Routers
    "feature_management_router",
]
