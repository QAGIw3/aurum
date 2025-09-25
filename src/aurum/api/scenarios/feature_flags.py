"""Feature flags for scenario functionality.

This module defines feature flags that control various aspects of
scenario functionality and experimental features.
"""

from __future__ import annotations

from typing import Dict, Any
from enum import Enum


class ScenarioOutputFeature(str, Enum):
    """Feature flags for scenario output functionality."""
    STREAMING_OUTPUTS = "streaming_outputs"
    PAGINATED_OUTPUTS = "paginated_outputs"
    OUTPUT_COMPRESSION = "output_compression"
    OUTPUT_CACHING = "output_caching"
    SCENARIO_OUTPUTS_ENABLED = "scenario_outputs_enabled"
    BULK_SCENARIO_RUNS = "bulk_scenario_runs"


def is_feature_enabled(feature: str, context: Dict[str, Any] = None) -> bool:
    """Check if a feature is enabled.

    Args:
        feature: Feature name to check
        context: Optional context for feature evaluation

    Returns:
        True if feature is enabled, False otherwise
    """
    # This is a placeholder implementation
    # In a real implementation, this would check feature flags
    # from a configuration store or environment variables
    import os

    env_var = f"AURUM_FEATURE_{feature.upper()}"
    return os.getenv(env_var, "false").lower() in ("true", "1", "yes")


def get_feature_context() -> Dict[str, Any]:
    """Get feature context for evaluation.

    Returns:
        Dictionary with feature context information
    """
    # This is a placeholder implementation
    # In a real implementation, this would gather context
    # like user info, tenant info, environment, etc.
    return {
        "environment": "development",
        "version": "1.0.0"
    }


def require_scenario_output_feature(feature: ScenarioOutputFeature):
    """Decorator to require a scenario output feature to be enabled.

    Args:
        feature: The feature that must be enabled

    Returns:
        Decorator function
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            if not is_feature_enabled(feature.value):
                raise ValueError(f"Feature {feature.value} is not enabled")
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def check_scenario_output_feature(feature: ScenarioOutputFeature) -> bool:
    """Check if a scenario output feature is enabled.

    Args:
        feature: The feature to check

    Returns:
        True if feature is enabled, False otherwise
    """
    return is_feature_enabled(feature.value)


def enforce_scenario_output_limits(data: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce scenario output limits based on feature flags.

    Args:
        data: Output data to potentially limit

    Returns:
        Potentially limited data
    """
    # This is a placeholder implementation
    # In a real implementation, this would enforce limits
    # based on feature flags and user permissions
    return data


__all__ = [
    "ScenarioOutputFeature",
    "is_feature_enabled",
    "get_feature_context",
    "require_scenario_output_feature",
    "check_scenario_output_feature",
    "enforce_scenario_output_limits"
]
