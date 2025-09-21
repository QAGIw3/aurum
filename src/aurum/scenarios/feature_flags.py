"""Feature flags for scenario outputs per tenant."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

from ..telemetry.context import get_correlation_id, get_tenant_id, log_structured
from .storage import get_scenario_store


class ScenarioOutputFeature(str, Enum):
    """Scenario output features that can be feature-flagged."""

    # Core scenario output functionality
    SCENARIO_OUTPUTS_ENABLED = "scenario_outputs_enabled"
    BULK_SCENARIO_RUNS = "bulk_scenario_runs"
    SCENARIO_OUTPUTS_PAGINATION = "scenario_outputs_pagination"
    SCENARIO_OUTPUTS_FILTERING = "scenario_outputs_filtering"
    SCENARIO_OUTPUTS_CSV_EXPORT = "scenario_outputs_csv_export"
    SCENARIO_OUTPUTS_STREAMING = "scenario_outputs_streaming"

    # Advanced features
    SCENARIO_OUTPUTS_REAL_TIME = "scenario_outputs_real_time"
    SCENARIO_OUTPUTS_ANALYTICS = "scenario_outputs_analytics"
    SCENARIO_OUTPUTS_DASHBOARD = "scenario_outputs_dashboard"
    SCENARIO_OUTPUTS_API = "scenario_outputs_api"

    # Performance features
    SCENARIO_OUTPUTS_COMPRESSION = "scenario_outputs_compression"
    SCENARIO_OUTPUTS_CACHING = "scenario_outputs_caching"
    SCENARIO_OUTPUTS_OPTIMIZATION = "scenario_outputs_optimization"

    # Security and compliance
    SCENARIO_OUTPUTS_AUDIT_LOG = "scenario_outputs_audit_log"
    SCENARIO_OUTPUTS_DATA_RETENTION = "scenario_outputs_data_retention"
    SCENARIO_OUTPUTS_PII_FILTERING = "scenario_outputs_pii_filtering"

    # Integration features
    SCENARIO_OUTPUTS_WEBHOOKS = "scenario_outputs_webhooks"
    SCENARIO_OUTPUTS_INTEGRATIONS = "scenario_outputs_integrations"
    SCENARIO_OUTPUTS_NOTIFICATIONS = "scenario_outputs_notifications"


class ScenarioOutputLimits(BaseModel):
    """Limits for scenario outputs per tenant."""

    max_outputs_per_run: int = Field(default=10000, description="Maximum outputs per scenario run")
    max_outputs_per_request: int = Field(default=1000, description="Maximum outputs per API request")
    max_concurrent_requests: int = Field(default=10, description="Maximum concurrent output requests")
    max_retention_days: int = Field(default=90, description="Maximum data retention days")
    max_file_size_mb: int = Field(default=100, description="Maximum output file size in MB")

    @field_validator("max_outputs_per_run", "max_outputs_per_request", "max_concurrent_requests")
    @classmethod
    def validate_positive_int(cls, v: int, info) -> int:
        """Validate positive integer fields."""
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return v

    @field_validator("max_retention_days")
    @classmethod
    def validate_retention_days(cls, v: int) -> int:
        """Validate retention days."""
        if v < 1 or v > 3650:  # Max 10 years
            raise ValueError("max_retention_days must be between 1 and 3650")
        return v

    @field_validator("max_file_size_mb")
    @classmethod
    def validate_file_size(cls, v: int) -> int:
        """Validate file size."""
        if v < 1 or v > 1000:  # Max 1GB
            raise ValueError("max_file_size_mb must be between 1 and 1000")
        return v


class ScenarioOutputConfiguration(BaseModel):
    """Configuration for scenario output features."""

    limits: ScenarioOutputLimits = Field(default_factory=ScenarioOutputLimits)
    enabled_features: List[str] = Field(default_factory=list, description="List of enabled feature names")
    disabled_features: List[str] = Field(default_factory=list, description="List of disabled feature names")
    custom_settings: Dict[str, Any] = Field(default_factory=dict, description="Custom feature settings")

    @model_validator(mode="after")
    def validate_feature_lists(self) -> "ScenarioOutputConfiguration":
        """Validate feature lists don't overlap."""
        enabled = set(self.enabled_features)
        disabled = set(self.disabled_features)

        if enabled & disabled:
            overlapping = enabled & disabled
            raise ValueError(f"Features cannot be both enabled and disabled: {overlapping}")

        return self


class ScenarioOutputFeatureManager:
    """Manager for scenario output feature flags per tenant."""

    def __init__(self, scenario_store):
        self.store = scenario_store

    # === FEATURE FLAG MANAGEMENT ===

    async def is_feature_enabled(
        self,
        feature: ScenarioOutputFeature,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Check if a scenario output feature is enabled for a tenant."""
        flag = await self.store.get_feature_flag(feature.value)

        if not flag:
            # Return default behavior for unknown features
            return self._get_default_feature_state(feature)

        return flag.get("enabled", False)

    async def set_feature_enabled(
        self,
        feature: ScenarioOutputFeature,
        enabled: bool,
        tenant_id: Optional[str] = None,
        configuration: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Enable or disable a scenario output feature for a tenant."""
        return await self.store.set_feature_flag(
            feature_name=feature.value,
            enabled=enabled,
            configuration=configuration
        )

    async def get_feature_configuration(
        self,
        feature: ScenarioOutputFeature,
        tenant_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get configuration for a scenario output feature."""
        flag = await self.store.get_feature_flag(feature.value)
        return flag.get("configuration") if flag else None

    async def update_feature_configuration(
        self,
        feature: ScenarioOutputFeature,
        configuration: Dict[str, Any],
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update configuration for a scenario output feature."""
        return await self.store.set_feature_flag(
            feature_name=feature.value,
            enabled=True,  # Keep current enabled state
            configuration=configuration
        )

    # === BULK OPERATIONS ===

    async def set_multiple_features(
        self,
        features: Dict[ScenarioOutputFeature, bool],
        tenant_id: Optional[str] = None,
        configuration: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Enable/disable multiple features at once."""
        results = {}

        for feature, enabled in features.items():
            try:
                result = await self.set_feature_enabled(
                    feature=feature,
                    enabled=enabled,
                    configuration=configuration
                )
                results[feature.value] = result
            except Exception as e:
                results[feature.value] = {"error": str(e)}

        return {
            "results": results,
            "total": len(features),
            "successful": len([r for r in results.values() if "error" not in r]),
            "failed": len([r for r in results.values() if "error" in r])
        }

    async def get_all_features(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get status of all scenario output features."""
        features = {}

        for feature in ScenarioOutputFeature:
            flag = await self.store.get_feature_flag(feature.value)
            if flag:
                features[feature.value] = flag
            else:
                features[feature.value] = {
                    "enabled": self._get_default_feature_state(feature),
                    "configuration": {},
                    "created_at": None,
                    "updated_at": None
                }

        return features

    # === VALIDATION AND ENFORCEMENT ===

    async def validate_scenario_output_request(
        self,
        feature: ScenarioOutputFeature,
        request_data: Dict[str, Any],
        tenant_id: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """Validate a scenario output request against feature flags and limits."""
        # Check if feature is enabled
        if not await self.is_feature_enabled(feature):
            return False, f"Feature {feature.value} is not enabled"

        # Get configuration
        config = await self.get_feature_configuration(feature)
        if not config:
            return True, None  # No specific limits

        limits = ScenarioOutputLimits(**config.get("limits", {}))

        # Validate based on feature type
        if feature == ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION:
            limit = request_data.get("limit", 100)
            if limit > limits.max_outputs_per_request:
                return False, f"Request limit {limit} exceeds maximum {limits.max_outputs_per_request}"

        elif feature == ScenarioOutputFeature.SCENARIO_OUTPUTS_CSV_EXPORT:
            # Additional validation for CSV exports could go here
            pass

        elif feature == ScenarioOutputFeature.SCENARIO_OUTPUTS_STREAMING:
            # Additional validation for streaming requests
            pass

        return True, None

    async def enforce_output_limits(
        self,
        outputs: List[Dict[str, Any]],
        tenant_id: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Enforce output limits based on feature flags."""
        config = await self.get_feature_configuration(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)
        if not config:
            return outputs, None

        limits = ScenarioOutputLimits(**config.get("limits", {}))

        if len(outputs) > limits.max_outputs_per_request:
            return outputs[:limits.max_outputs_per_request], f"Output count limited to {limits.max_outputs_per_request}"

        return outputs, None

    # === DEFAULT FEATURE STATES ===

    def _get_default_feature_state(self, feature: ScenarioOutputFeature) -> bool:
        """Get default state for a feature when not explicitly configured."""
        # Core features are enabled by default
        core_features = {
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_FILTERING,
        }

        # Advanced features are disabled by default
        return feature in core_features

    # === UTILITY METHODS ===

    async def get_feature_stats(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get feature flag statistics for scenario outputs."""
        all_features = await self.get_all_features()

        enabled_count = sum(1 for f in all_features.values() if f.get("enabled", False))
        disabled_count = len(all_features) - enabled_count

        # Get configuration coverage
        configured_count = sum(1 for f in all_features.values() if f.get("configuration"))

        return {
            "total_features": len(all_features),
            "enabled_features": enabled_count,
            "disabled_features": disabled_count,
            "configured_features": configured_count,
            "configuration_coverage": configured_count / len(all_features) if all_features else 0,
            "features": {
                feature.value: {
                    "enabled": feature_data.get("enabled", False),
                    "has_configuration": bool(feature_data.get("configuration")),
                }
                for feature, feature_data in zip(ScenarioOutputFeature, all_features.values())
            }
        }

    async def reset_to_defaults(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Reset all feature flags to their default states."""
        results = {}

        for feature in ScenarioOutputFeature:
            try:
                default_enabled = self._get_default_feature_state(feature)
                result = await self.set_feature_enabled(
                    feature=feature,
                    enabled=default_enabled,
                    configuration=None
                )
                results[feature.value] = {"success": True, "enabled": default_enabled}
            except Exception as e:
                results[feature.value] = {"success": False, "error": str(e)}

        return {
            "results": results,
            "total": len(results),
            "successful": len([r for r in results.values() if r.get("success")]),
            "failed": len([r for r in results.values() if not r.get("success")])
        }

    async def enable_all_features(
        self,
        configuration: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Enable all scenario output features."""
        features = {feature: True for feature in ScenarioOutputFeature}
        return await self.set_multiple_features(
            features=features,
            configuration=configuration
        )

    async def disable_all_features(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Disable all scenario output features."""
        features = {feature: False for feature in ScenarioOutputFeature}
        return await self.set_multiple_features(features=features)


# Global feature manager instance
_scenario_output_feature_manager: Optional[ScenarioOutputFeatureManager] = None


def get_scenario_output_feature_manager() -> ScenarioOutputFeatureManager:
    """Get the global scenario output feature manager instance."""
    if _scenario_output_feature_manager is None:
        raise RuntimeError("ScenarioOutputFeatureManager not initialized")
    return _scenario_output_feature_manager


async def initialize_scenario_output_feature_manager() -> ScenarioOutputFeatureManager:
    """Initialize the global scenario output feature manager."""
    global _scenario_output_feature_manager
    if _scenario_output_feature_manager is None:
        store = get_scenario_store()
        _scenario_output_feature_manager = ScenarioOutputFeatureManager(store)
    return _scenario_output_feature_manager


# === UTILITY FUNCTIONS FOR SCENARIO OUTPUT ENDPOINTS ===

async def check_scenario_output_feature(
    feature: ScenarioOutputFeature,
    require_enabled: bool = True
) -> bool:
    """Check if a scenario output feature is enabled."""
    manager = get_scenario_output_feature_manager()
    return await manager.is_feature_enabled(feature)


async def enforce_scenario_output_limits(
    feature: ScenarioOutputFeature,
    request_data: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """Enforce limits for a scenario output feature."""
    manager = get_scenario_output_feature_manager()
    return await manager.validate_scenario_output_request(feature, request_data)


async def get_scenario_output_configuration(
    feature: ScenarioOutputFeature
) -> Optional[Dict[str, Any]]:
    """Get configuration for a scenario output feature."""
    manager = get_scenario_output_feature_manager()
    return await manager.get_feature_configuration(feature)


# === DECORATORS FOR FEATURE PROTECTION ===

def require_scenario_output_feature(feature: ScenarioOutputFeature):
    """Decorator to require a scenario output feature to be enabled."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            if not await check_scenario_output_feature(feature):
                from ..api.exceptions import ForbiddenException
                raise ForbiddenException(
                    resource_type="feature",
                    resource_id=feature.value,
                    detail=f"Feature {feature.value} is not enabled",
                    request_id=get_correlation_id()
                )
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def validate_scenario_output_request(feature: ScenarioOutputFeature):
    """Decorator to validate scenario output requests against feature limits."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Extract request data from args/kwargs
            request_data = {}
            # This would be enhanced to extract actual request parameters

            is_valid, error = await enforce_scenario_output_limits(feature, request_data)
            if not is_valid:
                from ..api.exceptions import ValidationException
                raise ValidationException(
                    field="request",
                    message=error or "Request validation failed",
                    request_id=get_correlation_id()
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator
