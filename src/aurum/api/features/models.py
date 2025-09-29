"""Feature flag models and data structures."""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod


class FeatureFlagStatus(Enum):
    """Status of a feature flag."""
    DISABLED = "disabled"
    ENABLED = "enabled"
    CONDITIONAL = "conditional"


class UserSegment(Enum):
    """Predefined user segments for targeting."""
    ALL_USERS = "all_users"
    PREMIUM_USERS = "premium_users"
    ENTERPRISE_USERS = "enterprise_users"
    BETA_TESTERS = "beta_testers"
    INTERNAL_USERS = "internal_users"
    NEW_USERS = "new_users"  # Users created in last 30 days
    POWER_USERS = "power_users"  # Users with high activity


class RolloutStrategy(Enum):
    """Rollout strategies for feature flags."""
    PERCENTAGE = "percentage"  # Percentage of users
    GRADUAL = "gradual"  # Gradual ramp-up over time
    TARGETED = "targeted"  # Specific users/segments
    SCHEDULED = "scheduled"  # Time-based rollout
    DEPENDENT = "dependent"  # Depends on other flags


class ABTestVariant(Enum):
    """A/B test variant types."""
    CONTROL = "control"
    VARIANT_A = "variant_a"
    VARIANT_B = "variant_b"
    VARIANT_C = "variant_c"
    VARIANT_D = "variant_d"


@dataclass
class FeatureFlagRule:
    """A single rule for feature flag evaluation."""
    name: str
    conditions: Dict[str, Any]  # Field -> value mapping
    rollout_percentage: float = 100.0
    user_segments: List[UserSegment] = field(default_factory=list)
    required_flags: List[str] = field(default_factory=list)  # Flags that must be enabled
    excluded_flags: List[str] = field(default_factory=list)  # Flags that must be disabled

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate if this rule matches the given context."""
        # Check required flags
        for flag_name in self.required_flags:
            if not context.get(f"flag_{flag_name}", False):
                return False

        # Check excluded flags
        for flag_name in self.excluded_flags:
            if context.get(f"flag_{flag_name}", False):
                return False

        # Check user segments
        user_segment = context.get("user_segment")
        if user_segment and self.user_segments and user_segment not in self.user_segments:
            return False

        # Evaluate conditions
        if not self._evaluate_conditions(context):
            return False

        # Check rollout percentage
        if self.rollout_percentage < 100.0:
            user_id = context.get("user_id", "")
            if user_id:
                # Deterministic rollout based on user ID
                user_hash = hashlib.md5(user_id.encode()).hexdigest()
                user_percentage = (int(user_hash[:8], 16) % 100)
                if user_percentage > self.rollout_percentage:
                    return False

        return True

    def _evaluate_conditions(self, context: Dict[str, Any]) -> bool:
        """Evaluate conditions against context."""
        if not self.conditions:
            return True

        for field_path, condition in self.conditions.items():
            if not self._evaluate_condition(field_path, condition, context):
                return False

        return True

    def _evaluate_condition(self, field_path: str, condition: Any, context: Dict[str, Any]) -> bool:
        """Evaluate a single condition."""
        # Handle nested field access (e.g., "user.role", "tenant.plan")
        field_value = self._get_nested_value(field_path, context)
        if field_value is None:
            return False

        # Handle different condition types
        if isinstance(condition, dict):
            # Operator-based condition: {"op": "eq", "value": "premium"}
            if "op" in condition and "value" in condition:
                operator = condition["op"]
                expected_value = condition["value"]
                return self._evaluate_operator(field_value, expected_value, operator)
            else:
                # Nested condition - not supported yet
                return True
        else:
            # Simple equality check
            return field_value == condition

    def _get_nested_value(self, field_path: str, context: Dict[str, Any]) -> Any:
        """Get value from nested dictionary using dot notation."""
        keys = field_path.split(".")
        current = context

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None

        return current

    def _evaluate_operator(self, actual_value: Any, expected_value: Any, operator: str) -> bool:
        """Evaluate operator-based conditions."""
        if operator == "eq":
            return actual_value == expected_value
        elif operator == "neq":
            return actual_value != expected_value
        elif operator == "gt":
            return actual_value > expected_value
        elif operator == "gte":
            return actual_value >= expected_value
        elif operator == "lt":
            return actual_value < expected_value
        elif operator == "lte":
            return actual_value <= expected_value
        elif operator == "in":
            return actual_value in expected_value
        elif operator == "nin":
            return actual_value not in expected_value
        elif operator == "contains":
            return expected_value in actual_value
        elif operator == "startswith":
            return str(actual_value).startswith(str(expected_value))
        elif operator == "endswith":
            return str(actual_value).endswith(str(expected_value))
        elif operator == "regex":
            import re
            return bool(re.match(expected_value, str(actual_value)))
        else:
            # Unknown operator, default to equality
            return actual_value == expected_value


@dataclass
class ABTestConfiguration:
    """Configuration for A/B testing."""
    variants: Dict[str, float] = field(default_factory=dict)  # variant_name -> percentage
    control_variant: str = "control"
    track_events: List[str] = field(default_factory=list)  # Events to track for analysis
    end_date: Optional[datetime] = None

    def get_variant_for_user(self, user_id: str) -> str:
        """Get A/B test variant for a specific user."""
        if not user_id:
            return self.control_variant

        # Create deterministic assignment
        user_hash = hashlib.md5(user_id.encode()).hexdigest()
        user_score = int(user_hash[:8], 16) % 10000  # 0-9999
        user_percentage = user_score / 100.0  # 0.0-99.99

        cumulative_percentage = 0.0
        for variant, percentage in self.variants.items():
            cumulative_percentage += percentage
            if user_percentage <= cumulative_percentage:
                return variant

        return self.control_variant


@dataclass
class FeatureFlag:
    """A feature flag with all its configuration."""
    name: str
    key: str  # Unique identifier for the flag
    description: str = ""
    status: FeatureFlagStatus = FeatureFlagStatus.DISABLED
    default_value: bool = False
    rules: List[FeatureFlagRule] = field(default_factory=list)
    ab_test_config: Optional[ABTestConfiguration] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""
    tags: List[str] = field(default_factory=list)
    # Note: rollout_plan is managed separately via RolloutEvaluator for performance

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature flag to dictionary for serialization."""
        data = {
            "name": self.name,
            "key": self.key,
            "description": self.description,
            "status": self.status.value,
            "default_value": self.default_value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "tags": self.tags,
        }

        # Handle rules
        if self.rules:
            data["rules"] = [
                {
                    "name": rule.name,
                    "conditions": rule.conditions,
                    "rollout_percentage": rule.rollout_percentage,
                    "user_segments": [s.value for s in rule.user_segments],
                    "required_flags": rule.required_flags,
                    "excluded_flags": rule.excluded_flags,
                }
                for rule in self.rules
            ]

        # Handle A/B test config
        if self.ab_test_config:
            data["ab_test_config"] = {
                "variants": self.ab_test_config.variants,
                "control_variant": self.ab_test_config.control_variant,
                "track_events": self.ab_test_config.track_events,
                "end_date": self.ab_test_config.end_date.isoformat() if self.ab_test_config.end_date else None,
            }

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FeatureFlag':
        """Create feature flag from dictionary."""
        # Handle datetime conversion
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))

        # Handle status conversion
        status = FeatureFlagStatus(data.get("status", "disabled"))

        # Handle rules
        rules_data = data.get("rules", [])
        rules = []
        for rule_data in rules_data:
            user_segments = [UserSegment(s) for s in rule_data.get("user_segments", [])]
            rules.append(FeatureFlagRule(
                name=rule_data.get("name", "Unnamed Rule"),
                conditions=rule_data.get("conditions", {}),
                rollout_percentage=rule_data.get("rollout_percentage", 100.0),
                user_segments=user_segments,
                required_flags=rule_data.get("required_flags", []),
                excluded_flags=rule_data.get("excluded_flags", []),
            ))

        # Handle A/B test config
        ab_config_data = data.get("ab_test_config")
        ab_config = None
        if ab_config_data:
            end_date = ab_config_data.get("end_date")
            if end_date:
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

            ab_config = ABTestConfiguration(
                variants=ab_config_data.get("variants", {}),
                control_variant=ab_config_data.get("control_variant", "control"),
                track_events=ab_config_data.get("track_events", []),
                end_date=end_date,
            )

        return cls(
            name=data.get("name", ""),
            key=data.get("key", ""),
            description=data.get("description", ""),
            status=status,
            default_value=data.get("default_value", False),
            rules=rules,
            ab_test_config=ab_config,
            created_at=created_at or datetime.utcnow(),
            updated_at=updated_at or datetime.utcnow(),
            created_by=data.get("created_by", ""),
            tags=data.get("tags", []),
        )

    def evaluate(
        self,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> Union[bool, str]:  # Returns bool or variant name for A/B tests
        """Evaluate the feature flag for a given context."""
        if self.status == FeatureFlagStatus.DISABLED:
            return False

        # Combine contexts
        context = {**user_context, **feature_context}

        # Check if it's an A/B test
        if self.ab_test_config and self.status == FeatureFlagStatus.ENABLED:
            user_id = context.get("user_id", "")
            variant = self.ab_test_config.get_variant_for_user(user_id)
            return variant

        # Check rules
        for rule in self.rules:
            if rule.evaluate(context):
                return True

        # Default value
        return self.default_value

    def is_enabled_for_user(
        self,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> bool:
        """Check if the feature is enabled for a specific user."""
        result = self.evaluate(user_context, feature_context)
        return result is True or result == "control" or result.startswith("variant_")

    def get_variant_for_user(
        self,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> Optional[str]:
        """Get the A/B test variant for a user."""
        if not self.ab_test_config:
            return None

        result = self.evaluate(user_context, feature_context)
        if isinstance(result, str):
            return result
        return None


class FeatureFlagStore(ABC):
    """Abstract base class for feature flag storage."""

    @abstractmethod
    async def get_flag(self, key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        pass

    @abstractmethod
    async def set_flag(self, flag: FeatureFlag) -> None:
        """Store a feature flag."""
        pass

    @abstractmethod
    async def delete_flag(self, key: str) -> None:
        """Delete a feature flag."""
        pass

    @abstractmethod
    async def list_flags(self) -> List[FeatureFlag]:
        """List all feature flags."""
        pass

    @abstractmethod
    async def get_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        """Get all feature flag values for a user."""
        pass
