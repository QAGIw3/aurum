"""Feature flags system for gradual rollouts and A/B testing."""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

from ..telemetry.context import get_request_id
from ..cache.cache import AsyncCache, CacheManager


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


class ScenarioFeatureFlagAdapter(FeatureFlagStore):
    """Adapter to bridge scenario feature flags with the generic system."""

    def __init__(self, generic_store: FeatureFlagStore, scenario_store):
        self.generic_store = generic_store
        self.scenario_store = scenario_store
        self.scenario_prefix = "scenario:"

    def _is_scenario_flag(self, key: str) -> bool:
        """Check if a flag key is scenario-related."""
        return key.startswith(self.scenario_prefix)

    def _get_scenario_flag_name(self, key: str) -> str:
        """Extract scenario flag name from key."""
        return key[len(self.scenario_prefix):]

    async def get_flag(self, key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        if self._is_scenario_flag(key):
            # Handle scenario flags
            scenario_flag_name = self._get_scenario_flag_name(key)
            scenario_flag_data = await self.scenario_store.get_feature_flag(scenario_flag_name)

            if scenario_flag_data:
                # Convert scenario flag to generic format
                return FeatureFlag(
                    name=f"Scenario {scenario_flag_name.replace('_', ' ').title()}",
                    key=key,
                    description=f"Scenario output feature: {scenario_flag_name}",
                    status=FeatureFlagStatus.ENABLED if scenario_flag_data.get("enabled", False) else FeatureFlagStatus.DISABLED,
                    default_value=scenario_flag_data.get("enabled", False),
                    rules=[],
                    created_at=scenario_flag_data.get("created_at") or datetime.utcnow(),
                    updated_at=scenario_flag_data.get("updated_at") or datetime.utcnow(),
                    created_by="scenario_system",
                    tags=["scenario", "auto-generated"]
                )
            return None
        else:
            # Handle generic flags
            return await self.generic_store.get_flag(key)

    async def set_flag(self, flag: FeatureFlag) -> None:
        """Store a feature flag."""
        if self._is_scenario_flag(flag.key):
            # Handle scenario flags
            scenario_flag_name = self._get_scenario_flag_name(flag.key)
            await self.scenario_store.set_feature_flag(
                feature_name=scenario_flag_name,
                enabled=flag.status == FeatureFlagStatus.ENABLED,
                configuration={}  # Scenario flags don't use complex configuration yet
            )
        else:
            # Handle generic flags
            await self.generic_store.set_flag(flag)

    async def delete_flag(self, key: str) -> None:
        """Delete a feature flag."""
        if self._is_scenario_flag(key):
            # Scenario flags are managed through the scenario system
            # For now, we'll disable them rather than delete
            scenario_flag_name = self._get_scenario_flag_name(key)
            await self.scenario_store.set_feature_flag(
                feature_name=scenario_flag_name,
                enabled=False,
                configuration={}
            )
        else:
            # Handle generic flags
            await self.generic_store.delete_flag(key)

    async def list_flags(self) -> List[FeatureFlag]:
        """List all feature flags."""
        # Get generic flags
        generic_flags = await self.generic_store.list_flags()

        # Get scenario flags
        scenario_flags = []
        from ..scenarios.feature_flags import ScenarioOutputFeature
        for feature in ScenarioOutputFeature:
            scenario_flag_data = await self.scenario_store.get_feature_flag(feature.value)
            if scenario_flag_data:
                scenario_flags.append(FeatureFlag(
                    name=f"Scenario {feature.value.replace('_', ' ').title()}",
                    key=f"{self.scenario_prefix}{feature.value}",
                    description=f"Scenario output feature: {feature.value}",
                    status=FeatureFlagStatus.ENABLED if scenario_flag_data.get("enabled", False) else FeatureFlagStatus.DISABLED,
                    default_value=scenario_flag_data.get("enabled", False),
                    rules=[],
                    created_at=scenario_flag_data.get("created_at") or datetime.utcnow(),
                    updated_at=scenario_flag_data.get("updated_at") or datetime.utcnow(),
                    created_by="scenario_system",
                    tags=["scenario", "auto-generated"]
                ))

        return generic_flags + scenario_flags

    async def get_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        """Get all feature flag values for a user."""
        # Get generic flag evaluations
        generic_evaluations = await self.generic_store.get_flags_for_user(user_context)

        # Get scenario flag evaluations
        scenario_evaluations = {}
        from ..scenarios.feature_flags import ScenarioOutputFeature
        for feature in ScenarioOutputFeature:
            scenario_flag_data = await self.scenario_store.get_feature_flag(feature.value)
            if scenario_flag_data:
                scenario_evaluations[f"{self.scenario_prefix}{feature.value}"] = scenario_flag_data.get("enabled", False)

        # Combine results
        return {**generic_evaluations, **scenario_evaluations}


class InMemoryFeatureFlagStore(FeatureFlagStore):
    """In-memory feature flag store for development/testing."""

    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = asyncio.Lock()

    async def get_flag(self, key: str) -> Optional[FeatureFlag]:
        async with self._lock:
            return self._flags.get(key)

    async def set_flag(self, flag: FeatureFlag) -> None:
        async with self._lock:
            flag.updated_at = datetime.utcnow()
            self._flags[flag.key] = flag

    async def delete_flag(self, key: str) -> None:
        async with self._lock:
            self._flags.pop(key, None)

    async def list_flags(self) -> List[FeatureFlag]:
        async with self._lock:
            return list(self._flags.values())

    async def get_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            result = {}
            for flag in self._flags.values():
                result[flag.key] = flag.evaluate(user_context, {})
            return result


class RedisFeatureFlagStore(FeatureFlagStore):
    """Redis-based feature flag store for production."""

    def __init__(self, redis_url: str, namespace: str = "feature_flags"):
        self.redis_url = redis_url
        self.namespace = namespace
        self._redis_client = None

    async def _get_redis_client(self):
        """Get Redis client (lazy initialization)."""
        if self._redis_client is None:
            try:
                import redis.asyncio as redis
                self._redis_client = redis.from_url(self.redis_url, decode_responses=True)
                await self._redis_client.ping()
            except ImportError:
                raise RuntimeError("redis package not available")
            except Exception as e:
                raise RuntimeError(f"Failed to connect to Redis: {e}")
        return self._redis_client

    def _make_key(self, flag_key: str) -> str:
        """Create Redis key for flag."""
        return f"{self.namespace}:{flag_key}"

    async def get_flag(self, key: str) -> Optional[FeatureFlag]:
        redis_client = await self._get_redis_client()
        flag_key = self._make_key(key)

        try:
            data = await redis_client.get(flag_key)
            if data:
                # Deserialize flag using JSON
                import json
                flag_dict = json.loads(data)
                return FeatureFlag.from_dict(flag_dict)
        except Exception as e:
            # Log error but don't raise - return None instead
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to deserialize feature flag {key}: {e}")

        return None

    async def set_flag(self, flag: FeatureFlag) -> None:
        redis_client = await self._get_redis_client()
        flag_key = self._make_key(flag.key)

        try:
            # Serialize flag using JSON
            import json
            data = json.dumps(flag.to_dict(), default=str)
            await redis_client.set(flag_key, data)
        except Exception as e:
            # Log error but don't raise - silently fail for now
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to serialize feature flag {flag.key}: {e}")
            raise

    async def delete_flag(self, key: str) -> None:
        redis_client = await self._get_redis_client()
        flag_key = self._make_key(key)

        try:
            await redis_client.delete(flag_key)
        except Exception:
            pass

    async def list_flags(self) -> List[FeatureFlag]:
        redis_client = await self._get_redis_client()

        try:
            pattern = f"{self.namespace}:*"
            keys = await redis_client.keys(pattern)
            flags = []

            for key in keys:
                data = await redis_client.get(key)
                if data:
                    try:
                        import json
                        flag_dict = json.loads(data)
                        flags.append(FeatureFlag.from_dict(flag_dict))
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f"Failed to deserialize feature flag from key {key}: {e}")
                        continue

            return flags
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to list feature flags: {e}")
            return []

    async def get_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        flags = await self.list_flags()
        result = {}

        for flag in flags:
            result[flag.key] = flag.evaluate(user_context, {})

        return result


class FeatureFlagManager:
    """Manages feature flags and their evaluation."""

    def __init__(self, store: FeatureFlagStore, cache_manager: Optional[CacheManager] = None):
        self.store = store
        self.cache_manager = cache_manager
        self._flag_cache: Dict[str, FeatureFlag] = {}
        self._cache_ttl = 300  # 5 minutes

    async def get_flag(self, key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        # Check cache first
        if key in self._flag_cache:
            cached_flag = self._flag_cache[key]
            if datetime.utcnow() - cached_flag.updated_at < timedelta(seconds=self._cache_ttl):
                return cached_flag

        # Get from store
        flag = await self.store.get_flag(key)
        if flag:
            self._flag_cache[key] = flag

        return flag

    async def set_flag(self, flag: FeatureFlag) -> None:
        """Store a feature flag."""
        await self.store.set_flag(flag)
        self._flag_cache[flag.key] = flag

        # Invalidate cache for user flag evaluations
        if self.cache_manager:
            await self.cache_manager.invalidate_pattern("user_flags:*")

    async def delete_flag(self, key: str) -> None:
        """Delete a feature flag."""
        await self.store.delete_flag(key)
        self._flag_cache.pop(key, None)

        # Invalidate cache for user flag evaluations
        if self.cache_manager:
            await self.cache_manager.invalidate_pattern("user_flags:*")

    async def list_flags(self) -> List[FeatureFlag]:
        """List all feature flags."""
        return await self.store.list_flags()

    async def evaluate_flag(
        self,
        flag_key: str,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> Union[bool, str]:
        """Evaluate a feature flag for a user."""
        flag = await self.get_flag(flag_key)
        if not flag:
            return False

        return flag.evaluate(user_context, feature_context)

    async def is_enabled(
        self,
        flag_key: str,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> bool:
        """Check if a feature flag is enabled for a user."""
        flag = await self.get_flag(flag_key)
        if not flag:
            return False

        return flag.is_enabled_for_user(user_context, feature_context)

    async def get_variant(
        self,
        flag_key: str,
        user_context: Dict[str, Any],
        feature_context: Dict[str, Any]
    ) -> Optional[str]:
        """Get A/B test variant for a user."""
        flag = await self.get_flag(flag_key)
        if not flag:
            return None

        return flag.get_variant_for_user(user_context, feature_context)

    async def get_all_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        """Get all feature flag evaluations for a user."""
        # Check cache first
        cache_key = f"user_flags:{hash(str(sorted(user_context.items())))}"
        if self.cache_manager:
            cached_result = await self.cache_manager.get_cache_entry(cache_key)
            if cached_result:
                return cached_result

        # Get from store
        result = await self.store.get_flags_for_user(user_context)

        # Cache the result
        if self.cache_manager:
            await self.cache_manager.set_cache_entry(
                cache_key,
                result,
                ttl_seconds=300  # 5 minutes
            )

        return result

    async def create_flag(
        self,
        name: str,
        key: str,
        description: str = "",
        default_value: bool = False,
        status: FeatureFlagStatus = FeatureFlagStatus.DISABLED,
        created_by: str = "system"
    ) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            name=name,
            key=key,
            description=description,
            default_value=default_value,
            status=status,
            created_by=created_by
        )

        await self.set_flag(flag)
        return flag

    async def update_flag_status(self, key: str, status: FeatureFlagStatus) -> bool:
        """Update feature flag status."""
        flag = await self.get_flag(key)
        if not flag:
            return False

        flag.status = status
        flag.updated_at = datetime.utcnow()
        await self.set_flag(flag)
        return True

    async def add_rule(
        self,
        flag_key: str,
        rule: FeatureFlagRule
    ) -> bool:
        """Add a rule to a feature flag."""
        flag = await self.get_flag(flag_key)
        if not flag:
            return False

        flag.rules.append(rule)
        flag.updated_at = datetime.utcnow()
        await self.set_flag(flag)
        return True

    async def set_ab_test(
        self,
        flag_key: str,
        ab_config: ABTestConfiguration
    ) -> bool:
        """Set A/B test configuration for a feature flag."""
        flag = await self.get_flag(flag_key)
        if not flag:
            return False

        flag.ab_test_config = ab_config
        flag.updated_at = datetime.utcnow()
        await self.set_flag(flag)
        return True

    async def get_feature_stats(self) -> Dict[str, Any]:
        """Get feature flag usage statistics."""
        flags = await self.list_flags()
        total_flags = len(flags)

        status_counts = {}
        for flag in flags:
            status = flag.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        # Get usage statistics from cache if available
        usage_stats = {
            "total_flags": total_flags,
            "status_distribution": status_counts,
            "ab_test_flags": sum(1 for f in flags if f.ab_test_config),
            "rules_based_flags": sum(1 for f in flags if f.rules),
        }

        return usage_stats


# Global feature flag manager
_feature_manager: Optional[FeatureFlagManager] = None


def get_feature_manager() -> FeatureFlagManager:
    """Get the global feature flag manager."""
    global _feature_manager
    if _feature_manager is None:
        store = InMemoryFeatureFlagStore()
        _feature_manager = FeatureFlagManager(store)
    return _feature_manager


async def initialize_feature_flags(
    redis_url: Optional[str] = None,
    cache_manager: Optional[CacheManager] = None,
    scenario_store = None
) -> FeatureFlagManager:
    """Initialize the feature flag system."""
    global _feature_manager

    if _feature_manager is None:
        if redis_url:
            generic_store = RedisFeatureFlagStore(redis_url)
        else:
            generic_store = InMemoryFeatureFlagStore()

        # Use adapter if scenario store is available
        if scenario_store:
            store = ScenarioFeatureFlagAdapter(generic_store, scenario_store)
        else:
            store = generic_store

        _feature_manager = FeatureFlagManager(store, cache_manager)

    return _feature_manager
