"""Feature flag storage implementations."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable

from .models import FeatureFlag, FeatureFlagStore, FeatureFlagStatus


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
                flag_dict = json.loads(data)
                return FeatureFlag.from_dict(flag_dict)
        except Exception as e:
            # Log error but don't raise - return None instead
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to deserialize feature flag {key}: {e}")

        return None

    async def set_flag(self, flag: FeatureFlag) -> None:
        redis_client = await self._get_redis_client()
        flag_key = self._make_key(flag.key)

        try:
            # Serialize flag using JSON
            data = json.dumps(flag.to_dict(), default=str)
            await redis_client.set(flag_key, data)

            # Publish update event for cross-process invalidation
            update_event = {
                "flag_key": flag.key,
                "updated_at": flag.updated_at.isoformat(),
                "action": "set"
            }
            await redis_client.publish("feature_flags:updates", json.dumps(update_event))
        except Exception as e:
            # Log error but don't raise - silently fail for now
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to serialize feature flag {flag.key}: {e}")
            raise

    async def delete_flag(self, key: str) -> None:
        redis_client = await self._get_redis_client()
        flag_key = self._make_key(key)

        try:
            await redis_client.delete(flag_key)

            # Publish delete event for cross-process invalidation
            delete_event = {
                "flag_key": key,
                "action": "delete"
            }
            await redis_client.publish("feature_flags:updates", json.dumps(delete_event))
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
                        flag_dict = json.loads(data)
                        flags.append(FeatureFlag.from_dict(flag_dict))
                    except Exception as e:
                        logger = logging.getLogger(__name__)
                        logger.warning(f"Failed to deserialize feature flag from key {key}: {e}")
                        continue

            return flags
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to list feature flags: {e}")
            return []

    async def get_flags_for_user(self, user_context: Dict[str, Any]) -> Dict[str, Any]:
        flags = await self.list_flags()
        result = {}

        for flag in flags:
            result[flag.key] = flag.evaluate(user_context, {})

        return result
