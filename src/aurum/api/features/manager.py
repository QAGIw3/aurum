"""Feature flag manager implementation."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from ..cache.cache import CacheManager
from .models import FeatureFlag, FeatureFlagRule, FeatureFlagStatus, ABTestConfiguration
from .stores import FeatureFlagStore, InMemoryFeatureFlagStore, RedisFeatureFlagStore, ScenarioFeatureFlagAdapter
from .rollouts import RolloutEvaluator, get_rollout_evaluator
from .analytics import get_analytics_collector


class FeatureFlagManager:
    """Manages feature flags and their evaluation."""

    def __init__(self, store: FeatureFlagStore, cache_manager: Optional[CacheManager] = None):
        self.store = store
        self.cache_manager = cache_manager
        self._flag_cache: Dict[str, FeatureFlag] = {}
        self._cache_ttl = 300  # 5 minutes
        self._subscription_task = None
        self._redis_client = None
        self._config_subscriber = None

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
        start_time = time.perf_counter()
        analytics_collector = get_analytics_collector()

        flag = await self.get_flag(flag_key)
        if not flag:
            # Record that flag was not found
            await analytics_collector.record_flag_evaluation(
                flag_key, False, user_context, time.perf_counter() - start_time
            )
            return False

        # Check rollout plan first
        rollout_evaluator = get_rollout_evaluator()
        rollout_blocked = not rollout_evaluator.evaluate_with_rollout(flag_key, user_context)

        if rollout_blocked:
            # Record dependency/rollout block
            await analytics_collector.record_dependency_block(
                flag_key, ["rollout_plan"], user_context, time.perf_counter() - start_time
            )
            return False

        # Evaluate the flag
        result = flag.evaluate(user_context, feature_context)

        # Record the evaluation
        is_enabled = result is True or result == "control" or result.startswith("variant_")
        await analytics_collector.record_flag_evaluation(
            flag_key, is_enabled, user_context, time.perf_counter() - start_time
        )

        # Record A/B test exposure if applicable
        if isinstance(result, str) and flag.ab_test_config:
            await analytics_collector.record_ab_test_exposure(
                flag_key, result, user_context, time.perf_counter() - start_time
            )

        return result

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

        # Get rollout plan statistics
        rollout_evaluator = get_rollout_evaluator()
        rollout_plans = await rollout_evaluator.list_rollout_plans()
        rollout_stats = {}
        for flag_key, plan in rollout_plans.items():
            rollout_stats[flag_key] = await rollout_evaluator.get_rollout_stats(flag_key)

        # Get usage statistics from cache if available
        usage_stats = {
            "total_flags": total_flags,
            "status_distribution": status_counts,
            "ab_test_flags": sum(1 for f in flags if f.ab_test_config),
            "rules_based_flags": sum(1 for f in flags if f.rules),
            "rollout_plans": len(rollout_plans),
            "rollout_stats": rollout_stats,
        }

        return usage_stats

    # Rollout management methods
    async def create_rollout_plan(
        self,
        flag_key: str,
        strategy: str,
        name: str = "",
        description: str = "",
        created_by: str = "system",
        **kwargs
    ) -> bool:
        """Create a rollout plan for a feature flag."""
        from .rollouts import RolloutStrategy, RolloutPlan

        try:
            plan = RolloutPlan(
                strategy=RolloutStrategy(strategy),
                name=name,
                description=description,
                created_by=created_by,
                **kwargs
            )

            rollout_evaluator = get_rollout_evaluator()
            await rollout_evaluator.set_rollout_plan(flag_key, plan)
            return True
        except Exception:
            return False

    async def get_rollout_plan(self, flag_key: str) -> Optional[Dict[str, Any]]:
        """Get the rollout plan for a feature flag."""
        rollout_evaluator = get_rollout_evaluator()
        plan = await rollout_evaluator.get_rollout_plan(flag_key)
        if plan:
            return plan.to_dict()
        return None

    async def update_rollout_plan(
        self,
        flag_key: str,
        **kwargs
    ) -> bool:
        """Update the rollout plan for a feature flag."""
        rollout_evaluator = get_rollout_evaluator()
        plan = await rollout_evaluator.get_rollout_plan(flag_key)
        if not plan:
            return False

        # Update the plan with new values
        for key, value in kwargs.items():
            if hasattr(plan, key):
                setattr(plan, key, value)

        plan.updated_at = datetime.utcnow()
        await rollout_evaluator.set_rollout_plan(flag_key, plan)
        return True

    async def delete_rollout_plan(self, flag_key: str) -> bool:
        """Delete the rollout plan for a feature flag."""
        rollout_evaluator = get_rollout_evaluator()
        await rollout_evaluator.delete_rollout_plan(flag_key)
        return True

    # Redis subscription and cache invalidation
    async def start_redis_subscription(self) -> None:
        """Start Redis pub/sub subscription for cache invalidation."""
        if not isinstance(self.store, RedisFeatureFlagStore):
            return  # Only applicable for Redis stores

        try:
            redis_client = await self.store._get_redis_client()
            self._redis_client = redis_client

            # Start subscription in background
            self._subscription_task = asyncio.create_task(self._subscribe_to_updates())
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to start Redis subscription: {e}")

    async def stop_redis_subscription(self) -> None:
        """Stop Redis pub/sub subscription."""
        if self._subscription_task:
            self._subscription_task.cancel()
            try:
                await self._subscription_task
            except asyncio.CancelledError:
                pass
            self._subscription_task = None

    async def _subscribe_to_updates(self) -> None:
        """Background task to subscribe to Redis updates and invalidate caches."""
        if not self._redis_client:
            return

        try:
            pubsub = self._redis_client.pubsub()
            await pubsub.subscribe("feature_flags:updates")

            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        update_data = json.loads(message["data"])
                        flag_key = update_data.get("flag_key")
                        action = update_data.get("action")

                        if flag_key:
                            # Invalidate local cache
                            self._flag_cache.pop(flag_key, None)

                            # Invalidate user flag caches
                            if self.cache_manager:
                                await self.cache_manager.invalidate_pattern("user_flags:*")

                            import logging
                            logger = logging.getLogger(__name__)
                            logger.info(f"Invalidated cache for flag {flag_key} due to {action} update")
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f"Failed to process Redis update message: {e}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Redis subscription error: {e}")

    # Analytics methods
    async def get_flag_analytics(self, flag_key: str, hours: int = 24) -> Dict[str, Any]:
        """Get analytics for a specific feature flag."""
        analytics_collector = get_analytics_collector()
        return await analytics_collector.get_flag_analytics(flag_key, hours)

    async def get_all_flags_analytics(self, hours: int = 24) -> Dict[str, Any]:
        """Get analytics for all feature flags."""
        analytics_collector = get_analytics_collector()
        return await analytics_collector.get_all_flags_analytics(hours)

    async def get_ab_test_analytics(self, flag_key: str, hours: int = 24) -> Dict[str, Any]:
        """Get A/B test analytics for a specific flag."""
        analytics_collector = get_analytics_collector()
        return await analytics_collector.get_ab_test_analytics(flag_key, hours)

    async def subscribe_to_config_changes(self) -> None:
        """Subscribe to configuration changes for feature flag defaults."""
        try:
            from aurum.config.dynamic_config import DynamicConfigService

            # Get the dynamic config service
            config_service = DynamicConfigService()

            # Subscribe to configuration changes
            def on_config_change(snapshot):
                """Handle configuration changes."""
                asyncio.create_task(self._update_feature_flag_defaults(snapshot.config))

            config_service.subscribe(on_config_change)
            self._config_subscriber = on_config_change

            # Initial update of defaults
            current_config = config_service.get()
            await self._update_feature_flag_defaults(current_config)

        except Exception as e:
            logger.warning(f"Failed to subscribe to config changes for feature flags: {e}")

    async def _update_feature_flag_defaults(self, config: Dict[str, Any]) -> None:
        """Update feature flag defaults from configuration."""
        try:
            feature_flags_config = config.get("feature_flags", {})

            # Update cache TTL from configuration
            if "cache_ttl_seconds" in feature_flags_config:
                self._cache_ttl = feature_flags_config["cache_ttl_seconds"]

            # Update cache enabled setting
            cache_enabled = feature_flags_config.get("cache_enabled", True)

            # Get feature flag overrides from configuration
            overrides = feature_flags_config.get("overrides", {})

            # Apply overrides to existing flags if they exist
            for flag_key, enabled in overrides.items():
                try:
                    flag = await self.get_flag(flag_key)
                    if flag:
                        # Create a temporary override flag
                        from .models import FeatureFlagStatus
                        override_flag = FeatureFlag(
                            name=flag.name,
                            key=flag.key,
                            description=flag.description,
                            status=FeatureFlagStatus.ENABLED if enabled else FeatureFlagStatus.DISABLED,
                            default_value=enabled,
                            rules=flag.rules,
                            ab_test_config=flag.ab_test_config,
                            created_at=flag.created_at,
                            updated_at=datetime.utcnow(),
                            created_by=flag.created_by,
                            tags=flag.tags
                        )

                        # Update the flag in cache
                        self._flag_cache[flag_key] = override_flag

                        logger.info(f"Applied config override for feature flag '{flag_key}': {enabled}")

                except Exception as e:
                    logger.warning(f"Failed to apply override for feature flag '{flag_key}': {e}")

            logger.debug("Updated feature flag defaults from configuration")

        except Exception as e:
            logger.error(f"Failed to update feature flag defaults from configuration: {e}")


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

        # Start Redis subscription if using Redis store
        await _feature_manager.start_redis_subscription()

        # Subscribe to configuration changes for feature flag defaults
        await _feature_manager.subscribe_to_config_changes()

    return _feature_manager
