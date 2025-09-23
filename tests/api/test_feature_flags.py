from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from aurum.api.features.feature_flags import (
    FeatureFlag,
    FeatureFlagManager,
    FeatureFlagRule,
    FeatureFlagStatus,
    InMemoryFeatureFlagStore,
    RedisFeatureFlagStore,
    UserSegment,
    ABTestConfiguration,
)


class TestFeatureFlagRule:
    """Test FeatureFlagRule evaluation logic."""

    def test_rule_evaluation_basic(self):
        """Test basic rule evaluation without conditions."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={},
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        context = {"user_id": "user123", "user_segment": "all_users"}
        assert rule.evaluate(context) is True

    def test_rule_evaluation_with_conditions(self):
        """Test rule evaluation with conditions."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={
                "user.role": {"op": "eq", "value": "admin"},
                "tenant.plan": {"op": "in", "value": ["premium", "enterprise"]}
            },
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        # Matching context
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {"role": "admin"},
            "tenant": {"plan": "premium"}
        }
        assert rule.evaluate(context) is True

        # Non-matching context
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {"role": "user"},  # Wrong role
            "tenant": {"plan": "premium"}
        }
        assert rule.evaluate(context) is False

    def test_rule_evaluation_operators(self):
        """Test various condition operators."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={
                "user.age": {"op": "gte", "value": 18},
                "user.name": {"op": "startswith", "value": "John"},
                "user.tags": {"op": "in", "value": ["beta"]}
            },
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {
                "age": 25,
                "name": "John Doe",
                "tags": ["beta", "new"]
            }
        }
        assert rule.evaluate(context) is True

        # Test failure case
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {
                "age": 15,  # Too young
                "name": "John Doe",
                "tags": ["beta", "new"]
            }
        }
        assert rule.evaluate(context) is False

    def test_rule_evaluation_rollout_percentage(self):
        """Test rollout percentage logic."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={},
            rollout_percentage=50.0,  # 50% rollout
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        # Test with same user ID - should be deterministic
        context1 = {"user_id": "user123", "user_segment": "all_users"}
        context2 = {"user_id": "user456", "user_segment": "all_users"}

        # Run multiple times to ensure deterministic behavior
        for _ in range(10):
            result1 = rule.evaluate(context1)
            result2 = rule.evaluate(context2)
            assert result1 == rule.evaluate(context1)  # Same result for same user
            assert result2 == rule.evaluate(context2)  # Same result for same user

    def test_rule_evaluation_required_excluded_flags(self):
        """Test required and excluded flags logic."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={},
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=["feature_a", "feature_b"],
            excluded_flags=["feature_c"]
        )

        # Missing required flag
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "flag_feature_a": True,
            "flag_feature_b": False,  # Missing
            "flag_feature_c": False
        }
        assert rule.evaluate(context) is False

        # Has excluded flag
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "flag_feature_a": True,
            "flag_feature_b": True,
            "flag_feature_c": True  # Excluded
        }
        assert rule.evaluate(context) is False

        # Valid context
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "flag_feature_a": True,
            "flag_feature_b": True,
            "flag_feature_c": False
        }
        assert rule.evaluate(context) is True


class TestFeatureFlag:
    """Test FeatureFlag evaluation logic."""

    def test_feature_flag_basic_evaluation(self):
        """Test basic feature flag evaluation."""
        flag = FeatureFlag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.ENABLED,
            default_value=False,
            rules=[]
        )

        context = {"user_id": "user123", "user_segment": "all_users"}
        assert flag.evaluate(context, {}) is True
        assert flag.is_enabled_for_user(context, {}) is True

    def test_feature_flag_disabled(self):
        """Test disabled feature flag."""
        flag = FeatureFlag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.DISABLED,
            default_value=True,
            rules=[]
        )

        context = {"user_id": "user123", "user_segment": "all_users"}
        assert flag.evaluate(context, {}) is False
        assert flag.is_enabled_for_user(context, {}) is False

    def test_feature_flag_with_rules(self):
        """Test feature flag with rules."""
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={"user.role": "admin"},
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        flag = FeatureFlag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.CONDITIONAL,
            default_value=False,
            rules=[rule]
        )

        # User matches rule
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {"role": "admin"}
        }
        assert flag.evaluate(context, {}) is True

        # User doesn't match rule
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {"role": "user"}
        }
        assert flag.evaluate(context, {}) is False

    def test_feature_flag_ab_test(self):
        """Test A/B test functionality."""
        ab_config = ABTestConfiguration(
            variants={"control": 50.0, "variant_a": 50.0},
            control_variant="control"
        )

        flag = FeatureFlag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.ENABLED,
            default_value=False,
            rules=[],
            ab_test_config=ab_config
        )

        context = {"user_id": "user123", "user_segment": "all_users"}

        # Should return a variant, not a boolean
        result = flag.evaluate(context, {})
        assert result in ["control", "variant_a"]
        assert isinstance(result, str)


class TestFeatureFlagManager:
    """Test FeatureFlagManager functionality."""

    @pytest.fixture
    def store(self):
        return InMemoryFeatureFlagStore()

    @pytest.fixture
    def manager(self, store):
        return FeatureFlagManager(store)

    @pytest.mark.asyncio
    async def test_create_flag(self, manager):
        """Test creating a feature flag."""
        flag = await manager.create_flag(
            name="Test Feature",
            key="test_feature",
            description="A test feature",
            default_value=False,
            status=FeatureFlagStatus.ENABLED,
            created_by="test_user"
        )

        assert flag.name == "Test Feature"
        assert flag.key == "test_feature"
        assert flag.status == FeatureFlagStatus.ENABLED
        assert flag.created_by == "test_user"

    @pytest.mark.asyncio
    async def test_get_flag(self, manager):
        """Test getting a feature flag."""
        # Create a flag
        created_flag = await manager.create_flag(
            name="Test Feature",
            key="test_feature",
            created_by="test_user"
        )

        # Retrieve it
        retrieved_flag = await manager.get_flag("test_feature")
        assert retrieved_flag is not None
        assert retrieved_flag.key == created_flag.key
        assert retrieved_flag.name == created_flag.name

    @pytest.mark.asyncio
    async def test_list_flags(self, manager):
        """Test listing feature flags."""
        # Create multiple flags
        await manager.create_flag("Feature 1", "feature_1", created_by="test_user")
        await manager.create_flag("Feature 2", "feature_2", created_by="test_user")

        flags = await manager.list_flags()
        assert len(flags) == 2
        assert all(f.key in ["feature_1", "feature_2"] for f in flags)

    @pytest.mark.asyncio
    async def test_evaluate_flag(self, manager):
        """Test evaluating a feature flag."""
        # Create a flag with a rule
        rule = FeatureFlagRule(
            name="Test Rule",
            conditions={"user.role": "admin"},
            rollout_percentage=100.0,
            user_segments=[],
            required_flags=[],
            excluded_flags=[]
        )

        flag = await manager.create_flag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.CONDITIONAL,
            created_by="test_user"
        )

        await manager.add_rule("test_feature", rule)

        # Evaluate for admin user
        context = {
            "user_id": "user123",
            "user_segment": "all_users",
            "user": {"role": "admin"}
        }

        result = await manager.evaluate_flag("test_feature", context, {})
        assert result is True

    @pytest.mark.asyncio
    async def test_feature_stats(self, manager):
        """Test getting feature statistics."""
        # Create some flags
        await manager.create_flag("Feature 1", "feature_1", status=FeatureFlagStatus.ENABLED, created_by="test_user")
        await manager.create_flag("Feature 2", "feature_2", status=FeatureFlagStatus.DISABLED, created_by="test_user")

        stats = await manager.get_feature_stats()
        assert stats["total_flags"] == 2
        assert stats["status_distribution"]["enabled"] == 1
        assert stats["status_distribution"]["disabled"] == 1


class TestRedisFeatureFlagStore:
    """Test RedisFeatureFlagStore functionality."""

    @pytest.fixture
    def redis_store(self):
        # Mock Redis for testing
        store = RedisFeatureFlagStore("redis://localhost:6379")
        store._redis_client = AsyncMock()
        return store

    @pytest.mark.asyncio
    async def test_set_and_get_flag(self, redis_store):
        """Test setting and getting a feature flag."""
        flag = FeatureFlag(
            name="Test Feature",
            key="test_feature",
            status=FeatureFlagStatus.ENABLED,
            created_by="test_user"
        )

        # Mock Redis responses
        redis_store._redis_client.set = AsyncMock()
        redis_store._redis_client.get = AsyncMock(return_value='{"key": "test_feature", "name": "Test Feature", "status": "enabled", "created_by": "test_user"}')

        await redis_store.set_flag(flag)
        redis_store._redis_client.set.assert_called_once()

        retrieved_flag = await redis_store.get_flag("test_feature")
        assert retrieved_flag is not None
        redis_store._redis_client.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_flags(self, redis_store):
        """Test listing feature flags."""
        # Mock Redis responses
        redis_store._redis_client.keys = AsyncMock(return_value=["feature_flags:test_feature1", "feature_flags:test_feature2"])
        redis_store._redis_client.get = AsyncMock(return_value='{"key": "test_feature1", "name": "Test Feature 1", "status": "enabled"}')

        flags = await redis_store.list_flags()
        assert len(flags) > 0
        redis_store._redis_client.keys.assert_called_once()


class TestScenarioFeatureFlagAdapter:
    """Test ScenarioFeatureFlagAdapter functionality."""

    @pytest.fixture
    def mock_scenario_store(self):
        store = AsyncMock()
        store.get_feature_flag = AsyncMock(return_value={
            "enabled": True,
            "configuration": {},
            "created_at": None,
            "updated_at": None
        })
        store.set_feature_flag = AsyncMock(return_value={})
        return store

    @pytest.fixture
    def adapter(self, mock_scenario_store):
        generic_store = InMemoryFeatureFlagStore()
        return ScenarioFeatureFlagAdapter(generic_store, mock_scenario_store)

    @pytest.mark.asyncio
    async def test_scenario_flag_get(self, adapter, mock_scenario_store):
        """Test getting a scenario feature flag."""
        flag = await adapter.get_flag("scenario:test_feature")
        assert flag is not None
        assert flag.key == "scenario:test_feature"
        assert "scenario" in flag.tags

        mock_scenario_store.get_feature_flag.assert_called_once_with("test_feature")

    @pytest.mark.asyncio
    async def test_scenario_flag_set(self, adapter, mock_scenario_store):
        """Test setting a scenario feature flag."""
        flag = FeatureFlag(
            name="Scenario Test Feature",
            key="scenario:test_feature",
            status=FeatureFlagStatus.ENABLED,
            created_by="test_user"
        )

        await adapter.set_flag(flag)
        mock_scenario_store.set_feature_flag.assert_called_once_with(
            feature_name="test_feature",
            enabled=True,
            configuration={}
        )

    @pytest.mark.asyncio
    async def test_generic_flag_operations(self, adapter, mock_scenario_store):
        """Test that generic flags still work through the adapter."""
        # Create a generic flag
        flag = FeatureFlag(
            name="Generic Feature",
            key="generic_feature",
            status=FeatureFlagStatus.ENABLED,
            created_by="test_user"
        )

        await adapter.set_flag(flag)
        retrieved_flag = await adapter.get_flag("generic_feature")

        assert retrieved_flag is not None
        assert retrieved_flag.key == "generic_feature"
        # Scenario store should not be called for generic flags
        mock_scenario_store.get_feature_flag.assert_called_once()  # Only called for scenario flag test
