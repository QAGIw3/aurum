# Feature Flags System

This document describes the comprehensive feature flags system in Aurum, which provides gradual rollouts, A/B testing, feature targeting, analytics, and monitoring capabilities.

## Overview

The feature flags system allows you to:
- **Gradual Rollouts**: Deploy features incrementally using percentage-based or time-based strategies
- **A/B Testing**: Run experiments with different variants and measure their impact
- **User Targeting**: Target specific user segments, roles, or custom conditions
- **Runtime Updates**: Update flag configurations without deployments via Redis Pub/Sub
- **Analytics & Monitoring**: Track usage patterns, performance, and rollout progress
- **Testing Utilities**: Comprehensive test helpers for deterministic testing

## Architecture

### Core Components

- **FeatureFlag**: Core model representing a feature flag with rules, A/B config, and metadata
- **FeatureFlagRule**: Rules for conditional feature activation with conditions and targeting
- **FeatureFlagManager**: Main interface for managing and evaluating feature flags
- **FeatureFlagStore**: Storage abstraction (Redis, In-Memory, or unified adapter)
- **RolloutPlan**: Advanced rollout strategies with scheduling and targeting
- **RolloutEvaluator**: Evaluates rollout plans and integrates with flag evaluation
- **FeatureAnalyticsCollector**: Collects and aggregates usage analytics
- **ScenarioFeatureFlagAdapter**: Bridges scenario-specific flags with the generic system

### Storage Options

1. **InMemoryFeatureFlagStore**: For development and testing
2. **RedisFeatureFlagStore**: For production with Redis backend and Pub/Sub
3. **ScenarioFeatureFlagAdapter**: Unified store that handles both generic and scenario-specific flags

### Evaluation Flow

```
User Request → FeatureFlagManager → RolloutPlan (if exists) → FeatureFlagRules → Default Value
                                ↓
                         FeatureAnalyticsCollector (records event)
                                ↓
                         Cache (user_flags:*, flag_cache)
                                ↓
                         Store (Redis/InMemory)
```

### Runtime Updates

```
Flag Update → Redis Store → Redis Pub/Sub → All Managers → Cache Invalidation → Consistent State
```

## Basic Usage

### Public API (Recommended)

```python
from aurum.api.features import is_enabled, get_variant, evaluate_flag

# Check if feature is enabled
user_context = {
    "user_id": "user123",
    "user_segment": "premium_users",
    "user": {"role": "admin"}
}

enabled = await is_enabled("new_dashboard_widget", user_context)
if enabled:
    show_dashboard_widget()

# Get A/B test variant
variant = await get_variant("button_color", user_context)
if variant == "blue":
    show_blue_button()
elif variant == "green":
    show_green_button()

# Raw evaluation (returns bool or variant string)
result = await evaluate_flag("feature_with_ab_test", user_context)
```

### Manager API (Advanced)

```python
from aurum.api.features import get_feature_manager, FeatureFlagStatus

manager = get_feature_manager()

# Create a feature flag
flag = await manager.create_flag(
    name="New Dashboard Widget",
    key="dashboard_widget",
    description="Shows a new widget on the dashboard",
    default_value=False,
    status=FeatureFlagStatus.ENABLED,
    created_by="user@example.com"
)

# Evaluate with context
is_enabled = await manager.is_enabled(
    "dashboard_widget",
    user_context,
    {}
)
```

## Advanced Features

### Rollout Plans

Rollout plans provide sophisticated deployment strategies beyond simple percentage rollouts.

#### Percentage-Based Rollout

```python
from aurum.api.features import RolloutPlan, RolloutStrategy

# 25% rollout to all users
plan = RolloutPlan(
    strategy=RolloutStrategy.PERCENTAGE,
    percentage=25.0,
    name="Gradual Feature Rollout"
)

await manager.create_rollout_plan("new_feature", plan.strategy.value, **plan.to_dict())
```

#### Gradual Rollout (Time-Based)

```python
from datetime import datetime, timedelta

# Start at 10%, increase by 20% every 24 hours, up to 100%
plan = RolloutPlan(
    strategy=RolloutStrategy.GRADUAL,
    start_percentage=10.0,
    end_percentage=100.0,
    step_percentage=20.0,
    step_interval_hours=24,
    start_time=datetime.utcnow(),
    name="Gradual Rollout"
)
```

#### Scheduled Rollout

```python
# Roll out during business hours only
plan = RolloutPlan(
    strategy=RolloutStrategy.SCHEDULED,
    start_time=datetime(2024, 1, 1, 9, 0),  # 9 AM
    end_time=datetime(2024, 1, 1, 17, 0),   # 5 PM
    name="Business Hours Rollout"
)
```

#### Targeted Rollout

```python
from aurum.api.features import UserSegment

# Only premium users
plan = RolloutPlan(
    strategy=RolloutStrategy.TARGETED,
    user_segments=[UserSegment.PREMIUM_USERS],
    required_flags=["payment_enabled"],
    name="Premium Feature Rollout"
)
```

### Rules and Conditions

Rules provide sophisticated targeting based on user context, tenant information, and custom conditions.

```python
from aurum.api.features import FeatureFlagRule

# Create a rule with complex conditions
rule = FeatureFlagRule(
    name="Premium Users with Enterprise Plan",
    conditions={
        "user.role": {"op": "eq", "value": "premium"},
        "tenant.plan": {"op": "in", "value": ["enterprise", "premium_plus"]},
        "user.created_at": {"op": "gte", "value": "2023-01-01"},
        "request.feature_context.beta_tester": {"op": "eq", "value": True}
    },
    rollout_percentage=50.0,  # 50% of matching users
    user_segments=[UserSegment.PREMIUM_USERS],
    required_flags=["core_functionality"],
    excluded_flags=["legacy_mode"]
)

await manager.add_rule("advanced_feature", rule)
```

#### Available Operators

- `eq`: Equal
- `neq`: Not equal
- `gt`: Greater than
- `gte`: Greater than or equal
- `lt`: Less than
- `lte`: Less than or equal
- `in`: In list
- `nin`: Not in list
- `contains`: String contains
- `startswith`: String starts with
- `endswith`: String ends with
- `regex`: Regular expression match

### A/B Testing

```python
from aurum.api.features import ABTestConfiguration

# Configure A/B test with multiple variants
ab_config = ABTestConfiguration(
    variants={"control": 50.0, "variant_a": 25.0, "variant_b": 25.0},
    control_variant="control",
    track_events=["button_click", "page_view", "conversion"],
    end_date=datetime(2024, 6, 1)  # Optional end date
)

await manager.set_ab_test("button_design", ab_config)

# Check variant for user
variant = await manager.get_variant("button_design", user_context)
```

## Analytics and Monitoring

### Viewing Analytics

```python
# Get analytics for a specific flag
analytics = await manager.get_flag_analytics("new_feature", hours=24)
print(f"Total evaluations: {analytics['total_evaluations']}")
print(f"Enable rate: {analytics['enable_rate']:.2%}")
print(f"Average decision time: {analytics['avg_decision_time_ms']:.2f}ms")

# Get A/B test analytics
ab_analytics = await manager.get_ab_test_analytics("button_design", hours=24)
print(f"Variant distribution: {ab_analytics['variant_distribution']}")

# Get all flags analytics
all_analytics = await manager.get_all_flags_analytics(hours=24)
```

### Admin Endpoints

#### Rollout Management
```bash
# Create rollout plan
POST /v1/admin/features/{flag_key}/rollout
{
  "strategy": "gradual",
  "name": "Gradual Feature Rollout",
  "start_percentage": 10.0,
  "end_percentage": 100.0,
  "step_percentage": 20.0,
  "step_interval_hours": 24
}

# Get rollout plan
GET /v1/admin/features/{flag_key}/rollout

# Update rollout plan
PUT /v1/admin/features/{flag_key}/rollout
{
  "percentage": 75.0
}

# Delete rollout plan
DELETE /v1/admin/features/{flag_key}/rollout
```

#### Analytics Endpoints
```bash
# Get all flags analytics
GET /v1/admin/features/analytics?hours=24

# Get specific flag analytics
GET /v1/admin/features/{flag_key}/analytics?hours=24

# Get A/B test analytics
GET /v1/admin/features/{flag_key}/ab-analytics?hours=24
```

## Testing

### Test Utilities

```python
import pytest
from tests.common.feature_flags import (
    FlagOverride,
    feature_flag_override,
    create_test_flag,
    create_test_rollout_plan,
    setup_test_feature_flags
)

class TestMyFeature:
    @pytest.mark.asyncio
    async def test_feature_enabled(self):
        # Override flag for test
        async with FlagOverride("my_feature", True):
            result = await is_enabled("my_feature")
            assert result is True

    @pytest.mark.asyncio
    async def test_feature_disabled(self):
        async with feature_flag_override("my_feature", False):
            result = await is_enabled("my_feature")
            assert result is False

    @pytest.mark.asyncio
    async def test_ab_test_variant(self):
        async with FlagOverride("ab_feature", "variant_a"):
            variant = await get_variant("ab_feature")
            assert variant == "variant_a"

    @pytest.mark.asyncio
    async def test_rollout_plan(self):
        # Create test rollout plan
        plan = create_test_rollout_plan(
            "test_flag",
            strategy=RolloutStrategy.PERCENTAGE,
            percentage=50.0
        )

        # Set up test flags
        manager = await setup_test_feature_flags(
            flags=[create_test_flag("test_flag")],
            rollout_plans={"test_flag": plan}
        )

        # Test evaluation respects rollout
        user_context = {"user_id": "user123"}
        result = await manager.evaluate_flag("test_flag", user_context, {})
        # Result depends on deterministic hashing
```

### Deterministic Testing

```python
# Test that the same user always gets the same variant
@pytest.mark.asyncio
async def test_deterministic_ab_test():
    user_context = {"user_id": "user123"}

    # Same user should always get same variant
    variant1 = await get_variant("ab_feature", user_context)
    variant2 = await get_variant("ab_feature", user_context)
    assert variant1 == variant2

    # Different user should get different variant
    user_context2 = {"user_id": "user456"}
    variant3 = await get_variant("ab_feature", user_context2)
    # May or may not be same as variant1 depending on hash distribution
```

## Configuration

### Environment Variables

```bash
# Redis configuration for production
AURUM_REDIS_URL=redis://localhost:6379/0

# Feature flag namespace (optional)
AURUM_FEATURE_FLAG_NAMESPACE=feature_flags

# Cache configuration
AURUM_CACHE_TTL=300
```

### Initialization

```python
from aurum.api.features import initialize_feature_flags

# Initialize with Redis for production
manager = await initialize_feature_flags(
    redis_url="redis://localhost:6379/0",
    cache_manager=cache_manager,
    scenario_store=scenario_store
)

# Or use in-memory for development/testing
manager = await initialize_feature_flags()
```

## Best Practices

### Flag Naming
- Use kebab-case: `new-dashboard-widget`
- Prefix tenant-scoped flags: `tenant:tenant-id:feature-name`
- Keep names descriptive and stable

### Rollout Strategy
1. Start with percentage-based rollouts (10-25%)
2. Use gradual rollouts for high-risk features
3. Target specific segments for controlled releases
4. Monitor analytics during rollout
5. Have rollback plans ready

### A/B Testing
1. Define clear success metrics
2. Use appropriate sample sizes
3. Run tests for sufficient duration
4. Consider statistical significance
5. Document test results

### Monitoring
1. Monitor enable/disable rates
2. Track performance impact
3. Watch for dependency issues
4. Alert on rollout anomalies
5. Review analytics regularly

## Troubleshooting

### Common Issues

**Flag not evaluating correctly**
- Check flag status (ENABLED/CONDITIONAL/DISABLED)
- Verify user context matches rules
- Check rollout plan constraints
- Review dependency requirements

**Performance issues**
- Enable caching for high-traffic flags
- Use Redis store for production
- Monitor decision time metrics
- Consider flag complexity

**Cache consistency**
- Redis Pub/Sub handles cross-process updates
- Local cache invalidation on updates
- Monitor cache hit rates

### Debug Mode

```python
import logging

# Enable debug logging
logging.getLogger('aurum.api.features').setLevel(logging.DEBUG)

# Check flag evaluation details
user_context = {"user_id": "debug_user", "user_segment": "debug"}
result = await evaluate_flag("problematic_flag", user_context)
```

## Migration Guide

### From Environment Variables

```python
# Old way
import os
if os.getenv("MY_FEATURE_ENABLED", "false").lower() == "true":
    enable_feature()

# New way
from aurum.api.features import is_enabled
enabled = await is_enabled("my_feature", {"user_id": "user123"})
if enabled:
    enable_feature()
```

### From Direct Manager Usage

```python
# Old way
manager = get_feature_manager()
enabled = await manager.is_enabled("feature", user_context, {})

# New way (recommended)
enabled = await is_enabled("feature", user_context)
```

## Operational Guide

### Runtime Updates
- Changes propagate via Redis Pub/Sub
- Cache invalidation happens automatically
- Updates are visible within seconds
- Monitor for subscription health

### Monitoring Alerts
- High dependency block rates
- Unusual enable/disable patterns
- Performance degradation
- Cache miss rates
- Rollout anomalies

### Backup and Recovery
- Flags stored in Redis (persistent)
- In-memory fallback available
- Export configurations for backup
- Test restore procedures


```python
from aurum.api.features.feature_flags import FeatureFlagManager, FeatureFlagStatus

manager = FeatureFlagManager.get_manager()

# Create a simple feature flag
flag = await manager.create_flag(
    name="New Dashboard Widget",
    key="dashboard_widget",
    description="Shows a new widget on the dashboard",
    default_value=False,
    status=FeatureFlagStatus.ENABLED,
    created_by="user@example.com"
)
```

### Evaluating a Feature Flag

```python
# Check if feature is enabled for a user
user_context = {
    "user_id": "user123",
    "user_segment": "premium_users",
    "user": {"role": "admin"}
}

is_enabled = await manager.is_enabled(
    "dashboard_widget",
    user_context,
    {}
)

if is_enabled:
    # Show the feature
    show_dashboard_widget()
```

### Using Rules with Conditions

```python
from aurum.api.features.feature_flags import FeatureFlagRule

# Create a rule with conditions
rule = FeatureFlagRule(
    name="Premium Users Only",
    conditions={
        "user.role": {"op": "eq", "value": "premium"},
        "tenant.plan": {"op": "in", "value": ["enterprise", "premium"]}
    },
    rollout_percentage=100.0,
    user_segments=[],
    required_flags=[],
    excluded_flags=[]
)

await manager.add_rule("dashboard_widget", rule)
```

## API Endpoints

### Admin Endpoints

All admin endpoints require admin privileges and are protected by the admin guard.

#### Create Feature Flag
```
POST /v1/admin/features
```

Request:
```json
{
    "name": "Dashboard Widget",
    "key": "dashboard_widget",
    "description": "New dashboard widget feature",
    "default_value": false,
    "status": "enabled",
    "tags": ["ui", "dashboard"]
}
```

#### List Feature Flags
```
GET /v1/admin/features?page=1&limit=50&status=enabled&tag=ui
```

#### Get Feature Flag
```
GET /v1/admin/features/{flag_key}
```

#### Update Feature Status
```
PUT /v1/admin/features/{flag_key}/status
```

Request:
```json
{
    "status": "disabled"
}
```

#### Add Rule to Feature
```
POST /v1/admin/features/{flag_key}/rules
```

Request:
```json
{
    "name": "Premium Users",
    "conditions": {
        "user.role": {"op": "eq", "value": "premium"}
    },
    "rollout_percentage": 100.0,
    "user_segments": ["premium_users"],
    "required_flags": [],
    "excluded_flags": []
}
```

#### Configure A/B Test
```
POST /v1/admin/features/{flag_key}/ab-test
```

Request:
```json
{
    "variants": {
        "control": 50.0,
        "variant_a": 30.0,
        "variant_b": 20.0
    },
    "control_variant": "control",
    "track_events": ["widget_view", "widget_click"],
    "end_date": "2025-12-31T23:59:59"
}
```

#### Get Feature Statistics
```
GET /v1/admin/features/stats
```

#### Evaluate Features for User
```
GET /v1/admin/features/evaluate?user_id=user123&user_segment=premium_users
```

#### Get A/B Tests
```
GET /v1/admin/features/ab-tests?active_only=true
```

## Condition Operators

The feature flag system supports various condition operators:

- `eq`: Equal to
- `neq`: Not equal to
- `gt`: Greater than
- `gte`: Greater than or equal to
- `lt`: Less than
- `lte`: Less than or equal to
- `in`: Value is in list
- `nin`: Value is not in list
- `contains`: List contains value
- `startswith`: String starts with value
- `endswith`: String ends with value
- `regex`: Matches regular expression

### Examples

```python
# Age-based targeting
conditions = {
    "user.age": {"op": "gte", "value": 18}
}

# Role-based access
conditions = {
    "user.role": {"op": "in", "value": ["admin", "moderator"]}
}

# Feature dependencies
conditions = {
    "user.subscription": {"op": "eq", "value": "premium"}
}
```

## A/B Testing

### Setting up an A/B Test

```python
from aurum.api.features.feature_flags import ABTestConfiguration

ab_config = ABTestConfiguration(
    variants={"control": 50.0, "variant_a": 30.0, "variant_b": 20.0},
    control_variant="control",
    track_events=["feature_view", "feature_click"],
    end_date=datetime(2025, 12, 31)
)

await manager.set_ab_test("new_feature", ab_config)
```

### Evaluating A/B Test Variants

```python
# Get variant for user
variant = await manager.get_variant(
    "new_feature",
    user_context,
    feature_context
)

if variant == "variant_a":
    show_variant_a()
elif variant == "variant_b":
    show_variant_b()
else:
    show_control()
```

## User Segments

Predefined user segments for targeting:

- `ALL_USERS`: All users
- `PREMIUM_USERS`: Premium subscription users
- `ENTERPRISE_USERS`: Enterprise customers
- `BETA_TESTERS`: Users in beta program
- `INTERNAL_USERS`: Internal company users
- `NEW_USERS`: Users created in last 30 days
- `POWER_USERS`: Users with high activity

## CLI Usage

The feature flag system includes a CLI for ops workflows:

```bash
# Install the CLI
pip install -e .

# List all feature flags
aurum-feature list --status enabled

# Create a new feature flag
aurum-feature create \
    --name "New Feature" \
    --key "new_feature" \
    --description "Description of the feature" \
    --status enabled

# Get feature details
aurum-feature get --key new_feature

# Update feature status
aurum-feature update --key new_feature --status disabled

# Evaluate features for a user
aurum-feature eval --user-id user123 --user-segment premium_users

# Get feature statistics
aurum-feature stats
```

## Integration Examples

### FastAPI Integration

```python
from fastapi import Depends
from aurum.api.features.feature_flags import get_feature_manager

@app.get("/api/dashboard")
async def get_dashboard(
    user_id: str,
    manager: FeatureFlagManager = Depends(get_feature_manager)
):
    user_context = {"user_id": user_id, "user_segment": "all_users"}

    if await manager.is_enabled("dashboard_widget", user_context, {}):
        return {"dashboard": "with_widget"}
    else:
        return {"dashboard": "without_widget"}
```

### Celery Task Integration

```python
from celery import shared_task
from aurum.api.features.feature_flags import get_feature_manager

@shared_task
async def process_data(user_id: str):
    manager = get_feature_manager()
    user_context = {"user_id": user_id}

    if await manager.is_enabled("advanced_processing", user_context, {}):
        # Use advanced processing
        result = advanced_process_data()
    else:
        # Use standard processing
        result = standard_process_data()

    return result
```

## Monitoring and Analytics

The feature flag system provides usage statistics:

```python
stats = await manager.get_feature_stats()
print(f"Total flags: {stats['total_flags']}")
print(f"Enabled flags: {stats['status_distribution']['enabled']}")
print(f"A/B test flags: {stats['ab_test_flags']}")
```

## Best Practices

1. **Use Descriptive Keys**: Use clear, hierarchical keys like `dashboard.widget.new_feature`
2. **Default to Disabled**: New features should default to disabled for safety
3. **Gradual Rollouts**: Use rollout percentages for safe deployments
4. **Clean Up**: Remove unused feature flags regularly
5. **Test Rules**: Always test your rules with various user contexts
6. **Monitor Performance**: Track the performance impact of feature flags
7. **Use Tags**: Tag flags for better organization and filtering

## Security Considerations

- All admin endpoints require admin privileges
- Feature flag evaluations are cached for performance
- Sensitive feature flags should use additional security measures
- Audit logs track feature flag changes
- Consider rate limiting for feature evaluation endpoints

## Troubleshooting

### Common Issues

1. **Feature not showing**: Check if the flag is enabled and rules match user context
2. **Performance issues**: Ensure proper caching configuration
3. **Redis connection errors**: Verify Redis configuration and connectivity
4. **Rule evaluation errors**: Check condition syntax and field paths

### Debug Mode

Enable debug logging to troubleshoot rule evaluation:

```python
import logging
logging.getLogger("aurum.api.features").setLevel(logging.DEBUG)
```

## Migration Guide

### From Manual Feature Flags

If you're currently using manual feature flags, here's how to migrate:

1. Create feature flags for existing features
2. Set appropriate default states
3. Add rules to match current behavior
4. Update code to use feature flag manager
5. Remove old manual flag logic
6. Test thoroughly before removing old code

### From Other Feature Flag Systems

The system is designed to be compatible with common patterns:

- Conditions use standard operators
- Rules support common targeting scenarios
- Storage abstraction allows easy migration
- API follows REST conventions
