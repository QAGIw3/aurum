# Feature Flags System

This document describes the feature flags system in Aurum, which provides gradual rollouts, A/B testing, and feature targeting capabilities.

## Overview

The feature flags system allows you to:
- Gradually roll out new features to users
- Run A/B tests with different variants
- Target specific user segments or conditions
- Enable/disable features dynamically without deployments
- Monitor feature usage and performance

## Architecture

### Components

- **FeatureFlag**: Core model representing a feature flag
- **FeatureFlagRule**: Rules for conditional feature activation
- **FeatureFlagManager**: Main interface for managing feature flags
- **FeatureFlagStore**: Storage abstraction (Redis, In-Memory, or unified adapter)
- **ScenarioFeatureFlagAdapter**: Bridges scenario-specific flags with the generic system

### Storage Options

1. **InMemoryFeatureFlagStore**: For development and testing
2. **RedisFeatureFlagStore**: For production with Redis backend
3. **ScenarioFeatureFlagAdapter**: Unified store that handles both generic and scenario-specific flags

## Basic Usage

### Creating a Feature Flag

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
