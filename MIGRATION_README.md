# Aurum Refactoring Migration Guide

This guide explains how to use the feature-flagged refactoring system for the gradual migration from legacy to simplified components with comprehensive monitoring and rollback capabilities.

## Overview

The Aurum codebase has been refactored to support incremental migration with:
- **Feature Flags**: Enable/disable new components without code changes
- **Hybrid Systems**: Run both legacy and simplified systems simultaneously
- **Comprehensive Monitoring**: Track performance, errors, and migration health
- **Safe Rollback**: Quick reversion capabilities at any point

## Architecture

### Migration Phases

1. **Legacy Phase**: Original complex implementation
2. **Hybrid Phase**: Both legacy and simplified systems available
3. **Simplified Phase**: Only simplified systems active

### Components Migrated

- ✅ **Settings System**: Complex 900+ line system → Clean 130+ line system
- ✅ **Database Layer**: Complex Trino client → Simple client
- 🔄 **API Factory**: Simplified middleware and routing
- 🔄 **Dependency Injection**: Enhanced container system

## Quick Start

### 1. Enable Migration Monitoring

```bash
export AURUM_ENABLE_MIGRATION_MONITORING=1
export AURUM_USE_SIMPLIFIED_SETTINGS=1
export AURUM_USE_SIMPLE_DB_CLIENT=1
```

### 2. Run Migration Demo

```bash
# Full migration demo
python migration_demo.py

# Test specific phases
python migration_demo.py setup      # Setup environment
python migration_demo.py test-settings  # Test settings migration
python migration_demo.py test-database  # Test database migration
python migration_demo.py advance    # Advance to hybrid phase
python migration_demo.py monitor    # Show migration status
python migration_demo.py health     # Validate health
python migration_demo.py rollback   # Rollback to legacy
```

### 3. Check Migration Status

```python
from src.aurum.core.settings import get_migration_metrics, log_migration_status

# Log current status
log_migration_status()

# Get detailed metrics
metrics = get_migration_metrics()
status = metrics.get_migration_status()
print(f"Settings phase: {status['settings_phase']}")
print(f"Database phase: {status['database_phase']}")
```

## Feature Flags

### Settings Migration Flags

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| Enable Simplified Settings | `AURUM_USE_SIMPLIFIED_SETTINGS=1` | Use new simplified settings system |
| Migration Phase | `AURUM_SETTINGS_MIGRATION_PHASE=hybrid` | Current migration phase |
| Enable Monitoring | `AURUM_ENABLE_MIGRATION_MONITORING=1` | Track migration metrics |

### Database Migration Flags

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| Enable Simple DB Client | `AURUM_USE_SIMPLE_DB_CLIENT=1` | Use simplified Trino client |
| Migration Phase | `AURUM_DB_MIGRATION_PHASE=hybrid` | Current migration phase |
| Enable Monitoring | `AURUM_ENABLE_DB_MIGRATION_MONITORING=1` | Track database metrics |

## Migration Commands

### Settings Migration

```python
from src.aurum.core.settings import (
    AurumSettings,
    advance_migration_phase,
    rollback_migration_phase,
    validate_migration_health
)

# Create settings with current migration phase
settings = AurumSettings.from_env()

# Advance to hybrid phase
success = advance_migration_phase("settings", "hybrid")

# Check if migration is healthy
health = validate_migration_health()
if not health["healthy"]:
    print(f"Issues: {health['issues']}")

# Rollback if needed
rollback_migration_phase("settings")
```

### Database Migration

```python
from src.aurum.api.database.trino_client import (
    get_trino_client,
    advance_db_migration_phase,
    rollback_db_migration_phase,
    get_db_migration_status
)

# Get client (automatically uses appropriate type based on flags)
client = get_trino_client("iceberg")

# Advance database migration
advance_db_migration_phase("hybrid")

# Check status
status = get_db_migration_status()
print(f"Using simple client: {status['using_simple']}")

# Rollback if needed
rollback_db_migration_phase()
```

## Monitoring & Metrics

### Migration Metrics File

Metrics are stored in: `~/.aurum/migration_metrics.json`

```json
{
  "settings_migration": {
    "legacy_calls": 150,
    "simplified_calls": 50,
    "errors": 2,
    "performance_ms": [45.2, 23.1, 67.8],
    "migration_phase": "hybrid"
  },
  "database_migration": {
    "legacy_calls": 25,
    "simplified_calls": 75,
    "errors": 0,
    "performance_ms": [12.3, 8.7, 15.2],
    "migration_phase": "hybrid"
  }
}
```

### Health Validation

```python
from src.aurum.core.settings import validate_migration_health

health = validate_migration_health()
if health["healthy"]:
    print("✅ Migration is healthy")
else:
    print("⚠️ Issues found:")
    for issue in health["issues"]:
        print(f"  - {issue}")
```

### Performance Monitoring

The system tracks performance metrics for both legacy and simplified systems:

- **Response Times**: Average, min, max for each system
- **Error Rates**: Track failures in each system
- **Usage Ratios**: Percentage of traffic using simplified systems
- **Health Indicators**: Overall system stability

## Rollback Procedures

### Automatic Rollback

The system includes automatic rollback capabilities:

```python
# Check if rollback is needed
health = validate_migration_health()
if not health["healthy"]:
    # Auto-rollback to legacy
    rollback_migration_phase("settings")
    rollback_db_migration_phase()
```

### Manual Rollback

```bash
# Rollback settings
export AURUM_USE_SIMPLIFIED_SETTINGS=0
export AURUM_SETTINGS_MIGRATION_PHASE=legacy

# Rollback database
export AURUM_USE_SIMPLE_DB_CLIENT=0
export AURUM_DB_MIGRATION_PHASE=legacy

# Restart services
```

### Gradual Rollback

```python
# Rollback in phases
advance_migration_phase("settings", "legacy")  # Full rollback
advance_migration_phase("settings", "hybrid")  # Partial rollback
```

## Best Practices

### 1. Gradual Migration

```bash
# Phase 1: Enable monitoring only
export AURUM_ENABLE_MIGRATION_MONITORING=1

# Phase 2: Test simplified systems
export AURUM_USE_SIMPLIFIED_SETTINGS=1
export AURUM_USE_SIMPLE_DB_CLIENT=1

# Phase 3: Hybrid operation
export AURUM_SETTINGS_MIGRATION_PHASE=hybrid
export AURUM_DB_MIGRATION_PHASE=hybrid

# Phase 4: Full migration
export AURUM_SETTINGS_MIGRATION_PHASE=simplified
export AURUM_DB_MIGRATION_PHASE=simplified
```

### 2. Monitor Before Advancing

```python
# Check metrics before advancing
metrics = get_migration_metrics()
settings_ratio = metrics._get_simplified_ratio("settings_migration")

if settings_ratio > 0.8:  # 80% of traffic on simplified
    advance_migration_phase("settings", "simplified")
```

### 3. Use Feature Flags for Testing

```python
# Enable for specific users/components
if user_id in test_users:
    os.environ["AURUM_USE_SIMPLIFIED_SETTINGS"] = "1"

# Enable for specific operations
if operation in ["read", "health_check"]:
    os.environ["AURUM_USE_SIMPLE_DB_CLIENT"] = "1"
```

### 4. Setup Alerts

```python
# Alert on high error rates
health = validate_migration_health()
if not health["healthy"]:
    # Send alert to monitoring system
    send_alert("Migration health degraded", health["issues"])

# Alert on performance degradation
if settings_ratio < 0.5 and migration_phase == "simplified":
    send_alert("Migration performance degraded", "Consider rollback")
```

## Troubleshooting

### Common Issues

1. **High Error Rate**
   ```bash
   # Check migration health
   python migration_demo.py health

   # Rollback if needed
   python migration_demo.py rollback
   ```

2. **Performance Degradation**
   ```bash
   # Check metrics
   cat ~/.aurum/migration_metrics.json

   # Rollback to hybrid
   export AURUM_SETTINGS_MIGRATION_PHASE=hybrid
   ```

3. **Feature Flag Conflicts**
   ```bash
   # Check current flags
   env | grep AURUM

   # Reset to safe defaults
   export AURUM_SETTINGS_MIGRATION_PHASE=legacy
   export AURUM_USE_SIMPLIFIED_SETTINGS=0
   ```

### Debug Mode

```bash
# Enable debug logging
export AURUM_LOG_LEVEL=DEBUG

# Run with verbose output
python migration_demo.py monitor
```

## Migration Checklist

- [ ] Enable migration monitoring
- [ ] Test simplified settings system
- [ ] Test simplified database client
- [ ] Advance to hybrid phase
- [ ] Monitor performance for 24-48 hours
- [ ] Validate migration health
- [ ] Advance to simplified phase
- [ ] Monitor for additional 24 hours
- [ ] Remove legacy code (final step)

## Support

For migration issues:
1. Check the migration demo: `python migration_demo.py health`
2. Review metrics: `cat ~/.aurum/migration_metrics.json`
3. Rollback if needed: `python migration_demo.py rollback`
4. Check logs for detailed error information

The migration system is designed to be safe and reversible at every step. Always monitor health metrics before advancing phases.
