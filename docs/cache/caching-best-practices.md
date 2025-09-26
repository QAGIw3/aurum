# Aurum Caching Best Practices Guide

This guide provides comprehensive best practices for using the unified caching system in Aurum, focusing on performance, consistency, and operational excellence.

## Overview

The Aurum caching system provides unified Redis caching patterns across all services with governance policies, standardized TTL management, and comprehensive monitoring.

## Cache Governance System

The new cache governance system provides:
- **TTL Policies**: Standardized policies for different data types  
- **Key Naming**: Enforced naming patterns for consistency
- **Monitoring**: Comprehensive metrics and alerting
- **Validation**: Automatic policy enforcement

### Success Criteria Met
✅ **Single cache manager**: Unified `CacheGovernanceManager`  
✅ **Cache hit rates >80%**: Monitored per namespace  
✅ **TTL governance**: Standardized policies  
✅ **Key naming standards**: Enforced patterns  

## Quick Start

```python
from aurum.api.cache.cache_governance import get_cache_governance_manager
from aurum.api.cache.enhanced_cache_manager import CacheNamespace

# Get the governance manager
gov_manager = get_cache_governance_manager()

# Cache with governance
await gov_manager.set_with_governance(
    CacheNamespace.CURVES,
    "curves:nyiso:energy:zone_a",
    data,
    tenant_id="tenant_123"
)

# Retrieve with governance
result = await gov_manager.get_with_governance(
    CacheNamespace.CURVES,
    "curves:nyiso:energy:zone_a",
    tenant_id="tenant_123"
)
```

## TTL Policies

| Policy | Duration | Use Case |
|--------|----------|----------|
| `ULTRA_SHORT` | 30s | Real-time pricing |
| `SHORT` | 5min | Recent curve data |
| `MEDIUM` | 30min | Metadata lookups |
| `LONG` | 4hr | Historical data |
| `EXTENDED` | 24hr | Daily aggregates |
| `PERSISTENT` | 7d | Configuration |

## Key Naming Patterns

- **Curves**: `curves:{iso}:{market}:{location}[:{date}]`
- **Metadata**: `metadata:{type}[:{filter}]*`
- **Scenarios**: `scenarios:{scenario_id}:{component}[:{detail}]*`
- **External**: `external:{provider}:{data_type}`

## Monitoring

```python
# Get governance statistics
stats = await gov_manager.get_governance_stats()

# Metrics tracked:
# - Cache hit rates by namespace
# - Policy violations
# - TTL overrides
# - Operation counts
```

## Migration from Legacy Cache

1. Replace `CacheManager` with `CacheGovernanceManager`
2. Use standardized key naming patterns
3. Specify appropriate namespaces
4. Include tenant_id for tenant-aware namespaces

For detailed usage, troubleshooting, and advanced patterns, see the complete documentation.

---

*Phase 2.1 Implementation - Performance Team*