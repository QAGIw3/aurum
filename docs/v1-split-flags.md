V1 Router Split Feature Flags

This codebase supports splitting large v1 domains from the monolithic router into dedicated, maintainable routers. Each split is gated by feature flags that can be controlled both via environment variables (for backward compatibility) and through the runtime feature flag API for dynamic control.

## Enhanced Feature Flag System

As of the latest enhancement, V1 split flags are integrated with the advanced feature flag system, providing:

- **Runtime Toggle Capability**: Change flag states without service restart
- **API Management**: Use REST endpoints to control flags
- **Audit Trail**: Track who changed what flags and when
- **Validation Testing**: Comprehensive test coverage for flag behavior
- **Backward Compatibility**: Existing environment variable usage continues to work

## Available Flags

All flags default to 0/off except where noted:

- **AURUM_API_V1_SPLIT_EIA=1**
  - Module: `src/aurum/api/v1/eia.py`
  - Features: EIA datasets, dataset detail, series JSON/CSV, dimensions
  - Runtime Flag: `v1_split_eia`

- **AURUM_API_V1_SPLIT_ISO=1**
  - Module: `src/aurum/api/v1/iso.py` 
  - Features: ISO LMP last-24h/hourly/daily/negative, JSON/CSV with ETag 304
  - Runtime Flag: `v1_split_iso`

- **AURUM_API_V1_SPLIT_CURVES** *(enabled by default)*
  - Module: `src/aurum/api/v1/curves.py`
  - Features: Curves router (always loaded by default)
  - Runtime Flag: `v1_split_curves`
  - Note: Flag retained for backwards compatibility but always enabled

- **AURUM_API_V1_SPLIT_PPA=1** *(enabled by default)*
  - Module: `src/aurum/api/v1/ppa.py`
  - Features: PPA router (enabled by default)
  - Runtime Flag: `v1_split_ppa`
  - Note: Flag retained for backwards compatibility but enabled by default

- **AURUM_API_V1_SPLIT_DROUGHT=1**
  - Module: `src/aurum/api/v1/drought.py`
  - Features: Drought tiles/info; dimensions/indices/usdm/layers
  - Runtime Flag: `v1_split_drought`

- **AURUM_API_V1_SPLIT_ADMIN=1**
  - Module: `src/aurum/api/v1/admin.py`
  - Features: Cache invalidation endpoints and admin utilities
  - Runtime Flag: `v1_split_admin`

## Runtime API Endpoints

### List All V1 Split Flags
```bash
GET /v1/admin/features/v1-splits
```

Response:
```json
{
  "meta": {
    "request_id": "req-123",
    "query_time_ms": 12.34,
    "total_flags": 6,
    "enabled_flags": 2
  },
  "data": {
    "eia": {
      "enabled": false,
      "env_var": "AURUM_API_V1_SPLIT_EIA",
      "module_path": "aurum.api.v1.eia",
      "description": "EIA datasets, dataset detail, series JSON/CSV, dimensions API router",
      "default_enabled": false,
      "last_updated": "2024-01-01T12:00:00",
      "feature_key": "v1_split_eia"
    },
    "ppa": {
      "enabled": true,
      "env_var": "AURUM_API_V1_SPLIT_PPA", 
      "module_path": "aurum.api.v1.ppa",
      "description": "PPA router (enabled by default)",
      "default_enabled": true,
      "last_updated": "2024-01-01T12:00:00",
      "feature_key": "v1_split_ppa"
    }
  }
}
```

### Get Specific V1 Split Flag
```bash
GET /v1/admin/features/v1-splits/{flag_name}
```

### Toggle V1 Split Flag
```bash
PUT /v1/admin/features/v1-splits/{flag_name}
```

Request body:
```json
true
```

Response:
```json
{
  "message": "V1 split flag 'eia' enabled successfully",
  "flag_name": "eia",
  "enabled": true,
  "changed_by": "admin_user",
  "env_var": "AURUM_API_V1_SPLIT_EIA",
  "module_path": "aurum.api.v1.eia",
  "meta": {
    "request_id": "req-456",
    "query_time_ms": 8.92
  }
}
```

## Usage Examples

### Environment Variable (Traditional)
```bash
# Enable EIA split
export AURUM_API_V1_SPLIT_EIA=1

# Restart service for changes to take effect
systemctl restart aurum-api
```

### Runtime API (Enhanced)
```bash
# Enable EIA split immediately (no restart needed)
curl -X PUT "https://api.example.com/v1/admin/features/v1-splits/eia" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d true

# Check status
curl "https://api.example.com/v1/admin/features/v1-splits" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Disable flag
curl -X PUT "https://api.example.com/v1/admin/features/v1-splits/eia" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d false
```

### Python Integration
```python
from aurum.api.features.v1_split_integration import get_v1_split_manager

async def toggle_eia_router():
    manager = get_v1_split_manager()
    
    # Initialize flags
    await manager.initialize_v1_flags()
    
    # Enable EIA router
    success = await manager.set_v1_split_enabled("eia", True, "admin_user")
    if success:
        print("EIA router enabled")
    
    # Check status
    is_enabled = await manager.is_v1_split_enabled("eia")
    print(f"EIA router is {'enabled' if is_enabled else 'disabled'}")
    
    # Get all status
    status = await manager.get_v1_split_status()
    print(f"Total flags: {len(status)}")
```

## Migration Strategy

1. **Phase 1**: Deploy enhanced system with environment variable compatibility
2. **Phase 2**: Begin using runtime API for flag management
3. **Phase 3**: Gradually migrate from environment variables to API control
4. **Phase 4**: Retain environment variables for container/deployment configuration

## Key Features

### Runtime Control
- **No Restart Required**: Flag changes take effect immediately
- **API Management**: RESTful endpoints for all flag operations
- **Validation**: Input validation and error handling

### Monitoring & Observability
- **Audit Trail**: All flag changes are logged with user attribution
- **Status Tracking**: Real-time flag status and metadata
- **Request Tracing**: Request IDs for debugging

### Backward Compatibility
- **Environment Variables**: Existing deployment scripts continue to work
- **Automatic Sync**: Runtime changes update environment variables
- **Fallback**: Graceful fallback to environment variables if runtime system fails

### Validation Testing
- **Comprehensive Tests**: Full test coverage for flag behavior
- **Integration Tests**: API endpoint testing
- **Error Handling**: Robust error scenarios tested

## Notes

- Flags are additive; enable one domain at a time for verification
- After parity is validated for a domain, disable the corresponding monolith handlers and make the split default
- v2 endpoints are unaffected; pagination across v2 is standardized with prev/next cursors and canonical ETags
- Trino access paths are consolidated through the pooled client with resilience and metrics
- Runtime changes are immediately reflected in environment variables for backward compatibility
- Flag states persist in the feature flag store (Redis/in-memory) and survive service restarts

## Authentication

All runtime API endpoints require admin privileges. Configure with:
- `AURUM_API_ADMIN_GROUP`: Admin group name for OIDC/JWT
- See `docs/auth/oidc-forward-auth.md` for authentication setup
