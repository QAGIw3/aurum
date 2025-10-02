# Router Extraction Plan

**Phase:** 2.2 - Router Registry Migration  
**Status:** Active Implementation  
**Priority:** HIGH

## Executive Summary

This plan details the extraction of API routes from monolithic implementations into domain-specific routers following RESTful principles and clean architecture patterns.

## Current State Analysis

### Router Registry Structure
- **v1 routers**: Legacy API endpoints with deprecation headers
- **v2 routers**: Modern, domain-focused routers with tenant scoping
- **Router Registry**: Central management in `router_registry.py`

### Required v1 Routers (from router_registry.py)
```python
mandatory_modules = (
    "aurum.api.v1.catalog",      # ❌ Not exists
    "aurum.api.v1.scenarios",    # ❌ Not exists  
    "aurum.api.v1.metadata",     # ❌ Not exists
    "aurum.api.v1.search",       # ❌ Not exists
    "aurum.api.v1.ppa",          # ❌ Not exists
    "aurum.api.v1.model_registry", # ❌ Not exists
    "aurum.api.v1.notifications", # ❌ Not exists
    "aurum.api.v1.curves",       # ❌ Not exists
)

optional_modules = {
    "AURUM_API_V1_SPLIT_EIA": "aurum.api.v1.eia",      # ❌ Not exists
    "AURUM_API_V1_SPLIT_ISO": "aurum.api.v1.iso",      # ❌ Not exists
    "AURUM_API_V1_SPLIT_DROUGHT": "aurum.api.v1.drought", # ❌ Not exists
    "AURUM_API_V1_SPLIT_ADMIN": "aurum.api.v1.admin",  # ❌ Not exists
}
```

### Existing v2 Routers ✅
All v2 routers are already created in `/src/aurum/api/v2/`:
- admin.py, auto_reforecast.py, bidding.py, carbon_rec.py
- curves.py, dbt_management.py, developer_workspace.py
- drought.py, eia.py, explainability.py, forecasting.py
- iso.py, market_streaming.py, metadata.py, model_registry.py
- performance_monitoring.py, plugin_system.py, ppa.py
- regulatory_tracker.py, renewables.py, risk_engine.py
- scenarios.py, search.py, signals.py, stress_testing.py

## Implementation Plan

### Phase 1: Create v1 Router Structure

Since v2 routers already exist, we need to create v1 routers that:
1. Wrap existing legacy service calls
2. Apply deprecation headers automatically
3. Maintain backward compatibility
4. Forward to v2 implementations where possible

### Router Template

```python
"""V1 {Domain} Router - Legacy API endpoints with deprecation notices."""

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List, Dict, Any

from ..deps import get_current_user, resolve_tenant
from ..services.{domain}_service import {Domain}Service
from ..http import PaginatedResponse
from ..exceptions import NotFoundError

router = APIRouter(
    prefix="/v1/{domain}",
    tags=["{Domain}", "v1"],
)

# Service instance (to be replaced with DI)
service = {Domain}Service()

@router.get("/", response_model=PaginatedResponse[{Model}])
async def list_{domain}(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    tenant_id: Optional[str] = Depends(resolve_tenant),
    current_user: Optional[str] = Depends(get_current_user),
):
    """
    List {domain} (DEPRECATED - use /v2/{domain}).
    
    This endpoint is deprecated and will be removed on 2025-10-30.
    Please migrate to the v2 API.
    """
    # Forward to v2 service
    results, total = await service.list_{domain}(
        tenant_id=tenant_id,
        offset=offset,
        limit=limit
    )
    
    return PaginatedResponse(
        data=results,
        total=total,
        offset=offset,
        limit=limit
    )
```

### Migration Strategy

1. **Wrapper Pattern**: v1 routers act as thin wrappers around services
2. **Deprecation Headers**: Applied automatically by router registry
3. **Minimal Logic**: No business logic in v1 routers
4. **Service Reuse**: Use existing services, don't duplicate
5. **Forward Compatibility**: Design to easily redirect to v2

## Router Creation Checklist

### Core Domain Routers

- [ ] **curves.py** - Energy curves and pricing data
  - GET /v1/curves
  - GET /v1/curves/{curve_id}
  - GET /v1/curves/aggregated
  - GET /v1/curves/diff

- [ ] **metadata.py** - System metadata and dimensions
  - GET /v1/metadata/dimensions
  - GET /v1/metadata/units
  - GET /v1/metadata/calendars
  - GET /v1/metadata/locations

- [ ] **scenarios.py** - Scenario management
  - GET /v1/scenarios
  - POST /v1/scenarios
  - GET /v1/scenarios/{scenario_id}
  - PUT /v1/scenarios/{scenario_id}
  - DELETE /v1/scenarios/{scenario_id}

- [ ] **ppa.py** - Power Purchase Agreements
  - GET /v1/ppa/contracts
  - GET /v1/ppa/valuations
  - POST /v1/ppa/calculate

- [ ] **search.py** - Unified search
  - GET /v1/search
  - GET /v1/search/suggestions
  - GET /v1/search/filters

- [ ] **catalog.py** - Data catalog
  - GET /v1/catalog/datasets
  - GET /v1/catalog/schemas
  - GET /v1/catalog/tables

- [ ] **model_registry.py** - ML model registry
  - GET /v1/models
  - POST /v1/models
  - GET /v1/models/{model_id}/versions
  - POST /v1/models/{model_id}/versions

- [ ] **notifications.py** - Alert notifications
  - GET /v1/notifications
  - POST /v1/notifications
  - PUT /v1/notifications/{id}/read

### Optional Domain Routers

- [ ] **eia.py** - EIA data integration
- [ ] **iso.py** - ISO market data
- [ ] **drought.py** - Drought monitoring
- [ ] **admin.py** - Administrative endpoints

## Success Criteria

### Technical Requirements
- [ ] All v1 endpoints properly deprecated
- [ ] No business logic in routers
- [ ] Consistent error handling
- [ ] Proper request/response models
- [ ] Comprehensive OpenAPI documentation

### Migration Goals
- [ ] Zero breaking changes for v1 clients
- [ ] Clear migration path to v2
- [ ] Performance parity or better
- [ ] Reduced code duplication
- [ ] Improved testability

## Risk Mitigation

### Backward Compatibility
- Maintain exact same request/response formats
- Keep same URL patterns
- Preserve query parameter names
- Support legacy authentication

### Testing Strategy
- Unit tests for each router
- Integration tests for v1→v2 flow
- Contract tests for API compatibility
- Load tests for performance validation

### Rollout Plan
1. Create v1 routers in shadow mode
2. Route small % of traffic for validation
3. Monitor for errors and performance
4. Gradually increase traffic %
5. Full cutover with fallback ready

## Next Steps

1. **Immediate Actions**:
   - Create v1 router directory structure ✅
   - Implement first router (curves.py)
   - Set up router tests
   - Validate deprecation headers

2. **Short Term** (1-2 weeks):
   - Complete mandatory routers
   - Update OpenAPI documentation
   - Create migration guide

3. **Medium Term** (3-4 weeks):
   - Implement optional routers
   - Performance optimization
   - Client SDK updates

---

**Owner:** Platform Team  
**Review:** Weekly during standup  
**Deadline:** 2025-10-30 (v1 sunset date)
