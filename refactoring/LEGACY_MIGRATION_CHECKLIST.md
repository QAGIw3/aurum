# Legacy Code Migration Checklist

**Date:** October 2025  
**Status:** Active Migration  
**Phase:** 1.2 - Legacy Code Removal Preparation

## Executive Summary

This checklist tracks all legacy code dependencies that must be migrated before removal. Based on comprehensive audit, we have 3 active sync DAOs and their dependent services that need migration.

## Legacy DAO Usage Audit

### 1. CurvesDao (curves_dao.py)
**Status:** ⚠️ ACTIVE - Cannot remove yet

**Used By:**
- `src/aurum/api/curves_v2_service.py:122` - Direct instantiation
- `src/libs/services/curves_service.py:101` - Optional dependency with fallback

**Migration Path:**
1. ✅ New async DAO exists: `src/aurum/data/dao/trino.py`
2. ✅ Repository exists: `src/aurum/data/repositories/curves.py`
3. ✅ New service exists: `src/aurum/services/core/curves.py`
4. ⏳ Update `curves_v2_service.py` to use new service
5. ⏳ Update `libs/services/curves_service.py` to use new service
6. ⏳ Remove `curves_dao.py`

### 2. MetadataDao (metadata_dao.py)
**Status:** ⚠️ ACTIVE - Cannot remove yet

**Used By:**
- `src/aurum/api/services/metadata_service.py:28` - Direct instantiation

**Migration Path:**
1. ✅ New async DAO exists: `src/aurum/data/dao/trino.py`
2. ✅ Repository exists: `src/aurum/data/repositories/metadata.py`
3. ✅ New service exists: `src/aurum/services/core/metadata.py`
4. ⏳ Migrate remaining methods from legacy service
5. ⏳ Update v1/v2 endpoints to use new service
6. ⏳ Remove `metadata_dao.py`

### 3. PpaDao (ppa_dao.py)
**Status:** ⚠️ ACTIVE - Cannot remove yet

**Used By:**
- `src/aurum/api/services/ppa_service.py:165` - Direct instantiation

**Migration Path:**
1. ✅ New async DAO exists: `src/aurum/data/dao/trino.py`
2. ✅ Repository exists: `src/aurum/data/repositories/ppa.py`
3. ✅ New service exists: `src/aurum/services/core/ppa.py`
4. ⏳ Migrate complex valuation logic from legacy service
5. ⏳ Update `ppa_v2_service.py` to use new service
6. ⏳ Remove `ppa_dao.py`

### 4. EiaDao
**Status:** ✅ NOT FOUND - No sync EiaDao exists
- Only `eia_async_dao.py` exists
- No legacy migration needed

## Service Dependencies

### Legacy Services Using Sync DAOs
1. **metadata_service.py** → Uses MetadataDao
   - Location: `src/aurum/api/services/metadata_service.py`
   - New service: `src/aurum/services/core/metadata.py`
   - Status: Partially migrated

2. **ppa_service.py** → Uses PpaDao
   - Location: `src/aurum/api/services/ppa_service.py`
   - New service: `src/aurum/services/core/ppa.py`
   - Status: Complex valuation logic not migrated

3. **curves_v2_service.py** → Uses CurvesDao
   - Location: `src/aurum/api/curves_v2_service.py`
   - New service: `src/aurum/services/core/curves.py`
   - Status: Needs adapter update

## Migration Checklist

### Phase 1: Service Migration (Current)
- [ ] Complete metadata service migration
  - [ ] Migrate `list_dimensions` method
  - [ ] Migrate `list_units` method
  - [ ] Migrate `list_calendars` method
  - [ ] Migrate `list_iso_locations` method
  - [ ] Update tests
  
- [ ] Complete PPA service migration
  - [ ] Migrate `list_contracts` method
  - [ ] Migrate `list_contract_valuation_rows` method
  - [ ] Migrate complex valuation logic
  - [ ] Update tests

- [ ] Update curves v2 service
  - [ ] Replace CurvesDao with new service
  - [ ] Update SharedCurvesService integration
  - [ ] Update tests

### Phase 2: Route Updates
- [ ] Update metadata routes to use new service
- [ ] Update PPA routes to use new service
- [ ] Update curves routes to use new service
- [ ] Add deprecation warnings to v1 endpoints

### Phase 3: DAO Removal
- [ ] Remove `curves_dao.py`
- [ ] Remove `metadata_dao.py`
- [ ] Remove `ppa_dao.py`
- [ ] Update `dao/__init__.py` exports
- [ ] Remove legacy imports

### Phase 4: Legacy Service Cleanup
- [ ] Archive or remove `metadata_service.py`
- [ ] Archive or remove `ppa_service.py`
- [ ] Clean up unused imports
- [ ] Update documentation

## Blocking Dependencies

### Critical Path Items
1. **Service Layer Completion** - Must finish migrating all methods before removing DAOs
2. **Route Updates** - All endpoints must use new services
3. **Test Coverage** - Ensure >85% coverage on new services
4. **Backwards Compatibility** - Maintain v1 API contracts

### Risk Mitigation
1. **Feature Flags** - Use flags to switch between old/new implementations
2. **Parallel Running** - Run both implementations with comparison
3. **Gradual Rollout** - Migrate one service at a time
4. **Rollback Plan** - Keep legacy code until fully validated

## Progress Tracking

| Component | Legacy Files | Migration Status | Can Remove? |
|-----------|--------------|------------------|-------------|
| Curves DAO | curves_dao.py | Service exists, routes need update | ❌ Not yet |
| Metadata DAO | metadata_dao.py | Partial migration | ❌ Not yet |
| PPA DAO | ppa_dao.py | Complex logic remains | ❌ Not yet |
| EIA DAO | N/A | Already async | ✅ N/A |

## Next Steps

1. **Immediate:** Complete metadata service migration
2. **Next:** Migrate PPA valuation logic
3. **Then:** Update curves v2 service adapter
4. **Finally:** Remove all sync DAOs

## Success Criteria

- ✅ All services using async DAOs
- ✅ Zero imports of sync DAO classes
- ✅ All tests passing
- ✅ Performance benchmarks met
- ✅ Zero regression in API responses

---

**Last Updated:** October 2025  
**Owner:** Refactoring Team  
**Review Date:** Weekly during migration
