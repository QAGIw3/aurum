# Legacy Code Audit

This document tracks legacy code usage during the refactoring process.

## Legacy Imports Audit (Phase 1)

### Status: IN PROGRESS

### Active Legacy Imports

#### 1. `src/aurum/data/backend_adapter.py`
**Import:** `from aurum.api.legacy.async_service_legacy import AsyncCurveService, AsyncScenarioService`

**Usage:** Base classes for `CurveServiceAdapter` and `ScenarioServiceAdapter`
- These adapters bridge the legacy service layer with the new pluggable backend system
- Used as abstract base classes, not for implementation

**Migration Plan:**
- Phase 2.3: Replace with new service interfaces when services are refactored
- Priority: MEDIUM (not urgent, adapter pattern is acceptable temporarily)
- Dependencies: Complete service layer refactor first

#### 2. `src/aurum/api/rate_limiting/__init__.py`
**Import:** `from .sliding_window import RateLimitMiddleware as LegacyRateLimitMiddleware`

**Usage:** Backward compatibility exports
- Intentional legacy support for gradual migration
- New consolidated interface is recommended and exported
- Legacy interfaces clearly marked

**Migration Plan:**
- Maintain for backward compatibility during transition
- Can be removed in v2.0 when all consumers migrate
- Priority: LOW (intentional backward compatibility)

#### 3. `src/aurum/api/logging/structured_logger.py`
**Import:** Compatibility wrapper forwarding to `aurum.logging.structured_logger`

**Usage:** Compatibility layer for API-specific imports
- Simple forwarding wrapper
- Allows API code to use legacy import paths
- No actual legacy code, just import compatibility

**Migration Plan:**
- Update import statements in API layer to use canonical path
- Remove wrapper once all imports updated
- Priority: LOW (trivial, can be done anytime)

## Legacy DAO Modules (Phase 1.2)

### To Be Removed

The following synchronous DAO modules are deprecated and should be removed after migrating all usages to async equivalents:

1. **`src/aurum/api/dao/eia_dao.py`**
   - Replaced by: `eia_async_dao.py`
   - Used by: `src/aurum/api/services/eia_service.py`
   - Status: ⚠️ ACTIVE USAGE - Cannot remove yet

2. **`src/aurum/api/dao/curves_dao.py`**
   - Replaced by: async methods in repository layer
   - Used by: 
     - `src/aurum/api/curves_v2_service.py`
     - `src/libs/services/curves_service.py`
   - Status: ⚠️ ACTIVE USAGE - Cannot remove yet

3. **`src/aurum/api/dao/metadata_dao.py`**
   - Replaced by: async methods in repository layer
   - Used by: `src/aurum/api/services/metadata_service.py`
   - Status: ⚠️ ACTIVE USAGE - Cannot remove yet

4. **`src/aurum/api/dao/ppa_dao.py`**
   - Replaced by: async methods in repository layer
   - Used by: `src/aurum/api/services/ppa_service.py`
   - Status: ⚠️ ACTIVE USAGE - Cannot remove yet

### Migration Steps

1. ✅ Audit legacy imports across codebase
2. ✅ Search for usages of sync DAO classes
3. ⏳ Migrate services to use async DAOs (Phase 2.3)
   - `EiaService` → use `EiaAsyncDao`
   - `MetadataService` → use async repository
   - `PpaService` → use async repository
   - `CurvesV2Service` → use async repository
4. ⏳ Remove deprecated sync DAO modules
5. ⏳ Update `src/aurum/api/dao/__init__.py`

### Conclusion

All sync DAOs are actively used by services. **Cannot remove until Phase 2.3 (Service Layer Refactor) is complete.**

## Legacy API Code (Phase 1.2)

### To Be Evaluated

1. **`src/aurum/api/legacy/` directory**
   - Contains: `async_service_legacy.py`, `models.py`, `scenario_models.py`
   - Used by: `backend_adapter.py` (as base classes)
   - Decision: Keep until service layer refactor (Phase 2.3)

2. **`src/aurum/api/v1_retired.py`**
   - Status: ✅ Cleaned up duplicate code
   - Purpose: RFC 7807 stub for retired v1 endpoints
   - Decision: Keep for API versioning support

## Next Steps

1. Search for direct usages of sync DAO classes
2. Document migration path for each usage
3. Create async equivalents where missing
4. Update import statements
5. Remove deprecated modules

## Notes

- Maintain backward compatibility during migration
- Use deprecation warnings before removing code
- Test thoroughly before removing any legacy code
- Keep this document updated as refactoring progresses

