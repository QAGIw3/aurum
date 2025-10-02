# Aurum Platform Refactoring Plan - October 2025

## Executive Summary

This comprehensive refactoring plan addresses technical debt, architectural improvements, and code quality enhancements for the Aurum energy trading platform. Based on thorough analysis of the codebase, this plan prioritizes high-impact changes that improve maintainability, performance, and developer experience.

### Current State Analysis

The Aurum platform has undergone significant modernization with:
- ✅ 37% of services migrated to new architecture (13/35 services)
- ✅ Complete async DAO layer with 4 database backends
- ✅ Unified dependency injection container
- ✅ Settings consolidation (83% code reduction)

However, significant technical debt remains:
- 🔴 Monolithic service.py (2,775 lines) and routes.py (2,600 lines)
- 🔴 Legacy synchronous DAOs still in active use
- 🔴 Global state management anti-patterns
- 🔴 Duplicate and redundant documentation
- 🔴 Inconsistent external data ingestion patterns

## Refactoring Phases

### Phase 1: Critical Cleanup & Debt Reduction (2 weeks)

#### 1.1 Documentation Consolidation
**Priority: HIGH** | **Impact: Medium** | **Effort: Small**

- **Remove old refactoring docs:**
  - Delete `REFACTORING_COMPLETE_FINAL.md` (415 lines)
  - Delete `REFACTORING_IMPLEMENTATION_FINAL.md`
  - Consolidate essential content into `docs/refactoring/README.md`

- **Consolidate duplicate roadmap docs:**
  - Merge `development-roadmap-2024.md`, `comprehensive-development-tasks.md`, `comprehensive-refactoring-tasks.md`
  - Create single `docs/ROADMAP.md` with prioritized tasks
  - Remove `next-development-steps.md`, `comprehensive-tasks-summary.md`

- **Clean demo/test files:**
  - Remove `test_container_compat.py`, `test_tier1_services.py` from root
  - Keep examples in `examples/` directory only
  - Remove unused demo scripts from `scripts/`

**Success Criteria:**
- Single source of truth for roadmap and planning
- No duplicate documentation files
- Clean root directory

#### 1.2 Legacy Code Removal Preparation
**Priority: HIGH** | **Impact: High** | **Effort: Medium**

- **Audit legacy DAO usage:**
  ```
  src/aurum/api/dao/
  ├── curves_dao.py      → Used by: curves_v2_service.py
  ├── metadata_dao.py    → Used by: metadata_service.py
  ├── ppa_dao.py        → Used by: ppa_service.py
  └── eia_dao.py        → Used by: eia_service.py
  ```

- **Create migration checklist:**
  - Document all sync DAO usages
  - Map to async DAO replacements
  - Identify blocking dependencies

**Success Criteria:**
- Complete inventory of legacy code dependencies
- Clear migration path documented

### Phase 2: Service Layer Modernization (4-6 weeks)

#### 2.1 Complete Service Decomposition
**Priority: CRITICAL** | **Impact: Very High** | **Effort: Large**

- **Decompose monolithic service.py:**
  ```python
  # Current: src/aurum/api/service.py (2,775 lines)
  # Target structure:
  src/aurum/services/
  ├── core/
  │   ├── curves.py         # Already done ✅
  │   ├── metadata.py       # Already done ✅
  │   ├── scenarios.py      # Already done ✅
  │   ├── ppa.py           # Already done ✅
  │   ├── iso.py           # Already done ✅
  │   └── drought.py       # Already done ✅
  ├── external/
  │   ├── eia.py           # Already done ✅
  │   ├── fred.py          # TODO
  │   ├── noaa.py          # TODO
  │   └── worldbank.py     # TODO
  └── platform/
      ├── search.py        # TODO
      ├── export.py        # TODO
      └── analytics.py     # TODO
  ```

- **Migrate remaining 22 services:**
  - External services: FRED, NOAA, WorldBank (3)
  - Platform services: Search, Export, Analytics (3)
  - ML services: Remaining ML services (8)
  - Domain services: Remaining domain services (8)

**Success Criteria:**
- service.py reduced to <200 lines (utilities only)
- All 35 services following SOLID principles
- 100% async-first implementation
- Test coverage >85% for all services

#### 2.2 Router Registry & V1 Extraction
**Priority: CRITICAL** | **Impact: High** | **Effort: Large**

- **Decompose routes.py (2,600 lines):**
  ```python
  # Target structure:
  src/aurum/api/routes/
  ├── v1/
  │   ├── curves.py
  │   ├── metadata.py
  │   ├── scenarios.py
  │   ├── ppa.py
  │   └── ...
  └── v2/
      ├── curves.py      # Already exists
      ├── metadata.py    # Already exists
      └── ...
  ```

- **Complete router registry integration:**
  - Extract all v1 endpoints to dedicated routers
  - Implement feature flags for v1/v2 switching
  - Add deprecation warnings to v1 endpoints

**Success Criteria:**
- routes.py completely removed
- All endpoints in domain-specific routers
- Router registry fully operational
- V1 deprecation path clear

### Phase 3: Architecture & Infrastructure (3-4 weeks)

#### 3.1 Global State Elimination
**Priority: HIGH** | **Impact: High** | **Effort: Medium**

- **Replace global state in state.py:**
  - Convert to dependency injection pattern
  - Use FastAPI's dependency system
  - Implement proper request context

- **Remove singleton anti-patterns:**
  - Cache managers
  - Database connections
  - Configuration objects

**Success Criteria:**
- Zero global variables
- All dependencies injected
- Testable without mocking globals

#### 3.2 Legacy DAO Removal
**Priority: HIGH** | **Impact: Medium** | **Effort: Medium**

- **Remove sync DAOs after service migration:**
  ```bash
  # Files to remove:
  src/aurum/api/dao/curves_dao.py
  src/aurum/api/dao/metadata_dao.py
  src/aurum/api/dao/ppa_dao.py
  src/aurum/api/dao/eia_dao.py
  ```

- **Update imports and __init__.py:**
  - Remove legacy exports
  - Update documentation
  - Add migration warnings

**Success Criteria:**
- All sync DAOs removed
- Zero legacy DAO imports
- All services using async DAOs

#### 3.3 Cache Architecture Unification
**Priority: MEDIUM** | **Impact: Medium** | **Effort: Medium**

- **Consolidate cache implementations:**
  - Single cache manager interface
  - Consistent TTL policies
  - Proper cache key namespacing

- **Remove duplicate cache logic:**
  - Service-level caching
  - DAO-level caching
  - Route-level caching

**Success Criteria:**
- Single cache implementation
- Consistent caching policies
- Cache metrics and monitoring

### Phase 4: External Data Standardization (4-5 weeks)

#### 4.1 Canonical Contract Implementation
**Priority: HIGH** | **Impact: High** | **Effort: Large**

- **Standardize external ingestion:**
  ```python
  # Target pattern for all providers:
  src/aurum/external_contracts/
  ├── base.py           # Abstract contract interface
  ├── eia/
  │   ├── contract.py
  │   └── parser.py
  ├── fred/
  │   ├── contract.py
  │   └── parser.py
  └── ...
  ```

- **Implement for all providers:**
  - EIA (enhance existing)
  - FRED
  - NOAA
  - WorldBank
  - ISO providers

**Success Criteria:**
- All providers using canonical contracts
- Consistent error handling
- Standardized data validation

#### 4.2 Airflow DAG Consolidation
**Priority: MEDIUM** | **Impact: Medium** | **Effort: Large**

- **Implement dataset-driven orchestration:**
  - Use Airflow 2.4+ dataset triggers
  - Consolidate similar DAGs
  - Remove redundant tasks

- **Standardize DAG patterns:**
  - Consistent naming conventions
  - Shared utility functions
  - Proper error handling

**Success Criteria:**
- 50% reduction in DAG count
- Dataset-driven dependencies
- Improved observability

### Phase 5: Quality & Standards (2-3 weeks)

#### 5.1 Error Handling Standardization
**Priority: MEDIUM** | **Impact: Medium** | **Effort: Medium**

- **Implement RFC7807 everywhere:**
  - Consistent error responses
  - Proper error codes
  - Detailed error context

- **Remove custom error patterns:**
  - Service-specific errors
  - Legacy error formats

**Success Criteria:**
- 100% RFC7807 compliance
- Consistent error handling
- Improved debugging

#### 5.2 Test Organization
**Priority: MEDIUM** | **Impact: Low** | **Effort: Medium**

- **Complete test migration:**
  ```
  tests/
  ├── unit/          # Pure unit tests
  ├── integration/   # Integration tests
  ├── e2e/          # End-to-end tests
  └── fixtures/      # Shared fixtures
  ```

- **Improve test quality:**
  - Remove redundant tests
  - Add missing coverage
  - Standardize fixtures

**Success Criteria:**
- Clear test organization
- >85% test coverage
- Fast test execution

#### 5.3 Dependency & Code Cleanup
**Priority: LOW** | **Impact: Low** | **Effort: Small**

- **Update dependencies:**
  - Upgrade to latest stable versions
  - Remove unused packages
  - Security vulnerability fixes

- **Remove dead code:**
  - Unused imports
  - Commented code
  - Unreachable code

**Success Criteria:**
- Zero security vulnerabilities
- Clean dependency tree
- No dead code

## Implementation Timeline

### Month 1: Foundation
- Week 1-2: Phase 1 (Cleanup & Debt Reduction)
- Week 3-4: Start Phase 2.1 (Service Decomposition)

### Month 2: Core Refactoring
- Week 5-6: Continue Phase 2.1
- Week 7-8: Phase 2.2 (Router Registry)

### Month 3: Architecture
- Week 9-10: Phase 3 (Architecture & Infrastructure)
- Week 11-12: Start Phase 4.1 (External Data)

### Month 4: Completion
- Week 13-14: Complete Phase 4
- Week 15-16: Phase 5 (Quality & Standards)

## Success Metrics

### Code Quality
- **Before:** 2,775-line service.py, 2,600-line routes.py
- **After:** All files <500 lines, following SRP
- **Improvement:** 80%+ reduction in file sizes

### Architecture
- **Before:** 37% services migrated, legacy DAOs active
- **After:** 100% services migrated, zero legacy code
- **Improvement:** Complete modernization

### Performance
- **Before:** Synchronous I/O, global state
- **After:** Async-first, dependency injection
- **Improvement:** 10x potential throughput increase

### Maintainability
- **Before:** Monolithic, tightly coupled
- **After:** Modular, loosely coupled
- **Improvement:** 50% reduction in change complexity

## Risk Mitigation

### Technical Risks
1. **Breaking Changes:** Use feature flags for gradual rollout
2. **Performance Regression:** Benchmark before/after each phase
3. **Data Integrity:** Comprehensive integration tests

### Process Risks
1. **Scope Creep:** Strict phase boundaries
2. **Resource Allocation:** Dedicated refactoring time
3. **Knowledge Transfer:** Pair programming, documentation

## Conclusion

This refactoring plan provides a systematic approach to modernizing the Aurum platform. By following these phases, we can eliminate technical debt while maintaining system stability and improving developer productivity.

The key to success is incremental progress - each phase builds on the previous one and delivers tangible improvements. With proper execution, the Aurum platform will emerge as a modern, maintainable, and scalable system ready for future growth.

### Next Steps
1. Review and approve this plan
2. Allocate resources (3-4 engineers for 4 months)
3. Set up tracking dashboards
4. Begin Phase 1 immediately

---

*Document Version: 1.0*  
*Date: October 2025*  
*Status: Ready for Review*
