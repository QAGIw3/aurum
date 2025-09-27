# Aurum Refactoring Tasks Analysis

**Analysis Date:** January 2025  
**Codebase Version:** Based on current main branch  
**Context:** Energy trading platform API undergoing monolith-to-modular refactoring

## Executive Summary

After comprehensive analysis of the Aurum codebase, documentation, and ongoing refactoring efforts, this document presents 10 prioritized tasks for continuing the architectural evolution from monolithic v1 to modular v2 patterns. Tasks are ordered by impact, risk, and alignment with the established ADR-0001 incremental refactoring strategy.

## Task Categorization

### High Priority (Foundational Cleanup)
Tasks 1-5 address immediate technical debt and infrastructure issues that block or complicate other refactoring work.

### Medium Priority (Architectural Evolution)  
Tasks 6-10 implement new patterns and capabilities that support long-term architectural goals.

---

## Task 1: Remove Duplicate HTTP Utilities
**Priority:** High | **Effort:** Medium | **Risk:** Low

### Problem Statement
HTTP utility functions are duplicated across:
- `src/aurum/api/http/__init__.py` (centralized utilities)
- `src/aurum/api/scenarios/http.py` (domain-specific duplicates)

Functions like `respond_with_etag()`, `decode_cursor()`, `encode_cursor()`, and `normalize_cursor_input()` exist in both locations with slight variations.

### Implementation Approach
1. **Audit Phase:** Compare implementations for functional differences
2. **Consolidation:** Move canonical versions to `src/aurum/api/http/`
3. **Migration:** Update imports in scenarios module  
4. **Testing:** Ensure response format parity with existing tests
5. **Cleanup:** Remove duplicate implementations

### Expected Outcomes
- Reduced code duplication (~50 lines eliminated)
- Consistent HTTP utility behavior across domains
- Simplified maintenance and bug fixes
- Foundation for standardizing v2 response patterns

### Dependencies
- None (can be done immediately)

---

## Task 2: Complete Router Registry Migration
**Priority:** High | **Effort:** Medium | **Risk:** Medium

### Problem Statement
Router registry system exists but has incomplete migration:
- V1 PPA router shows duplicate registration warnings
- Some v1 domains still embedded in monolithic `routes.py`
- Feature flags for v1 splits need validation testing

### Implementation Approach
1. **Fix Duplicates:** Resolve PPA router double-registration
2. **Extract Remaining:** Move final v1 handlers from routes.py to domain routers
3. **Registry Hardening:** Add registration validation and conflict detection
4. **Testing Infrastructure:** Create contract tests for router parity
5. **Feature Flag Validation:** Add integration tests for flag combinations

### Expected Outcomes
- Clean router registry without conflicts
- `routes.py` reduced to middleware/utilities only
- Robust feature flag testing preventing production issues
- Complete v1 domain extraction enabling independent evolution

### Dependencies
- Requires coordination with domain teams for handler migration

---

## Task 3: Decompose Monolithic Service Layer  
**Priority:** High | **Effort:** Large | **Risk:** High

### Problem Statement
`src/aurum/api/service.py` contains 2,775 lines mixing:
- Multiple domain concerns (curves, EIA, ISO, metadata, drought)
- Business logic with infrastructure code
- Synchronous and asynchronous patterns
- Direct database access with caching logic

### Implementation Approach
1. **Domain Boundaries:** Map existing functions to domain services
2. **Service Interfaces:** Define async service contracts per domain
3. **Gradual Extraction:** Move one domain at a time with feature flags
4. **DAO Layer:** Introduce data access objects for each persistence layer
5. **Integration Testing:** Maintain API contract compatibility throughout

### Service Decomposition Plan
```
service.py (2,775 LOC) → 
├── curves_service.py (~500 LOC)
├── eia_service.py (~400 LOC) 
├── iso_service.py (~600 LOC)
├── metadata_service.py (~300 LOC)
├── drought_service.py (~200 LOC)
├── ppa_service.py (~400 LOC)
└── shared_service_utils.py (~375 LOC)
```

### Expected Outcomes
- Domain services with clear responsibilities
- Improved testability and maintainability
- Reduced coupling between business domains
- Foundation for team ownership boundaries

### Dependencies
- Router registry completion (Task 2)
- DAO pattern implementation (Task 6)

---

## Task 4: Standardize Health Check Infrastructure
**Priority:** High | **Effort:** Medium | **Risk:** Low

### Problem Statement
Health check implementations are inconsistent:
- Mix of sync/async patterns with `asyncio.run()` blocking event loop
- Different status aggregation logic across endpoints
- External service probes use different HTTP clients
- No unified health check provider pattern

### Implementation Approach
1. **Async Conversion:** Convert all health checks to async/await
2. **HTTP Client Unification:** Use `httpx.AsyncClient` consistently
3. **Status Aggregation:** Implement robust healthy/disabled interpretation
4. **Provider Pattern:** Abstract health check implementations
5. **Monitoring Integration:** Add structured logging and metrics

### Expected Outcomes
- Event-loop safe health checks eliminating performance bottlenecks
- Consistent health status reporting across all endpoints
- Extensible health check framework for new services
- Improved observability of service dependencies

### Dependencies
- None (can start immediately)

---

## Task 5: Dependency Version Alignment
**Priority:** High | **Effort:** Small | **Risk:** Low

### Problem Statement
`pyproject.toml` has significant duplication across dependency groups:
- Core dependencies repeated in `api`, `worker`, `ingest` groups
- Version conflicts possible between overlapping dependencies
- Maintenance overhead when updating common packages

### Implementation Approach
1. **Constraint File:** Create shared dependency constraints
2. **Group Optimization:** Define minimal groups with inheritance patterns
3. **Version Unification:** Ensure consistent versions across all groups
4. **Validation:** Add checks for dependency conflicts in CI
5. **Documentation:** Update dependency management practices

### Expected Outcomes
- Single source of truth for dependency versions
- Eliminated version conflicts and resolver issues  
- Simplified dependency updates and security patching
- Reduced `pyproject.toml` complexity

### Dependencies
- None (immediate improvement)

---

## Task 6: Implement Async DAO Pattern
**Priority:** Medium | **Effort:** Large | **Risk:** Medium

### Problem Statement
Database access is scattered throughout service layer:
- Direct SQL queries mixed with business logic
- Multiple database clients (Trino, TimescaleDB, ClickHouse) with different patterns
- Connection pooling inconsistencies
- Limited query observability and caching hooks

### Implementation Approach
1. **DAO Contracts:** Define async interfaces for each data store
2. **Connection Management:** Implement pooled async clients
3. **Query Builder:** Create composable query construction utilities
4. **Observability:** Add query logging, timing, and error tracking
5. **Caching Integration:** Hook cache layer into DAO operations

### DAO Architecture
```
├── dao/
│   ├── base.py (AsyncDAO base class)
│   ├── trino_dao.py (Trino queries)
│   ├── timescale_dao.py (TimescaleDB operations)
│   ├── clickhouse_dao.py (ClickHouse analytics)
│   └── cache_dao.py (Redis operations)
```

### Expected Outcomes
- Clean separation of data access from business logic
- Consistent async patterns across all database operations
- Improved query observability and performance monitoring
- Foundation for database-specific optimizations

### Dependencies
- Service layer decomposition (Task 3)

---

## Task 7: Enhance Feature Flag System
**Priority:** Medium | **Effort:** Medium | **Risk:** Low

### Problem Statement
Current feature flag system:
- Basic environment variable flags (`AURUM_API_V1_SPLIT_*`)
- No runtime toggle capability
- Limited testing infrastructure for flag combinations
- No gradual rollout or percentage-based flags

### Implementation Approach
1. **Dynamic Flags:** Implement runtime-configurable feature flags
2. **Percentage Rollouts:** Add gradual rollout capabilities
3. **Testing Framework:** Create flag combination validation tests
4. **Admin Interface:** Build feature flag management UI
5. **Monitoring:** Add flag usage metrics and audit logging

### Expected Outcomes
- Safe incremental rollouts with instant rollback capability
- Comprehensive testing of feature flag combinations
- Operational visibility into flag usage and impact
- Reduced deployment risk for experimental features

### Dependencies
- None (enhances existing flag system)

---

## Task 8: Centralize Cache Management
**Priority:** Medium | **Effort:** Medium | **Risk:** Medium

### Problem Statement
Caching is implemented inconsistently:
- Multiple cache implementations across different domains
- Varied TTL policies and invalidation strategies
- Cache warm-up scripts specific to individual caches
- No unified cache observability or management

### Implementation Approach
1. **Cache Abstraction:** Extend `CacheManager` for all cache operations  
2. **Policy Standardization:** Define consistent TTL and eviction policies
3. **Invalidation Patterns:** Implement tag-based cache invalidation
4. **Warm-up Framework:** Create generic cache preloading infrastructure
5. **Monitoring:** Add cache hit/miss metrics and performance tracking

### Expected Outcomes
- Unified cache management reducing operational complexity
- Consistent cache behavior improving predictability
- Simplified cache warm-up and invalidation procedures
- Enhanced cache observability and performance tuning

### Dependencies
- Service layer decomposition (Task 3)

---

## Task 9: Strengthen Error Handling
**Priority:** Medium | **Effort:** Medium | **Risk:** Low

### Problem Statement
Error handling patterns are inconsistent:
- Mix of custom exceptions and HTTP exceptions
- Incomplete RFC7807 problem detail implementation
- Async exception handling gaps
- Inconsistent error response formats between v1 and v2

### Implementation Approach
1. **Exception Hierarchy:** Design domain-specific exception classes
2. **RFC7807 Implementation:** Complete problem detail response format
3. **Async Exception Handling:** Add proper async exception middleware
4. **Error Response Standardization:** Unify error formats across versions
5. **Observability:** Add structured error logging and metrics

### Expected Outcomes
- Consistent error responses improving API usability
- Proper async exception handling preventing event loop issues
- Enhanced error observability for debugging and monitoring
- Foundation for API client error handling

### Dependencies
- Router registry completion (Task 2)

---

## Task 10: API Documentation Generation
**Priority:** Medium | **Effort:** Medium | **Risk:** Low

### Problem Statement
API documentation is incomplete:
- OpenAPI specs not automatically generated
- Limited examples and validation
- No contract testing for schema compliance
- Domain-specific documentation scattered

### Implementation Approach
1. **Schema Generation:** Automate OpenAPI spec generation from routers
2. **Validation Framework:** Implement schema validation testing
3. **Example Generation:** Create realistic API examples and responses
4. **Contract Testing:** Add Schemathesis-based contract validation
5. **Documentation Site:** Build searchable API documentation portal

### Expected Outcomes
- Comprehensive, automatically updated API documentation
- Schema-driven contract testing preventing API regressions
- Improved developer experience for API consumers
- Foundation for API versioning and deprecation workflows

### Dependencies
- Router registry completion (Task 2)

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)
Tasks 1, 4, 5 - Address immediate technical debt

### Phase 2: Architecture (Weeks 5-12)  
Tasks 2, 3, 6 - Core architectural changes

### Phase 3: Enhancement (Weeks 13-16)
Tasks 7, 8, 9, 10 - Advanced capabilities and observability

## Risk Mitigation

### High-Risk Tasks
- **Task 3 (Service Decomposition):** Use feature flags, incremental migration, comprehensive testing
- **Task 8 (Cache Centralization):** Maintain cache warming scripts, gradual migration per domain

### Change Management
- All changes guarded by feature flags where possible
- Comprehensive contract testing before/after migrations  
- Performance baseline monitoring during rollouts
- Immediate rollback procedures for each phase

## Success Metrics

### Technical Metrics
- Lines of code in monolithic files (target: routes.py <100 LOC, service.py eliminated)
- Test coverage maintenance (target: >85% throughout refactoring)
- API response time preservation (target: p95 latency unchanged)
- Deployment frequency improvement (target: 2x faster deployments)

### Operational Metrics
- Reduced production incidents from architectural issues
- Faster feature development velocity per domain team
- Improved developer onboarding time
- Enhanced system observability and debugging capability

---

*This analysis is based on the current state of the Aurum codebase and aligns with the established ADR-0001 incremental refactoring strategy. Tasks should be prioritized based on team capacity, business needs, and risk tolerance.*