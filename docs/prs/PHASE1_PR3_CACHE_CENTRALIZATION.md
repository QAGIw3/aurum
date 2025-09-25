# Phase 1 — PR 3: Cache Centralization

## Summary
Eliminate direct Redis access from API paths; use `CacheManager` with helper utilities and analytics.

## Scope
- src/aurum/api/service.py
- src/aurum/api/routes.py (metadata fallback only)
- src/aurum/api/cache/utils.py (new)
- src/aurum/api/cache/decorators.py (new)
- src/aurum/api/cache/cache_analytics.py

## Checklist
- [ ] Replace ad-hoc Redis calls with CacheManager
- [ ] Introduce `cache_get_or_set` and `@cached`
- [ ] Namespaced keys and TTLs per endpoint
- [ ] Hit/miss metrics and index maintenance

## Verification
- Integration: cache hit/miss behavior
- Perf: metadata and curves p95 under targets

## Risks
- Invalidation bugs; add index and safe fallbacks

