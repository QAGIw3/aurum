---
name: "Phase 1 – Cache Centralization (PR 3)"
about: Use CacheManager everywhere; add analytics; unify TTL policy
labels: ["phase:1", "area:caching", "type:refactor", "perf", "priority:P0"]
---

## Summary
Replace direct Redis usage with `CacheManager`, add helper decorator/utilities, implement namespaced keys and cache analytics.

## Scope
- src/aurum/api/service.py
- src/aurum/api/routes.py (metadata cache fallback only)
- src/aurum/api/cache/utils.py (new)
- src/aurum/api/cache/decorators.py (new)
- src/aurum/api/cache/cache_analytics.py

## Tasks
- [ ] Remove direct Redis usage from API paths
- [ ] Use `cache_get_or_set` / `@cached` where applicable
- [ ] Namespaced cache keys per endpoint; TTLs from settings
- [ ] Emit hit/miss and size metrics

## Acceptance Criteria
- [ ] No direct `redis.*` usage in API code
- [ ] Hot metadata p95 < 50ms (cache enabled)
- [ ] Cached curves p95 < 120ms

## Notes
Keep in-process fallback for metadata only.

