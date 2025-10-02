# Golden Query Cache

High-impact queries are cached with pattern-aware keys and TTLs to reduce latency and load. This layer augments the general cache manager with query-type specific TTLs and bust-on-write hooks.

Code reference: `src/aurum/api/golden_query_cache.py`.

## Query Types and TTLs

TTL values derive from `AurumSettings.api.cache` (env-prefixed `AURUM_API_...`). Key mappings:

- `CURVE_DATA` → `AURUM_API_CACHE_TTL_CURVE_DATA`
- `CURVE_DIFF` → `AURUM_API_CURVE_DIFF_TTL` or `AURUM_API_CACHE_TTL_MEDIUM_FREQUENCY`
- `CURVE_STRIPS` → `AURUM_API_CURVE_STRIP_TTL`
- `METADATA_DIMENSIONS` → `AURUM_API_CACHE_TTL_METADATA`
- `SCENARIO_OUTPUTS` / `SCENARIO_METRICS` → `AURUM_API_CACHE_TTL_SCENARIO_DATA`
- `CUSTOM` → `AURUM_API_CACHE_TTL_LOW_FREQUENCY`

See `src/aurum/core/settings.py` for exact field names and defaults.

## Patterns and Keys

Default patterns identify common query shapes and generate cache keys with tenant scoping:

- `curve_data_by_date`: `curve_observation` by `asof` and `iso`
- `curve_data_by_range`: `asof BETWEEN start_date AND end_date`
- `curve_diff_by_dates`: compares two `asof` dates for an `iso`
- `curve_strips_by_type`: strip queries by `tenor_type`
- `metadata_dimensions`: distinct dimension queries
- `scenario_outputs_by_scenario`: scenario outputs with `LIMIT`
- `scenario_metrics_latest`: latest metric per scenario

Keys include the tenant id: `tenant:{query_type}:{key_template}`.

## Invalidation

Strategies:

- `ttl_only` (default)
- `bust_on_write` for tables like `curve_observation` and `scenario_output`
- Dependency-based invalidation is supported via `invalidate_dependencies([...])` using table names.

Helpers:

- `invalidate_on_write(table_name)` decorator to auto-invalidate after write operations
- `invalidate_on_asof_publish(table, asof, tenant)` for as-of data publishes

## Warming and Analytics

- Warm-up arbitrary queries by calling `warm_cache([{ "query": "...", "compute_func": async_fn }])`.
- Fetch stats: `get_cache_stats()` returns global and per-pattern hit/miss metrics.
- See also cache analytics endpoints exposed by the advanced cache manager:
  - `GET /v1/cache/analytics`
  - `GET /v1/cache/stats`
  - `GET /v1/cache/health`
  - Warming control: `POST /v1/cache/warming/{start,stop}` and tasks at `POST /v1/cache/warming/tasks`

## Usage Patterns

- Wrap async handlers with `@cache_golden_query(QueryType.CURVE_DATA)` when the handler’s inputs uniquely determine the underlying query.
- Register additional patterns with `register_custom_pattern(...)` to tune key shape and TTL per route.
- Tune TTLs via env without code changes; see `docs/configuration.md`.

