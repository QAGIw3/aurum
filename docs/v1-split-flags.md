V1 Router Split Feature Flags

This codebase supports splitting large v1 domains from the monolithic router into dedicated, maintainable routers. Each split is gated by an environment flag so you can enable a domain incrementally and validate parity before retiring the monolith handlers.

Flags (all default to 0/off)

- AURUM_API_V1_SPLIT_EIA=1
  - Includes src/aurum/api/v1/eia.py (datasets, dataset detail, series JSON/CSV, dimensions)

- AURUM_API_V1_SPLIT_ISO=1
  - Includes src/aurum/api/v1/iso.py (LMP last-24h/hourly/daily/negative, JSON/CSV with ETag 304)

- AURUM_API_V1_SPLIT_PPA=1
  - Includes src/aurum/api/v1/ppa.py (contracts CRUD + valuations list; adhoc valuation)

- AURUM_API_V1_SPLIT_DROUGHT=1
  - Includes src/aurum/api/v1/drought.py (tiles/info; dimensions/indices/usdm/layers)

- AURUM_API_V1_SPLIT_METADATA=1
  - Includes src/aurum/api/metadata.py as its own router (v1 metadata endpoints)

- AURUM_API_V1_SPLIT_ADMIN=1
  - Includes src/aurum/api/v1/admin.py (cache invalidation endpoints)

Notes

- Flags are additive; enable one domain at a time for verification.
- After parity is validated for a domain, disable the corresponding monolith handlers and make the split default.
- v2 endpoints are unaffected; pagination across v2 is standardized with prev/next cursors and canonical ETags.
- Trino access paths are consolidated through the pooled client with resilience and metrics.

