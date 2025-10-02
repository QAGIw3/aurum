Aurum dbt Project

Target adapters
- Primary: Trino + Iceberg (prod)
- Dev: DuckDB (local) via target.name branching in macros

Conventions
- Use centralized Iceberg macros for table properties: `iceberg_config_timeseries`, `iceberg_config_fact_table`, `iceberg_config_dimension`.
- Prefer dbt incremental merge where possible; custom merge macros are experimental.
- Keep surrogate keys stable; continue using `aurum_text_hash` in dimensions.

Docs persistence
- `persist_docs` is enabled for relations and columns on models and seeds. On Trino + Iceberg, dbt applies `COMMENT ON TABLE/COLUMN` where supported by the connector.
- Ensure the Trino role used for dbt has privileges to set comments in the target catalog/schema.

Selectors
- Domain selectors added: `curve`, `series`, `scenario`, `iso`, `external`.
- CI selector `ci` excludes `iceberg` and `timeseries` tags for speed.

Notes
- Sources for ISO data are declared under source name `external` with schema `external` in the Iceberg catalog.
- The `analysis` path exists for ad‑hoc SQL used in docs and exploration.
