# ISO Canonical Contract

This document describes the canonical ISO contract used across ingestion, Kafka,
staging (dbt), and curated marts.

## Goals

- Provide consistent market/product/location semantics for all ISO providers.
- Keep Kafka metadata schemas stable (map<string,string>) by stringifying values.
- Make downstream modeling predictable via top-level `iso_*` columns.

## Canonical Fields

- `iso_code`: ISO identifier (e.g., PJM, ISO-NE, ERCOT, CAISO, SPP, NYISO, MISO)
- `iso_market`: DA/DAM/RT/RTM/etc.
- `iso_product`: LMP, LOAD, GENERATION, CONGESTION, ANCILLARY, ...
- `iso_location_type`: NODE, ZONE, HUB, EHV, etc.
- `iso_location_id`: Primary location identifier (node/zone/hub id)
- `iso_location_name`: Human-readable location name when available
- `iso_timezone`: Canonical timezone (IANA)
- `iso_interval_minutes`: Observation cadence in minutes (e.g., 5, 15, 60)
- `iso_unit`: Canonical unit (typically USD/MWh)
- `iso_subject`: Higher-level subject used for aggregation/contracting
- `iso_curve_role`: How the series is consumed (pricing, load, etc.)

## Derivation Order

For each attribute the helpers apply:

1. Explicit top-level field on the record (if present)
2. Metadata hints (e.g., `metadata['iso_market']` or `metadata['market']`)
3. Provider defaults from `ISO_PROVIDER_DEFAULTS`
4. Heuristic (e.g., infer market/product from `series_id`/`dataset_code`)
5. Fallback to `UNKNOWN` (or sensible default)

See: `src/aurum/data/iso_catalog.py`.

## Serialization Rule

- Metadata is Avro `map<string,string>`; non-string values are stringified.
- Complex objects (e.g., `iso_contract`) are serialized with JSON, preserving
  fidelity while keeping the schema simple and stable.

## Where It Appears

- Kafka: `kafka/schemas/ExtTimeseriesObsV1.avsc`, `ExtSeriesCatalogUpsertV1.avsc`
- dbt staging: `dbt/models/stg/external/stg_external__obs.sql`, `stg_external__series_catalog.sql`
- dbt intermediate: `dbt/models/int/external/int_external__obs_conformed.sql`
- Marts: `dbt/models/marts/external/mart_external_series_catalog.sql`,
  `dbt/models/marts/external/cur_external__obs_mapped.sql`

## Validation

- dbt tests: `dbt/models/marts/external/schema.yml`
- Great Expectations: `ge/expectations/mart_external_series_catalog.json`

Run helper to refresh and validate:

```
scripts/dbt/refresh_iso_contracts.py --target <env>
```

## Migration Notes

- Add columns to existing Iceberg tables prior to deployment (`ALTER TABLE ... ADD COLUMN`).
- Historical rows may lack `iso_*` columns; marts fall back to metadata hints
  and defaults. Consider backfills to populate missing attributes where needed.

