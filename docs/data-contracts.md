# Aurum Data Contracts

This guide captures the structured interfaces that external consumers rely on. Every contract is versioned and monitored; breaking changes require a major version bump and a migration plan.

## Iceberg Tables

### `iceberg.market.curve_observation`
- **Primary key:** `(curve_key, tenor_label, asof_date)`
- **Schema stability:** backwards-compatible additions are allowed (nullable columns, metadata). Renames/removals require `aurum/curve.observation` schema bump + migration.
- **Partition spec:** `days(asof_date)`, `iso`, `product`
- **Quality gates:** Great Expectations `curve_schema`, `curve_business`, and dbt tests (`publish_curve_observation`, `fct_curve_observation`).

### `fact.fct_curve_observation`
- **Primary key:** `(curve_key, tenor_label, asof_date)`
- **Dimension FKs:** `iso_sk`, `market_sk`, `block_sk`, `product_sk`, `asof_sk`
- **Change log:** Snapshotted via `snapshots.curve_observation_snapshot`
- **Intended use:** downstream marts, API diff (`mart_curve_asof_diff`), reconciliation.

### `mart.mart_curve_latest`
- Latest record per `(curve_key, tenor_label)`; always points to `fct_curve_observation`
- Consumers should store Nessie snapshot IDs or `version_hash` when caching.

### `mart.mart_curve_asof_diff`
- Pre-computed deltas between consecutive as-of dates.
- Pairs with API `GET /v1/curves/diff` (planned) to avoid double-scanning facts.

### `mart.mart_eia_series_latest`
- Latest normalized EIA value per `series_id`.
- Guarantee: `value`, `unit_normalized`, `currency_normalized` are non-null.

### `iceberg.market.curve_dead_letter`
- DLQ capture table populated by ingestion workers when schema drift or serialization errors occur.
- Columns align with `aurum.ingest.error.v1` (`source`, `error_message`, `context`, `severity`, `recommended_action`, `ingest_ts`) plus `raw_payload` (stringified original record).
- Partition spec: `days(ingest_ts)`
- Use cases: monitoring dashboards (`mart_curve_dead_letter_summary`), root-cause analysis, alerting integrations.

### `mart.mart_curve_dead_letter_summary`
- Aggregated DLQ counts grouped by `source`, `severity`, and `ingest_day` with sample error messages.
- Serves Superset dashboards and alerting thresholds; pairs with `ops_metrics` for unified observability.

### Seeded dimensions
- `dim_iso`, `dim_market`, `dim_block`, `dim_product`, `dim_asof` provide surrogate keys + descriptive attributes.
- Seeds live under `dbt/seeds/ref/**` with not-null/unique tests.
- Any additions must keep existing identifiers stable; changes are versioned in Git and surfaced via release notes.

## Kafka / Avro Topics

| Topic | Schema | Contract notes |
| --- | --- | --- |
| `aurum.curve.observation.v1` | `curve.observation.v1.avsc` | Emits when canonical row lands in Iceberg. `IsoCode` enum now includes `UNKNOWN` for forward compatibility. |
| `aurum.iso.*.lmp.v1` | `iso.lmp.v1.avsc` | LMP payloads; `IsoCode` enum shared across LMP/load/genmix. |
| `aurum.iso.*.load.v1` | `iso.load.v1.avsc` | Load payloads use same enum + field names as LMP | 
| `aurum.iso.*.genmix.v1` | `iso.genmix.v1.avsc` | Generation mix payloads harmonized with `IsoCode`. |

### Contract Catalog

- Contracts are centralized in `kafka/schemas/contracts.yml`; every subject entry pins the schema file, logical Kafka topic, and the enforced compatibility mode.
- Subject names must match `^aurum\.[a-z0-9_.]+\.v[0-9]+(-key|-value)?$` (see `exceptions` inside the catalog for the narrow set of grandfathered subjects).
- Default compatibility is `BACKWARD`; individual subjects may override via the `compatibility` key.
- CI uses `scripts/ci/register_schemas.py --contracts kafka/schemas/contracts.yml kafka/schemas` to validate + register subjects. Contract mismatches now fail the job before hitting Schema Registry.

### Schema Evolution Policy
- **Semantic versioning**: MAJOR change = incompatible (field removal/rename, enum removal); MINOR = backwards-compatible addition (nullable field, enum append); PATCH = documentation/metadata update.
- Avro schemas pinned via `kafka/schemas/subjects.json`. Update process:
  1. Modify Avro schema
  2. Run `pytest tests/kafka/test_schemas.py`
  3. Bump semantic version or add `aliases` in docstring.

## API Surface

### `GET /v1/curves`
- Mirrors `mart_curve_latest` with filters: `iso`, `market`, `product`, `as_of`, `tenor_type`, pagination.
- Response envelope: `{ "meta": {"as_of": "2024-02-01"}, "data": [ ... rows ... ] }`
- Enum alignment: `iso`, `market`, `product`, `block` values must match `dim_*` seeds; API guards via normalization + 400 for unknown enums.

### `GET /v1/curves/diff` (planned)
- Will leverage `mart_curve_asof_diff`; request parameters `current_as_of`, `previous_as_of`, `iso`, `market`, `product`. Deltas surfaced per `(curve_key, tenor_label)`.

### `GET /v1/eia/series/latest`
- Mirrors `mart_eia_series_latest`. Guarantees normalized units/currency.

### Version discipline
- **OpenAPI** lives in `openapi/aurum.yaml`; contract changes require updating examples + running `make docs-serve`.
- API follows semantic versioning using media type suffix: `application/vnd.aurum.v1+json`. Bump MAJOR for breaking field changes.

## Semantic Versioning Process

| Artifact | Version source | Trigger for bump |
| --- | --- | --- |
| Avro schemas | Schema `doc` + Git tag (`schemas/vX.Y.Z`) | Enum removal/rename = MAJOR; optional field addition = MINOR |
| dbt project | `dbt_project.yml` (`version`) | Model contract change (new column) = MINOR; breaking rename/remove = MAJOR |
| API | `pyproject.toml` (`aurum-api`), OpenAPI `info.version` | Breaking response change = MAJOR; additive field = MINOR |

1. Align versions in a release PR and document in `docs/release-notes.md` (create if absent).
2. Register updated Avro schemas via `make kafka-register-schemas`.
3. Capture Nessie snapshot ID for Iceberg changes; store in release notes.
4. Tag repo: `git tag data-vX.Y.Z` covering aggregated change.

## Sample & Validation Assets

- **Sample data:** `dbt/seeds/sample/curve_observation_sample.csv` (anonymized). Run `dbt seed --select curve_observation_sample` for quick demos.
- **Validation notebooks:** `notebooks/iso_validation_template.ipynb` + `make trino-harness`/`make reconcile-kafka-lake` to capture metrics.
- **Reference strips macro:** `aurum_reference_strips(start_date, end_date)` generates calendar/weekly/peak strips off `dim_asof`.

## Change Management Checklist

1. Update dbt models + seeds; run `dbt test` and refresh GE suites.
2. If Kafka schema changes, bump Avro version and publish to Schema Registry (ensure compatibility mode).
3. Re-run query harness and reconciliation scripts; capture outputs in PR description.
4. Update documentation (`docs/data-contracts.md`, OpenAPI examples).
5. Tag release and notify consumers (API, SQL, file drops) with migration notes.
