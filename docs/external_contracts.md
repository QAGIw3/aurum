# External Contracts Reference

This document describes the canonical external data contracts spanning Kafka, Avro schemas, and Iceberg storage for provider timeseries ingestion.

## Iceberg namespace and tables

All contracts land in the `iceberg.external` namespace. Apply `sql/iceberg/external/*.sql` with `scripts/trino/run_sql.py` or include the files in automated deployments.

| Table | Primary key | Partitioning | Notes |
|-------|--------------|--------------|-------|
| `series_catalog` | (`provider`, `series_id`) | unpartitioned | Metadata view over upstream series definitions. Includes optional `tags`, `version`, and audit timestamps. |
| `timeseries_observation` | (`provider`, `series_id`, `ts`, `asof_date`) | `provider`, `series_id`, `days(ts)` | Fact table for normalized observations. Sorted by provider/series/timestamp for efficient upserts and compaction. |
| `unit` | `unit_code` | unpartitioned | Canonical unit definitions with SI conversion factors where available. |
| `frequency` | `frequency_code` | unpartitioned | Supported reporting cadences with interval metadata. |
| `geo` | `geo_id` | unpartitioned | Minimal geography seed covering ISO country codes. |
| `dataset` | `dataset_code` | unpartitioned | Provider dataset dictionary with default unit/frequency hints. |

Seed records for the dimension tables live in `sql/iceberg/external/010_seed_dimensions.sql` and can be re-run idempotently. The file ends with `SELECT COUNT(*)` sanity checks that should yield non-zero counts.

## Idempotent merge patterns

Upsert logic is centralized in `sql/merge/obs_merge.sql` and `sql/merge/catalog_merge.sql`. Each script expects a staging view named `staging_external_timeseries_observation` or `staging_external_series_catalog` and applies a `MERGE` keyed by the columns listed above. Only changed fields are updated, making repeat executions safe.

For ad-hoc loads, `scripts/test_fixtures/load_external_fixtures.py` will render the merge SQL dynamically from local fixtures and call Trino directly.

## Kafka topics & Avro schemas

Two Kafka topics back the contracts:

| Topic | Partitions | Retention | Cleanup policy | Schema |
|-------|------------|-----------|----------------|--------|
| `aurum.ext.timeseries.obs.v1` | 12 (6 in kind) | 60 days | delete | `kafka/schemas/ExtTimeseriesObsV1.avsc` |
| `aurum.ext.series_catalog.upsert.v1` | 6 (3 in kind) | compact | compact | `kafka/schemas/ExtSeriesCatalogUpsertV1.avsc` |

The schemas are registered in `kafka/schemas/subjects.json` and covered by tests in `tests/kafka/test_schemas.py`, including snapshot-based backward compatibility guards and round-trip validation via `fastavro`.

Messages on both topics should use a compound Kafka key of `provider|series_id` to guarantee stable partitioning and idempotent upserts. Observations include `ts` and `asof_date` in the value payload; the Iceberg merge logic enforces uniqueness on those columns.

### Trino catalog defaults

The Trino Iceberg catalog (`trino/catalog/iceberg.properties`) is pinned to Iceberg format v2 with merge-on-read enabled and ZSTD compression. Default session overrides in `conf/trino/session.properties` turn on predicate pushdown and spill to match the merge and compaction workload characteristics.

## Sample fixtures

`testdata/external/` contains small `series_catalog.jsonl` and `timeseries_observation.csv` samples. Run the loader script to push the fixtures into a running Trino/Iceberg stack:

```bash
./scripts/test_fixtures/load_external_fixtures.py --host localhost --port 8080 --user dev
```

The script reads the merge templates, injects fixture payloads, and prints confirmation queries.

## CI & automation

The `External Contracts` GitHub Actions workflow runs on relevant changes and performs:

1. `sqlfluff` linting for `sql/iceberg/external` and `sql/merge`.
2. `pytest tests/kafka/test_schemas.py` for Avro parsing + compatibility snapshots.
3. `scripts/trino/test_external_contracts.sh`, which bootstraps a lightweight Trino + Nessie + MinIO stack with Docker Compose, applies the DDL, loads fixtures, and executes smoke queries to ensure data lands in the Iceberg tables.

For local validation, the same shell script can be invoked directly from the repository root.
