# Storage Plane Configuration Plan

This note captures the work required to finish hardening the storage tier before we automate Iceberg compaction and retention. Execute `make minio-bootstrap` to apply the bucket plan locally (script wraps `scripts/storage/bootstrap_minio.py`).

## MinIO (S3-compatible)
- Buckets: `aurum-raw` (landed vendor files), `aurum-quarantine`, `aurum-curated` (Iceberg warehouse), `aurum-exports`, `aurum-logs`.
- Enable bucket versioning on `aurum-curated` and `aurum-quarantine`; keep raw uploads immutable via bucket policy.
- Server-side encryption: SSE-S3 for non-sensitive buckets, SSE-KMS for `aurum-curated` using KMS key `aurum-curated-kms`.
- Lifecycle: raw and quarantine buckets transition to Glacier/Deep Archive after 90 days, delete after 400 days; exports expire after 60 days.

## lakeFS
- Repository: `aurum` (already in `.env.example`).
- Branch model: `main`, `eod_{YYYYMMDD}`, `hotfix_*`, `client_release_*`.
- Hooks:
  1. **Pre-commit**: execute Great Expectations landing suite (`ge/expectations/curve_landing.json`) and refuse commit on failure (see `lakefs/hooks/pre_commit_curve_landing.sh`).
  2. **Post-commit**: enqueue Iceberg metadata refresh for Trino and record lineage event.
- lakeFS credentials will be provisioned via Vault role `aurum-lakefs` with read/write to `aurum` repo only.

## Iceberg Tables
- Warehouse path: `s3://aurum-curated/iceberg`.
- Tables:
  - `iceberg.raw.curve_landing` (append-only staging table, partitioned by `asof_date`).
  - `iceberg.market.curve_observation` (canonical), partitioned by `year(asof_date)`, `month(asof_date)` (already in DDL).
  - `iceberg.market.curve_observation_quarantine` to persist quarantined rows for auditing (same schema + `quarantine_reason`).
- Sort order: `asof_date`, `asset_class`, `iso`, `tenor_label` to improve predicate pruning.
- Publishing handled via dbt incremental model `publish_curve_observation` (merge on `curve_key|tenor_label|asof_date`).
- Metrics/Retention:
  - Snapshot expiration: keep 14 days on `raw` and `market` namespaces.
  - Compaction: weekly rewrite targeting 128 MB files using existing `maintenance.rewrite_data_files` helper.
  - Metadata cleanup: purge orphan files weekly via Airflow op.

## Trino
- Catalog entries for `iceberg_raw` and `iceberg_market` referencing Nessie branch `main` by default.
- Resource groups to protect long-running compaction queries: limit `iceberg_maintenance` to 2 concurrent tasks, 50% cluster share.

## Next Actions
1. Create buckets + policies via Terraform (module `infra/storage` pending).
2. Add lakeFS hook scripts invoking `aurum.parsers.runner --dry-run --validate` for landing suite.
3. Define Airflow DAG steps to publish from `raw.curve_landing` → `market.curve_observation`, pushing quarantined rows to `iceberg.market.curve_observation_quarantine` and DLQ topic.
4. Schedule Iceberg maintenance DAG (`expire_snapshots`, `rewrite_data_files`) using plan above.
