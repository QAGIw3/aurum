# Vendor Curve Ingestion Runbook

## Purpose
Daily ingestion of PW, EUGP, and RP vendor workbooks into the Aurum data plane. The pipeline lands enriched rows in `iceberg.raw.curve_landing`, publishes validated curves to `iceberg.market.curve_observation`, and captures failures in `iceberg.market.curve_observation_quarantine` and Kafka DLQs.

## Preconditions
- MinIO buckets bootstrapped (`make minio-bootstrap`).
- Iceberg schemas deployed via `trino/ddl/iceberg_market.sql`.
- Airflow DAG `ingest_vendor_curves_eod` enabled.
- DBT profiles configured (`dbt run`/`dbt test` working locally).

## Daily Run
1. Airflow creates a lakeFS branch `eod_<YYYYMMDD>`.
2. Parser task per vendor:
   - Loads drop directory (`$AURUM_VENDOR_DROP_DIR`).
   - Enriches units/currency, partitions quarantine rows.
   - Writes canonical files to `$AURUM_PARSED_OUTPUT_DIR` and DLQ JSONL payloads to `dlq/`.
   - Appends clean rows to `iceberg.raw.curve_landing` and quarantined rows to `iceberg.market.curve_observation_quarantine`.
3. DBT tasks:
   - `dbt run -m publish_curve_observation` merges landing rows into the canonical table.
   - `dbt run -m int_curve_monthly int_curve_calendar int_curve_quarter mart_curve_latest` refreshes marts.
   - `dbt test -m stg_curve_observation int_curve_monthly mart_curve_latest` validates curated models.
4. Great Expectations business suite runs on the written Parquet exports.
5. Scenario outputs validated, OpenLineage/metadata hooks fire, lakeFS branch merged/tagged.

## Backfill / Replay
- Generate synthetic curves for load testing: `python scripts/parsers/generate_synthetic_curves.py --start 2025-01-01 --days 2 --output artifacts/synthetic.parquet`.
- Re-run historical vendors: `python scripts/ingest/backfill_curves.py --start 2025-01-01 --end 2025-01-05 --vendor pw --vendor eugp --runner-arg --dry-run`.
- To publish to Iceberg during backfill add `--runner-arg --write-iceberg` and export `AURUM_ICEBERG_TABLE=iceberg.raw.curve_landing` before running DBT publish/marts.

## SLAs & Monitoring
- Landing freshness warning at 6h, error at 12h (DBT source freshness).
- Canonical freshness warning at 12h, error at 24h.
- Airflow DAG duration target < 30 min; row count XComs expose clean/quarantine totals per vendor.
- LakeFS commit metadata records daily totals; check OpenLineage events for `outputStatistics` row counts.
- Daily DAG `monitor_curve_quarantine` queries Trino for quarantine counts; adjust `AURUM_QUARANTINE_THRESHOLD` to trigger alerts in Slack/PagerDuty.

## Remediation
- Quarantine rows live in `iceberg.market.curve_observation_quarantine` with `quarantine_reason` and DLQ context for debugging.
- Update unit mappings under `config/units_map.csv` and re-run `dbt run -m publish_curve_observation` after applying fixes.
- For schema changes, update `ge/expectations/curve_landing.json` and `stg_curve_landing` to keep validations aligned.

## Point of Contact
- Platform Operations (`platform@aurum.local`)
- Data Engineering On-call (`aurum-ops@example.com`)
