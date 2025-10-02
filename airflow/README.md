### Airflow DAG consolidation and backfill contract

This repository consolidates DAG creation behind a factory and standardized helpers.

#### Feature flags
- `AURUM_USE_CONSOLIDATED_DAGS` (true/false): enable consolidated factory DAGs.
- `AURUM_PIPELINE_MIGRATION_PHASE` (1/2/3): controls migration stage.
- `AURUM_ENABLE_DAG_TEMPLATES` (true/false): optional DAG templates.

#### Factory helpers
- `create_ingest_chain(dag, task_prefix, job_name, source_name, env_entries, pool=None, queue=None, pre_lines=None, extra_lines=None, render_timeout_minutes=10, execute_timeout_minutes=20, k8s_timeout_seconds=600, watermark_policy="hour|day|exact")` → returns `(render_task, execute_task, watermark_task)`.
- `create_backfill_task(dag, task_id, provider, default_chunk_hours, datasets_variable=None, data_types=None)` → returns a PythonOperator that implements the unified backfill contract.

#### Unified backfill contract (dag_run.conf)
Backfill-capable DAGs accept the following `dag_run.conf` keys:
- `backfill`: true (required to trigger)
- `start`: ISO8601 timestamp (required)
- `end`: ISO8601 timestamp (required)
- `chunk_hours`: integer chunk size (optional; defaults per provider)
- `datasets`: comma-separated list or JSON list (provider-specific, optional)

Idempotency is maintained via watermarks; chunks already covered by a watermark are skipped.

#### Recommended pools and SLAs (ops notes)
- Use these pools (slots are starting points; tune in Airflow UI by demand):
  - `api_iso`: 6, `api_caiso`: 4, `api_isone`: 4, `api_pjm`: 4, `api_miso`: 4, `api_spp`: 3, `api_nyiso`: 3, `api_aeso`: 3
  - `api_eia`: 3, `api_noaa`: 4, `kafka_producers`: 3, `heavy_processing`: 2
- SLAs via `SLA_CONFIGS`:
  - High-frequency (e.g., RTM LMP): 30 minutes
  - Medium-frequency (hourly loads/gen mix): 2 hours
  - Low-frequency (daily jobs): 6 hours
  - Bulk loads: 24 hours

#### Migration notes
- Legacy DAGs calling `iso_utils.create_seatunnel_ingest_chain` are being migrated to the factory `create_ingest_chain` helper.
- Bespoke backfill DAGs are deprecated in favor of the unified backfill contract exposed by production ingestion DAGs.


