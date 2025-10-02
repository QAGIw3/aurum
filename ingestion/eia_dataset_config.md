# EIA Dataset Config

`config/eia_ingest_datasets.json` drives which EIA API datasets run via the dynamic Airflow DAG. Each entry accepts the following keys:

| Key | Required | Description |
| --- | --- | --- |
| `source_name` | ✓ | Unique name for watermarks and ingest source registration. |
| `path` | ✓ | Dataset route in the EIA API (e.g. `natural-gas/stor/wkly`). |
| `data_path` | optional | Overrides the API path when data lives under `/data`. Defaults to `path`. |
| `description` | optional | Description used for ingest source registration. |
| `schedule` | optional | Cron expression for the dataset task; if omitted the generator maps common frequencies to sensible defaults (hourly → `5 * * * *`, daily → `20 6 * * *`, weekly → `30 6 * * 1`, monthly → `0 7 3 * *`, quarterly → `0 7 5 1,4,7,10 *`, annual → `0 7 5 1 *`). |
| `topic_var` | ✓ | Airflow Variable used to resolve the Kafka topic. |
| `default_topic` | ✓ | Default Kafka topic if the Airflow Variable is unset. |
| `series_id_expr` | ✓ | SQL expression in SeaTunnel that emits the `series_id`. |
| `frequency` | ✓ | Frequency label written into the Avro record. |
| `units_var` | optional | Airflow Variable with the units string. |
| `default_units` | optional | Fallback units string. |
| `area_expr` / `sector_expr` | optional | SQL expressions for contextual fields. |
| `description_expr` | optional | SQL expression for the record description field. |
| `source_expr` | optional | SQL expression for the source attribution. |
| `dataset_expr` | optional | SQL expression for dataset metadata. |
| `metadata_expr` | optional | SQL expression for arbitrary JSON metadata. |
| `filter_expr` | optional | SQL predicate appended to the query `WHERE` clause. |
| `param_overrides` | optional | Array of dictionaries/tuples (`param` → `value`) injected into the HTTP params section. |
| `page_limit` | optional | Overrides the per-request `length` parameter (defaults to 5000). |
| `window_hours` / `window_days` / `window_months` / `window_years` | optional | Size of the look-back window used to derive `start`/`end` tokens when the Airflow DAG materializes the job. |
| `dlq_topic` | optional | Kafka topic to receive dirty records (default `aurum.ref.eia.series.dlq.v1`). |
| `extra_env` | optional | Additional environment variable assignments for the SeaTunnel job. |

Update the file and re-run `python3.11 -m pytest tests/seatunnel/test_run_job.py` to ensure templating passes.

## Bulk dataset config

Bulk archives defined in `config/eia_bulk_datasets.json` drive the `eia_bulk_to_kafka` SeaTunnel job
and the new `ingest_eia_bulk` Airflow DAG. Each entry accepts the following keys:

| Key | Required | Description |
| --- | --- | --- |
| `source_name` | ✓ | Unique identifier for watermark updates and DAG task ids. |
| `url` | ✓ | HTTPS URL to the ZIP/CSV archive exposed by the EIA bulk endpoint. |
| `description` | optional | Used when registering the ingest source. |
| `schedule` | optional | Cron expression applied to the Airflow task (defaults to daily). |
| `topic_var` | ✓ | Airflow Variable key that resolves the Kafka topic. |
| `default_topic` | ✓ | Fallback Kafka topic when the Airflow Variable is unset. |
| `frequency` | ✓ | Frequency label written into the Avro record. |
| `series_id_expr` | ✓ | SQL expression that emits the `series_id` column. |
| `period_expr` | ✓ | Expression capturing the period token (string). |
| `value_expr` | ✓ | Expression converted to the numeric value. |
| `raw_value_expr` | optional | Raw string representation of the value (defaults to `value`). |
| `units_expr`, `area_expr`, `sector_expr`, `description_expr`, `source_expr`, `dataset_expr`, `metadata_expr` | optional | SQL expressions for auxiliary metadata fields. |
| `filter_expr` | optional | Predicate applied to the SQL transform (defaults to `TRUE`). |
| `schema_fields` | optional | Array describing the CSV schema (name/type pairs). |
| `csv_delimiter` | optional | CSV delimiter (default `,`). |
| `skip_header` | optional | Number of header rows to skip (default `1`). |
| `extra_env` | optional | Additional environment variables passed to the SeaTunnel job. |

Use `python scripts/eia/backfill_series.py --job bulk --source <name>` to replay a bulk
dataset without waiting for the scheduled DAG.
