# EIA Dataset Config

`config/eia_ingest_datasets.json` drives which EIA API datasets run via the dynamic Airflow DAG. Each entry accepts the following keys:

| Key | Required | Description |
| --- | --- | --- |
| `source_name` | ✓ | Unique name for watermarks and ingest source registration. |
| `path` | ✓ | Dataset route in the EIA API (e.g. `natural-gas/stor/wkly`). |
| `data_path` | optional | Overrides the API path when data lives under `/data`. Defaults to `path`. |
| `description` | optional | Description used for ingest source registration. |
| `schedule` | optional | Cron expression for the dataset task; defaults to hourly if omitted. |
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
| `extra_env` | optional | Additional environment variable assignments for the SeaTunnel job. |

Update the file and re-run `python3.11 -m pytest tests/seatunnel/test_run_job.py` to ensure templating passes.
