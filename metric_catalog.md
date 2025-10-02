# Metric Catalog Publication
Published metric catalog enumerating scenario and market KPIs sourced from marts.

**Owner:** Data Platform (data-platform@aurum.local)

## mart_scenario_metric_latest
Helper view exposing the latest value per scenario and metric.

| Column | Description | Tests |
| --- | --- | --- |
| `scenario_id` |  | not_null |
| `run_id` | Run identifier corresponding to the latest metric value. | not_null |
| `tenant_id` |  | not_null |
| `metric` |  | not_null |
| `latest_value` | Most recent value for the metric. |  |
| `latest_asof_date` | As-of date associated with the latest metric value. |  |

## mart_cpi_series_latest
Latest CPI observation per tenant and series identifier.

| Column | Description | Tests |
| --- | --- | --- |
| `tenant_id` |  | not_null |
| `series_id` |  | not_null |
| `period` |  | not_null |
| `ingest_ts` |  | not_null |

## mart_eia_series_latest
Latest observation per series identifier with normalized units and currency.

| Column | Description | Tests |
| --- | --- | --- |
| `tenant_id` |  | not_null |
| `series_id` |  | relationships |
| `period_start` |  | not_null |
| `value` |  | not_null |
| `currency_normalized` |  | not_null |
| `unit_normalized` |  | not_null |
| `ingest_ts` |  | not_null |
| `ingest_job_id` |  |  |
| `ingest_run_id` |  |  |
| `ingest_batch_id` |  |  |
| `period_date` |  | not_null |

## mart_fred_series_latest
Latest FRED observation per tenant and series identifier.

| Column | Description | Tests |
| --- | --- | --- |
| `tenant_id` |  | not_null |
| `series_id` |  | not_null |
| `obs_date` |  | not_null |
| `ingest_ts` |  | not_null |
