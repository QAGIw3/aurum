# Data Dictionary Publication
Markdown data dictionary generated from dbt documentation and exposures metadata.

**Owner:** Data Platform (data-platform@aurum.local)

## dim_iso
Canonical ISO/RTO dimension with metadata used by curve facts.

| Column | Description | Tests |
| --- | --- | --- |
| `iso_sk` |  | not_null, unique |
| `iso_code` |  | not_null, unique |
| `default_timezone` |  | not_null |

## dim_market
ISO market dimension aligning run types with settlement intervals.

| Column | Description | Tests |
| --- | --- | --- |
| `market_sk` |  | not_null, unique |
| `iso_sk` |  | not_null, relationships |
| `settlement_interval_minutes` |  | not_null |

## fct_curve_observation
Canonical curve fact table joining dimensions for downstream marts.

| Column | Description | Tests |
| --- | --- | --- |
| `curve_key` |  | not_null |
| `tenor_label` |  | not_null |
| `asof_date` |  | not_null, relationships |
| `iso_sk` |  | relationships |
| `market_sk` |  | relationships |
| `product_sk` |  | relationships |
| `block_sk` |  | relationships |
| `mid` |  | not_null |
| `currency` |  | not_null |
| `lineage_tags` | Pipe-delimited lineage tag string (source|topic|table|fact). | not_null |

## mart_scenario_output
Latest scenario outputs materialized as a Trino view.

| Column | Description | Tests |
| --- | --- | --- |
| `scenario_id` |  | not_null, relationships, relationships, relationships |
| `run_id` | Identifier of the scenario run that produced the output. | not_null |
| `tenant_id` |  | not_null, relationships |
| `value` |  | not_null |
| `metric_currency` | Currency code parsed from the metric unit metadata. |  |
| `metric_unit_denominator` | Unit denominator parsed from the metric unit metadata (e.g., MWh). |  |
