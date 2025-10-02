# Airflow Variables Reference

Centralized reference for Airflow Variables used by Aurum DAGs. Apply via the Makefile helpers or the Airflow UI.

## Common

- `aurum_kafka_bootstrap` — Kafka bootstrap servers (e.g., `broker:29092`)
- `aurum_schema_registry` — Schema Registry URL (e.g., `http://schema-registry:8081`)
- `aurum_timescale_jdbc` — JDBC URL for Timescale (e.g., `jdbc:postgresql://timescale:5432/timeseries`)

## PJM

- `aurum_pjm_topic` — base topic for LMP (default `aurum.iso.pjm.lmp.v1`)
- `aurum_pjm_load_endpoint` — inst load endpoint
- `aurum_pjm_load_topic` — load topic (default `aurum.iso.pjm.load.v1`)
- `aurum_pjm_genmix_endpoint` — generation by fuel endpoint
- `aurum_pjm_genmix_topic` — gen mix topic (default `aurum.iso.pjm.genmix.v1`)
- `aurum_pjm_pnodes_endpoint` — PNODES endpoint
- `aurum_pjm_pnodes_topic` — PNODES topic (default `aurum.iso.pjm.pnode.v1`)
- `aurum_pjm_row_limit` — API row limit (default `10000`)

## NOAA

- `aurum_noaa_api_token` — NOAA token for CDO API
- `aurum_noaa_topic` — Kafka topic for NOAA GHCND (`aurum.ref.noaa.weather.v1`)
- `aurum_noaa_timescale_table` — Timescale table (`noaa_weather_timeseries`)

## EIA

- `aurum_eia_topic` — base Kafka topic (`aurum.ref.eia.series.v1`)
- `aurum_eia_topic_pattern` — sink pattern (`aurum\.ref\.eia\..*\.v1`)
- `aurum_eia_series_table` — Timescale table (`eia_series_timeseries`)

## FRED

- `aurum_fred_topic` — base Kafka topic (`aurum.ref.fred.series.v1`)
- `aurum_fred_topic_pattern` — sink pattern (`aurum\.ref\.fred\..*\.v1`)
- `aurum_fred_series_table` — Timescale table (`fred_series_timeseries`)

## CPI (FRED)

- `aurum_cpi_topic` — base Kafka topic (`aurum.ref.cpi.series.v1`)
- `aurum_cpi_topic_pattern` — sink pattern (`aurum\.ref\.cpi\..*\.v1`)
- `aurum_cpi_series_table` — Timescale table (`cpi_series_timeseries`)

## ISO LMP (shared)

- `aurum_iso_lmp_topic_pattern` — pattern for ISO LMP topics (`aurum\.iso\..*\.lmp\.v1`)

## Helpers

- Print variables to apply: `make airflow-print-vars`
- Apply variables: `make airflow-apply-vars`
- List variables: `make airflow-list-vars` or `make airflow-list-aurum-vars`

