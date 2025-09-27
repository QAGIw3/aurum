CREATE OR REPLACE VIEW iceberg.market.iso_lmp_unified AS
SELECT
    interval_start,
    interval_end,
    delivery_date,
    iso_code,
    market,
    location_id,
    location_name,
    location_type,
    price_total,
    price_energy,
    price_congestion,
    price_loss,
    currency,
    uom,
    settlement_point,
    source_run_id,
    ingest_ts,
    record_hash,
    metadata
FROM timescale.public.iso_lmp_timeseries;

CREATE OR REPLACE VIEW iceberg.market.iso_lmp_last_24h AS
SELECT *
FROM iceberg.market.iso_lmp_unified
WHERE interval_start >= current_timestamp - INTERVAL '24' hour;

CREATE OR REPLACE VIEW iceberg.market.iso_lmp_hourly AS
SELECT
    bucket_start AS interval_start,
    iso_code,
    location_id,
    market,
    currency,
    uom,
    price_avg,
    price_min,
    price_max,
    price_stddev,
    sample_count
FROM timescale.public.iso_lmp_agg_1h;

CREATE OR REPLACE VIEW iceberg.market.iso_lmp_daily AS
SELECT
    bucket_start AS interval_start,
    iso_code,
    location_id,
    market,
    currency,
    uom,
    price_avg,
    price_min,
    price_max,
    price_stddev,
    sample_count
FROM timescale.public.iso_lmp_agg_1d;

CREATE OR REPLACE VIEW iceberg.market.iso_lmp_negative_7d AS
SELECT *
FROM iceberg.market.iso_lmp_unified
WHERE price_total < 0
  AND interval_start >= current_timestamp - INTERVAL '7' day;

CREATE OR REPLACE VIEW iceberg.market.curve_latest_view AS
SELECT
    curve_key,
    tenor_label,
    tenor_type,
    contract_month,
    asof_date,
    currency,
    per_unit,
    mid,
    bid,
    ask,
    version_hash
FROM iceberg.market.mart_curve_latest;
