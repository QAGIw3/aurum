-- Timescale-backed views for ISO system and zonal load observations

CREATE OR REPLACE VIEW public.iso_load_unified AS
SELECT
    interval_start,
    interval_end,
    iso_code,
    area,
    interval_minutes,
    mw,
    ingest_ts,
    metadata
FROM public.load_timeseries;

CREATE OR REPLACE VIEW public.iso_load_last_24h AS
SELECT *
FROM public.iso_load_unified
WHERE interval_start >= now() - INTERVAL '24 hours';

CREATE OR REPLACE VIEW public.iso_load_hourly AS
SELECT
    bucket_start AS interval_start,
    iso_code,
    area,
    mw_avg,
    mw_min,
    mw_max,
    mw_stddev,
    sample_count
FROM public.iso_load_agg_1h;

CREATE OR REPLACE VIEW public.iso_load_daily AS
SELECT
    bucket_start AS interval_start,
    iso_code,
    area,
    mw_avg,
    mw_min,
    mw_max,
    mw_stddev,
    sample_count
FROM public.iso_load_agg_1d;

CREATE OR REPLACE VIEW public.iso_load_peaks_7d AS
SELECT *
FROM public.iso_load_unified
WHERE interval_start >= now() - INTERVAL '7 days'
ORDER BY mw DESC
LIMIT 500;
