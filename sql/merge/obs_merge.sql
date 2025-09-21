-- Idempotent merge for external timeseries observations.
-- Expect a staging view or table named staging_external_timeseries_observation providing the canonical columns.
MERGE INTO iceberg.external.timeseries_observation AS target
USING staging_external_timeseries_observation AS source
    ON target.provider = source.provider
   AND target.series_id = source.series_id
   AND target.ts = source.ts
   AND target.asof_date = source.asof_date
WHEN MATCHED AND (
       target.value IS DISTINCT FROM source.value
    OR target.value_raw IS DISTINCT FROM source.value_raw
    OR target.unit_code IS DISTINCT FROM source.unit_code
    OR target.geo_id IS DISTINCT FROM source.geo_id
    OR target.dataset_code IS DISTINCT FROM source.dataset_code
    OR target.frequency_code IS DISTINCT FROM source.frequency_code
    OR target.status IS DISTINCT FROM source.status
    OR target.quality_flag IS DISTINCT FROM source.quality_flag
    OR target.ingest_ts IS DISTINCT FROM source.ingest_ts
    OR target.source_event_id IS DISTINCT FROM source.source_event_id
    OR target.metadata IS DISTINCT FROM source.metadata
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    value = source.value,
    value_raw = source.value_raw,
    unit_code = source.unit_code,
    geo_id = source.geo_id,
    dataset_code = source.dataset_code,
    frequency_code = source.frequency_code,
    status = source.status,
    quality_flag = source.quality_flag,
    ingest_ts = source.ingest_ts,
    source_event_id = source.source_event_id,
    metadata = source.metadata,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    provider,
    series_id,
    ts,
    asof_date,
    value,
    value_raw,
    unit_code,
    geo_id,
    dataset_code,
    frequency_code,
    status,
    quality_flag,
    ingest_ts,
    source_event_id,
    metadata,
    updated_at
) VALUES (
    source.provider,
    source.series_id,
    source.ts,
    source.asof_date,
    source.value,
    source.value_raw,
    source.unit_code,
    source.geo_id,
    source.dataset_code,
    source.frequency_code,
    source.status,
    source.quality_flag,
    source.ingest_ts,
    source.source_event_id,
    source.metadata,
    source.updated_at
);
