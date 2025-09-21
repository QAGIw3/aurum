-- Idempotent merge for external series catalog facts keyed by provider + series_id.
-- Expect a staging view or table named staging_external_series_catalog providing the canonical columns.
MERGE INTO iceberg.external.series_catalog AS target
USING staging_external_series_catalog AS source
    ON target.provider = source.provider
   AND target.series_id = source.series_id
WHEN MATCHED AND (
       target.dataset_code IS DISTINCT FROM source.dataset_code
    OR target.title IS DISTINCT FROM source.title
    OR target.description IS DISTINCT FROM source.description
    OR target.unit_code IS DISTINCT FROM source.unit_code
    OR target.frequency_code IS DISTINCT FROM source.frequency_code
    OR target.geo_id IS DISTINCT FROM source.geo_id
    OR target.status IS DISTINCT FROM source.status
    OR target.category IS DISTINCT FROM source.category
    OR target.source_url IS DISTINCT FROM source.source_url
    OR target.notes IS DISTINCT FROM source.notes
    OR target.start_ts IS DISTINCT FROM source.start_ts
    OR target.end_ts IS DISTINCT FROM source.end_ts
    OR target.last_observation_ts IS DISTINCT FROM source.last_observation_ts
    OR target.asof_date IS DISTINCT FROM source.asof_date
    OR target.tags IS DISTINCT FROM source.tags
    OR target.metadata IS DISTINCT FROM source.metadata
    OR target.version IS DISTINCT FROM source.version
    OR target.ingest_ts IS DISTINCT FROM source.ingest_ts
    OR target.updated_at IS DISTINCT FROM source.updated_at
)
THEN UPDATE SET
    dataset_code = source.dataset_code,
    title = source.title,
    description = source.description,
    unit_code = source.unit_code,
    frequency_code = source.frequency_code,
    geo_id = source.geo_id,
    status = source.status,
    category = source.category,
    source_url = source.source_url,
    notes = source.notes,
    start_ts = source.start_ts,
    end_ts = source.end_ts,
    last_observation_ts = source.last_observation_ts,
    asof_date = source.asof_date,
    tags = source.tags,
    metadata = source.metadata,
    version = source.version,
    ingest_ts = source.ingest_ts,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    provider,
    series_id,
    dataset_code,
    title,
    description,
    unit_code,
    frequency_code,
    geo_id,
    status,
    category,
    source_url,
    notes,
    start_ts,
    end_ts,
    last_observation_ts,
    asof_date,
    tags,
    metadata,
    version,
    updated_at,
    created_at,
    ingest_ts
) VALUES (
    source.provider,
    source.series_id,
    source.dataset_code,
    source.title,
    source.description,
    source.unit_code,
    source.frequency_code,
    source.geo_id,
    source.status,
    source.category,
    source.source_url,
    source.notes,
    source.start_ts,
    source.end_ts,
    source.last_observation_ts,
    source.asof_date,
    source.tags,
    source.metadata,
    source.version,
    source.updated_at,
    COALESCE(source.created_at, source.updated_at),
    source.ingest_ts
);
