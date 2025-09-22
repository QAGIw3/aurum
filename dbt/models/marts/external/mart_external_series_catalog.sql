{{ iceberg_config_dimension(sort_columns=['tenant_id', 'provider', 'series_id']) }}
{{
    config(
        materialized='table',
        schema='mart',
        alias='external_series_catalog',
        tags=['external', 'dimension', 'iceberg']
    )
}}

WITH catalog AS (
    SELECT
        tenant_id,
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
        created_at,
        updated_at,
        ingest_ts,
        version
    FROM {{ ref('stg_external__series_catalog') }}
),
geo_mapping AS (
    SELECT
        provider,
        provider_geo_code,
        canonical_region_id,
        canonical_region_name,
        geography_type,
        mapping_status
    FROM {{ ref('int_external__geo_mapping') }}
),
ranked AS (
    SELECT
        c.*, 
        geo_mapping.canonical_region_id,
        geo_mapping.canonical_region_name,
        geo_mapping.geography_type,
        geo_mapping.mapping_status
    FROM catalog c
    LEFT JOIN geo_mapping
        ON c.provider = geo_mapping.provider
       AND c.geo_id = geo_mapping.provider_geo_code
)
SELECT
    tenant_id,
    provider,
    series_id,
    dataset_code,
    title,
    description,
    unit_code,
    frequency_code,
    geo_id AS provider_geo_code,
    canonical_region_id,
    canonical_region_name,
    geography_type,
    mapping_status,
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
    created_at,
    updated_at,
    ingest_ts,
    version,
    {{ aurum_text_hash("provider || ':' || series_id") }} AS series_catalog_sk
FROM ranked
