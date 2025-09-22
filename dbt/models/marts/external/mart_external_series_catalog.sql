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
),
canonical AS (
    SELECT
        ranked.*,
        COALESCE(
            ranked.iso_code,
            CASE
                WHEN ranked.provider LIKE 'iso.%' THEN UPPER(SPLIT_PART(ranked.provider, '.', 2))
                WHEN ranked.provider IN ('pjm', 'miso', 'isone', 'ercot', 'spp', 'nyiso', 'caiso') THEN UPPER(ranked.provider)
                ELSE COALESCE(ranked.metadata['iso_code']::TEXT, UPPER(ranked.provider))
            END
        ) AS iso_code,
        UPPER(
            COALESCE(
                ranked.iso_market,
                ranked.metadata['iso_market']::TEXT,
                ranked.metadata['market']::TEXT,
                ranked.metadata['market_type']::TEXT,
                CASE
                    WHEN ranked.dataset_code ILIKE '%_da_%' OR ranked.dataset_code ILIKE 'da\_%' THEN 'DA'
                    WHEN ranked.dataset_code ILIKE '%_rt_%' OR ranked.dataset_code ILIKE 'rt\_%' THEN 'RT'
                    ELSE NULL
                END,
                'UNKNOWN'
            )
        ) AS iso_market,
        UPPER(
            COALESCE(
                ranked.iso_product,
                ranked.metadata['iso_product']::TEXT,
                ranked.metadata['product']::TEXT,
                CASE
                    WHEN ranked.dataset_code ILIKE '%lmp%' THEN 'LMP'
                    WHEN ranked.dataset_code ILIKE '%load%' THEN 'LOAD'
                    WHEN ranked.dataset_code ILIKE '%gen%' THEN 'GENERATION'
                    WHEN ranked.dataset_code ILIKE '%ancillary%' THEN 'ANCILLARY'
                    ELSE NULL
                END,
                'UNKNOWN'
            )
        ) AS iso_product,
        UPPER(
            COALESCE(
                ranked.iso_location_type,
                ranked.metadata['iso_contract']['location_type']::TEXT,
                ranked.metadata['iso_location_type']::TEXT,
                ranked.metadata['location_type']::TEXT,
                ranked.metadata['lmp_type']::TEXT,
                CASE
                    WHEN ranked.provider IN ('nyiso', 'iso.nyiso') THEN 'ZONE'
                    ELSE 'NODE'
                END
            )
        ) AS iso_location_type,
        COALESCE(
            ranked.iso_location_id,
            ranked.metadata['iso_location_id']::TEXT,
            ranked.metadata['location_id']::TEXT,
            ranked.geo_id,
            'UNKNOWN'
        ) AS iso_location_id,
        COALESCE(
            ranked.iso_location_name,
            ranked.metadata['iso_location_name']::TEXT,
            ranked.metadata['location_name']::TEXT,
            ranked.metadata['lmp_name']::TEXT,
            ranked.geo_id
        ) AS iso_location_name,
        COALESCE(
            ranked.iso_timezone,
            ranked.metadata['iso_timezone']::TEXT,
            CASE
                WHEN ranked.provider IN ('pjm', 'iso.pjm', 'isone', 'iso.isone', 'nyiso', 'iso.nyiso') THEN 'America/New_York'
                WHEN ranked.provider IN ('miso', 'iso.miso', 'spp', 'iso.spp', 'ercot', 'iso.ercot') THEN 'America/Chicago'
                WHEN ranked.provider IN ('caiso', 'iso.caiso') THEN 'America/Los_Angeles'
                ELSE 'UTC'
            END
        ) AS iso_timezone,
        COALESCE(
            ranked.iso_interval_minutes,
            CAST(ranked.metadata['interval_minutes']::TEXT AS INTEGER),
            CAST(ranked.metadata['iso_interval_minutes']::TEXT AS INTEGER),
            CASE
                WHEN ranked.provider IN ('ercot', 'iso.ercot') THEN 15
                WHEN ranked.provider IN ('caiso', 'iso.caiso') THEN 5
                ELSE 60
            END
        ) AS iso_interval_minutes,
        UPPER(
            COALESCE(
                ranked.iso_unit,
                ranked.metadata['iso_unit']::TEXT,
                ranked.metadata['unit']::TEXT,
                ranked.unit_code,
                'USD/MWH'
            )
        ) AS iso_unit,
        UPPER(
            COALESCE(
                ranked.iso_subject,
                ranked.metadata['iso_subject']::TEXT,
                ranked.metadata['subject']::TEXT,
                ranked.dataset_code
            )
        ) AS iso_subject,
        COALESCE(
            ranked.iso_curve_role,
            ranked.metadata['iso_curve_role']::TEXT,
            ranked.metadata['curve_role']::TEXT,
            'pricing'
        ) AS iso_curve_role
    FROM ranked
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
    iso_code,
    iso_market,
    iso_product,
    iso_location_type,
    iso_location_name,
    iso_location_id,
    iso_timezone,
    iso_interval_minutes,
    iso_unit,
    iso_subject,
    iso_curve_role,
    {{ aurum_text_hash("provider || ':' || series_id") }} AS series_catalog_sk
FROM canonical
