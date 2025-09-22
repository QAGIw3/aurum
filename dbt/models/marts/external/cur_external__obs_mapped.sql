{{ iceberg_config_external_observations(
    partition_fields=['asof_date', 'provider'],
    partition_granularity='day',
    sort_columns=['provider', 'series_id', 'asof_date'],
    target_file_size_mb=96,
    write_compression='ZSTD'
) }}
{{ config(
    materialized='incremental',
    schema='mart',
    alias='external_obs_mapped',
    unique_key=['curve_key', 'tenor_label', 'asof_date'],
    incremental_strategy='merge',
    on_schema_change='sync',
    tags=['external', 'observations', 'iceberg', 'timeseries']
) }}

-- Curated model for external observations mapped to curve_observation format.
--
-- Notes:
-- - Uses canonical ISO fields to set iso/market/product and spark_location.
-- - Keeps lineage explicit, including iso_code, for auditability.

WITH external_mapped_observations AS (
    SELECT
        -- Join external observations with series-curve mapping
        obs.provider,
        obs.series_id,
        obs.ts_utc as observation_ts,
        obs.asof_date,
        obs.value_usd_per_mwh as value,
        obs.value_raw,
        obs.unit_code_canonical,
        obs.unit_code_original,
        obs.frequency_code_normalized,
        obs.canonical_region_id,
        obs.canonical_region_name,
        obs.provider_geo_code,
        obs.quality_status,
        obs.conformed_at,
        obs.iso_code,
        obs.iso_market,
        obs.iso_product,
        obs.iso_location_type,
        obs.iso_location_id,
        obs.iso_location_name,
        obs.iso_timezone,
        obs.iso_interval_minutes,
        obs.iso_unit,
        obs.iso_subject,
        obs.iso_curve_role,

        -- Map external series to curve keys using series_curve_map
        scm.curve_key,

        -- Extract metadata for curve mapping
        CASE
            WHEN obs.metadata['dataset_code'] IS NOT NULL
                THEN obs.metadata['dataset_code']::TEXT
            ELSE obs.dataset_code
        END as dataset_code,

        CASE
            WHEN obs.metadata['status'] IS NOT NULL
                THEN obs.metadata['status']::TEXT
            ELSE obs.status
        END as observation_status,

        obs.ingest_ts,
        obs.source_event_id,
        obs.metadata

    FROM {{ ref('int_external__obs_conformed') }} obs
    LEFT JOIN {{ source('iceberg_market', 'series_curve_map') }} scm
        ON obs.provider = scm.external_provider
        AND obs.series_id = scm.external_series_id
        AND scm.is_active = TRUE
        AND scm.mapping_confidence >= 0.8  -- Only use high-confidence mappings
    WHERE obs.quality_status = 'VALID'  -- Only include validated observations
),

curve_formatted_observations AS (
    SELECT
        -- Transform to match curve_observation schema
        asof_date,
        'external_data' as source_file,
        'external_observation' as sheet_name,

        -- Map external provider to asset class (simplified mapping)
        CASE
            WHEN provider IN ('FRED', 'EIA') THEN 'power'
            ELSE 'other'
        END as asset_class,

        canonical_region_name as region,
        COALESCE(iso_code, provider, 'EXTERNAL') as iso,
        provider_geo_code as location,

        -- Map to market/product structure
        CASE
            WHEN upper(COALESCE(iso_market, '')) IN ('DA', 'DAM', 'DAY_AHEAD') THEN 'DAY_AHEAD'
            WHEN upper(COALESCE(iso_market, '')) IN ('RT', 'RTM', 'REAL_TIME') THEN 'REAL_TIME'
            WHEN upper(COALESCE(iso_market, '')) IN ('BALANCING', 'BAL') THEN 'BALANCING'
            ELSE 'UNKNOWN'
        END as market,

        UPPER(COALESCE(iso_product, dataset_code, 'CUSTOM')) as product,

        'DAILY' as block,  -- External data typically daily
        COALESCE(iso_location_id, provider_geo_code) as spark_location,

        'MID' as price_type,  -- External data typically midpoint
        unit_code_original as units_raw,
        'USD' as currency,
        'MWh' as per_unit,

        -- Frequency mapping
        CASE
            WHEN frequency_code_normalized = 'DAILY' THEN 'DAILY'
            WHEN frequency_code_normalized = 'MONTHLY' THEN 'MONTHLY'
            WHEN frequency_code_normalized = 'QUARTERLY' THEN 'QUARTERLY'
            ELSE 'CUSTOM'
        END as tenor_type,

        -- Create contract month from observation date
        DATE_TRUNC('month', observation_ts)::DATE as contract_month,

        -- Create tenor label (for daily data, use date)
        TO_CHAR(observation_ts, 'YYYY-MM-DD') as tenor_label,

        -- Use the value as MID price
        value as mid,
        NULL as bid,  -- No bid/ask for external data
        NULL as ask,

        -- Use curve_key from mapping, or create synthetic key
        COALESCE(curve_key, 'EXTERNAL_' || provider || '_' || series_id) as curve_key,

        -- Create version hash from observation metadata
        MD5(
            provider || '|' ||
            series_id || '|' ||
            TO_CHAR(observation_ts, 'YYYY-MM-DD HH24:MI:SS') || '|' ||
            COALESCE(value::TEXT, '')
        ) as version_hash,

        ingest_ts as _ingest_ts,

        -- Lineage tracking
        'source=iceberg.external.timeseries_observation|provider=' || provider || '|series=' || series_id || '|iso=' || COALESCE(iso_code, 'UNKNOWN') || '|table=iceberg.market.curve_observation' as lineage_tags

    FROM external_mapped_observations
    WHERE value IS NOT NULL  -- Only include observations with valid values
      AND asof_date IS NOT NULL
      AND observation_ts IS NOT NULL
{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  AND ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
)

SELECT * FROM curve_formatted_observations
