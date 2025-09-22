-- Conformance layer for external observations
-- This model standardizes values across providers through unit conversions,
-- frequency normalization, timezone alignment, and value validation

WITH raw_observations AS (
    SELECT
        tenant_id,
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
        -- Extract metadata for additional processing
        metadata['timezone']::TEXT as source_timezone,
        metadata['currency']::TEXT as currency_code,
        metadata['original_unit']::TEXT as original_unit_code,
        iso_code,
        iso_market,
        iso_product,
        iso_location_type,
        iso_location_id,
        iso_location_name,
        iso_timezone,
        iso_interval_minutes,
        iso_unit,
        iso_subject,
        iso_curve_role
    FROM {{ ref('stg_external__obs') }}
),

standardized_observations AS (
    SELECT
        tenant_id,
        provider,
        series_id,
        ts,
        asof_date,
        -- Apply unit conversion to canonical USD/MWh
        {{ convert_unit('value', 'COALESCE(original_unit_code, unit_code)', "'USD/MWh'") }} as value_usd_per_mwh,

        -- Keep raw value for reference
        value as value_raw,
        value_raw as value_raw_string,

        -- Normalize frequency to canonical format
        {{ normalize_frequency('frequency_code') }} as frequency_code_normalized,

        -- Normalize timezone to UTC
        {{ normalize_timezone('ts', 'COALESCE(source_timezone, \'UTC\')') }} as ts_utc,

        -- Validate value ranges and set to NULL if implausible
        {{ validate_value_range('value', 'provider', 'COALESCE(original_unit_code, unit_code)') }} as value_validated,

        -- Canonical units
        'USD/MWh' as unit_code_canonical,
        unit_code as unit_code_original,

        -- Geo and other metadata with canonical mapping
        geo_mapping.canonical_region_id,
        geo_mapping.canonical_region_name,
        geo_id as provider_geo_code,
        dataset_code,
        status,
        quality_flag,
        ingest_ts,
        source_event_id,
        metadata,
        COALESCE(raw_observations.iso_code, metadata['iso_code']::TEXT, provider) as iso_code,
        COALESCE(raw_observations.iso_market, metadata['iso_market']::TEXT, 'UNKNOWN') as iso_market,
        COALESCE(raw_observations.iso_product, metadata['iso_product']::TEXT, dataset_code) as iso_product,
        COALESCE(raw_observations.iso_location_type, metadata['iso_location_type']::TEXT, 'NODE') as iso_location_type,
        COALESCE(raw_observations.iso_location_id, metadata['iso_location_id']::TEXT, provider_geo_code) as iso_location_id,
        COALESCE(raw_observations.iso_location_name, metadata['iso_location_name']::TEXT, canonical_region_name) as iso_location_name,
        COALESCE(raw_observations.iso_timezone, metadata['iso_timezone']::TEXT, source_timezone, 'UTC') as iso_timezone,
        COALESCE(
            raw_observations.iso_interval_minutes,
            TRY_CAST(metadata['iso_interval_minutes'] AS integer),
            60
        ) as iso_interval_minutes,
        COALESCE(raw_observations.iso_unit, metadata['iso_unit']::TEXT, unit_code_canonical) as iso_unit,
        COALESCE(raw_observations.iso_subject, metadata['iso_subject']::TEXT, dataset_code) as iso_subject,
        COALESCE(raw_observations.iso_curve_role, metadata['iso_curve_role']::TEXT, 'pricing') as iso_curve_role,

        -- Add data quality flags
        CASE
            WHEN value IS NULL THEN 'NULL_VALUE'
            WHEN value = 0 THEN 'ZERO_VALUE'
            ELSE 'VALID'
        END as quality_status,

        -- Add processing timestamp
        CURRENT_TIMESTAMP as conformed_at

    FROM raw_observations
    LEFT JOIN {{ ref('int_external__geo_mapping') }} geo_mapping
        ON raw_observations.provider = geo_mapping.provider
        AND raw_observations.geo_id = geo_mapping.provider_geo_code
    WHERE value IS NOT NULL  -- Filter out null values
)

SELECT * FROM standardized_observations
