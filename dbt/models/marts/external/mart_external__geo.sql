-- External geography dimension table
-- Canonical geography mappings for external data providers

WITH distinct_geo_mappings AS (
    SELECT
        provider,
        provider_geo_code,
        state_code,
        country_code,
        zone_name,
        geography_type,
        canonical_region_id,
        canonical_region_name,
        ROW_NUMBER() OVER (
            PARTITION BY provider, provider_geo_code
            ORDER BY created_at DESC
        ) as rn
    FROM {{ ref('int_external__geo_mapping') }}
)

SELECT
    -- Create a surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'provider',
        'provider_geo_code'
    ]) }} as geo_key,

    provider,
    provider_geo_code,
    state_code,
    country_code,
    zone_name,
    geography_type,
    canonical_region_id,
    canonical_region_name,

    -- Add metadata
    CASE
        WHEN canonical_region_id IS NOT NULL THEN 'MAPPED'
        ELSE 'UNMAPPED'
    END as mapping_status,

    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM distinct_geo_mappings
WHERE rn = 1
