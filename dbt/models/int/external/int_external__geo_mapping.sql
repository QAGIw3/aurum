-- External geo dimension build
-- Maps provider-specific geo codes to canonical geographies
-- Maintains referential integrity with external observations

WITH provider_geo_mapping AS (
    -- ISO provider mappings based on known provider conventions
    SELECT
        'PJM' as provider,
        zone as provider_geo_code,
        CASE
            WHEN zone = 'AECO' THEN 'PA'
            WHEN zone = 'AEP' THEN 'OH'
            WHEN zone = 'APS' THEN 'AZ'
            WHEN zone = 'ATSI' THEN 'OH'
            WHEN zone = 'BGE' THEN 'MD'
            WHEN zone = 'COMED' THEN 'IL'
            WHEN zone = 'DAYTON' THEN 'OH'
            WHEN zone = 'DEOK' THEN 'OH'
            WHEN zone = 'DOM' THEN 'VA'
            WHEN zone = 'DPL' THEN 'DE'
            WHEN zone = 'DUQ' THEN 'PA'
            WHEN zone = 'EKPC' THEN 'KY'
            WHEN zone = 'JCPL' THEN 'NJ'
            WHEN zone = 'METED' THEN 'PA'
            WHEN zone = 'OVEC' THEN 'OH'
            WHEN zone = 'PECO' THEN 'PA'
            WHEN zone = 'PENELEC' THEN 'PA'
            WHEN zone = 'PEPCO' THEN 'MD'
            WHEN zone = 'PPL' THEN 'PA'
            WHEN zone = 'PSEG' THEN 'NJ'
            WHEN zone = 'RECO' THEN 'NJ'
            ELSE 'US'  -- Default to national level
        END as state_code,
        'US' as country_code,
        zone as zone_name,
        'ISO_ZONE' as geography_type
    FROM {{ source('iceberg_raw', 'curve_landing') }}
    WHERE iso = 'PJM'
    GROUP BY zone

    UNION ALL

    -- CAISO mappings
    SELECT
        'CAISO' as provider,
        zone as provider_geo_code,
        CASE
            WHEN zone = 'PGAE' THEN 'CA'
            WHEN zone = 'SCE' THEN 'CA'
            WHEN zone = 'SDGE' THEN 'CA'
            WHEN zone = 'VEA' THEN 'CA'
            WHEN zone = 'PGE' THEN 'CA'
            WHEN zone = 'NEVP' THEN 'CA'
            WHEN zone = 'WALC' THEN 'CA'
            WHEN zone = 'IID' THEN 'CA'
            ELSE 'CA'  -- Default to California
        END as state_code,
        'US' as country_code,
        zone as zone_name,
        'ISO_ZONE' as geography_type
    FROM {{ source('iceberg_raw', 'curve_landing') }}
    WHERE iso = 'CAISO'
    GROUP BY zone

    UNION ALL

    -- NYISO mappings
    SELECT
        'NYISO' as provider,
        zone as provider_geo_code,
        'NY' as state_code,
        'US' as country_code,
        zone as zone_name,
        'ISO_ZONE' as geography_type
    FROM {{ source('iceberg_raw', 'curve_landing') }}
    WHERE iso = 'NYISO'
    GROUP BY zone

    UNION ALL

    -- MISO mappings
    SELECT
        'MISO' as provider,
        zone as provider_geo_code,
        'US' as state_code,  -- MISO spans multiple states, use national
        'US' as country_code,
        zone as zone_name,
        'ISO_ZONE' as geography_type
    FROM {{ source('iceberg_raw', 'curve_landing') }}
    WHERE iso = 'MISO'
    GROUP BY zone

    UNION ALL

    -- SPP mappings
    SELECT
        'SPP' as provider,
        zone as provider_geo_code,
        'US' as state_code,  -- SPP spans multiple states, use national
        'US' as country_code,
        zone as zone_name,
        'ISO_ZONE' as geography_type
    FROM {{ source('iceberg_raw', 'curve_landing') }}
    WHERE iso = 'SPP'
    GROUP BY zone

    UNION ALL

    -- AESO mappings
    SELECT
        'AESO' as provider,
        'Alberta' as provider_geo_code,
        'AB' as state_code,
        'CA' as country_code,
        'Alberta' as zone_name,
        'PROVINCE' as geography_type
),

canonical_geo_mapping AS (
    -- Join provider mappings with canonical geographies
    SELECT
        pg.provider,
        pg.provider_geo_code,
        pg.state_code,
        pg.country_code,
        pg.zone_name,
        pg.geography_type,
        g.region_id as canonical_region_id,
        g.region_name as canonical_region_name
    FROM provider_geo_mapping pg
    LEFT JOIN {{ ref('geographies') }} g
        ON g.region_type = 'STATE'
        AND g.region_id = pg.state_code
)

SELECT
    provider,
    provider_geo_code,
    state_code,
    country_code,
    zone_name,
    geography_type,
    canonical_region_id,
    canonical_region_name,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM canonical_geo_mapping
