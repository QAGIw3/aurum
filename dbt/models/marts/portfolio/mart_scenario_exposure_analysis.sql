-- Scenario-based exposure analysis combining portfolio positions with scenario outcomes
-- This table shows how different scenarios impact portfolio exposures across the hierarchy

WITH scenario_outcomes AS (
    SELECT
        scenario_id,
        curve_key,
        tenor_label,
        asof_date,
        mid as scenario_price
    FROM {{ ref('mart_scenario_output') }}
    WHERE asof_date >= CURRENT_DATE - INTERVAL '30 days'
),

base_prices AS (
    SELECT
        curve_key,
        AVG(mid) as base_price
    FROM {{ ref('mart_curve_latest') }}
    GROUP BY curve_key
),

position_impacts AS (
    SELECT
        pe.position_id,
        pe.curve_key,
        pe.quantity,
        pe.location_id,
        pe.price as base_position_price,
        pe.exposure_amount as base_exposure,

        -- Scenario impact
        COALESCE(so.scenario_price, bp.base_price) as scenario_position_price,
        pe.quantity * COALESCE(so.scenario_price, bp.base_price) as scenario_exposure,
        pe.quantity * COALESCE(so.scenario_price, bp.base_price) - pe.exposure_amount as exposure_delta,

        so.scenario_id,
        s.name as scenario_name,
        s.description as scenario_description

    FROM {{ ref('stg_portfolio_positions') }} pe
    LEFT JOIN scenario_outcomes so
        ON pe.curve_key = so.curve_key
    LEFT JOIN base_prices bp
        ON pe.curve_key = bp.curve_key
    LEFT JOIN {{ ref('mart_scenario_latest') }} s
        ON so.scenario_id = s.scenario_id
    WHERE pe.position_date = CURRENT_DATE
),

node_scenario_exposures AS (
    SELECT
        n.location_id as entity_id,
        'node' as entity_type,
        n.iso_code,
        pi.scenario_id,
        pi.scenario_name,
        SUM(pi.scenario_exposure) as scenario_exposure,
        SUM(pi.base_exposure) as base_exposure,
        SUM(pi.exposure_delta) as exposure_delta,
        COUNT(*) as num_positions,
        CURRENT_DATE as as_of_date
    FROM position_impacts pi
    LEFT JOIN {{ ref('stg_isone_nodes') }} n
        ON pi.location_id = n.location_id AND n.iso_code = 'ISONE'
    GROUP BY n.location_id, n.iso_code, pi.scenario_id, pi.scenario_name

    UNION ALL

    -- Add base case (no scenario)
    SELECT
        n.location_id as entity_id,
        'node' as entity_type,
        n.iso_code,
        'BASELINE' as scenario_id,
        'Baseline' as scenario_name,
        SUM(pi.base_exposure) as scenario_exposure,
        SUM(pi.base_exposure) as base_exposure,
        0 as exposure_delta,
        COUNT(*) as num_positions,
        CURRENT_DATE as as_of_date
    FROM position_impacts pi
    LEFT JOIN {{ ref('stg_isone_nodes') }} n
        ON pi.location_id = n.location_id AND n.iso_code = 'ISONE'
    GROUP BY n.location_id, n.iso_code
),

zone_scenario_exposures AS (
    SELECT
        n.zone as entity_id,
        'zone' as entity_type,
        n.iso_code,
        nse.scenario_id,
        nse.scenario_name,
        SUM(nse.scenario_exposure) as scenario_exposure,
        SUM(nse.base_exposure) as base_exposure,
        SUM(nse.exposure_delta) as exposure_delta,
        SUM(nse.num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM node_scenario_exposures nse
    LEFT JOIN {{ ref('stg_isone_nodes') }} n
        ON nse.entity_id = n.location_id AND nse.iso_code = n.iso_code
    GROUP BY n.zone, n.iso_code, nse.scenario_id, nse.scenario_name
),

all_scenario_exposures AS (
    SELECT * FROM node_scenario_exposures
    UNION ALL
    SELECT * FROM zone_scenario_exposures

    UNION ALL

    -- Total across all scenarios
    SELECT
        'TOTAL' as entity_id,
        'total' as entity_type,
        iso_code,
        scenario_id,
        scenario_name,
        SUM(scenario_exposure) as scenario_exposure,
        SUM(base_exposure) as base_exposure,
        SUM(exposure_delta) as exposure_delta,
        SUM(num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM node_scenario_exposures
    GROUP BY iso_code, scenario_id, scenario_name
)

SELECT
    {{ aurum_text_hash("entity_id || ':' || entity_type || ':' || scenario_id || ':' || cast(as_of_date as varchar)") }} as scenario_exposure_sk,
    entity_id,
    entity_type,
    iso_code,
    scenario_id,
    scenario_name,
    scenario_exposure,
    base_exposure,
    exposure_delta,
    CASE
        WHEN base_exposure != 0 THEN (exposure_delta / base_exposure) * 100
        ELSE 0
    END as exposure_change_percent,
    num_positions,
    as_of_date,
    CURRENT_TIMESTAMP as created_at
FROM all_scenario_exposures

ORDER BY
    entity_type,
    entity_id,
    scenario_id
