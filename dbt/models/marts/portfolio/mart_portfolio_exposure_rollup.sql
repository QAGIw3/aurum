-- Portfolio exposure rollup by hierarchical levels (node, hub, zone, BA, ISO)
-- This table provides consolidated exposure views across geographic and organizational boundaries

WITH position_exposures AS (
    SELECT
        position_id,
        curve_key,
        location_id,
        quantity,
        price,
        quantity * price as exposure_amount,
        'USD' as exposure_currency,
        -- Risk metrics (simplified - would be calculated by the Python engine)
        exposure_amount * 0.01 as delta,
        0.0 as gamma,
        0.0 as vega,
        0.0 as theta,
        CASE
            WHEN curve_key LIKE '%LMP%' THEN 'price'
            WHEN curve_key LIKE '%LOAD%' THEN 'load'
            WHEN curve_key LIKE '%GEN%' THEN 'generation'
            ELSE 'other'
        END as position_type
    FROM {{ ref('stg_portfolio_positions') }}
    WHERE position_date = CURRENT_DATE
),

node_exposures AS (
    SELECT
        n.location_id as entity_id,
        'node' as entity_type,
        n.iso_code,
        SUM(pe.exposure_amount) as exposure_amount,
        SUM(pe.delta) as delta,
        SUM(pe.gamma) as gamma,
        SUM(pe.vega) as vega,
        SUM(pe.theta) as theta,
        ARRAY_AGG(pe.position_id) as position_ids,
        COUNT(*) as num_positions,
        CURRENT_DATE as as_of_date
    FROM position_exposures pe
    LEFT JOIN {{ ref('stg_isone_nodes') }} n
        ON pe.location_id = n.location_id AND n.iso_code = 'ISONE'
    GROUP BY n.location_id, n.iso_code

    UNION ALL

    -- Add similar logic for other ISOs when data is available
    SELECT
        location_id as entity_id,
        'node' as entity_type,
        'PJM' as iso_code,
        exposure_amount,
        delta,
        gamma,
        vega,
        theta,
        ARRAY[position_id] as position_ids,
        1 as num_positions,
        CURRENT_DATE as as_of_date
    FROM position_exposures
    WHERE curve_key LIKE '%PJM%'  -- Placeholder for PJM positions
),

zone_exposures AS (
    SELECT
        n.zone as entity_id,
        'zone' as entity_type,
        n.iso_code,
        SUM(ne.exposure_amount) as exposure_amount,
        SUM(ne.delta) as delta,
        SUM(ne.gamma) as gamma,
        SUM(ne.vega) as vega,
        SUM(ne.theta) as theta,
        ARRAY_AGG(DISTINCT pe_id) as position_ids,
        SUM(ne.num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM node_exposures ne
    LEFT JOIN {{ ref('stg_isone_nodes') }} n
        ON ne.entity_id = n.location_id AND ne.iso_code = n.iso_code
    GROUP BY n.zone, n.iso_code
),

ba_exposures AS (
    SELECT
        'NEPOOL' as entity_id,  -- Simplified for ISO-NE
        'ba' as entity_type,
        'ISONE' as iso_code,
        SUM(ze.exposure_amount) as exposure_amount,
        SUM(ze.delta) as delta,
        SUM(ze.gamma) as gamma,
        SUM(ze.vega) as vega,
        SUM(ze.theta) as theta,
        ARRAY_AGG(DISTINCT pe_id) as position_ids,
        SUM(ze.num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM zone_exposures ze
    WHERE ze.iso_code = 'ISONE'
    GROUP BY ze.iso_code
),

iso_exposures AS (
    SELECT
        iso_code as entity_id,
        'iso' as entity_type,
        iso_code,
        SUM(be.exposure_amount) as exposure_amount,
        SUM(be.delta) as delta,
        SUM(be.gamma) as gamma,
        SUM(be.vega) as vega,
        SUM(be.theta) as theta,
        ARRAY_AGG(DISTINCT pe_id) as position_ids,
        SUM(be.num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM ba_exposures be
    GROUP BY iso_code

    UNION ALL

    SELECT
        'ISONE' as entity_id,
        'iso' as entity_type,
        'ISONE' as iso_code,
        SUM(ne.exposure_amount) as exposure_amount,
        SUM(ne.delta) as delta,
        SUM(ne.gamma) as gamma,
        SUM(ne.vega) as vega,
        SUM(ne.theta) as theta,
        ARRAY_AGG(DISTINCT pe_id) as position_ids,
        SUM(ne.num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM node_exposures ne
    WHERE ne.iso_code = 'ISONE'
    GROUP BY ne.iso_code
),

total_exposure AS (
    SELECT
        'TOTAL' as entity_id,
        'total' as entity_type,
        'ALL' as iso_code,
        SUM(exposure_amount) as exposure_amount,
        SUM(delta) as delta,
        SUM(gamma) as gamma,
        SUM(vega) as vega,
        SUM(theta) as theta,
        ARRAY_AGG(DISTINCT pe_id) as position_ids,
        SUM(num_positions) as num_positions,
        CURRENT_DATE as as_of_date
    FROM iso_exposures
),

all_exposures AS (
    SELECT * FROM node_exposures
    UNION ALL
    SELECT * FROM zone_exposures
    UNION ALL
    SELECT * FROM ba_exposures
    UNION ALL
    SELECT * FROM iso_exposures
    UNION ALL
    SELECT * FROM total_exposure
)

SELECT
    {{ aurum_text_hash("entity_id || ':' || entity_type || ':' || iso_code || ':' || cast(as_of_date as varchar)") }} as exposure_sk,
    entity_id,
    entity_type,
    iso_code,
    exposure_amount,
    exposure_currency,
    delta,
    gamma,
    vega,
    theta,
    -- Risk metrics (placeholder - would be calculated by Python engine)
    exposure_amount * 0.02 as var_95,
    exposure_amount * 0.03 as var_99,
    exposure_amount * 0.1 as max_drawdown,
    position_ids,
    num_positions,
    as_of_date,
    CURRENT_TIMESTAMP as created_at
FROM all_exposures

ORDER BY
    CASE entity_type
        WHEN 'node' THEN 1
        WHEN 'zone' THEN 2
        WHEN 'ba' THEN 3
        WHEN 'iso' THEN 4
        WHEN 'total' THEN 5
    END,
    entity_id
