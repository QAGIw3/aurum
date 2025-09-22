-- Stress test results and scenario impact analysis
-- This table stores the results of stress test scenarios for reporting and analysis

WITH stress_test_scenarios AS (
    SELECT
        scenario_id,
        template_id,
        scenario_name,
        scenario_type,
        severity,
        duration_hours,
        probability,
        impact_multiplier,
        affected_regions,
        created_at as scenario_created_at
    FROM {{ ref('stg_stress_test_scenarios') }}
),

scenario_impacts AS (
    SELECT
        scenario_id,
        affected_curve_key,
        price_impact_multiplier,
        volume_impact_multiplier,
        confidence_level,
        created_at as impact_calculated_at
    FROM {{ ref('stg_scenario_impacts') }}
),

risk_metrics AS (
    SELECT
        scenario_id,
        var_95,
        var_99,
        max_drawdown,
        portfolio_impact,
        affected_positions_count
    FROM {{ ref('stg_scenario_risk_metrics') }}
),

combined_results AS (
    SELECT
        sts.scenario_id,
        sts.template_id,
        sts.scenario_name,
        sts.scenario_type,
        sts.severity,
        sts.duration_hours,
        sts.probability,
        sts.impact_multiplier,
        sts.affected_regions,
        sts.scenario_created_at,

        -- Impact details
        si.affected_curve_key,
        si.price_impact_multiplier,
        si.volume_impact_multiplier,
        si.confidence_level,
        si.impact_calculated_at,

        -- Risk metrics
        rm.var_95,
        rm.var_99,
        rm.max_drawdown,
        rm.portfolio_impact,
        rm.affected_positions_count,

        -- Calculated fields
        CASE
            WHEN sts.scenario_type = 'policy' THEN 'Policy Shock'
            WHEN sts.scenario_type = 'outage' THEN 'Equipment Outage'
            WHEN sts.scenario_type = 'weather' THEN 'Weather Event'
            ELSE 'Other'
        END as scenario_category_name,

        -- Risk classification
        CASE
            WHEN rm.portfolio_impact > 1000000 THEN 'Critical'
            WHEN rm.portfolio_impact > 500000 THEN 'High'
            WHEN rm.portfolio_impact > 100000 THEN 'Medium'
            ELSE 'Low'
        END as risk_level,

        -- Expected loss calculation
        sts.probability * rm.portfolio_impact as expected_loss,

        -- Recovery time classification
        CASE
            WHEN sts.duration_hours <= 24 THEN 'Short-term'
            WHEN sts.duration_hours <= 168 THEN 'Medium-term'
            ELSE 'Long-term'
        END as duration_category

    FROM stress_test_scenarios sts
    LEFT JOIN scenario_impacts si ON sts.scenario_id = si.scenario_id
    LEFT JOIN risk_metrics rm ON sts.scenario_id = rm.scenario_id
)

SELECT
    {{ aurum_text_hash("scenario_id || ':' || COALESCE(affected_curve_key, 'TOTAL') || ':' || cast(scenario_created_at as varchar)") }} as stress_test_result_sk,
    scenario_id,
    template_id,
    scenario_name,
    scenario_type,
    scenario_category_name,
    severity,
    duration_hours,
    duration_category,
    probability,
    impact_multiplier,
    affected_regions,
    affected_curve_key,
    price_impact_multiplier,
    volume_impact_multiplier,
    confidence_level,
    var_95,
    var_99,
    max_drawdown,
    portfolio_impact,
    expected_loss,
    affected_positions_count,
    risk_level,
    scenario_created_at,
    impact_calculated_at,
    CURRENT_TIMESTAMP as created_at
FROM combined_results

ORDER BY
    scenario_created_at DESC,
    risk_level,
    portfolio_impact DESC
