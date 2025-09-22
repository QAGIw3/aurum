-- Comprehensive scenario analytics summary for Superset workbooks
-- This table provides consolidated scenario performance and attribution metrics

WITH scenario_base AS (
    SELECT
        s.id as scenario_id,
        s.name as scenario_name,
        s.description as scenario_description,
        s.status as scenario_status,
        s.created_at as scenario_created_at,
        s.tenant_id,
        s.assumptions,
        s.parameters,

        -- Extract curve families from metadata
        COALESCE(s.metadata->>'curve_families', '[]')::json as curve_families,

        -- Scenario type detection
        CASE
            WHEN s.name ~* 'forecast' OR s.parameters->>'scenario_type' = 'forecasting' THEN 'forecasting'
            WHEN s.name ~* 'stress' OR s.parameters->>'scenario_type' = 'stress_test' THEN 'stress_test'
            WHEN s.name ~* 'monte' OR s.parameters->>'scenario_type' = 'monte_carlo' THEN 'monte_carlo'
            WHEN s.name ~* 'shock' OR s.name ~* 'outage' THEN 'shock_outage'
            ELSE 'other'
        END as scenario_category,

        -- Time-based features
        EXTRACT(hour FROM s.created_at) as created_hour,
        EXTRACT(dayofweek FROM s.created_at) as created_day_of_week,
        EXTRACT(month FROM s.created_at) as created_month,
        EXTRACT(quarter FROM s.created_at) as created_quarter,
        EXTRACT(year FROM s.created_at) as created_year

    FROM {{ ref('mart_scenario_latest') }} s
    WHERE s.status IN ('completed', 'succeeded')
),

scenario_runs AS (
    SELECT
        sr.scenario_id,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN sr.status = 'succeeded' THEN 1 END) as successful_runs,
        COUNT(CASE WHEN sr.status = 'failed' THEN 1 END) as failed_runs,
        AVG(CASE WHEN sr.duration_seconds IS NOT NULL THEN sr.duration_seconds END) as avg_duration_seconds,
        MIN(sr.created_at) as first_run_at,
        MAX(sr.created_at) as last_run_at,

        -- Performance metrics
        AVG(CASE WHEN sr.duration_seconds IS NOT NULL THEN sr.duration_seconds END) as avg_execution_time,
        STDDEV(CASE WHEN sr.duration_seconds IS NOT NULL THEN sr.duration_seconds END) as std_execution_time

    FROM {{ ref('mart_scenario_run_latest') }} sr
    GROUP BY sr.scenario_id
),

scenario_outputs AS (
    SELECT
        so.scenario_id,
        so.metric_name,
        COUNT(*) as output_count,
        AVG(so.value) as avg_value,
        STDDEV(so.value) as std_value,
        MIN(so.value) as min_value,
        MAX(so.value) as max_value,
        MIN(so.timestamp) as first_output_at,
        MAX(so.timestamp) as last_output_at,

        -- Metric categorization
        CASE
            WHEN so.metric_name LIKE '%price%' THEN 'price'
            WHEN so.metric_name LIKE '%load%' OR so.metric_name LIKE '%demand%' THEN 'load_demand'
            WHEN so.metric_name LIKE '%generation%' OR so.metric_name LIKE '%supply%' THEN 'generation'
            WHEN so.metric_name LIKE '%forecast%' THEN 'forecast'
            WHEN so.metric_name LIKE '%error%' OR so.metric_name LIKE '%mape%' THEN 'error_metrics'
            WHEN so.metric_name LIKE '%var%' OR so.metric_name LIKE '%risk%' THEN 'risk_metrics'
            ELSE 'other'
        END as metric_category

    FROM {{ ref('mart_scenario_output') }} so
    GROUP BY so.scenario_id, so.metric_name
),

scenario_aggregated_outputs AS (
    SELECT
        scenario_id,
        COUNT(DISTINCT metric_name) as unique_metrics,
        COUNT(*) as total_outputs,
        SUM(CASE WHEN metric_category = 'price' THEN 1 ELSE 0 END) as price_metrics_count,
        SUM(CASE WHEN metric_category = 'load_demand' THEN 1 ELSE 0 END) as load_metrics_count,
        SUM(CASE WHEN metric_category = 'generation' THEN 1 ELSE 0 END) as generation_metrics_count,
        SUM(CASE WHEN metric_category = 'forecast' THEN 1 ELSE 0 END) as forecast_metrics_count,
        SUM(CASE WHEN metric_category = 'error_metrics' THEN 1 ELSE 0 END) as error_metrics_count,
        SUM(CASE WHEN metric_category = 'risk_metrics' THEN 1 ELSE 0 END) as risk_metrics_count
    FROM scenario_outputs
    GROUP BY scenario_id
),

curve_family_analysis AS (
    SELECT
        sb.scenario_id,
        cfm.curve_family,
        COUNT(*) as curve_family_count
    FROM scenario_base sb
    CROSS JOIN LATERAL json_array_elements_text(sb.curve_families) as cfm(curve_family)
    GROUP BY sb.scenario_id, cfm.curve_family
),

forecast_performance AS (
    SELECT
        sb.scenario_id,
        AVG(CASE WHEN so.metric_name LIKE '%mape%' THEN so.avg_value END) as avg_mape,
        AVG(CASE WHEN so.metric_name LIKE '%smape%' THEN so.avg_value END) as avg_smape,
        AVG(CASE WHEN so.metric_name LIKE '%rmse%' THEN so.avg_value END) as avg_rmse,
        MIN(CASE WHEN so.metric_name LIKE '%r2%' THEN so.avg_value END) as best_r2_score
    FROM scenario_base sb
    LEFT JOIN scenario_outputs so ON sb.scenario_id = so.scenario_id
    WHERE so.metric_category = 'error_metrics'
    GROUP BY sb.scenario_id
),

portfolio_impacts AS (
    SELECT
        sb.scenario_id,
        COALESCE(AVG(CASE WHEN so.metric_name LIKE '%portfolio%' THEN so.avg_value END), 0) as avg_portfolio_impact,
        COALESCE(AVG(CASE WHEN so.metric_name LIKE '%var_95%' THEN so.avg_value END), 0) as avg_var_95,
        COALESCE(AVG(CASE WHEN so.metric_name LIKE '%var_99%' THEN so.avg_value END), 0) as avg_var_99,
        COALESCE(AVG(CASE WHEN so.metric_name LIKE '%max_drawdown%' THEN so.avg_value END), 0) as avg_max_drawdown
    FROM scenario_base sb
    LEFT JOIN scenario_outputs so ON sb.scenario_id = so.scenario_id
    WHERE so.metric_category = 'risk_metrics'
    GROUP BY sb.scenario_id
),

combined_analytics AS (
    SELECT
        sb.scenario_id,
        sb.scenario_name,
        sb.scenario_description,
        sb.scenario_category,
        sb.scenario_status,
        sb.tenant_id,
        sb.scenario_created_at,
        sb.created_hour,
        sb.created_day_of_week,
        sb.created_month,
        sb.created_quarter,
        sb.created_year,

        -- Run statistics
        COALESCE(sr.total_runs, 0) as total_runs,
        COALESCE(sr.successful_runs, 0) as successful_runs,
        COALESCE(sr.failed_runs, 0) as failed_runs,
        COALESCE(sr.avg_duration_seconds, 0) as avg_duration_seconds,
        COALESCE(sr.first_run_at, sb.scenario_created_at) as first_run_at,
        COALESCE(sr.last_run_at, sb.scenario_created_at) as last_run_at,

        -- Output statistics
        COALESCE(sao.unique_metrics, 0) as unique_metrics,
        COALESCE(sao.total_outputs, 0) as total_outputs,
        COALESCE(sao.price_metrics_count, 0) as price_metrics_count,
        COALESCE(sao.load_metrics_count, 0) as load_metrics_count,
        COALESCE(sao.generation_metrics_count, 0) as generation_metrics_count,
        COALESCE(sao.forecast_metrics_count, 0) as forecast_metrics_count,
        COALESCE(sao.error_metrics_count, 0) as error_metrics_count,
        COALESCE(sao.risk_metrics_count, 0) as risk_metrics_count,

        -- Performance metrics
        COALESCE(fp.avg_mape, 0) as avg_mape,
        COALESCE(fp.avg_smape, 0) as avg_smape,
        COALESCE(fp.avg_rmse, 0) as avg_rmse,
        COALESCE(fp.best_r2_score, 0) as best_r2_score,

        -- Portfolio impact
        COALESCE(pi.avg_portfolio_impact, 0) as avg_portfolio_impact,
        COALESCE(pi.avg_var_95, 0) as avg_var_95,
        COALESCE(pi.avg_var_99, 0) as avg_var_99,
        COALESCE(pi.avg_max_drawdown, 0) as avg_max_drawdown,

        -- Success rate
        CASE
            WHEN sr.total_runs > 0 THEN (sr.successful_runs::float / sr.total_runs::float) * 100
            ELSE 100.0
        END as success_rate_percent,

        -- Scenario complexity score (based on metrics and curve families)
        (COALESCE(sao.unique_metrics, 0) * 2 +
         COALESCE(sao.total_outputs, 0) * 0.1 +
         COALESCE(sr.total_runs, 0) * 0.5) as complexity_score,

        -- Risk score (based on portfolio impact and VaR)
        CASE
            WHEN pi.avg_portfolio_impact > 1000000 THEN 10
            WHEN pi.avg_portfolio_impact > 500000 THEN 7
            WHEN pi.avg_portfolio_impact > 100000 THEN 4
            WHEN pi.avg_portfolio_impact > 50000 THEN 2
            ELSE 1
        END as risk_score,

        -- Performance classification
        CASE
            WHEN fp.avg_mape < 5 THEN 'Excellent'
            WHEN fp.avg_mape < 10 THEN 'Good'
            WHEN fp.avg_mape < 20 THEN 'Fair'
            WHEN fp.avg_mape < 50 THEN 'Poor'
            ELSE 'Very Poor'
        END as forecast_performance_class,

        -- Scenario health score (0-100)
        CASE
            WHEN sr.total_runs > 0 AND fp.avg_mape IS NOT NULL THEN
                (success_rate_percent * 0.4 +
                 CASE WHEN fp.avg_mape < 50 THEN 100 - fp.avg_mape ELSE 0 END * 0.4 +
                 (100 - risk_score * 10) * 0.2)
            ELSE 50.0
        END as scenario_health_score

    FROM scenario_base sb
    LEFT JOIN scenario_runs sr ON sb.scenario_id = sr.scenario_id
    LEFT JOIN scenario_aggregated_outputs sao ON sb.scenario_id = sao.scenario_id
    LEFT JOIN forecast_performance fp ON sb.scenario_id = fp.scenario_id
    LEFT JOIN portfolio_impacts pi ON sb.scenario_id = pi.scenario_id
)

SELECT
    {{ aurum_text_hash("scenario_id || ':' || cast(scenario_created_at as varchar)") }} as scenario_analytics_sk,
    scenario_id,
    scenario_name,
    scenario_description,
    scenario_category,
    scenario_status,
    tenant_id,
    scenario_created_at,
    created_hour,
    created_day_of_week,
    created_month,
    created_quarter,
    created_year,

    -- Execution metrics
    total_runs,
    successful_runs,
    failed_runs,
    success_rate_percent,
    avg_duration_seconds,
    first_run_at,
    last_run_at,

    -- Output metrics
    unique_metrics,
    total_outputs,
    price_metrics_count,
    load_metrics_count,
    generation_metrics_count,
    forecast_metrics_count,
    error_metrics_count,
    risk_metrics_count,

    -- Performance metrics
    avg_mape,
    avg_smape,
    avg_rmse,
    best_r2_score,
    forecast_performance_class,

    -- Portfolio impact
    avg_portfolio_impact,
    avg_var_95,
    avg_var_99,
    avg_max_drawdown,

    -- Derived metrics
    complexity_score,
    risk_score,
    scenario_health_score,

    -- For time-series analysis
    CURRENT_TIMESTAMP as analytics_generated_at

FROM combined_analytics

ORDER BY scenario_created_at DESC
