-- Materialized views for common analytics patterns
-- These views are designed to be frequently accessed and provide
-- fast query performance for dashboard and reporting use cases

-- Daily curve price summary by market and product
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.market.curve_price_daily_summary AS
WITH daily_prices AS (
    SELECT
        asof_date,
        iso_code,
        market_code,
        product_code,
        currency,
        uom,
        -- Price statistics
        AVG(mid) as avg_price,
        MIN(mid) as min_price,
        MAX(mid) as max_price,
        STDDEV_POP(mid) as price_stddev,
        COUNT(*) as sample_count,

        -- Volume-weighted metrics
        SUM(mid * volume) / SUM(volume) as vwap,

        -- Price change indicators
        LAG(AVG(mid)) OVER (
            PARTITION BY iso_code, market_code, product_code
            ORDER BY asof_date
        ) as prev_avg_price,

        -- Daily range
        MAX(mid) - MIN(mid) as daily_range,

        -- Price percentiles
        APPROX_PERCENTILE(mid, 0.25) as p25_price,
        APPROX_PERCENTILE(mid, 0.5) as median_price,
        APPROX_PERCENTILE(mid, 0.75) as p75_price,
        APPROX_PERCENTILE(mid, 0.95) as p95_price

    FROM iceberg.fact.fct_curve_observation
    WHERE asof_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY asof_date, iso_code, market_code, product_code, currency, uom
)
SELECT
    *,
    -- Price change percentage
    CASE
        WHEN prev_avg_price IS NOT NULL AND prev_avg_price > 0
        THEN (avg_price - prev_avg_price) / prev_avg_price
        ELSE NULL
    END as daily_price_change_pct,

    -- Volatility indicator
    CASE
        WHEN daily_range > 0
        THEN price_stddev / daily_range
        ELSE 0
    END as volatility_ratio,

    -- Price quality score
    CASE
        WHEN sample_count >= 100 THEN 'HIGH'
        WHEN sample_count >= 50 THEN 'MEDIUM'
        WHEN sample_count >= 10 THEN 'LOW'
        ELSE 'INSUFFICIENT'
    END as data_quality

FROM daily_prices;

-- External data summary by provider and dataset
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.market.external_data_summary AS
WITH provider_summary AS (
    SELECT
        provider,
        dataset_code,
        asof_date,
        geo_id,
        unit_code,
        -- Value statistics
        AVG(value_usd_per_mwh) as avg_value,
        MIN(value_usd_per_mwh) as min_value,
        MAX(value_usd_per_mwh) as max_value,
        STDDEV_POP(value_usd_per_mwh) as value_stddev,
        COUNT(*) as observation_count,

        -- Quality metrics
        COUNT(CASE WHEN quality_status = 'VALID' THEN 1 END) as valid_count,
        COUNT(CASE WHEN quality_status = 'NULL_VALUE' THEN 1 END) as null_count,
        COUNT(CASE WHEN quality_status = 'ZERO_VALUE' THEN 1 END) as zero_count,

        -- Currency distribution
        COUNT(DISTINCT currency) as currency_count,

        -- Geographic coverage
        COUNT(DISTINCT geo_id) as geo_count

    FROM iceberg.mart.external_obs_mapped
    WHERE asof_date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY provider, dataset_code, asof_date, geo_id, unit_code
)
SELECT
    *,
    -- Data completeness ratio
    CASE
        WHEN observation_count > 0
        THEN CAST(valid_count AS DOUBLE) / observation_count
        ELSE 0
    END as completeness_ratio,

    -- Data quality score
    CASE
        WHEN completeness_ratio >= 0.95 THEN 'EXCELLENT'
        WHEN completeness_ratio >= 0.85 THEN 'GOOD'
        WHEN completeness_ratio >= 0.70 THEN 'FAIR'
        ELSE 'POOR'
    END as quality_score

FROM provider_summary;

-- ISO LMP price volatility and congestion analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.market.lmp_volatility_analysis AS
WITH hourly_volatility AS (
    SELECT
        iso_code,
        location_id,
        market,
        DATE_TRUNC('hour', interval_start) as hour_bucket,
        -- Price volatility within hour
        STDDEV_POP(price_total) as hourly_volatility,
        AVG(price_total) as avg_price,
        MIN(price_total) as min_price,
        MAX(price_total) as max_price,
        COUNT(*) as sample_count,

        -- Congestion indicators
        AVG(price_congestion) as avg_congestion,
        MAX(ABS(price_congestion)) as max_congestion_impact,

        -- Loss factors
        AVG(price_loss) as avg_loss_factor

    FROM iceberg.external.timeseries_observation
    WHERE provider = 'ISO'
      AND series_id LIKE '%LMP%'
      AND interval_start >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY iso_code, location_id, market, DATE_TRUNC('hour', interval_start)
    HAVING COUNT(*) >= 10
)
SELECT
    *,
    -- Volatility classification
    CASE
        WHEN hourly_volatility / NULLIF(avg_price, 0) > 0.10 THEN 'HIGH'
        WHEN hourly_volatility / NULLIF(avg_price, 0) > 0.05 THEN 'MEDIUM'
        WHEN hourly_volatility / NULLIF(avg_price, 0) > 0.02 THEN 'LOW'
        ELSE 'STABLE'
    END as volatility_class,

    -- Congestion severity
    CASE
        WHEN max_congestion_impact > 50 THEN 'SEVERE'
        WHEN max_congestion_impact > 20 THEN 'MODERATE'
        WHEN max_congestion_impact > 5 THEN 'MILD'
        ELSE 'MINIMAL'
    END as congestion_severity

FROM hourly_volatility;

-- Load forecast accuracy metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.market.load_forecast_accuracy AS
WITH forecast_vs_actual AS (
    SELECT
        iso_code,
        area,
        DATE_TRUNC('day', interval_start) as forecast_date,
        -- Day-ahead vs real-time comparison
        LAG(mw) OVER (
            PARTITION BY iso_code, area
            ORDER BY interval_start
        ) as day_ahead_forecast,

        mw as actual_load,

        -- Forecast error
        CASE
            WHEN LAG(mw) OVER (
                PARTITION BY iso_code, area
                ORDER BY interval_start
            ) > 0 THEN
                ABS(mw - LAG(mw) OVER (
                    PARTITION BY iso_code, area
                    ORDER BY interval_start
                )) / LAG(mw) OVER (
                    PARTITION BY iso_code, area
                    ORDER BY interval_start
                )
            ELSE NULL
        END as forecast_error_pct

    FROM iceberg.external.timeseries_observation
    WHERE provider = 'ISO'
      AND series_id LIKE '%LOAD%'
      AND interval_start >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT
    iso_code,
    area,
    forecast_date,
    AVG(forecast_error_pct) as mean_absolute_error_pct,
    STDDEV_POP(forecast_error_pct) as error_stddev,
    COUNT(*) as sample_count,

    -- Accuracy classification
    CASE
        WHEN AVG(forecast_error_pct) <= 0.05 THEN 'EXCELLENT'
        WHEN AVG(forecast_error_pct) <= 0.10 THEN 'GOOD'
        WHEN AVG(forecast_error_pct) <= 0.15 THEN 'FAIR'
        ELSE 'POOR'
    END as forecast_accuracy

FROM forecast_vs_actual
WHERE forecast_error_pct IS NOT NULL
GROUP BY iso_code, area, forecast_date;

-- Price correlation analysis across markets
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.market.price_correlation_matrix AS
WITH price_matrix AS (
    SELECT
        asof_date,
        iso_code,
        market_code,
        product_code,
        AVG(mid) as avg_price
    FROM iceberg.fact.fct_curve_observation
    WHERE asof_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY asof_date, iso_code, market_code, product_code
    HAVING COUNT(*) >= 10
)
SELECT
    a.iso_code as iso_a,
    a.market_code as market_a,
    a.product_code as product_a,
    b.iso_code as iso_b,
    b.market_code as market_b,
    b.product_code as product_b,

    -- Pearson correlation coefficient
    CORR(a.avg_price, b.avg_price) as price_correlation,

    -- Sample size
    COUNT(*) as sample_count,

    -- Correlation strength
    CASE
        WHEN ABS(CORR(a.avg_price, b.avg_price)) >= 0.8 THEN 'STRONG'
        WHEN ABS(CORR(a.avg_price, b.avg_price)) >= 0.6 THEN 'MODERATE'
        WHEN ABS(CORR(a.avg_price, b.avg_price)) >= 0.3 THEN 'WEAK'
        ELSE 'NONE'
    END as correlation_strength

FROM price_matrix a
JOIN price_matrix b ON a.asof_date = b.asof_date
WHERE a.iso_code || '_' || a.market_code || '_' || a.product_code <
      b.iso_code || '_' || b.market_code || '_' || b.product_code
GROUP BY a.iso_code, a.market_code, a.product_code, b.iso_code, b.market_code, b.product_code
HAVING COUNT(*) >= 30;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_curve_price_daily_summary_date
ON iceberg.market.curve_price_daily_summary (asof_date DESC);

CREATE INDEX IF NOT EXISTS idx_curve_price_daily_summary_market
ON iceberg.market.curve_price_daily_summary (iso_code, market_code, product_code);

CREATE INDEX IF NOT EXISTS idx_external_data_summary_provider
ON iceberg.market.external_data_summary (provider, dataset_code, asof_date DESC);

CREATE INDEX IF NOT EXISTS idx_lmp_volatility_location
ON iceberg.market.lmp_volatility_analysis (iso_code, location_id, hour_bucket DESC);

CREATE INDEX IF NOT EXISTS idx_load_forecast_iso
ON iceberg.market.load_forecast_accuracy (iso_code, area, forecast_date DESC);
