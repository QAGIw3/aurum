-- Cross-asset feature store table for weather→load→price analysis
-- This table combines data from multiple sources to create features for ML modeling

WITH weather_data AS (
    SELECT
        timestamp,
        geography,
        temperature,
        humidity,
        wind_speed,
        solar_irradiance
    FROM {{ ref('stg_noaa_weather') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'
),

load_data AS (
    SELECT
        timestamp,
        geography,
        load_mw,
        load_forecast_mw
    FROM {{ ref('stg_isone_load') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        load_mw,
        load_forecast_mw
    FROM {{ ref('stg_miso_load') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        load_mw,
        load_forecast_mw
    FROM {{ ref('stg_pjm_load') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        load_mw,
        load_forecast_mw
    FROM {{ ref('stg_caiso_load') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'
),

price_data AS (
    SELECT
        timestamp,
        geography,
        lmp_price,
        congestion_price,
        loss_price
    FROM {{ ref('stg_isone_lmp') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        lmp_price,
        congestion_price,
        loss_price
    FROM {{ ref('stg_miso_lmp') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        lmp_price,
        congestion_price,
        loss_price
    FROM {{ ref('stg_pjm_lmp') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

    UNION ALL

    SELECT
        timestamp,
        geography,
        lmp_price,
        congestion_price,
        loss_price
    FROM {{ ref('stg_caiso_lmp') }}
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'
),

joined_data AS (
    SELECT
        w.timestamp,
        w.geography,

        -- Weather features
        w.temperature,
        w.humidity,
        w.wind_speed,
        w.solar_irradiance,

        -- Load features
        l.load_mw,
        l.load_forecast_mw,
        l.load_mw - l.load_forecast_mw as load_forecast_error,

        -- Price features
        p.lmp_price,
        p.congestion_price,
        p.loss_price,

        -- Temporal features
        EXTRACT(hour FROM w.timestamp) as hour_of_day,
        EXTRACT(dayofweek FROM w.timestamp) as day_of_week,
        EXTRACT(month FROM w.timestamp) as month_of_year,
        CASE WHEN EXTRACT(dayofweek FROM w.timestamp) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
        CASE WHEN EXTRACT(hour FROM w.timestamp) BETWEEN 17 AND 20 THEN 1 ELSE 0 END as is_peak_hour

    FROM weather_data w
    FULL OUTER JOIN load_data l ON w.timestamp = l.timestamp AND w.geography = l.geography
    FULL OUTER JOIN price_data p ON w.timestamp = p.timestamp AND w.geography = p.geography

    WHERE w.timestamp IS NOT NULL
      AND w.timestamp >= CURRENT_DATE - INTERVAL '1 year'
)

SELECT
    *,
    -- Weather-derived features
    temperature * temperature as temp_squared,
    temperature * temperature * temperature as temp_cubed,
    CASE WHEN temperature > 25 THEN 1 ELSE 0 END as is_hot,
    CASE WHEN temperature < 5 THEN 1 ELSE 0 END as is_cold,
    CASE WHEN humidity > 70 THEN 1 ELSE 0 END as high_humidity,

    -- Load-derived features
    load_mw - LAG(load_mw, 1) OVER (PARTITION BY geography ORDER BY timestamp) as load_change_1h,
    load_mw - LAG(load_mw, 24) OVER (PARTITION BY geography ORDER BY timestamp) as load_change_24h,

    -- Price-derived features
    lmp_price - LAG(lmp_price, 1) OVER (PARTITION BY geography ORDER BY timestamp) as price_change_1h,
    lmp_price - LAG(lmp_price, 24) OVER (PARTITION BY geography ORDER BY timestamp) as price_change_24h,

    -- Cross-asset features
    CORR(temperature, load_mw) OVER (
        PARTITION BY geography
        ORDER BY timestamp
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) as temp_load_correlation_24h,

    CORR(load_mw, lmp_price) OVER (
        PARTITION BY geography
        ORDER BY timestamp
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) as load_price_correlation_24h,

    -- Composite indicators
    (temp_load_correlation_24h + CORR(humidity, load_mw) OVER (
        PARTITION BY geography
        ORDER BY timestamp
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    )) / 2 as weather_sensitivity_index,

    load_price_correlation_24h as price_elasticity_index

FROM joined_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '1 year'

ORDER BY timestamp, geography
