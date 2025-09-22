{{
    config(
        materialized='table',
        schema='mart',
        alias='series_curve_mapping',
        tags=['curve', 'external', 'dimension']
    )
}}

WITH raw_mapping AS (
    SELECT
        external_provider,
        external_series_id,
        curve_key,
        mapping_confidence,
        mapping_method,
        mapping_notes,
        is_active,
        created_at,
        created_by,
        updated_at,
        updated_by
    FROM {{ source('iceberg_market', 'series_curve_map') }}
),
series_catalog AS (
    SELECT
        provider,
        series_id,
        title,
        dataset_code,
        unit_code,
        category
    FROM {{ ref('mart_external_series_catalog') }}
),
curve_meta AS (
    SELECT
        curve_key,
        any_value(iso_code) AS iso_code,
        any_value(market_code) AS market_code,
        any_value(product_code) AS product_code,
        any_value(block_code) AS block_code
    FROM {{ ref('fct_curve_observation') }}
    GROUP BY curve_key
)
SELECT
    m.external_provider,
    m.external_series_id,
    m.curve_key,
    m.mapping_confidence,
    m.mapping_method,
    m.mapping_notes,
    m.is_active,
    m.created_at,
    m.created_by,
    m.updated_at,
    m.updated_by,
    catalog.title AS external_series_title,
    catalog.dataset_code AS external_dataset_code,
    catalog.unit_code AS external_unit_code,
    catalog.category AS external_category,
    curve_meta.iso_code,
    curve_meta.market_code,
    curve_meta.product_code,
    curve_meta.block_code,
    {{ aurum_text_hash("m.external_provider || ':' || m.external_series_id || ':' || m.curve_key") }} AS mapping_sk
FROM raw_mapping m
LEFT JOIN series_catalog catalog
    ON m.external_provider = catalog.provider
   AND m.external_series_id = catalog.series_id
LEFT JOIN curve_meta
    ON m.curve_key = curve_meta.curve_key
