-- API-oriented views for common access patterns

CREATE OR REPLACE VIEW iceberg.market.curves_latest AS
SELECT
    curve_key,
    tenor_label,
    tenor_type,
    contract_month,
    asof_date,
    currency,
    per_unit,
    mid,
    bid,
    ask,
    version_hash
FROM iceberg.market.mart_curve_latest;

CREATE OR REPLACE VIEW iceberg.market.curves_asof AS
SELECT
    tenant_id,
    curve_key,
    contract_month,
    asof_date,
    mid,
    bid,
    ask
FROM iceberg.market.curve_observation;

CREATE OR REPLACE VIEW iceberg.market.curves_asof_diff AS
WITH base AS (
    SELECT tenant_id, curve_key, contract_month, asof_date, mid
    FROM iceberg.market.curve_observation
),
paired AS (
    SELECT
        b1.tenant_id,
        b1.curve_key,
        b1.contract_month,
        b1.asof_date AS asof_date_new,
        b1.mid AS mid_new,
        b2.asof_date AS asof_date_old,
        b2.mid AS mid_old
    FROM base b1
    JOIN base b2
      ON b1.tenant_id = b2.tenant_id
     AND b1.curve_key = b2.curve_key
     AND b1.contract_month = b2.contract_month
     AND b2.asof_date = b1.asof_date - INTERVAL '1' DAY
)
SELECT *, (mid_new - mid_old) AS mid_diff
FROM paired;

CREATE OR REPLACE VIEW iceberg.market.scenario_output_view AS
SELECT
    tenant_id,
    scenario_id,
    run_id,
    curve_key,
    metric,
    tenor_label,
    asof_date,
    value,
    band_lower,
    band_upper,
    computed_ts
FROM iceberg.market.scenario_output;


