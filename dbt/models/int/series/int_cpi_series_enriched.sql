with series as (
    select * from {{ ref('stg_cpi_series') }}
)
select
    tenant_id,
    series_id,
    period,
    case
        when length(period) = 7 then to_date(period || '-01', 'YYYY-MM-DD')
        when length(period) = 4 then to_date(period || '-01-01', 'YYYY-MM-DD')
        else null
    end as period_date,
    area,
    frequency,
    seasonal_adjustment,
    value,
    units,
    source,
    metadata,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id
from series
