with source_periods as (
    select
        series_id,
        period_start,
        ingest_ts,
        row_number() over (
            partition by series_id
            order by period_start desc, ingest_ts desc
        ) as rn
    from {{ ref('int_eia_series_enriched') }}
)
select
    latest.series_id,
    latest.period_start,
    expected.period_start as expected_period_start
from {{ ref('mart_eia_series_latest') }} latest
join source_periods expected
    on latest.series_id = expected.series_id
where expected.rn = 1
  and latest.period_start <> expected.period_start
