{{ config(materialized='view') }}

select
    source,
    severity,
    error_message,
    context,
    coalesce(recommended_action, 'UNSPECIFIED') as recommended_action,
    cast(ingest_ts as timestamp) as ingest_ts,
    raw_payload
from {{ source('iceberg_market', 'curve_dead_letter') }}
