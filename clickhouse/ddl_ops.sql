CREATE DATABASE IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.logs (
    timestamp DateTime DEFAULT now(),
    level LowCardinality(String),
    service LowCardinality(String),
    host String,
    trace_id String,
    span_id String,
    message String,
    fields Map(String, String)
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service, level)
SAMPLE BY cityHash64(trace_id, span_id)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ops.events (
    timestamp DateTime DEFAULT now(),
    event_type LowCardinality(String),
    source LowCardinality(String),
    severity LowCardinality(String),
    payload JSON,
    tenant String
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type)
SAMPLE BY cityHash64(event_type, source, tenant)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ops.query_metrics_raw (
    recorded_at DateTime DEFAULT now(),
    catalog LowCardinality(String),
    schema LowCardinality(String),
    query_name String,
    tenant LowCardinality(String),
    status LowCardinality(String),
    wall_time_ms UInt64,
    queued_time_ms UInt64,
    cpu_time_ms UInt64,
    bytes_scanned UInt64,
    rows_read UInt64,
    rows_written UInt64,
    spill_bytes UInt64 DEFAULT 0,
    source LowCardinality(String) DEFAULT 'trino'
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recorded_at)
ORDER BY (recorded_at, catalog, query_name, tenant)
SAMPLE BY cityHash64(catalog, query_name, tenant)
TTL recorded_at + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS ops.query_metrics_hourly
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, catalog, query_name, tenant)
SAMPLE BY cityHash64(catalog, query_name, tenant)
TTL hour + INTERVAL 365 DAY
AS
SELECT
    date_trunc('hour', recorded_at) AS hour,
    catalog,
    query_name,
    tenant,
    count() AS executions,
    sum(wall_time_ms) AS total_wall_time_ms,
    sum(bytes_scanned) AS total_bytes_scanned,
    sum(rows_read) AS total_rows_read,
    sum(rows_written) AS total_rows_written,
    max(wall_time_ms) AS max_wall_time_ms
FROM ops.query_metrics_raw
GROUP BY hour, catalog, query_name, tenant;

CREATE TABLE IF NOT EXISTS ops.ingest_activity (
    event_time DateTime DEFAULT now(),
    pipeline LowCardinality(String),
    dataset LowCardinality(String),
    tenant LowCardinality(String),
    records UInt64,
    status LowCardinality(String),
    error_code LowCardinality(String),
    latency_ms UInt64,
    metadata JSON
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, pipeline, dataset)
SAMPLE BY cityHash64(pipeline, dataset, tenant)
TTL event_time + INTERVAL 120 DAY
SETTINGS index_granularity = 8192;
