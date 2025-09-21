-- ClickHouse schema for structured logs
-- This should be applied to a ClickHouse database

CREATE TABLE IF NOT EXISTS aurum_logs (
    timestamp DateTime64(3) DEFAULT now(),
    level String,
    message String,
    event_type String,
    source_name Nullable(String),
    dataset Nullable(String),
    job_id Nullable(String),
    task_id Nullable(String),
    duration_ms Nullable(Float64),
    records_processed Nullable(Int64),
    bytes_processed Nullable(Int64),
    error_code Nullable(String),
    error_message Nullable(String),
    hostname Nullable(String),
    container_id Nullable(String),
    thread_id Nullable(String),
    metadata String,  -- JSON string
    session_id String DEFAULT 'unknown',

    -- Partitioning and ordering
    INDEX idx_level level TYPE minmax GRANULARITY 1,
    INDEX idx_source_name source_name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_event_type event_type TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, source_name, event_type, level)
SETTINGS index_granularity = 8192;

-- Materialized view for error logs
CREATE MATERIALIZED VIEW IF NOT EXISTS aurum_logs_errors
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, source_name, error_code)
SETTINGS index_granularity = 8192
AS SELECT
    timestamp,
    level,
    message,
    event_type,
    source_name,
    dataset,
    job_id,
    task_id,
    duration_ms,
    records_processed,
    bytes_processed,
    error_code,
    error_message,
    hostname,
    container_id,
    thread_id,
    metadata,
    session_id
FROM aurum_logs
WHERE level IN ('ERROR', 'FATAL');

-- View for log summary by source and day
CREATE VIEW IF NOT EXISTS aurum_logs_daily_summary AS
SELECT
    toDate(timestamp) as log_date,
    source_name,
    level,
    event_type,
    COUNT(*) as log_count,
    COUNT(DISTINCT session_id) as unique_sessions,
    AVG(duration_ms) as avg_duration_ms,
    SUM(records_processed) as total_records_processed,
    SUM(bytes_processed) as total_bytes_processed,
    COUNT(CASE WHEN level IN ('ERROR', 'FATAL') THEN 1 END) as error_count
FROM aurum_logs
GROUP BY log_date, source_name, level, event_type;

-- View for API call analysis
CREATE VIEW IF NOT EXISTS aurum_api_calls AS
SELECT
    toDate(timestamp) as call_date,
    source_name,
    JSONExtractString(metadata, 'api_endpoint') as api_endpoint,
    JSONExtractString(metadata, 'http_status_code') as http_status_code,
    COUNT(*) as call_count,
    AVG(JSONExtractFloat(metadata, 'response_time_ms')) as avg_response_time_ms,
    COUNT(CASE WHEN JSONExtractString(metadata, 'http_status_code') >= '400' THEN 1 END) as error_count
FROM aurum_logs
WHERE event_type = 'api_call'
  AND JSONExtractString(metadata, 'api_endpoint') != ''
GROUP BY call_date, source_name, api_endpoint, http_status_code;

-- View for data quality trends
CREATE VIEW IF NOT EXISTS aurum_data_quality AS
SELECT
    toDate(timestamp) as quality_date,
    source_name,
    JSONExtractFloat(metadata, 'quality_score') as quality_score,
    JSONExtractInt(metadata, 'valid_records') as valid_records,
    JSONExtractInt(metadata, 'invalid_records') as invalid_records,
    COUNT(*) as check_count
FROM aurum_logs
WHERE event_type = 'data_quality'
  AND JSONExtractFloat(metadata, 'quality_score') > 0
GROUP BY quality_date, source_name, quality_score, valid_records, invalid_records;
