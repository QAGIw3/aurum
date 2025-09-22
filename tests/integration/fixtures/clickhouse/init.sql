-- ClickHouse test fixtures for integration tests

-- Create test database
CREATE DATABASE IF NOT EXISTS aurum_test;

-- Use test database
USE aurum_test;

-- Test metrics table
CREATE TABLE IF NOT EXISTS test_metrics (
    timestamp DateTime,
    tenant_id String,
    metric_name String,
    metric_value Float64,
    labels Map(String, String)
) ENGINE = MergeTree()
ORDER BY (tenant_id, metric_name, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- Test logs table
CREATE TABLE IF NOT EXISTS test_logs (
    timestamp DateTime,
    level String,
    message String,
    request_id String,
    tenant_id String,
    user_id String,
    trace_id String,
    span_id String
) ENGINE = MergeTree()
ORDER BY timestamp
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 30 DAY;

-- Insert test data
INSERT INTO test_metrics VALUES
    (now(), '00000000-0000-0000-0000-000000000001', 'test_metric_1', 42.0, {'env': 'test', 'component': 'api'}),
    (now(), '00000000-0000-0000-0000-000000000002', 'test_metric_2', 24.0, {'env': 'test', 'component': 'worker'});

INSERT INTO test_logs VALUES
    (now(), 'INFO', 'Test log message alpha', 'test-req-001', '00000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 'test-trace-001', 'test-span-001'),
    (now(), 'WARN', 'Test log message beta', 'test-req-002', '00000000-0000-0000-0000-000000000002', '22222222-2222-2222-2222-222222222222', 'test-trace-002', 'test-span-002');
