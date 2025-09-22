-- PostgreSQL test fixtures for integration tests
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Test tenants
INSERT INTO tenants (id, name, created_at, updated_at) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Test Tenant Alpha', NOW(), NOW()),
    ('00000000-0000-0000-0000-000000000002', 'Test Tenant Beta', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

-- Test users
INSERT INTO users (id, tenant_id, email, name, created_at, updated_at) VALUES
    ('11111111-1111-1111-1111-111111111111', '00000000-0000-0000-0000-000000000001', 'test-alpha@example.com', 'Test User Alpha', NOW(), NOW()),
    ('22222222-2222-2222-2222-222222222222', '00000000-0000-0000-0000-000000000002', 'test-beta@example.com', 'Test User Beta', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

-- Test scenarios
INSERT INTO scenarios (id, tenant_id, name, description, created_by, created_at, updated_at) VALUES
    ('33333333-3333-3333-3333-333333333333', '00000000-0000-0000-0000-000000000001', 'Test Scenario Alpha', 'Integration test scenario', '11111111-1111-1111-1111-111111111111', NOW(), NOW()),
    ('44444444-4444-4444-4444-444444444444', '00000000-0000-0000-0000-000000000002', 'Test Scenario Beta', 'Another integration test scenario', '22222222-2222-2222-2222-222222222222', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

-- Test watermarks for ingestion testing
INSERT INTO ingest_watermark (source_name, watermark, updated_at) VALUES
    ('test-source-alpha', NOW() - INTERVAL '1 hour', NOW()),
    ('test-source-beta', NOW() - INTERVAL '30 minutes', NOW())
ON CONFLICT (source_name) DO UPDATE SET
    watermark = EXCLUDED.watermark,
    updated_at = EXCLUDED.updated_at;
