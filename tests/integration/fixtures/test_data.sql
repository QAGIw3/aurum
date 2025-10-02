-- Test data fixtures for integration tests
-- Run this to populate test databases with sample data

-- PostgreSQL/TimescaleDB Test Data

-- Create test schema
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Scenarios table (for ScenarioRepository tests)
CREATE TABLE IF NOT EXISTS scenarios (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    assumptions JSONB,
    tenant_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Sample scenarios
INSERT INTO scenarios (id, name, description, tenant_id)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'Test Scenario 1', 'Test scenario for integration tests', 'test-tenant'),
    ('00000000-0000-0000-0000-000000000002', 'Test Scenario 2', 'Another test scenario', 'test-tenant')
ON CONFLICT (id) DO NOTHING;

-- Metadata dimensions (for MetadataRepository tests)
CREATE TABLE IF NOT EXISTS metadata_dimensions (
    dimension_name VARCHAR(100),
    dimension_value VARCHAR(100),
    dataset VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dimension_name, dimension_value, dataset)
);

-- Sample metadata
INSERT INTO metadata_dimensions (dimension_name, dimension_value, dataset)
VALUES
    ('iso', 'PJM', 'curves'),
    ('iso', 'ERCOT', 'curves'),
    ('iso', 'CAISO', 'curves'),
    ('market', 'DA', 'curves'),
    ('market', 'RT', 'curves')
ON CONFLICT DO NOTHING;

-- PPA contracts (for PpaRepository tests)
CREATE TABLE IF NOT EXISTS ppa_contracts (
    contract_id VARCHAR(100) PRIMARY KEY,
    counterparty VARCHAR(255),
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Sample PPA contracts
INSERT INTO ppa_contracts (contract_id, counterparty, start_date, end_date)
VALUES
    ('PPA-TEST-001', 'Test Solar LLC', '2024-01-01', '2034-12-31'),
    ('PPA-TEST-002', 'Wind Power Corp', '2024-06-01', '2029-05-31')
ON CONFLICT (contract_id) DO NOTHING;

-- Drought indices (for DroughtRepository tests)
CREATE TABLE IF NOT EXISTS drought_indices (
    id SERIAL PRIMARY KEY,
    region_type VARCHAR(50),
    region_id VARCHAR(100),
    dataset VARCHAR(50),
    index_id VARCHAR(100),
    index_value FLOAT,
    date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Sample drought data
INSERT INTO drought_indices (region_type, region_id, dataset, index_id, index_value, date)
VALUES
    ('state', 'CA', 'spi', 'SPI-3', -1.5, '2024-01-15'),
    ('state', 'CA', 'spi', 'SPI-3', -2.0, '2024-01-16'),
    ('state', 'TX', 'spi', 'SPI-3', 0.5, '2024-01-15')
ON CONFLICT DO NOTHING;

-- Test cleanup function
CREATE OR REPLACE FUNCTION cleanup_test_data()
RETURNS void AS $$
BEGIN
    DELETE FROM scenarios WHERE tenant_id = 'test-tenant';
    DELETE FROM metadata_dimensions WHERE dataset = 'test';
    DELETE FROM ppa_contracts WHERE contract_id LIKE 'PPA-TEST-%';
    DELETE FROM drought_indices WHERE region_id IN ('TEST-REGION');
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL ON ALL TABLES IN SCHEMA public TO aurum;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO aurum;
GRANT ALL ON SCHEMA test_schema TO aurum;

