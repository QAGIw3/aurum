-- Migration: Add unique constraints for scenario store idempotency
-- Version: 002
-- Date: 2025-01-01

-- Ensure scenarios are unique per tenant/name combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_scenario_tenant_name_unique
ON scenario (tenant_id, lower(name))
WHERE tenant_id IS NOT NULL;

-- Add constraint name for better error handling
ALTER TABLE scenario
ADD CONSTRAINT scenario_tenant_name_unique
UNIQUE USING INDEX idx_scenario_tenant_name_unique;

-- Ensure model runs are unique per scenario and version hash
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_run_scenario_hash_unique
ON model_run (scenario_id, version_hash)
WHERE scenario_id IS NOT NULL AND version_hash IS NOT NULL;

-- Add constraint name for better error handling
ALTER TABLE model_run
ADD CONSTRAINT model_run_scenario_hash_unique
UNIQUE USING INDEX idx_model_run_scenario_hash_unique;

-- Ensure idempotency keys are unique (when provided)
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_run_idempotency_key_unique
ON model_run (idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Add constraint name for better error handling
ALTER TABLE model_run
ADD CONSTRAINT model_run_idempotency_key_unique
UNIQUE USING INDEX idx_model_run_idempotency_key_unique;

-- Add comments for documentation
COMMENT ON CONSTRAINT scenario_tenant_name_unique ON scenario IS
'Ensures scenario names are unique within each tenant';

COMMENT ON CONSTRAINT model_run_scenario_hash_unique ON model_run IS
'Ensures only one run exists per scenario version hash for idempotency';

COMMENT ON CONSTRAINT model_run_idempotency_key_unique ON model_run IS
'Ensures idempotency keys are unique when provided';

-- Create indexes for better query performance in hardened store
CREATE INDEX IF NOT EXISTS idx_scenario_tenant_id
ON scenario (tenant_id)
WHERE tenant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_model_run_scenario_id
ON model_run (scenario_id)
WHERE scenario_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_model_run_state
ON model_run (state)
WHERE state IS NOT NULL;

-- Add version_hash column if it doesn't exist (for run fingerprinting)
ALTER TABLE model_run
ADD COLUMN IF NOT EXISTS version_hash TEXT;

-- Add idempotency_key column if it doesn't exist
ALTER TABLE model_run
ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

-- Add comments for new columns
COMMENT ON COLUMN model_run.version_hash IS
'Hash of scenario version for idempotent run detection';

COMMENT ON COLUMN model_run.idempotency_key IS
'User-provided key for idempotent run creation';
