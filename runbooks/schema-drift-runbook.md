# Schema Drift Runbook

## Overview

This runbook provides procedures for responding to schema drift incidents in the Aurum platform, where database schema changes cause data inconsistencies, migration failures, or application errors.

## Contact Information

- **Primary On-call**: data-team@aurum.com
- **Platform Team**: platform-team@aurum.com
- **DBA Team**: dba@aurum.com
- **Emergency**: +1 (555) 123-4567

## Incident Classification

### Severity Levels

#### Critical (P0)
- Production schema corruption
- Complete data access failure
- Migration rollback required
- Business-critical data affected

#### High (P1)
- Schema inconsistencies between environments
- Failed migrations in production
- Partial data access issues
- Multiple services affected

#### Medium (P2)
- Development/Staging schema issues
- Minor data type mismatches
- Single service affected
- No immediate business impact

## Detection & Assessment

### 1. Initial Detection

#### Check Schema Monitoring
```bash
# Check for schema drift alerts
kubectl port-forward svc/prometheus 9090:9090 -n aurum-dev
open http://localhost:9090/alerts

# Query schema consistency status
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=schema_consistency_status

# Check migration status
kubectl get jobs -l component=database-migration
```

#### Verify Database Health
```bash
# Check database connectivity and basic operations
kubectl exec -it deployment/postgres -- psql -c "SELECT version();"

# Check for schema-related errors in logs
kubectl logs -l app.kubernetes.io/name=aurum-api --tail=100 | grep -i "schema\|migration\|column\|table"

# Verify data access patterns
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(postgres_errors_total[5m])
```

#### Assess Impact Scope
```bash
# Identify affected tables
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=schema_drift_detected_tables

# Check application error rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(aurum_api_errors_total[5m])

# Monitor data ingestion health
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=external_data_ingestion_success_rate
```

### 2. Root Cause Analysis

#### Schema Version Analysis
```bash
# Check current schema version
kubectl exec -it deployment/postgres -- psql -c "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 10;"

# Compare schema versions across environments
kubectl exec -it deployment/postgres-staging -- psql -c "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 10;"

# Check for unapplied migrations
kubectl logs -l component=database-migration --tail=50
```

#### Data Consistency Checks
```bash
# Verify table structures
kubectl exec -it deployment/postgres -- psql -c "\dt+;" | grep -E "(scenarios|curves|tenants)"

# Check column definitions
kubectl exec -it deployment/postgres -- psql -c "\d scenarios;" | head -20

# Identify missing columns or constraints
kubectl exec -it deployment/postgres -- psql -c "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;"
```

#### Application Impact Assessment
```bash
# Check for column reference errors
kubectl logs -l app.kubernetes.io/name=aurum-api --since=10m | grep -i "column.*does not exist\|relation.*does not exist"

# Verify data type compatibility
kubectl exec -it deployment/postgres -- psql -c "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND data_type IN ('integer', 'numeric', 'varchar', 'timestamp') ORDER BY table_name;"

# Check constraint violations
kubectl exec -it deployment/postgres -- psql -c "SELECT conname, conrelid::regclass AS table_name, pg_get_constraintdef(oid) FROM pg_constraint WHERE contype = 'c' ORDER BY conrelid::regclass;"
```

## Response Procedures

### Phase 1: Immediate Assessment (0-10 minutes)

#### Gather Schema Information
```bash
# Create schema comparison report
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;" > /tmp/current_schema.txt

# Compare with expected schema
kubectl cp deployment/postgres:/tmp/current_schema.txt ./current_schema.txt
kubectl cp ./expected_schema.txt deployment/postgres:/tmp/expected_schema.txt

# Generate diff report
kubectl exec -it deployment/postgres -- bash -c "
diff /tmp/current_schema.txt /tmp/expected_schema.txt > /tmp/schema_diff.txt || true
cat /tmp/schema_diff.txt
"
```

#### Identify Drift Type
```bash
# Check for missing tables
kubectl exec -it deployment/postgres -- psql -c "
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_type = 'BASE TABLE'
AND table_name NOT IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public');"

# Check for missing columns
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    t.table_name,
    t.column_name,
    t.data_type
FROM information_schema.columns t
WHERE t.table_schema = 'public'
AND t.table_name IN ('scenarios', 'curves', 'tenants')
ORDER BY t.table_name, t.ordinal_position;" | grep -v "expected_column"
```

#### Assess Data Impact
```bash
# Check for data loss or corruption
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;"

# Verify data integrity
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    table_name,
    COUNT(*) as row_count
FROM (
    SELECT 'scenarios' as table_name, COUNT(*) FROM scenarios
    UNION ALL
    SELECT 'curves' as table_name, COUNT(*) FROM curves
    UNION ALL
    SELECT 'tenants' as table_name, COUNT(*) FROM tenants
) t
GROUP BY table_name;"
```

### Phase 2: Containment & Recovery (10-30 minutes)

#### Enable Read-Only Mode
```bash
# Switch to read-only mode to prevent further damage
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_READ_ONLY_MODE": "true",
    "AURUM_API_READ_ONLY_MESSAGE": "System maintenance: Schema drift recovery in progress"
  }
}'

# Disable write operations
kubectl patch configmap/aurum-worker-config -n aurum-dev --patch '{
  "data": {
    "AURUM_WORKER_READ_ONLY_MODE": "true"
  }
}'
```

#### Create Schema Backup
```bash
# Create backup of current schema
kubectl exec -it deployment/postgres -- pg_dump --schema-only --no-owner --no-privileges aurum > /tmp/schema_backup_$(date +%Y%m%d_%H%M%S).sql

# Store backup securely
kubectl cp deployment/postgres:/tmp/schema_backup_$(date +%Y%m%d_%H%M%S).sql ./schema_backup_$(date +%Y%m%d_%H%M%S).sql
```

#### Apply Schema Corrections
```bash
# Create schema correction script
cat > /tmp/schema_correction.sql << 'EOF'
-- Schema Drift Correction Script
-- Generated: $(date)

BEGIN;

-- Add missing columns if needed
ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS new_column_name data_type;

-- Modify column types if necessary
ALTER TABLE curves ALTER COLUMN column_name TYPE new_data_type;

-- Recreate missing constraints
ALTER TABLE tenants ADD CONSTRAINT constraint_name CHECK (condition);

-- Update indexes
CREATE INDEX IF NOT EXISTS idx_new_column ON table_name(new_column);

COMMIT;
EOF

# Apply corrections
kubectl cp /tmp/schema_correction.sql deployment/postgres:/tmp/schema_correction.sql
kubectl exec -it deployment/postgres -- psql -d aurum -f /tmp/schema_correction.sql
```

### Phase 3: Validation & Restoration (30-60 minutes)

#### Validate Schema Consistency
```bash
# Verify schema structure
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    t.table_name,
    COUNT(c.column_name) as column_count
FROM information_schema.tables t
LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE t.table_schema = 'public'
GROUP BY t.table_name
ORDER BY t.table_name;" > /tmp/validated_schema.txt

# Compare with expected schema
kubectl exec -it deployment/postgres -- bash -c "
diff /tmp/validated_schema.txt /tmp/expected_schema.txt || echo 'Schema validation completed - differences found'
"
```

#### Test Application Compatibility
```bash
# Test basic database operations
kubectl exec -it deployment/aurum-api -- python3 -c "
from aurum.database import get_db_connection
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM scenarios LIMIT 1')
result = cursor.fetchone()
print(f'✅ Basic query successful: {result[0]} scenarios found')
conn.close()
"

# Verify API endpoints
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/health
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/v1/scenarios | head -c 200
```

#### Restore Normal Operations
```bash
# Re-enable write operations
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_READ_ONLY_MODE": "false",
    "AURUM_API_READ_ONLY_MESSAGE": ""
  }
}'

# Re-enable worker operations
kubectl patch configmap/aurum-worker-config -n aurum-dev --patch '{
  "data": {
    "AURUM_WORKER_READ_ONLY_MODE": "false"
  }
}'
```

## Communication Templates

### Initial Schema Drift Detection
```
🗄️ [SEVERITY] Schema Drift Detected

**Time**: $(date)
**Environment**: [Production / Staging / Development]
**Affected Tables**: [scenarios, curves, tenants, etc.]
**Impact**: [Data access issues, application errors, migration failures]
**Status**: Investigating

**Immediate Actions**:
- Enabled read-only mode
- Created schema backup
- Initiated drift analysis

**Expected Duration**: 30-60 minutes
**Contact**: data-team@aurum.com
```

### Assessment Update
```
📊 Schema Drift Assessment Progress

**Time**: $(date)
**Status**: [Assessing / Correcting / Validating]
**Environment**: [Environment name]

**Findings**:
- ✅ Schema backup: COMPLETED
- ✅ Drift analysis: IN PROGRESS
- ✅ Impact assessment: [Minimal / Moderate / Severe]

**Root Cause**: [Migration failure / Manual changes / Deployment issue]
**Next Steps**: [Schema correction / Rollback / Migration]
**Contact**: data-team@aurum.com
```

### Resolution Notification
```
✅ Schema Drift Incident Resolved

**Time**: $(date)
**Environment**: [Environment name]
**Resolution**: [Schema corrected, consistency restored]

**Actions Taken**:
- Schema backup created
- Missing elements restored
- Application compatibility verified
- Normal operations resumed

**Data Impact**: [None / Minimal data loss / Data restored]
**Monitoring**: Enhanced schema monitoring for 24 hours
**Contact**: data-team@aurum.com
```

## Recovery Procedures

### Emergency Schema Recovery
```bash
# Activate emergency recovery mode
kubectl create job schema-emergency-recovery-$(date +%s) --from=cronjob/schema-emergency-recovery

# Monitor recovery progress
kubectl logs -f job/schema-emergency-recovery-$(date +%s)

# Verify recovery completion
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    table_name,
    COUNT(*) as row_count
FROM (
    SELECT 'scenarios' as table_name, COUNT(*) FROM scenarios
    UNION ALL
    SELECT 'curves' as table_name, COUNT(*) FROM curves
    UNION ALL
    SELECT 'tenants' as table_name, COUNT(*) FROM tenants
) t
GROUP BY table_name;"
```

### Migration Rollback
```bash
# Identify rollback point
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    version,
    description,
    applied_at
FROM schema_migrations
ORDER BY version DESC
LIMIT 5;"

# Create rollback script
cat > /tmp/migration_rollback.sql << 'EOF'
-- Migration Rollback Script
BEGIN;

-- Rollback to previous version
DELETE FROM schema_migrations WHERE version > rollback_version;

-- Apply rollback SQL
-- [Rollback SQL statements]

COMMIT;
EOF

# Execute rollback
kubectl cp /tmp/migration_rollback.sql deployment/postgres:/tmp/migration_rollback.sql
kubectl exec -it deployment/postgres -- psql -d aurum -f /tmp/migration_rollback.sql
```

### Data Reconciliation
```bash
# Create data reconciliation job
kubectl create job data-reconciliation-$(date +%s) --from=cronjob/data-reconciliation

# Monitor reconciliation progress
kubectl logs -f job/data-reconciliation-$(date +%s)

# Verify data consistency
kubectl exec -it deployment/postgres -- psql -c "
SELECT
    'scenarios' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids
FROM scenarios
UNION ALL
SELECT
    'curves' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT curve_id) as unique_ids
FROM curves;"
```

## Prevention Measures

### Enhanced Schema Validation
```bash
# Enable schema change detection
kubectl patch configmap/database-config -n aurum-dev --patch '{
  "data": {
    "SCHEMA_CHANGE_DETECTION_ENABLED": "true",
    "SCHEMA_CHANGE_DETECTION_INTERVAL": "60",
    "SCHEMA_CHANGE_ALERT_THRESHOLD": "3"
  }
}'

# Implement schema version control
kubectl patch configmap/database-config -n aurum-dev --patch '{
  "data": {
    "SCHEMA_VERSION_CONTROL_ENABLED": "true",
    "SCHEMA_VERSION_AUTO_UPDATE": "true",
    "SCHEMA_VERSION_BACKUP_ENABLED": "true"
  }
}'
```

### Migration Safety
```bash
# Enable migration safety checks
kubectl patch configmap/database-config -n aurum-dev --patch '{
  "data": {
    "MIGRATION_PRE_CHECK_ENABLED": "true",
    "MIGRATION_BACKUP_ENABLED": "true",
    "MIGRATION_ROLLOUT_ENABLED": "true",
    "MIGRATION_VERIFICATION_ENABLED": "true"
  }
}'

# Configure migration monitoring
kubectl patch configmap/monitoring-config -n aurum-dev --patch '{
  "data": {
    "MIGRATION_MONITORING_ENABLED": "true",
    "MIGRATION_SUCCESS_RATE_ALERTS": "true",
    "MIGRATION_DURATION_ALERTS": "true"
  }
}'
```

### Environment Synchronization
```bash
# Enable environment sync checks
kubectl patch configmap/database-config -n aurum-dev --patch '{
  "data": {
    "ENVIRONMENT_SCHEMA_SYNC_ENABLED": "true",
    "ENVIRONMENT_SCHEMA_SYNC_INTERVAL": "300",
    "ENVIRONMENT_SCHEMA_SYNC_AUTO_FIX": "false"
  }
}'

# Implement drift prevention
kubectl patch configmap/database-config -n aurum-dev --patch '{
  "data": {
    "SCHEMA_DRIFT_PREVENTION_ENABLED": "true",
    "SCHEMA_CHANGE_APPROVAL_REQUIRED": "true",
    "SCHEMA_CHANGE_NOTIFICATION_ENABLED": "true"
  }
}'
```

## Escalation Procedures

### When to Escalate
- Schema drift affects production data integrity
- Recovery procedures take > 1 hour
- Multiple services experiencing errors
- Data loss detected or suspected

### Escalation Path
1. **Primary**: Data team lead
2. **Secondary**: Platform engineering director
3. **Emergency**: CTO/VP Engineering

## Post-Incident Tasks

### Immediate Actions
1. Document complete incident timeline
2. Review schema change procedures
3. Update monitoring thresholds
4. Communicate resolution to stakeholders

### Follow-up Tasks
1. **Post-mortem**: Schedule within 1 business day
2. **Schema Review**: Audit all schema changes
3. **Process Improvement**: Enhance change management
4. **Documentation**: Update schema management procedures

## Tools & Resources

### Monitoring Dashboards
- **Schema Health Dashboard**: https://grafana.aurum-dev.com/d/schema-health
- **Migration Dashboard**: https://grafana.aurum-dev.com/d/database-migrations
- **Data Consistency Dashboard**: https://grafana.aurum-dev.com/d/data-consistency

### Management Tools
- **Schema Manager**: http://schema-manager.aurum-dev.svc.cluster.local:8080
- **Migration Service**: http://migration-service.aurum-dev.svc.cluster.local:8080
- **Data Reconciliation**: http://data-reconciliation.aurum-dev.svc.cluster.local:8080

### Documentation
- **Database Schema Guide**: docs/database/schema/
- **Migration Guide**: docs/database/migrations/
- **Data Management Guide**: docs/data-management/

## Metrics & KPIs

### Response Time Targets
- **Detection**: < 5 minutes
- **Assessment**: < 10 minutes
- **Containment**: < 15 minutes
- **Full Recovery**: < 45 minutes

### Success Metrics
- **MTTR**: < 30 minutes
- **Data Loss**: < 0.1%
- **Schema Consistency**: 100%
- **False Positive Rate**: < 2% of alerts
