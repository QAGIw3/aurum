# Disaster Recovery Testing Runbook

## Overview

This runbook provides comprehensive procedures for testing disaster recovery capabilities of the Aurum platform, including backup/restore operations for all critical components.

## Contact Information

- **Primary On-call**: platform-team@aurum.com
- **SRE Team**: sre@aurum.com
- **DBA Team**: dba@aurum.com
- **Emergency**: +1 (555) 123-4567

## DR Objectives

### Recovery Time Objectives (RTO)
- **Critical Systems**: 4 hours (API, Database, Message Queue)
- **Important Systems**: 8 hours (Workers, Analytics, Monitoring)
- **Supporting Systems**: 24 hours (Development tools, CI/CD)

### Recovery Point Objectives (RPO)
- **Critical Data**: 1 hour (Transaction data, User data)
- **Important Data**: 4 hours (Analytics data, Configuration)
- **Supporting Data**: 24 hours (Logs, Metrics)

## Incident Classification

### Test Types

#### Planned Testing (P0)
- Scheduled DR tests
- Full system recovery testing
- RTO/RPO validation

#### Emergency Testing (P1)
- Unplanned system failures
- Partial component failures
- Data corruption scenarios

#### Component Testing (P2)
- Individual service recovery
- Data restoration testing
- Configuration recovery

## Pre-Test Checklist

### 1. Environment Preparation
```bash
# Verify test environment isolation
kubectl get namespaces | grep -E "(dr-test|backup-test)"

# Check resource availability
kubectl describe nodes | grep -A 10 "Allocatable"

# Verify backup systems
kubectl get cronjobs -l component=backup
kubectl get pvc -l component=backup-storage
```

### 2. Data Integrity Verification
```bash
# Verify recent backups
kubectl logs -l component=backup-manager --tail=20

# Check backup storage status
kubectl exec -it deployment/minio -- mc ls aurum-backups/

# Validate backup completeness
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=backup_last_success
```

### 3. Communication Setup
```bash
# Notify stakeholders
cat << EOF > /tmp/dr-test-notification.json
{
  "timestamp": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "type": "DR_TEST_START",
  "environment": "dr-test",
  "duration": "4_hours",
  "contact": "platform-team@aurum.com"
}
EOF

# Send notification
curl -X POST -H 'Content-type: application/json' --data-binary @/tmp/dr-test-notification.json ${SLACK_WEBHOOK_URL}
```

## Test Execution Procedures

### Phase 1: Environment Setup (0-30 minutes)

#### Create Isolated Test Environment
```bash
# Create DR test namespace
kubectl create namespace dr-test-$(date +%Y%m%d-%H%M)

# Label for isolation
kubectl label namespace dr-test-$(date +%Y%m%d-%H%M) \
  app.kubernetes.io/name=aurum-dr-test \
  app.kubernetes.io/component=disaster-recovery-test

# Deploy monitoring for test
kubectl apply -f k8s/dr-test/monitoring/ -n dr-test-$(date +%Y%m%d-%H%M)
```

#### Prepare Test Data
```bash
# Create test data sets
kubectl create job dr-test-data-prep-$(date +%s) \
  --from=cronjob/dr-test-data-preparation \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Verify test data creation
kubectl logs job/dr-test-data-prep-$(date +%s) -n dr-test-$(date +%Y%m%d-%H%M)

# Record baseline metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=dr_test_baseline > /tmp/baseline_metrics.json
```

### Phase 2: Component Failure Simulation (30-90 minutes)

#### Database Failure Test
```bash
# Simulate PostgreSQL failure
kubectl scale deployment postgres --replicas=0 -n dr-test-$(date +%Y%m%d-%H%M)

# Wait for failure detection
sleep 60

# Verify failure detection
kubectl get pods -l app.kubernetes.io/name=postgres -n dr-test-$(date +%Y%m%d-%H%M)
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=up{job="postgres"} == 0
```

#### Message Queue Failure Test
```bash
# Simulate Kafka failure
kubectl scale deployment kafka --replicas=0 -n dr-test-$(date +%Y%m%d-%H%M)

# Verify message queue unavailability
kubectl get pods -l app.kubernetes.io/name=kafka -n dr-test-$(date +%Y%m%d-%H%M)
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=kafka_brokers_online == 0
```

#### Storage Failure Test
```bash
# Simulate MinIO failure
kubectl scale deployment minio --replicas=0 -n dr-test-$(date +%Y%m%d-%H%M)

# Verify storage unavailability
kubectl get pvc -l component=storage -n dr-test-$(date +%Y%m%d-%H%M)
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=minio_cluster_health != 1
```

### Phase 3: Recovery Procedures (90-180 minutes)

#### Database Recovery
```bash
# Deploy recovery PostgreSQL instance
kubectl apply -f k8s/dr-test/postgres-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Wait for recovery instance readiness
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres-recovery -n dr-test-$(date +%Y%m%d-%H%M) --timeout=300s

# Restore from latest backup
kubectl create job postgres-restore-$(date +%s) \
  --from=cronjob/postgres-restore \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Monitor restore progress
kubectl logs -f job/postgres-restore-$(date +%s) -n dr-test-$(date +%Y%m%d-%H%M)
```

#### Message Queue Recovery
```bash
# Deploy recovery Kafka cluster
kubectl apply -f k8s/dr-test/kafka-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Wait for Kafka recovery
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka-recovery -n dr-test-$(date +%Y%m%d-%H%M) --timeout=600s

# Restore topics and messages
kubectl create job kafka-restore-$(date +%s) \
  --from=cronjob/kafka-restore \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Verify message recovery
kubectl exec -it deployment/kafka-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- \
  kafka-topics --bootstrap-server localhost:9092 --list
```

#### Storage Recovery
```bash
# Deploy recovery MinIO instance
kubectl apply -f k8s/dr-test/minio-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Wait for MinIO recovery
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=minio-recovery -n dr-test-$(date +%Y%m%d-%H%M) --timeout=300s

# Restore data from backup
kubectl create job minio-restore-$(date +%s) \
  --from=cronjob/minio-restore \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Verify storage recovery
kubectl exec -it deployment/minio-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- \
  mc ls aurum-backups-restored/
```

#### Application Recovery
```bash
# Deploy recovery applications
kubectl apply -f k8s/dr-test/application-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Scale applications appropriately
kubectl scale deployment aurum-api --replicas=3 -n dr-test-$(date +%Y%m%d-%H%M)
kubectl scale deployment aurum-worker --replicas=2 -n dr-test-$(date +%Y%m%d-%H%M)

# Verify application health
kubectl get pods -l app.kubernetes.io/name=aurum-api -n dr-test-$(date +%Y%m%d-%H%M)
curl -s http://aurum-api.dr-test-$(date +%Y%m%d-%H%M).svc.cluster.local:8080/health
```

### Phase 4: Validation & Testing (180-240 minutes)

#### Data Integrity Validation
```bash
# Create data validation job
kubectl create job dr-validation-$(date +%s) \
  --from=cronjob/dr-data-validation \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Monitor validation progress
kubectl logs -f job/dr-validation-$(date +%s) -n dr-test-$(date +%Y%m%d-%H%M)

# Check validation results
kubectl exec -it deployment/dr-validator -n dr-test-$(date +%Y%m%d-%H%M) -- \
  cat /tmp/validation_results.json
```

#### Functional Testing
```bash
# Run functional tests
kubectl create job dr-functional-test-$(date +%s) \
  --from=cronjob/dr-functional-tests \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Verify test results
kubectl logs job/dr-functional-test-$(date +%s) -n dr-test-$(date +%Y%m%d-%H%M) | grep -E "(PASSED|FAILED)"

# Test critical business operations
curl -X POST http://aurum-api.dr-test-$(date +%Y%m%d-%H%M).svc.cluster.local:8080/v1/scenarios \
  -H "Content-Type: application/json" \
  -d '{"scenario": "test-recovery-scenario"}'
```

#### Performance Testing
```bash
# Run performance tests
kubectl create job dr-performance-test-$(date +%s) \
  --from=cronjob/dr-performance-tests \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Monitor performance metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=dr_test_performance_score

# Verify RTO/RPO compliance
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=dr_test_rto_compliance
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=dr_test_rpo_compliance
```

## Component-Specific Recovery Procedures

### PostgreSQL Recovery
```bash
# Full PostgreSQL recovery procedure
kubectl create job postgres-full-recovery-$(date +%s) \
  --from=cronjob/postgres-disaster-recovery \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Detailed recovery steps
kubectl exec -it deployment/postgres-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- bash -c "
# 1. Restore base backup
pg_basebackup -h backup-source -D /var/lib/postgresql/data -U replicator -vP -R

# 2. Configure recovery
cat > /var/lib/postgresql/data/recovery.conf << EOF
restore_command = 'cp /backup/wal/%f %p'
recovery_target_time = '$(date -d '1 hour ago' +'%Y-%m-%d %H:%M:%S')'
EOF

# 3. Start recovery
pg_ctl start -D /var/lib/postgresql/data

# 4. Verify recovery
psql -c 'SELECT now(), pg_is_in_recovery();'
psql -c 'SELECT COUNT(*) FROM scenarios;'
"
```

### Kafka Recovery
```bash
# Full Kafka cluster recovery
kubectl apply -f k8s/dr-test/kafka-full-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Topic and message recovery
kubectl exec -it deployment/kafka-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- bash -c "
# 1. Restore topic configurations
kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name aurum-scenarios --alter --add-config retention.ms=604800000

# 2. Restore consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --group aurum-workers --reset-offsets --to-latest --execute --all-topics

# 3. Verify message flow
kafka-producer-perf-test --topic aurum-scenarios --num-records 1000 --record-size 100 --throughput 100 --producer-props bootstrap.servers=localhost:9092
"
```

### TimescaleDB Recovery
```bash
# TimescaleDB specific recovery
kubectl create job timescale-recovery-$(date +%s) \
  --from=cronjob/timescale-disaster-recovery \
  -n dr-test-$(date +%Y%m%d-%H%M)

# Verify TimescaleDB functionality
kubectl exec -it deployment/timescale-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- psql -c "
-- Check TimescaleDB extension
SELECT * FROM pg_extension WHERE extname = 'timescaledb';

-- Verify hypertables
SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;

-- Test continuous aggregates
SELECT view_name, view_definition FROM timescaledb_information.continuous_aggregates;

-- Verify data retention policies
SELECT hypertable_name, drop_after FROM timescaledb_information.retention_policies;
"
```

### Schema Registry Recovery
```bash
# Schema Registry recovery
kubectl apply -f k8s/dr-test/schema-registry-recovery/ -n dr-test-$(date +%Y%m%d-%H%M)

# Restore schemas
kubectl exec -it deployment/schema-registry-recovery -n dr-test-$(date +%Y%m%d-%H%M) -- bash -c "
# 1. List available schemas
curl -s http://localhost:8081/subjects/ | jq '.'

# 2. Restore specific schema
curl -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{\"schema\": \"{\\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"Scenario\\\", \\\"fields\\\": [{\\\"name\\\": \\\"id\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"data\\\", \\\"type\\\": \\\"string\\\"}]}\"}' \
  http://localhost:8081/subjects/aurum-scenarios/versions

# 3. Verify schema compatibility
curl -s http://localhost:8081/compatibility/subjects/aurum-scenarios/versions/latest
"
```

## RTO/RPO Validation

### Recovery Time Measurement
```bash
# Start RTO timer
START_TIME=$(date +%s)

# Execute recovery procedures
# [Recovery commands here]

# End RTO timer
END_TIME=$(date +%s)
RTO_SECONDS=$((END_TIME - START_TIME))
RTO_MINUTES=$((RTO_SECONDS / 60))

# Validate RTO compliance
if [ $RTO_MINUTES -le 240 ]; then
  echo "✅ RTO COMPLIANT: $RTO_MINUTES minutes (<= 4 hours)"
else
  echo "❌ RTO VIOLATION: $RTO_MINUTES minutes (> 4 hours)"
fi
```

### Recovery Point Measurement
```bash
# Measure data loss
LATEST_BACKUP=$(curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=backup_last_timestamp)
FAILURE_TIME=$(date +%s)
RPO_SECONDS=$((FAILURE_TIME - LATEST_BACKUP))

# Validate RPO compliance
if [ $RPO_SECONDS -le 3600 ]; then
  echo "✅ RPO COMPLIANT: $RPO_SECONDS seconds (<= 1 hour)"
else
  echo "❌ RPO VIOLATION: $RPO_SECONDS seconds (> 1 hour)"
fi
```

## Post-Test Procedures

### Test Results Analysis
```bash
# Generate comprehensive test report
cat > /tmp/dr-test-report.json << EOF
{
  "test_timestamp": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "test_duration": "$RTO_MINUTES minutes",
  "components_tested": [
    "PostgreSQL",
    "Kafka",
    "MinIO",
    "Aurum API",
    "Aurum Workers",
    "Schema Registry",
    "TimescaleDB"
  ],
  "rto_compliance": $(if [ $RTO_MINUTES -le 240 ]; then echo "true"; else echo "false"; fi),
  "rpo_compliance": $(if [ $RPO_SECONDS -le 3600 ]; then echo "true"; else echo "false"; fi),
  "data_integrity_score": 0.99,
  "functional_test_results": "PASSED",
  "performance_test_results": "PASSED"
}
EOF

# Store test report
kubectl create configmap dr-test-report-$(date +%Y%m%d-%H%M) \
  --from-file=report.json=/tmp/dr-test-report.json \
  -n dr-test-$(date +%Y%m%d-%H%M)
```

### Environment Cleanup
```bash
# Clean up test environment
kubectl delete namespace dr-test-$(date +%Y%m%d-%H%M) --ignore-not-found=true

# Archive test artifacts
kubectl cp /tmp/dr-test-report.json ./dr-test-reports/report-$(date +%Y%m%d-%H%M%S).json

# Clean up local artifacts
rm -f /tmp/dr-test-*.json /tmp/baseline_metrics.json
```

## Communication Templates

### Test Start Notification
```
🧪 [SEVERITY] DR Test Started

**Time**: $(date)
**Environment**: dr-test-$(date +%Y%m%d-%H%M)
**Duration**: 4 hours
**Components**: [PostgreSQL, Kafka, MinIO, Applications]
**Status**: IN PROGRESS

**Test Plan**:
- Environment setup and validation
- Component failure simulation
- Recovery procedure execution
- RTO/RPO compliance verification

**Expected Impact**: None (isolated test environment)
**Contact**: platform-team@aurum.com
```

### Test Progress Update
```
📊 DR Test Progress Report

**Time**: $(date)
**Phase**: [Setup / Failure Simulation / Recovery / Validation]
**Progress**: [60% Complete]

**Completed**:
- ✅ Environment isolation
- ✅ Test data preparation
- ✅ Component failure simulation
- 🔄 Recovery procedures (in progress)

**Next Steps**:
- Application recovery testing
- Data integrity validation
- RTO/RPO measurement

**Contact**: platform-team@aurum.com
```

### Test Completion Report
```
✅ DR Test Completed

**Time**: $(date)
**Overall Result**: [SUCCESS / PARTIAL SUCCESS / FAILURE]
**RTO Achievement**: [$RTO_MINUTES minutes / 4 hours]
**RPO Achievement**: [$RPO_SECONDS seconds / 1 hour]

**Test Results**:
- ✅ Database Recovery: [SUCCESS]
- ✅ Message Queue Recovery: [SUCCESS]
- ✅ Storage Recovery: [SUCCESS]
- ✅ Application Recovery: [SUCCESS]
- ✅ Data Integrity: [99.9%]

**Recommendations**:
- [Any improvements identified]
- [Process updates needed]

**Next Scheduled Test**: [Date/Time]
**Contact**: platform-team@aurum.com
```

## Escalation Procedures

### When to Escalate
- Test duration exceeds 6 hours
- Critical component recovery fails
- Data integrity issues detected
- RTO/RPO targets not achievable

### Escalation Path
1. **Primary**: Platform team lead
2. **Secondary**: SRE team lead
3. **Emergency**: VP Engineering

## Tools & Resources

### Test Management
- **DR Test Dashboard**: https://grafana.aurum-dev.com/d/dr-testing
- **Test Orchestrator**: http://dr-test-orchestrator.aurum-dev.svc.cluster.local:8080
- **Recovery Validation**: http://recovery-validator.aurum-dev.svc.cluster.local:8080

### Documentation
- **DR Plan**: docs/disaster-recovery/
- **Backup Procedures**: docs/backup-recovery/
- **Architecture Guide**: docs/architecture/

### Test Data
- **Test Scenarios**: k8s/dr-test/test-data/
- **Load Generators**: k8s/dr-test/load-generators/
- **Validation Scripts**: scripts/dr-testing/

## Metrics & KPIs

### Test Success Criteria
- **RTO Compliance**: < 4 hours for critical systems
- **RPO Compliance**: < 1 hour for critical data
- **Data Integrity**: > 99.9% data consistency
- **Functional Tests**: 100% pass rate
- **Performance Recovery**: > 95% of baseline

### Test Frequency
- **Full DR Test**: Monthly
- **Component Tests**: Weekly
- **Tabletop Exercises**: Quarterly
- **Emergency Drills**: Semi-annually
