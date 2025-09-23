# Multi-Region Failover Runbook

## Overview
This runbook provides procedures for failing over the Aurum platform from primary region (us-east-1) to secondary region (us-west-2) in case of a regional outage.

## Contact Information
- **Primary On-call**: oncall-platform@aurum.com
- **Platform Team**: platform-team@aurum.com
- **Emergency Escalation**: engineering-leadership@aurum.com
- **Phone**: +1 (555) 123-4567

## Prerequisites
- Cross-region backups are up-to-date (within 6 hours)
- Secondary region infrastructure is deployed and healthy
- DNS failover capability is configured
- Network connectivity between regions

## Failover Decision Criteria

### Automatic Failover Triggers
- Primary region availability < 95% for >15 minutes
- Multiple AZ failures in primary region
- Critical service unavailability in primary region
- Database replication lag > 1 hour

### Manual Failover Triggers
- Planned maintenance requiring region switch
- Security incident requiring geographic isolation
- Regulatory requirements for data residency
- Cost optimization through regional workload balancing

## Pre-Failover Checklist

### 1. Assessment Phase (15 minutes)
```bash
# Check primary region health
kubectl get nodes -l region=primary
kubectl get pods -A -l region=primary --field-selector=status.phase!=Running

# Check secondary region readiness
kubectl get nodes -l region=secondary
kubectl get pods -A -l region=secondary --field-selector=status.phase!=Running

# Verify cross-region backups
mc stat aurum-secondary/aurum-backups/replication-metadata.json

# Check data consistency
kubectl exec -n aurum-dev postgres-secondary -- psql -c "SELECT COUNT(*) FROM tenants"
```

### 2. Risk Assessment
- **Data Loss Risk**: Check RPO compliance
- **Downtime Impact**: Estimate user impact
- **Recovery Time**: Validate RTO targets
- **Dependency Impact**: Assess downstream systems

## Failover Execution

### Phase 1: Preparation (30 minutes)

#### 1.1 Notify Stakeholders
```bash
# Send initial notification
echo "🚨 INITIATING FAILOVER: Aurum Platform Region Migration

Time: $(date)
Reason: [Primary region outage / Planned maintenance / Security incident]
Expected Impact: 30-60 minute service interruption
Status: PREPARATION PHASE

Next Update: 15 minutes
Contact: oncall-platform@aurum.com" | mail -s "Aurum Platform Failover Initiated" stakeholders@aurum.com
```

#### 1.2 Prepare Secondary Region
```bash
# Scale up secondary region resources
kubectl scale deployment aurum-api-secondary --replicas=5 -n aurum-dev
kubectl scale deployment aurum-worker-secondary --replicas=10 -n aurum-dev

# Verify secondary database connectivity
kubectl exec -n aurum-dev postgres-secondary -- psql -c "SELECT 1"

# Check secondary Kafka cluster
kubectl exec -n aurum-dev kafka-secondary -- kafka-topics --bootstrap-server localhost:9092 --list
```

#### 1.3 Data Synchronization Check
```bash
# Verify latest backup is available in secondary region
LATEST_BACKUP=$(mc ls aurum-secondary/aurum-backups/postgresql/ | sort -k6 | tail -1 | awk '{print $6}')
echo "Latest backup available: $LATEST_BACKUP"

# Check backup integrity
mc stat "aurum-secondary/aurum-backups/postgresql/$LATEST_BACKUP"
```

### Phase 2: Service Migration (45 minutes)

#### 2.1 Database Failover
```bash
# 1. Stop writes to primary database
kubectl exec -n aurum-dev postgres-primary -- psql -c "SELECT pg_wal_replay_pause();"

# 2. Perform final backup
kubectl create job postgres-final-backup-$(date +%s) --from=cronjob/postgres-backup-schedule -n aurum-dev

# 3. Wait for backup completion
kubectl wait --for=condition=complete --timeout=600s job/postgres-final-backup-$(date +%s) -n aurum-dev

# 4. Restore to secondary database
kubectl create job postgres-failover-restore-$(date +%s) --from=cronjob/postgres-restore -n aurum-dev

# 5. Verify restore
kubectl logs job/postgres-failover-restore-$(date +%s) -n aurum-dev
```

#### 2.2 Application Failover
```bash
# 1. Stop primary region services
kubectl scale deployment aurum-api --replicas=0 -n aurum-dev
kubectl scale deployment aurum-worker --replicas=0 -n aurum-dev

# 2. Start secondary region services
kubectl scale deployment aurum-api-secondary --replicas=5 -n aurum-dev
kubectl scale deployment aurum-worker-secondary --replicas=10 -n aurum-dev

# 3. Verify service health
kubectl get pods -l app.kubernetes.io/name=aurum-api-secondary -n aurum-dev
kubectl get pods -l app.kubernetes.io/name=aurum-worker-secondary -n aurum-dev

# 4. Test critical endpoints
curl -f https://api-secondary.aurum.com/health
curl -f https://api-secondary.aurum.com/v1/curves
```

#### 2.3 DNS and Load Balancer Update
```bash
# Update DNS records to point to secondary region
# This step depends on your DNS provider and configuration

# Update Route53 (example)
aws route53 change-resource-record-sets \
  --hosted-zone-id Z1234567890ABC \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "api.aurum.com",
        "Type": "CNAME",
        "TTL": 300,
        "ResourceRecords": [{"Value": "api-secondary.aurum.com"}]
      }
    }]
  }'

# Verify DNS propagation
nslookup api.aurum.com
```

### Phase 3: Validation (30 minutes)

#### 3.1 Service Health Validation
```bash
# Check all critical services
kubectl get pods -A -l region=secondary --field-selector=status.phase!=Running

# Verify API endpoints
for endpoint in /health /v1/curves /v1/scenarios; do
  curl -f "https://api-secondary.aurum.com$endpoint" && echo "✅ $endpoint"
done

# Check database connectivity
kubectl exec -n aurum-dev postgres-secondary -- psql -c "SELECT COUNT(*) FROM tenants"

# Verify Kafka message flow
kubectl exec -n aurum-dev kafka-secondary -- kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-failover \
  --property "key.separator=:" \
  --property "parse.key=true" \
  < /dev/null &
```

#### 3.2 Data Consistency Validation
```bash
# Compare data between regions
PRIMARY_COUNT=$(kubectl exec -n aurum-dev postgres-primary -- psql -t -c "SELECT COUNT(*) FROM scenarios" 2>/dev/null || echo "0")
SECONDARY_COUNT=$(kubectl exec -n aurum-dev postgres-secondary -- psql -t -c "SELECT COUNT(*) FROM scenarios")

echo "Primary region scenarios: $PRIMARY_COUNT"
echo "Secondary region scenarios: $SECONDARY_COUNT"

if [ "$PRIMARY_COUNT" == "$SECONDARY_COUNT" ]; then
  echo "✅ Data consistency verified"
else
  echo "❌ Data inconsistency detected!"
  echo "Difference: $((PRIMARY_COUNT - SECONDARY_COUNT))"
fi
```

#### 3.3 Performance Validation
```bash
# Run basic performance tests
ab -n 100 -c 10 https://api-secondary.aurum.com/health

# Check resource utilization
kubectl top nodes -l region=secondary
kubectl top pods -A -l region=secondary

# Verify autoscaling
kubectl get hpa -n aurum-dev
```

### Phase 4: Monitoring and Stabilization (Continuous)

#### 4.1 Enable Enhanced Monitoring
```bash
# Increase monitoring frequency during stabilization
kubectl patch hpa aurum-api-secondary -n aurum-dev -p '{
  "spec": {
    "behavior": {
      "scaleDown": {
        "stabilizationWindowSeconds": 60
      }
    }
  }
}'

# Update alert thresholds
kubectl apply -f alerts/failover-monitoring.yaml
```

#### 4.2 Monitor Key Metrics
```bash
# API availability
curl http://prometheus-secondary.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_api_availability

# Database connections
curl http://prometheus-secondary.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_db_connections_active

# Worker queue depth
curl http://prometheus-secondary.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_scenario_queue_size
```

## Rollback Procedure

### If Issues Detected During Failover

#### 1. Quick Assessment
```bash
# Check secondary region health
kubectl get pods -A -l region=secondary --field-selector=status.phase!=Running | wc -l

# Check service endpoints
curl -f https://api-secondary.aurum.com/health || echo "❌ API not responding"
```

#### 2. Rollback Decision
- If secondary region issues are minor: Fix and continue
- If secondary region is unstable: Initiate rollback to primary
- If both regions have issues: Escalate to disaster recovery

#### 3. Rollback Execution
```bash
# Scale down secondary
kubectl scale deployment aurum-api-secondary --replicas=0 -n aurum-dev

# Scale up primary
kubectl scale deployment aurum-api --replicas=3 -n aurum-dev

# Update DNS back to primary
aws route53 change-resource-record-sets \
  --hosted-zone-id Z1234567890ABC \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "api.aurum.com",
        "Type": "CNAME",
        "TTL": 300,
        "ResourceRecords": [{"Value": "api.aurum.com"}]
      }
    }]
  }'
```

## Post-Failover Tasks

### Immediate Actions (0-4 hours)
1. **Document the incident** in the incident management system
2. **Update stakeholders** every 30 minutes during stabilization
3. **Monitor system performance** closely
4. **Address any immediate issues** found during validation

### Short-term Actions (4-24 hours)
1. **Resume normal monitoring** thresholds
2. **Enable cross-region replication** from new primary (secondary)
3. **Update documentation** with lessons learned
4. **Schedule post-mortem** review

### Long-term Actions (1-7 days)
1. **Conduct full post-mortem** analysis
2. **Implement improvements** based on findings
3. **Update runbooks** and procedures
4. **Test failover procedures** in non-production environment

## Communication Templates

### Initial Failover Notification
```
🚨 FAILOVER INITIATED: Aurum Platform Region Migration

**Time**: $(date)
**Reason**: [Brief description of reason]
**Current Status**: Preparation phase
**Expected Downtime**: 30-60 minutes
**Impact**: Service interruption during migration

**Next Steps**:
- Secondary region preparation
- Data synchronization
- Service migration
- Validation testing

**Contact**: oncall-platform@aurum.com
```

### Progress Update
```
📊 FAILOVER PROGRESS: Aurum Platform Region Migration

**Time**: $(date)
**Current Phase**: [Preparation/Service Migration/Validation]
**Completed**: [List of completed steps]
**In Progress**: [Current activity]
**Next**: [Upcoming steps]

**Status**: On track / Delayed / Issues detected
**Updated ETA**: [New estimated completion time]

**Contact**: oncall-platform@aurum.com
```

### Completion Notification
```
✅ FAILOVER COMPLETED: Aurum Platform Region Migration

**Time**: $(date)
**Result**: Successful
**New Primary Region**: us-west-2
**Service Status**: All systems operational
**Data Loss**: None
**Total Downtime**: [Actual downtime duration]

**Next Steps**:
- Monitoring increased for next 24 hours
- Cross-region replication being re-established
- Post-mortem scheduled for [date/time]

**Contact**: platform-team@aurum.com
```

## Metrics and KPIs

### Failover Success Metrics
- **Total Downtime**: < 4 hours (target)
- **Data Loss**: < 1 hour of data (RPO)
- **Service Recovery**: 100% of critical services
- **User Impact**: Minimal (clear communication)

### Monitoring During Failover
- **API Availability**: >95% in secondary region
- **Database Response Time**: <500ms
- **Error Rate**: <5% during migration
- **Queue Processing**: Active in secondary region

## Tools and Resources

### Monitoring Dashboards
- **Primary Region**: https://grafana-primary.aurum.com/d/region-health
- **Secondary Region**: https://grafana-secondary.aurum.com/d/region-health
- **Cross-region**: https://grafana-global.aurum.com/d/cross-region-replication

### Command Line Tools
- **kubectl**: For Kubernetes operations
- **aws CLI**: For DNS and AWS operations
- **mc**: For MinIO operations
- **psql**: For database operations

### Documentation
- **Architecture Guide**: docs/architecture/multi-region-setup.md
- **Backup Procedures**: docs/operations/backup-restore.md
- **Network Configuration**: docs/networking/cross-region.md

## Emergency Contacts

### Technical Support
- **Primary On-call**: oncall-platform@aurum.com
- **Platform Team**: platform-team@aurum.com
- **Infrastructure**: infra-team@aurum.com

### Business Stakeholders
- **Operations**: operations@aurum.com
- **Business Continuity**: bc-team@aurum.com
- **Executive**: leadership@aurum.com

### External Vendors
- **Cloud Provider**: aws-support@amazon.com
- **CDN Provider**: support@cloudflare.com
- **Monitoring**: support@datadog.com
