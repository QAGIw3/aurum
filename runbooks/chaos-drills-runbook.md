# Monthly Chaos Drills Runbook

## Overview

This runbook provides procedures for conducting monthly chaos engineering drills to improve system resilience and measure Mean Time to Recovery (MTTR) for various failure scenarios.

## Contact Information

- **Primary On-call**: platform-team@aurum.com
- **SRE Team**: sre@aurum.com
- **Chaos Engineering Team**: chaos@aurum.com
- **Emergency**: +1 (555) 123-4567

## Chaos Drill Objectives

### Resilience Testing Goals
- **System Resilience**: Verify system behavior under stress
- **Failure Detection**: Ensure monitoring detects issues quickly
- **Recovery Automation**: Validate automated recovery mechanisms
- **Performance Degradation**: Measure graceful degradation capabilities
- **MTTR Improvement**: Track and improve recovery times

### MTTR Targets
- **Critical Services**: < 5 minutes
- **Important Services**: < 15 minutes
- **Supporting Services**: < 30 minutes
- **Full System Recovery**: < 2 hours

## Drill Classification

### Drill Types

#### Infrastructure Drills (P0)
- Node failures and resource exhaustion
- Network latency and partition simulation
- Storage system failures
- Load balancer issues

#### Application Drills (P1)
- Service dependency failures
- Database connection pool exhaustion
- Cache system failures
- Message queue disruptions

#### External Dependency Drills (P2)
- External API failures
- Provider service outages
- Third-party system failures
- Network connectivity issues

## Pre-Drill Checklist

### 1. Environment Preparation
```bash
# Verify chaos testing environment
kubectl get namespaces | grep chaos

# Check chaos engineering tools
kubectl get pods -l app.kubernetes.io/name=chaos-mesh -n chaos-engineering
kubectl get pods -l app.kubernetes.io/name=litmus -n chaos-engineering

# Verify monitoring and alerting
kubectl get pods -l app.kubernetes.io/name=prometheus
kubectl get pods -l app.kubernetes.io/name=alertmanager
```

### 2. Safety Controls
```bash
# Verify circuit breakers are enabled
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state{state="closed"}

# Check rate limiting configuration
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -i rate.limit

# Verify graceful degradation settings
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -i graceful
```

### 3. Communication Setup
```bash
# Notify stakeholders
cat << EOF > /tmp/chaos-drill-notification.json
{
  "timestamp": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
  "type": "CHAOS_DRILL_START",
  "drill_type": "provider_failure_simulation",
  "duration": "60_minutes",
  "impact": "minimal_controlled",
  "contact": "platform-team@aurum.com"
}
EOF

# Send notification
curl -X POST -H 'Content-type: application/json' --data-binary @/tmp/chaos-drill-notification.json ${SLACK_WEBHOOK_URL}
```

## Monthly Drill Schedule

### Week 1: Infrastructure Resilience
- **Day**: First Monday
- **Focus**: Node failures, resource exhaustion
- **Duration**: 45 minutes
- **Impact**: Low

### Week 2: Application Resilience
- **Day**: Second Monday
- **Focus**: Service failures, dependency issues
- **Duration**: 60 minutes
- **Impact**: Medium

### Week 3: Data Resilience
- **Day**: Third Monday
- **Focus**: Database issues, cache failures
- **Duration**: 75 minutes
- **Impact**: Medium

### Week 4: Full System Resilience
- **Day**: Fourth Monday
- **Focus**: Multi-component failures, recovery testing
- **Duration**: 90 minutes
- **Impact**: High (requires approval)

## Drill Execution Procedures

### Infrastructure Drills

#### Node Failure Simulation
```bash
# Simulate node failure
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: node-failure-drill
  namespace: chaos-engineering
spec:
  action: pod-failure
  mode: one
  selector:
    labelSelectors:
      app.kubernetes.io/name: aurum-worker
  scheduler:
    cron: "@every 5m"
EOF

# Monitor impact
kubectl get events --field-selector reason=PodFailure -n aurum-dev

# Verify recovery
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=up{job="aurum-worker"}
```

#### Resource Exhaustion Drill
```bash
# Create resource stress
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: cpu-stress-drill
  namespace: chaos-engineering
spec:
  mode: one
  selector:
    labelSelectors:
      app.kubernetes.io/name: aurum-api
  stressors:
    cpu:
      workers: 2
      load: 80
    memory:
      workers: 1
      size: "256MB"
  duration: "10m"
EOF

# Monitor system behavior
kubectl top nodes
kubectl top pods -l app.kubernetes.io/name=aurum-api

# Verify graceful degradation
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/health
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/metrics | grep -i "cpu\|memory"
```

#### Network Latency Drill
```bash
# Inject network latency
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: network-latency-drill
  namespace: chaos-engineering
spec:
  action: delay
  mode: all
  selector:
    labelSelectors:
      app.kubernetes.io/name: aurum-api
  delay:
    latency: "100ms"
    correlation: "25"
    jitter: "10ms"
  duration: "15m"
EOF

# Monitor application performance
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(aurum_api_request_duration_seconds_sum[5m])

# Verify timeout handling
curl --max-time 30 -s http://aurum-api.aurum-dev.svc.cluster.local:8080/v1/scenarios | head -c 100
```

### Application Drills

#### Database Connection Pool Exhaustion
```bash
# Simulate connection pool exhaustion
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: db-connection-drill
  namespace: chaos-engineering
spec:
  action: pod-failure
  mode: fixed-percent
  value: "20"
  selector:
    labelSelectors:
      app.kubernetes.io/name: aurum-api
  scheduler:
    cron: "@every 2m"
EOF

# Monitor connection pool metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_db_connections_active

# Verify connection pool behavior
kubectl logs -l app.kubernetes.io/name=aurum-api --tail=20 | grep -i "connection\|pool"
```

#### Cache Failure Simulation
```bash
# Simulate Redis failure
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: cache-failure-drill
  namespace: chaos-engineering
spec:
  action: pod-kill
  mode: one
  selector:
    labelSelectors:
      app.kubernetes.io/name: redis
  duration: "5m"
EOF

# Monitor cache hit rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(cache_hits_total[1m]) / rate(cache_requests_total[1m])

# Verify application behavior during cache failure
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/health
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/v1/curves | jq '. | length'
```

### External Provider Drills

#### External API Failure Simulation
```bash
# Block external API calls
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: external-api-failure-drill
  namespace: chaos-engineering
spec:
  action: reject
  mode: all
  selector:
    labelSelectors:
      app.kubernetes.io/name: external-data-collector
  target:
    selector:
      namespaces:
        - external-providers
    mode: all
  duration: "10m"
EOF

# Monitor external data ingestion
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=external_data_ingestion_success_rate

# Verify fallback mechanisms
kubectl get pods -l app.kubernetes.io/name=external-data-fallback
```

#### Slow Network Simulation
```bash
# Inject network delays to external services
kubectl create -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: slow-network-drill
  namespace: chaos-engineering
spec:
  action: delay
  mode: all
  selector:
    labelSelectors:
      app.kubernetes.io/name: external-data-collector
  delay:
    latency: "5000ms"
    correlation: "50"
    jitter: "1000ms"
  target:
    selector:
      namespaces:
        - external-providers
    mode: all
  duration: "15m"
EOF

# Monitor timeout handling
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(http_request_duration_seconds_bucket{le="30"}[5m])

# Verify circuit breaker activation
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state{state="open"}
```

## MTTR Measurement & Analysis

### Incident Detection Time
```bash
# Measure time to detection
DETECTION_TIME=$(curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=chaos_experiment_start_time)

# Calculate detection delay
DETECTION_DELAY=$((CURRENT_TIME - DETECTION_TIME))

# Log detection metrics
cat > /tmp/detection_metrics.json << EOF
{
  "experiment": "chaos_drill_$(date +%Y%m%d)",
  "detection_time_seconds": $DETECTION_DELAY,
  "target_detection_time": 60,
  "status": $(if [ $DETECTION_DELAY -le 60 ]; then echo "COMPLIANT"; else echo "VIOLATION"; fi)
}
EOF
```

### Recovery Time Measurement
```bash
# Measure recovery time
START_TIME=$(date +%s)
# [Recovery actions]
END_TIME=$(date +%s)
RECOVERY_TIME=$((END_TIME - START_TIME))

# Calculate MTTR
MTTR_MINUTES=$((RECOVERY_TIME / 60))

# Log recovery metrics
cat > /tmp/recovery_metrics.json << EOF
{
  "experiment": "chaos_drill_$(date +%Y%m%d)",
  "recovery_time_seconds": $RECOVERY_TIME,
  "target_mttr": 300,
  "status": $(if [ $RECOVERY_TIME -le 300 ]; then echo "COMPLIANT"; else echo "VIOLATION"; fi),
  "automated_recovery": true,
  "manual_intervention": false
}
EOF
```

### System Resilience Scoring
```bash
# Calculate resilience score
AVAILABILITY=$(curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=uptime_ratio)
ERROR_RATE=$(curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=error_rate)
LATENCY_P95=$(curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=request_latency_p95)

# Calculate composite score
RESILIENCE_SCORE=$(( (AVAILABILITY * 0.4) + ((1 - ERROR_RATE) * 0.3) + ((1 - LATENCY_P95/1000) * 0.3) ))

# Log resilience metrics
cat > /tmp/resilience_metrics.json << EOF
{
  "experiment": "chaos_drill_$(date +%Y%m%d)",
  "availability": $AVAILABILITY,
  "error_rate": $ERROR_RATE,
  "latency_p95_ms": $LATENCY_P95,
  "resilience_score": $RESILIENCE_SCORE,
  "grade": $(if (( $(echo "$RESILIENCE_SCORE > 0.95" | bc -l) )); then echo "A"; elif (( $(echo "$RESILIENCE_SCORE > 0.9" | bc -l) )); then echo "B"; elif (( $(echo "$RESILIENCE_SCORE > 0.8" | bc -l) )); then echo "C"; else echo "D"; fi)
}
EOF
```

## Post-Drill Analysis

### Automated Analysis
```bash
# Generate comprehensive drill report
kubectl create job chaos-drill-analysis-$(date +%s) \
  --from=cronjob/chaos-drill-analysis \
  -n chaos-engineering

# Wait for analysis completion
kubectl wait --for=condition=complete job/chaos-drill-analysis-$(date +%s) -n chaos-engineering --timeout=300s

# Retrieve analysis results
kubectl logs job/chaos-drill-analysis-$(date +%s) -n chaos-engineering > /tmp/chaos_analysis.txt

# Extract key metrics
grep -E "(MTTR|Detection Time|Resilience Score)" /tmp/chaos_analysis.txt
```

### Manual Review
```bash
# Review system behavior during chaos
kubectl logs -l app.kubernetes.io/name=aurum-api --since=1h | grep -E "(ERROR|WARN|circuit|timeout)" | head -20

# Check monitoring alerts
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/alerts | jq '.data.alerts[] | select(.state=="firing") | .labels.alertname'

# Review circuit breaker activity
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(circuit_breaker_transitions_total[30m])
```

### Improvement Planning
```bash
# Identify improvement opportunities
cat > /tmp/improvement_plan.md << 'EOF'
# Chaos Drill Improvement Plan

## Issues Identified
- [List of issues found during drill]

## Recommended Actions
- [Specific improvement actions]
- [Configuration changes needed]
- [Monitoring enhancements required]

## Timeline
- Immediate: [0-24 hours]
- Short-term: [1-7 days]
- Long-term: [1-4 weeks]

## Success Metrics
- [Measurable improvement targets]
EOF

# Track improvement items
kubectl create configmap chaos-improvement-plan-$(date +%Y%m%d) \
  --from-file=plan.md=/tmp/improvement_plan.md \
  -n chaos-engineering
```

## Communication Templates

### Drill Start Notification
```
🧪 [PRIORITY] Chaos Drill Starting

**Time**: $(date)
**Drill Type**: [Infrastructure / Application / External Dependency]
**Target System**: [API / Workers / Database / External Services]
**Duration**: [45-90 minutes]
**Expected Impact**: [Minimal / Moderate / Controlled]

**Drill Objectives**:
- Test system resilience under [specific failure type]
- Measure MTTR for [failure scenario]
- Validate monitoring and alerting
- Verify automated recovery mechanisms

**Safety Controls**:
- Circuit breakers: ENABLED
- Rate limiting: ACTIVE
- Graceful degradation: ENABLED

**Contact**: platform-team@aurum.com
```

### Drill Progress Update
```
📊 Chaos Drill Progress Report

**Time**: $(date)
**Phase**: [Injection / Observation / Recovery / Analysis]
**Status**: [IN PROGRESS / COMPLETED]

**Current Results**:
- ✅ Failure Injection: SUCCESS
- 🔄 System Response: [Normal / Degraded / Failed]
- 📊 MTTR: [X minutes / Target: Y minutes]

**Observations**:
- [Key observations and system behavior]
- [Monitoring effectiveness]
- [Recovery mechanism performance]

**Next Steps**:
- [Remaining drill activities]
- [Analysis and reporting]

**Contact**: platform-team@aurum.com
```

### Drill Completion Report
```
✅ Chaos Drill Completed

**Time**: $(date)
**Overall Result**: [SUCCESS / PARTIAL SUCCESS / ISSUES FOUND]
**MTTR Achieved**: [X minutes / Target: Y minutes]
**Resilience Score**: [Z% / Grade: A/B/C/D]

**Drill Summary**:
- **Type**: [Drill type]
- **Target**: [Target system]
- **Duration**: [Actual duration]
- **Impact**: [Actual impact level]

**Key Findings**:
- [Important observations]
- [System strengths identified]
- [Areas for improvement]

**Recommendations**:
- [Specific recommendations]
- [Priority improvements needed]

**Next Drill**: [Date/Time]
**Contact**: platform-team@aurum.com
```

## Drill Tools & Resources

### Chaos Engineering Platform
- **Chaos Mesh**: http://chaos-mesh.chaos-engineering.svc.cluster.local:8080
- **Litmus**: http://litmus.chaos-engineering.svc.cluster.local:8080
- **Chaos Dashboard**: https://grafana.aurum-dev.com/d/chaos-engineering

### Monitoring & Observability
- **Drill Metrics**: https://grafana.aurum-dev.com/d/chaos-drill-metrics
- **System Health**: https://grafana.aurum-dev.com/d/system-health
- **MTTR Dashboard**: https://grafana.aurum-dev.com/d/mttr-tracking

### Documentation
- **Chaos Engineering Guide**: docs/chaos-engineering/
- **Resilience Patterns**: docs/patterns/resilience/
- **Incident Response**: docs/incident-response/

## Safety & Controls

### Circuit Breakers
```bash
# Verify circuit breaker status
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_enabled

# Check failure thresholds
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_failure_threshold

# Monitor trip rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(circuit_breaker_trips_total[5m])
```

### Rate Limiting
```bash
# Check rate limit status
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate_limit_active

# Monitor rate limit hits
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(rate_limit_exceeded_total[5m])

# Verify rate limit configuration
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -A 5 -B 5 rate.limit
```

### Graceful Degradation
```bash
# Check degradation status
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=graceful_degradation_active

# Monitor degraded endpoints
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=degraded_endpoints_total

# Verify degradation policies
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -A 10 graceful
```

## Escalation Procedures

### When to Stop a Drill
- **System Unavailability**: > 5% of production traffic affected
- **Data Loss**: Any data corruption or loss detected
- **Security Impact**: Security controls compromised
- **Duration Exceeded**: Drill runs > 2x planned duration
- **Cascading Failures**: Multiple unrelated systems failing

### Emergency Stop Procedure
```bash
# Immediately stop all chaos experiments
kubectl delete chaos -n chaos-engineering --all

# Restore normal operations
kubectl scale deployment aurum-api --replicas=5
kubectl scale deployment aurum-worker --replicas=3

# Verify system stability
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=up > 0.95
```

### Escalation Path
1. **Immediate**: Stop drill and assess impact
2. **Primary**: Platform team lead
3. **Secondary**: SRE team lead
4. **Emergency**: VP Engineering

## Metrics & KPIs

### Drill Success Criteria
- **Detection Time**: < 60 seconds
- **MTTR**: < Target for service type
- **System Availability**: > 95% during drill
- **Data Integrity**: 100% maintained
- **Safety Controls**: 100% effective

### Monthly Targets
- **Drill Completion Rate**: > 90% of scheduled drills
- **MTTR Improvement**: 10% month-over-month
- **Resilience Score**: > 95% (Grade A)
- **False Positive Alerts**: < 5%
- **Drill Safety**: 100% (no production impact)

### Long-term Goals
- **System Resilience**: Continuous improvement
- **Automated Recovery**: > 80% of scenarios
- **Predictive Monitoring**: < 30s detection time
- **Zero Downtime**: Graceful handling of all failure types
