# Platform On-Call Runbook

## Overview
This runbook provides procedures for on-call engineers responding to platform incidents in the Aurum energy trading system.

## Contact Information
- **Primary On-call**: oncall-platform@aurum.com
- **Platform Team**: platform-team@aurum.com
- **Emergency Escalation**: engineering-leadership@aurum.com
- **Phone**: +1 (555) 123-4567

## Incident Response Process

### 1. Incident Detection & Assessment

#### Check Alert Sources
```bash
# Check Prometheus alerts
kubectl port-forward svc/prometheus 9090:9090 -n aurum-dev
open http://localhost:9090/alerts

# Check Grafana dashboards
kubectl port-forward svc/grafana 3000:3000 -n aurum-dev
open http://localhost:3000/d/aurum-comprehensive-observability
```

#### Assess System Health
```bash
# Check overall cluster health
kubectl get nodes
kubectl get pods -A --field-selector=status.phase!=Running

# Check critical services
kubectl get pods -l app.kubernetes.io/name=aurum-api
kubectl get pods -l app.kubernetes.io/name=aurum-worker
kubectl get pods -l app.kubernetes.io/name=postgres
```

#### Review Recent Changes
- Check recent deployments
- Review CI/CD pipeline status
- Check for recent configuration changes

### 2. Common Incident Types

#### API Service Issues

**Symptoms:**
- High error rates in API endpoints
- Increased latency
- Failed health checks

**Troubleshooting Steps:**
```bash
# Check API pod logs
kubectl logs -l app.kubernetes.io/name=aurum-api --tail=100 -f

# Check API service status
kubectl describe svc/aurum-api -n aurum-dev

# Verify database connectivity
kubectl exec -it deployment/aurum-api -- /bin/bash
pg_isready -h postgres.aurum-dev.svc.cluster.local -p 5432

# Check resource usage
kubectl top pods -l app.kubernetes.io/name=aurum-api
```

**Remediation:**
1. Scale API deployment: `kubectl scale deployment aurum-api --replicas=5`
2. Restart failing pods: `kubectl rollout restart deployment/aurum-api`
3. Check for memory leaks in application logs

#### Database Connection Issues

**Symptoms:**
- Database connection errors in logs
- Slow query performance
- Connection pool exhaustion

**Troubleshooting Steps:**
```bash
# Check PostgreSQL status
kubectl logs -l app.kubernetes.io/name=postgres -f

# Check database connections
kubectl exec -it deployment/postgres -- psql -c "SELECT state, count(*) FROM pg_stat_activity GROUP BY state;"

# Verify connection pool metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_db_connections_active
```

**Remediation:**
1. Restart database connections
2. Increase connection pool size in configuration
3. Scale database if needed

#### Worker Queue Backlog

**Symptoms:**
- Increasing queue depth
- Worker pods in error state
- Processing delays

**Troubleshooting Steps:**
```bash
# Check worker pod status
kubectl get pods -l app.kubernetes.io/name=aurum-worker

# Check queue metrics
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_scenario_queue_size

# Review worker logs
kubectl logs -l app.kubernetes.io/name=aurum-worker --tail=50
```

**Remediation:**
1. Scale worker deployment: `kubectl scale deployment aurum-worker --replicas=10`
2. Check for stuck scenarios and cancel if necessary
3. Restart worker pods: `kubectl rollout restart deployment/aurum-worker`

#### Infrastructure Issues

**Symptoms:**
- Node failures
- Resource exhaustion
- Network connectivity problems

**Troubleshooting Steps:**
```bash
# Check node status
kubectl describe nodes

# Check resource usage across cluster
kubectl top nodes
kubectl top pods -A

# Check network policies
kubectl get networkpolicies -A
```

**Remediation:**
1. Drain and restart failing nodes
2. Adjust resource requests/limits
3. Update network policies if needed

### 3. Probe Health Checks

#### Readiness Probe Failures
```bash
# Check readiness probe status
kubectl describe pod <pod-name> | grep -A 10 "Readiness"

# Test readiness endpoint manually
kubectl exec -it <pod-name> -- curl -f http://localhost:8080/health/ready

# Review application logs for readiness issues
kubectl logs <pod-name> --since=10m | grep -i readiness
```

#### Liveness Probe Failures
```bash
# Check liveness probe status
kubectl describe pod <pod-name> | grep -A 10 "Liveness"

# Test liveness endpoint manually
kubectl exec -it <pod-name> -- curl -f http://localhost:8080/health

# Check for application deadlocks or memory issues
kubectl logs <pod-name> --previous | tail -20
```

#### Startup Probe Failures
```bash
# Check startup probe status
kubectl describe pod <pod-name> | grep -A 10 "Startup"

# Review startup logs
kubectl logs <pod-name> -f

# Check resource allocation for startup
kubectl top pod <pod-name>
```

### 4. SLO Monitoring

#### API Availability
```bash
# Check current API availability
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_api_availability

# Review error rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(aurum_api_requests_total{status=~"5.."}[5m])
```

#### Worker Success Rate
```bash
# Check worker success rate
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=aurum_worker_success_rate

# Review worker errors
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(aurum_worker_errors_total[5m])
```

### 5. Recovery Procedures

#### Database Recovery
```bash
# Trigger database backup restore
kubectl create job postgres-restore-$(date +%s) --from=cronjob/postgres-restore

# Verify restore completion
kubectl logs job/postgres-restore-$(date +%s)

# Test database connectivity
kubectl exec -it deployment/aurum-api -- psql -h postgres -c "SELECT 1"
```

#### Service Restart
```bash
# Rolling restart of API service
kubectl rollout restart deployment/aurum-api

# Verify rollout status
kubectl rollout status deployment/aurum-api

# Restart specific worker pods
kubectl delete pod -l app.kubernetes.io/name=aurum-worker
```

### 6. Communication Templates

#### Initial Incident Response
```
🚨 [SEVERITY] Aurum Platform Incident Detected

**Time**: $(date)
**Component**: [API/Worker/Database/Infrastructure]
**Impact**: [Brief description of impact]
**Status**: Investigating

**Next Steps**:
- Assessing impact and root cause
- Implementing remediation measures
- Will provide update in 15 minutes

**Contact**: oncall-platform@aurum.com
```

#### Status Update Template
```
📊 Aurum Platform Incident Update

**Time**: $(date)
**Status**: [Investigating/Fixing/Monitoring/Resolved]
**Component**: [Component name]
**Impact**: [Current impact level]

**Progress**:
- [Completed actions]
- [In-progress actions]

**Next Steps**:
- [Planned actions]
- [Timeline if available]

**Contact**: oncall-platform@aurum.com
```

#### Resolution Template
```
✅ Aurum Platform Incident Resolved

**Time**: $(date)
**Component**: [Component name]
**Resolution**: [Brief resolution summary]

**Root Cause**: [Root cause analysis]
**Actions Taken**: [Summary of remediation steps]

**Next Steps**:
- Post-mortem scheduled for [date/time]
- Monitoring increased for [time period]

**Contact**: platform-team@aurum.com
```

### 7. Escalation Procedures

#### When to Escalate
- Incident affects >50% of production traffic
- Service unavailable for >30 minutes
- Multiple critical systems affected
- Database corruption or data loss detected
- Security incident suspected

#### Escalation Contacts
1. **Primary**: Platform team lead
2. **Secondary**: Engineering director
3. **Emergency**: CTO/VP Engineering

### 8. Post-Incident Procedures

#### Immediate Actions
1. Document incident timeline and actions taken
2. Update incident management system
3. Review and update monitoring thresholds if needed
4. Communicate resolution to stakeholders

#### Follow-up Tasks
1. **Post-mortem**: Schedule within 1 business day
2. **Action Items**: Track and complete remediation tasks
3. **Documentation**: Update runbooks and procedures
4. **Training**: Share lessons learned with team

### 9. Tools and Resources

#### Monitoring Dashboards
- **Main Dashboard**: https://grafana.aurum-dev.com/d/aurum-comprehensive-observability
- **API Metrics**: https://grafana.aurum-dev.com/d/aurum-api-latency
- **Worker Metrics**: https://grafana.aurum-dev.com/d/aurum-scenario-worker

#### Logging Systems
- **Application Logs**: Accessible via kubectl logs
- **Structured Logs**: Queryable via ClickHouse at clickhouse:8123
- **Audit Logs**: Available in Minio bucket aurum-logs

#### Documentation
- **Architecture Guide**: docs/architecture/
- **Deployment Guide**: docs/deployment/
- **Troubleshooting Guide**: docs/troubleshooting/

### 10. Metrics and KPIs

#### Response Time Targets
- **Detection**: < 5 minutes
- **Assessment**: < 15 minutes
- **Initial Response**: < 30 minutes
- **Resolution**: < 4 hours (critical), < 8 hours (important)

#### Success Metrics
- **MTTR (Mean Time to Resolution)**: < 2 hours
- **MTTD (Mean Time to Detection)**: < 5 minutes
- **Service Availability**: > 99.9% monthly
- **False Positive Rate**: < 5% of alerts
