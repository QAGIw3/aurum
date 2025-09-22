# Circuit Breaker Trips Runbook

## Overview

This runbook provides procedures for responding to circuit breaker activation events in the Aurum platform, which occur when services are experiencing failures or excessive load.

## Contact Information

- **Primary On-call**: platform-team@aurum.com
- **SRE Team**: sre@aurum.com
- **Emergency**: +1 (555) 123-4567

## Incident Classification

### Severity Levels

#### Critical (P0)
- Multiple circuit breakers tripped across services
- Complete service unavailability
- Cascading failure scenario

#### High (P1)
- Critical service circuit breaker tripped
- Partial service degradation
- High error rates sustained

#### Medium (P2)
- Non-critical service circuit breaker tripped
- Minimal impact on operations
- Temporary service disruption

## Detection & Assessment

### 1. Initial Detection

#### Check Circuit Breaker Status
```bash
# Check circuit breaker metrics
kubectl port-forward svc/prometheus 9090:9090 -n aurum-dev
open http://localhost:9090/alerts

# Query circuit breaker states
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state

# Check failed request rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(circuit_breaker_failures_total[5m])
```

#### Verify Service Health
```bash
# Check service availability
kubectl get endpoints -A

# Review service logs for errors
kubectl logs -l app.kubernetes.io/name=aurum-api --tail=50 | grep -i "circuit\|breaker\|failure"

# Check dependent service health
kubectl get pods -l app.kubernetes.io/name=postgres
kubectl get pods -l app.kubernetes.io/name=redis
```

#### Assess Impact Scope
```bash
# Identify affected endpoints
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_open_endpoints

# Check error rates by endpoint
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m])

# Monitor system load
kubectl top nodes
kubectl top pods -A
```

### 2. Root Cause Analysis

#### Service Dependency Issues
```bash
# Check database connectivity
kubectl exec -it deployment/aurum-api -- timeout 10 bash -c 'until pg_isready -h postgres -p 5432; do sleep 1; done'

# Check Redis connectivity
kubectl exec -it deployment/aurum-api -- redis-cli -h redis ping

# Verify external service availability
curl -s --max-time 5 https://api.eia.gov/v2/electricity/rto/region-data/data/
curl -s --max-time 5 https://webservices.iso-ne.com/api/v1.1/currentdayelec.xml
```

#### Resource Exhaustion
```bash
# Check memory usage
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=container_memory_usage_bytes / container_spec_memory_limit_bytes > 0.9

# Check CPU usage
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(container_cpu_usage_seconds_total[5m]) > 0.8

# Monitor file descriptors
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=process_open_fds > 1000
```

#### Configuration Issues
```bash
# Check circuit breaker configuration
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -i circuit

# Review recent configuration changes
kubectl logs -n kube-system -l k8s-app=kube-apiserver --since=1h | grep -i configmap

# Check for deployment issues
kubectl describe deployment aurum-api | grep -A 10 "Events"
```

## Response Procedures

### Phase 1: Immediate Response (0-5 minutes)

#### Assess Circuit Breaker State
```bash
# Check which circuit breakers are open
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state{state="open"}

# Identify affected services
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_open_services

# Check failure rates
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(circuit_breaker_failures_total[1m])
```

#### Implement Emergency Overrides
```bash
# Temporarily disable circuit breakers for critical services
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_CIRCUIT_BREAKER_ENABLED": "false"
  }
}'

# Adjust circuit breaker thresholds
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_CIRCUIT_BREAKER_THRESHOLD": "500",
    "AURUM_API_CIRCUIT_BREAKER_TIMEOUT": "60s"
  }
}'
```

#### Route Traffic to Healthy Services
```bash
# Scale up healthy service instances
kubectl scale deployment aurum-api --replicas=8

# Update load balancer configuration
kubectl patch svc/aurum-api -n aurum-dev --patch '{
  "spec": {
    "sessionAffinity": "None"
  }
}'
```

### Phase 2: Service Recovery (5-15 minutes)

#### Restore Failed Services
```bash
# Restart failed service instances
kubectl rollout restart deployment/aurum-api

# Verify service recovery
kubectl get pods -l app.kubernetes.io/name=aurum-api
kubectl logs -l app.kubernetes.io/name=aurum-api --tail=10

# Check service endpoints
kubectl get endpoints aurum-api -n aurum-dev
```

#### Reset Circuit Breakers
```bash
# Reset circuit breakers to half-open state
kubectl exec -it deployment/aurum-api -- curl -X POST http://localhost:8080/actuator/circuitbreakers/reset

# Monitor circuit breaker recovery
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state{state="half_open"}
```

#### Implement Gradual Recovery
```bash
# Enable traffic gradually
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_TRAFFIC_SHAPING_ENABLED": "true",
    "AURUM_API_TRAFFIC_SHAPING_RATE": "0.1"
  }
}'

# Monitor success rates during recovery
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=rate(circuit_breaker_success_total[1m]) / rate(circuit_breaker_requests_total[1m])
```

### Phase 3: Load Balancing & Optimization (15-30 minutes)

#### Distribute Load Effectively
```bash
# Implement intelligent load balancing
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_LOAD_BALANCING_STRATEGY": "weighted_round_robin",
    "AURUM_API_WEIGHTED_LOAD_ENABLED": "true"
  }
}'

# Scale worker services proportionally
kubectl scale deployment aurum-worker --replicas=6
```

#### Optimize Resource Allocation
```bash
# Adjust resource limits based on load
kubectl patch deployment aurum-api -n aurum-dev --patch '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "aurum-api",
          "resources": {
            "requests": {
              "cpu": "200m",
              "memory": "512Mi"
            },
            "limits": {
              "cpu": "1000m",
              "memory": "1Gi"
            }
          }
        }]
      }
    }
  }
}'
```

#### Enable Health Checks
```bash
# Implement comprehensive health checks
kubectl patch deployment aurum-api -n aurum-dev --patch '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "aurum-api",
          "livenessProbe": {
            "httpGet": {
              "path": "/health",
              "port": 8080
            },
            "initialDelaySeconds": 30,
            "periodSeconds": 10,
            "timeoutSeconds": 5,
            "failureThreshold": 3
          },
          "readinessProbe": {
            "httpGet": {
              "path": "/ready",
              "port": 8080
            },
            "initialDelaySeconds": 5,
            "periodSeconds": 5,
            "timeoutSeconds": 3,
            "failureThreshold": 3
          }
        }]
      }
    }
  }
}'
```

## Communication Templates

### Initial Breaker Trip Alert
```
⚡ [SEVERITY] Circuit Breaker Activated

**Time**: $(date)
**Service**: [API / Worker / External Service]
**Breaker State**: [OPEN / HALF_OPEN / CLOSED]
**Impact**: [Service unavailable, requests failing, degraded performance]
**Status**: Investigating

**Immediate Actions**:
- Emergency overrides activated
- Traffic rerouted to healthy instances
- Service recovery in progress

**Expected Duration**: 10-15 minutes
**Contact**: platform-team@aurum.com
```

### Recovery Progress Update
```
📊 Circuit Breaker Recovery Progress

**Time**: $(date)
**Status**: [Recovering / Stabilizing / Monitoring]
**Service**: [Service name]

**Progress**:
- ✅ Service instances: RECOVERED
- ✅ Circuit breakers: RESET
- ✅ Traffic shaping: ACTIVE
- ✅ Load balancing: OPTIMIZED

**Current Success Rate**: [95%]
**Next Update**: [Time]
**Contact**: platform-team@aurum.com
```

### Resolution Notification
```
✅ Circuit Breaker Incident Resolved

**Time**: $(date)
**Resolution**: [Services recovered, normal operation restored]
**Root Cause**: [Underlying service failure / resource exhaustion / configuration issue]

**Actions Taken**:
- Reset circuit breakers
- Scaled service instances
- Optimized resource allocation
- Enhanced monitoring

**Monitoring**: Increased circuit breaker monitoring for 24 hours
**Contact**: platform-team@aurum.com
```

## Recovery Procedures

### Emergency Service Recovery
```bash
# Force restart all affected services
kubectl rollout restart deployment/aurum-api
kubectl rollout restart deployment/aurum-worker

# Verify service recovery
kubectl get pods -l app.kubernetes.io/name=aurum-api
kubectl get pods -l app.kubernetes.io/name=aurum-worker

# Check service health
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/health
```

### Circuit Breaker Configuration Reset
```bash
# Reset circuit breaker configuration to defaults
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_CIRCUIT_BREAKER_ENABLED": "true",
    "AURUM_API_CIRCUIT_BREAKER_THRESHOLD": "1000",
    "AURUM_API_CIRCUIT_BREAKER_TIMEOUT": "30s",
    "AURUM_API_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD": "0.5",
    "AURUM_API_CIRCUIT_BREAKER_SUCCESS_THRESHOLD": "0.8"
  }
}'

# Verify configuration
kubectl get configmap aurum-api-config -n aurum-dev -o yaml | grep -i circuit
```

### Load Testing Verification
```bash
# Run load test to verify stability
kubectl create job load-test-$(date +%s) --from=cronjob/load-test

# Monitor during load test
curl http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query?query=circuit_breaker_state

# Verify system stability
kubectl top pods -l app.kubernetes.io/name=aurum-api
```

## Prevention Measures

### Enhanced Circuit Breaker Configuration
```bash
# Implement adaptive circuit breakers
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_CIRCUIT_BREAKER_ADAPTIVE": "true",
    "AURUM_API_CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD": "0.8",
    "AURUM_API_CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD": "10000"
  }
}'

# Configure circuit breaker metrics
kubectl patch configmap/monitoring-config -n aurum-dev --patch '{
  "data": {
    "CIRCUIT_BREAKER_METRICS_ENABLED": "true",
    "CIRCUIT_BREAKER_METRICS_INTERVAL": "30s"
  }
}'
```

### Service Resilience
```bash
# Implement bulkhead isolation
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_BULKHEAD_ENABLED": "true",
    "AURUM_API_BULKHEAD_MAX_CONCURRENT_CALLS": "50",
    "AURUM_API_BULKHEAD_MAX_WAIT_DURATION": "10000"
  }
}'

# Enable retry mechanisms
kubectl patch configmap/aurum-api-config -n aurum-dev --patch '{
  "data": {
    "AURUM_API_RETRY_ENABLED": "true",
    "AURUM_API_RETRY_MAX_ATTEMPTS": "3",
    "AURUM_API_RETRY_BACKOFF": "1000"
  }
}'
```

### Monitoring & Alerting
```bash
# Enhance circuit breaker monitoring
kubectl patch configmap/monitoring-config -n aurum-dev --patch '{
  "data": {
    "CIRCUIT_BREAKER_STATE_CHANGE_ALERTS": "true",
    "CIRCUIT_BREAKER_FAILURE_RATE_ALERTS": "true",
    "CIRCUIT_BREAKER_SLOW_CALL_ALERTS": "true"
  }
}'

# Set up predictive alerting
kubectl patch configmap/monitoring-config -n aurum-dev --patch '{
  "data": {
    "PREDICTIVE_CIRCUIT_BREAKER_ALERTS": "true",
    "CIRCUIT_BREAKER_PREDICTION_WINDOW": "300"
  }
}'
```

## Escalation Procedures

### When to Escalate
- Circuit breaker remains open > 15 minutes
- Multiple services affected
- Cascading failures observed
- Emergency overrides ineffective

### Escalation Path
1. **Primary**: Platform team lead
2. **Secondary**: SRE team lead
3. **Emergency**: VP Engineering

## Post-Incident Tasks

### Immediate Actions
1. Document complete incident timeline
2. Review circuit breaker configurations
3. Update monitoring thresholds
4. Communicate resolution to stakeholders

### Follow-up Tasks
1. **Post-mortem**: Schedule within 1 business day
2. **Configuration Review**: Audit circuit breaker settings
3. **Resilience Testing**: Conduct load tests
4. **Documentation**: Update resilience patterns

## Tools & Resources

### Monitoring Dashboards
- **Circuit Breaker Dashboard**: https://grafana.aurum-dev.com/d/circuit-breakers
- **Service Health Dashboard**: https://grafana.aurum-dev.com/d/service-health
- **System Resilience Dashboard**: https://grafana.aurum-dev.com/d/system-resilience

### Management Tools
- **Circuit Breaker Manager**: http://circuit-breaker-manager.aurum-dev.svc.cluster.local:8080
- **Service Manager**: http://service-manager.aurum-dev.svc.cluster.local:8080
- **Resilience Testing**: http://resilience-test.aurum-dev.svc.cluster.local:8080

### Documentation
- **Resilience Architecture Guide**: docs/resilience/
- **Circuit Breaker Patterns**: docs/patterns/circuit-breaker/
- **Service Mesh Guide**: docs/service-mesh/

## Metrics & KPIs

### Response Time Targets
- **Detection**: < 2 minutes
- **Assessment**: < 5 minutes
- **Mitigation**: < 10 minutes
- **Full Recovery**: < 20 minutes

### Success Metrics
- **MTTR**: < 10 minutes
- **Circuit Breaker Success Rate**: > 95%
- **False Trip Rate**: < 5%
- **Service Availability During Incident**: > 98%
