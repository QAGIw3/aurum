# Probe Health Checks Playbook

## Overview
This playbook provides detailed procedures for troubleshooting and resolving issues with Kubernetes readiness, liveness, and startup probes in the Aurum platform.

## Probe Configuration

### API Service Probes

#### Readiness Probe
```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: http
    scheme: HTTP
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
  successThreshold: 1
```

#### Liveness Probe
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: http
    scheme: HTTP
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
  successThreshold: 1
```

#### Startup Probe
```yaml
startupProbe:
  httpGet:
    path: /health
    port: http
    scheme: HTTP
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 30
  successThreshold: 1
```

### Worker Service Probes

#### Readiness Probe
```yaml
readinessProbe:
  exec:
    command:
      - python
      - -c
      - import sys; sys.exit(0)
  initialDelaySeconds: 10
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3
  successThreshold: 1
```

#### Liveness Probe
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: http
    scheme: HTTP
  initialDelaySeconds: 60
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 3
  successThreshold: 1
```

## Troubleshooting Procedures

### 1. Probe Status Investigation

#### Check Pod Status
```bash
# Get pod status and probe information
kubectl describe pod <pod-name> -n aurum-dev

# Check specific probe status
kubectl get pod <pod-name> -n aurum-dev -o jsonpath='{.status.conditions[*].type}: {.status.conditions[*].status}' | tr ' ' '\n'
```

#### Review Probe Events
```bash
# Get events for the pod
kubectl get events --field-selector involvedObject.name=<pod-name> -n aurum-dev

# Get events for all pods with probe issues
kubectl get events -n aurum-dev --field-selector reason=Unhealthy | grep -i probe
```

### 2. Manual Probe Testing

#### Test HTTP Probes
```bash
# Test readiness probe endpoint
kubectl exec -it <pod-name> -n aurum-dev -- curl -f http://localhost:8080/health/ready

# Test liveness probe endpoint
kubectl exec -it <pod-name> -n aurum-dev -- curl -f http://localhost:8080/health

# Test with custom headers if needed
kubectl exec -it <pod-name> -n aurum-dev -- curl -H "Authorization: Bearer <token>" http://localhost:8080/health
```

#### Test Exec Probes
```bash
# Test exec-based readiness probe
kubectl exec -it <pod-name> -n aurum-dev -- python -c "import sys; sys.exit(0)"

# Test custom exec command
kubectl exec -it <pod-name> -n aurum-dev -- /usr/local/bin/healthcheck.sh
```

### 3. Common Probe Issues and Solutions

#### Issue: Readiness Probe Failing
**Symptoms:**
- Pod shows as not ready
- Service traffic not reaching pod
- Readiness probe logs show failures

**Investigation:**
```bash
# Check readiness probe logs
kubectl logs <pod-name> -n aurum-dev | grep -i readiness

# Check service dependencies
kubectl exec -it <pod-name> -n aurum-dev -- nslookup postgres.aurum-dev.svc.cluster.local

# Test database connectivity
kubectl exec -it <pod-name> -n aurum-dev -- psql -h postgres -U aurum -c "SELECT 1"
```

**Solutions:**
1. **Increase initial delay**: Update probe configuration
2. **Check service dependencies**: Verify all required services are available
3. **Review resource limits**: Ensure pod has sufficient resources
4. **Fix application startup**: Address initialization issues

#### Issue: Liveness Probe Failing
**Symptoms:**
- Pods restarting frequently
- Liveness probe logs show failures
- Application appears to hang or deadlock

**Investigation:**
```bash
# Check liveness probe logs
kubectl logs <pod-name> -n aurum-dev --previous | tail -20

# Check resource usage
kubectl top pod <pod-name> -n aurum-dev

# Check for memory leaks
kubectl exec -it <pod-name> -n aurum-dev -- ps aux | head -20
```

**Solutions:**
1. **Increase probe timeouts**: Allow more time for health checks
2. **Fix application deadlocks**: Review and fix concurrency issues
3. **Adjust resource limits**: Increase memory or CPU limits
4. **Update probe intervals**: Reduce frequency if causing load

#### Issue: Startup Probe Failing
**Symptoms:**
- Pods stuck in pending state
- Startup probe taking too long
- Application failing to start properly

**Investigation:**
```bash
# Check startup logs
kubectl logs <pod-name> -n aurum-dev -f

# Check resource allocation
kubectl describe pod <pod-name> -n aurum-dev | grep -A 10 "Events"

# Test startup process manually
kubectl exec -it <pod-name> -n aurum-dev -- /bin/sh -c "timeout 30 /usr/local/bin/startup-script.sh"
```

**Solutions:**
1. **Increase startup probe timeout**: Allow more time for initialization
2. **Fix startup scripts**: Address application startup issues
3. **Increase initial resources**: Allocate more CPU/memory for startup
4. **Optimize startup process**: Reduce startup time

### 4. Probe Configuration Optimization

#### Adjust Probe Timing
```bash
# Update readiness probe timing
kubectl patch deployment aurum-api -n aurum-dev -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "api",
          "readinessProbe": {
            "initialDelaySeconds": 10,
            "periodSeconds": 10
          }
        }]
      }
    }
  }
}'

# Update liveness probe timing
kubectl patch deployment aurum-api -n aurum-dev -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "api",
          "livenessProbe": {
            "initialDelaySeconds": 60,
            "periodSeconds": 30
          }
        }]
      }
    }
  }
}'
```

#### Update Probe Endpoints
```bash
# Patch probe endpoints if changed
kubectl patch deployment aurum-api -n aurum-dev -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "api",
          "readinessProbe": {
            "httpGet": {
              "path": "/api/health/ready",
              "port": 8080
            }
          }
        }]
      }
    }
  }
}'
```

### 5. Monitoring Probe Health

#### Create Probe Monitoring Dashboard
```yaml
# Probe success rate query for Grafana
sum(rate(probe_success{job="kubernetes-pods"}[5m])) / sum(rate(probe_total{job="kubernetes-pods"}[5m]))

# Probe duration query
histogram_quantile(0.95, rate(probe_duration_seconds_bucket{job="kubernetes-pods"}[5m]))
```

#### Alert on Probe Issues
```yaml
# Prometheus alert for readiness probe failures
- alert: ReadinessProbeFailures
  expr: rate(probe_success{job="kubernetes-pods", probe="readiness"}[5m]) < 0.95
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Readiness probe failures detected"
    description: "More than 5% of readiness probes failing in the last 5 minutes"

# Prometheus alert for liveness probe failures
- alert: LivenessProbeFailures
  expr: rate(probe_success{job="kubernetes-pods", probe="liveness"}[5m]) < 0.9
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Liveness probe failures detected"
    description: "More than 10% of liveness probes failing in the last 2 minutes"
```

### 6. Best Practices

#### Probe Configuration Guidelines
1. **Readiness Probes**:
   - Check only service dependencies, not application health
   - Should be fast (1-2 seconds)
   - High success rate expected (>99%)

2. **Liveness Probes**:
   - Check application health only
   - Can be slower (5-10 seconds)
   - Lower tolerance for failures

3. **Startup Probes**:
   - Only use when startup time is highly variable
   - Must complete before liveness probe starts
   - Remove after startup issues are resolved

#### Testing Probe Changes
```bash
# Test probe configuration before applying
kubectl apply -f probe-test.yaml --dry-run=client

# Apply to a single pod for testing
kubectl patch deployment aurum-api -n aurum-dev --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds", "value": 15}]'

# Monitor probe behavior
kubectl get events -n aurum-dev --field-selector reason=Unhealthy | grep -i probe
```

### 7. Emergency Procedures

#### Bypass Failing Probes
```bash
# Temporarily disable liveness probe
kubectl patch deployment aurum-api -n aurum-dev -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "api",
          "livenessProbe": {
            "enabled": false
          }
        }]
      }
    }
  }
}'

# Restart pods to apply changes
kubectl rollout restart deployment/aurum-api -n aurum-dev
```

#### Debug Probe Issues in Production
```bash
# Add debug logging to probe endpoints
kubectl exec -it <pod-name> -n aurum-dev -- curl -v http://localhost:8080/health

# Check probe timing
kubectl describe pod <pod-name> -n aurum-dev | grep -A 15 "Probe"
```

### 8. Documentation and Review

#### Update Probe Documentation
- Document probe endpoints and expected behavior
- Include troubleshooting steps in runbooks
- Update deployment manifests with probe configurations

#### Regular Probe Reviews
- Review probe configurations quarterly
- Test probe changes in staging first
- Monitor probe metrics for optimization opportunities

### 9. Related Resources

#### Links
- [Kubernetes Probes Documentation](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [Probe Best Practices](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#configure-probes)
- [Debugging Probes](https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/)

#### Tools
- **kubectl**: For pod and probe inspection
- **curl**: For manual probe testing
- **Prometheus**: For probe metrics monitoring
- **Grafana**: For probe health visualization
