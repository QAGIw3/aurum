# Secure Vault Secrets Rotation System

This document describes the secure and simplified Vault secrets rotation system for the Aurum platform, replacing the complex CronJob pattern with a robust, monitored solution.

## Overview

The new Vault rotation system provides:

- **Secure Authentication**: Uses Kubernetes service account tokens instead of root tokens
- **Least Privilege**: Restricted RBAC permissions for the rotation service account
- **Automatic Retry**: Built-in retry logic with exponential backoff
- **Comprehensive Monitoring**: Prometheus metrics and alerting
- **Vault Agent Integration**: Optional sidecar for secret injection and lease renewal
- **Network Isolation**: Network policies for secure communication
- **Audit Trail**: Complete logging of all rotation activities

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Scheduled       │───▶│ Rotation Job        │───▶│ Vault Secrets    │
│ Rotation        │    │ (CronJob)           │    │ Rotation         │
│ (Weekly)        │    │                     │    │                  │
└─────────────────┘    └─────────────────────┘    └──────────────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Service Account │    │ Security Context    │    │ Monitoring &     │
│ Authentication  │    │ & RBAC             │    │ Alerting         │
└─────────────────┘    └─────────────────────┘    └──────────────────┘
```

## Components

### 1. CronJob Configuration (`vault-secrets-rotation-job.yaml`)

**Key Features:**
- **Secure Authentication**: Uses Kubernetes service account tokens
- **Concurrency Control**: `Forbid` concurrent job execution
- **Deadline Management**: `startingDeadlineSeconds` for missed schedules
- **Resource Limits**: CPU and memory constraints
- **Security Context**: Non-root execution with restricted capabilities
- **Health Checks**: Liveness, readiness, and startup probes
- **Metrics Export**: Prometheus metrics via Node Exporter sidecar

### 2. RBAC Configuration (`vault-secrets-rotator-rbac.yaml`)

**Security Features:**
- **Restricted Service Account**: Minimal permissions for rotation only
- **Role-Based Access**: Specific roles for different operations
- **Network Policies**: Controlled communication between components
- **Resource Quotas**: Limits on resource consumption
- **Security Policies**: Configurable compliance rules

### 3. Vault Agent Sidecar (`vault-agent-sidecar.yaml`)

**Advanced Features:**
- **Automatic Authentication**: Handles Vault login automatically
- **Secret Injection**: Injects secrets into application containers
- **Lease Renewal**: Automatically renews authentication tokens
- **Template Rendering**: Dynamic configuration generation
- **Caching**: Improved performance through response caching

### 4. Monitoring and Alerting (`vault-rotation-monitoring.yaml`)

**Observability Features:**
- **Prometheus Metrics**: Detailed metrics for all operations
- **Alert Rules**: Proactive alerting for issues
- **Grafana Dashboard**: Visual monitoring dashboard
- **Runbooks**: Automated troubleshooting procedures
- **Audit Logging**: Complete activity trails

## Security Improvements

### Before (Original System)
- ❌ Used root tokens mounted as secrets
- ❌ No RBAC restrictions
- ❌ Complex shell-based job creation
- ❌ No concurrency control
- ❌ Limited monitoring
- ❌ Manual error handling

### After (New System)
- ✅ Kubernetes service account authentication
- ✅ Least privilege RBAC
- ✅ Direct CronJob execution
- ✅ Concurrency policy enforcement
- ✅ Comprehensive monitoring and alerting
- ✅ Automatic error handling and retry

## Configuration

### Environment Variables

```bash
# Vault Configuration
VAULT_ADDR=https://vault.aurum-dev.svc.cluster.local:8200
VAULT_SKIP_VERIFY=false

# Rotation Configuration
ROTATION_SCHEDULE="0 2 * * 1"  # Weekly Monday 2 AM UTC
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=5

# Security Configuration
SECURITY_CONTEXT_RUN_AS_USER=1001
SECURITY_CONTEXT_RUN_AS_GROUP=1001
RESTRICTED_POD_SECURITY_STANDARD=restricted
```

### CronJob Schedule

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: vault-secrets-rotation
spec:
  schedule: "0 2 * * 1"  # Weekly on Monday at 2 AM UTC
  concurrencyPolicy: Forbid  # Don't allow concurrent runs
  startingDeadlineSeconds: 3600  # Give up if missed by >1 hour
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
```

### RBAC Permissions

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: aurum-secrets-rotator
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch"]
    resourceNames: ["vault-root-token"]
```

## Usage Examples

### Manual Rotation Trigger

```bash
# Trigger immediate rotation
kubectl create job vault-secrets-rotation-manual-$(date +%s) \
  --from=cronjob/vault-secrets-rotation \
  -n aurum-dev

# Monitor the rotation
kubectl logs -f job/vault-secrets-rotation-manual-$(date +%s) -n aurum-dev
```

### Check Rotation Status

```bash
# Check job history
kubectl get jobs -n aurum-dev -l app=vault,component=secrets-rotation

# Check CronJob status
kubectl get cronjob vault-secrets-rotation -n aurum-dev

# Check rotation reports in Vault
vault kv list secret/rotation-reports/
```

### View Rotation Metrics

```bash
# Query Prometheus metrics
curl -s http://prometheus.aurum-dev.svc.cluster.local:9090/api/v1/query \
  -d 'query=kube_job_status_succeeded{job_name=~"vault-secrets-rotation.*"}' | jq

# Check alert status
kubectl get prometheusrule vault-secrets-rotation -n aurum-dev -o yaml
```

## Monitoring

### Key Metrics

- **Job Success Rate**: `rate(kube_job_status_succeeded[5m])`
- **Job Duration**: `kube_job_status_completion_time - kube_job_status_start_time`
- **Failed Jobs**: `kube_job_status_failed`
- **Last Rotation Time**: `vault_secret_last_rotation`
- **Rotation Schedule Compliance**: Time since last successful rotation

### Alert Rules

1. **RotationJobFailed**: Alert when rotation job fails
2. **RotationJobStuck**: Alert when job runs too long
3. **RotationMissedSchedule**: Alert when rotation is overdue
4. **SecretsRotationOverdue**: Alert when secrets are too old

### Dashboard Access

- **Vault Rotation Dashboard**: `https://grafana.aurum-dev/d/vault-rotation`
- **Security Overview**: `https://grafana.aurum-dev/d/security-overview`

## Troubleshooting

### Common Issues

#### 1. Authentication Failures

**Symptoms:**
- "permission denied" errors
- Authentication timeout

**Resolution:**
```bash
# Check service account
kubectl describe sa aurum-secrets-rotator -n aurum-dev

# Test Vault authentication
kubectl run vault-test --rm -it --restart=Never \
  --image=curlimages/curl \
  -- vault login -method=kubernetes \
  role=aurum-secrets-rotator \
  jwt=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
```

#### 2. Job Scheduling Issues

**Symptoms:**
- No jobs created
- Missed schedules

**Resolution:**
```bash
# Check CronJob configuration
kubectl describe cronjob vault-secrets-rotation -n aurum-dev

# Check cluster events
kubectl get events -n aurum-dev --field-selector involvedObject.name=vault-secrets-rotation
```

#### 3. Vault Connectivity Issues

**Symptoms:**
- Connection timeouts
- Certificate errors

**Resolution:**
```bash
# Test Vault connectivity
kubectl run vault-connectivity-test --rm -it --restart=Never \
  --image=curlimages/curl \
  -- curl -k -v https://vault.aurum-dev.svc.cluster.local:8200/v1/sys/health

# Check Vault status
kubectl exec -it deployment/vault -n aurum-dev -- vault status
```

### Debug Mode

Enable detailed logging:
```bash
kubectl logs -f deployment/vault-agent -n aurum-dev -v=8
```

### Manual Recovery

```bash
# 1. Identify failed operations
kubectl logs job/<failed-job> -n aurum-dev

# 2. Manually rotate specific secrets
kubectl run manual-rotation --rm -it --restart=Never \
  --image=hashicorp/vault:1.15.5 \
  -- vault kv put secret/aurum/api-keys \
  rotated_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
  rotation_period="30d" \
  rotated_by="manual-recovery"
```

## Security Best Practices

### Authentication

- **Use Service Account Tokens**: Never mount root tokens
- **Short-lived Tokens**: Tokens expire automatically
- **RBAC Enforcement**: Least privilege principle
- **Audit Logging**: All access logged and monitored

### Network Security

- **Network Policies**: Restrict communication paths
- **TLS Encryption**: All Vault communication encrypted
- **Service Mesh**: Consider using Istio for mTLS
- **Private Endpoints**: Use private cluster endpoints

### Access Control

- **Time-based Access**: Limit access during maintenance windows
- **Location-based Access**: Restrict to specific namespaces
- **Action-based Access**: Only allow necessary operations
- **Regular Audits**: Review permissions quarterly

## Performance Optimization

### Resource Management

- **CPU/Memory Limits**: Set appropriate resource constraints
- **Job Parallelism**: Control concurrent job execution
- **Resource Quotas**: Prevent resource exhaustion
- **Priority Classes**: Ensure critical jobs run first

### Efficiency Improvements

- **Incremental Rotation**: Only rotate changed secrets
- **Batch Operations**: Group related operations
- **Caching**: Cache Vault responses when possible
- **Compression**: Compress large secret payloads

## Compliance and Standards

### SOX Compliance

- **Access Controls**: Proper segregation of duties
- **Audit Trails**: Complete rotation logs
- **Change Management**: Documented rotation procedures
- **Regular Reviews**: Quarterly access reviews

### PCI-DSS Compliance

- **Key Rotation**: Encryption keys rotated every 90 days
- **Access Logging**: All key access logged
- **Network Security**: Secure transmission of keys
- **Vulnerability Management**: Regular security updates

### ISO 27001 Compliance

- **Information Security Management**: Comprehensive security controls
- **Risk Assessment**: Regular risk assessments
- **Security Awareness**: Training for security procedures
- **Incident Response**: Documented response procedures

## Extending the System

### Adding New Secrets

1. **Update Rotation Job**:
```yaml
# Add to the rotation script
rotate_secret "secret/aurum/new-secret" "30d"
```

2. **Update RBAC**:
```yaml
# Add to role permissions
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]
  resourceNames: ["new-secret"]
```

3. **Update Monitoring**:
```yaml
# Add to alert rules
- alert: NewSecretRotationFailed
  expr: new_secret_rotation_failed > 0
```

### Custom Rotation Logic

```bash
#!/bin/bash
# Custom rotation script
set -euo pipefail

# Custom business logic for rotation
rotate_custom_application_secret() {
  local app_name=$1
  echo "Rotating secrets for $app_name"

  # Custom rotation logic here
  vault kv put "secret/aurum/$app_name" \
    rotated_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    app="$app_name"

  echo "✅ Custom rotation completed for $app_name"
}

# Use in rotation job
rotate_custom_application_secret "payment-service"
```

### Integration with External Systems

```bash
#!/bin/bash
# External system integration
notify_external_system() {
  local rotation_report=$1

  # Send notification to external monitoring system
  curl -X POST https://monitoring.external.com/webhook \
    -H "Content-Type: application/json" \
    -d @"$rotation_report"

  echo "✅ External notification sent"
}

# Use after rotation
generate_rotation_report "/tmp/report.json"
notify_external_system "/tmp/report.json"
```

## Emergency Procedures

### 1. Security Incident Response

```bash
# Immediate actions for security incidents
kubectl scale deployment vault-agent --replicas=0 -n aurum-dev
kubectl delete jobs -l app=vault,component=secrets-rotation -n aurum-dev

# Rotate all secrets immediately
kubectl create job emergency-rotation-$(date +%s) \
  --from=cronjob/vault-secrets-rotation \
  -n aurum-dev
```

### 2. System Recovery

```bash
# Restore from backup
kubectl apply -f k8s/base/vault-secrets-rotator-rbac.yaml
kubectl apply -f k8s/base/vault-secrets-rotation-job.yaml

# Verify restoration
kubectl get cronjob vault-secrets-rotation -n aurum-dev
kubectl get sa aurum-secrets-rotator -n aurum-dev
```

### 3. Manual Override

```bash
# Disable automated rotation
kubectl patch cronjob vault-secrets-rotation -n aurum-dev \
  -p '{"spec":{"suspend":true}}'

# Re-enable after resolution
kubectl patch cronjob vault-secrets-rotation -n aurum-dev \
  -p '{"spec":{"suspend":false}}'
```

## Maintenance

### Regular Tasks

1. **Update Vault Version**: Quarterly security updates
2. **Review RBAC**: Quarterly permission review
3. **Test Rotation**: Monthly test rotations
4. **Audit Logs**: Weekly log review
5. **Update Documentation**: Quarterly documentation updates

### Health Checks

```bash
# Daily health checks
kubectl get cronjob vault-secrets-rotation -n aurum-dev
kubectl get jobs -n aurum-dev -l app=vault,component=secrets-rotation

# Weekly comprehensive check
kubectl describe cronjob vault-secrets-rotation -n aurum-dev
kubectl logs -l app=vault,component=secrets-rotation -n aurum-dev --tail=100
```

### Performance Monitoring

```bash
# Monitor resource usage
kubectl top pods -n aurum-dev -l app=vault,component=secrets-rotation

# Check job completion times
kubectl get jobs -n aurum-dev -o jsonpath='{.items[*].status.completionTime}'

# Monitor Vault performance
kubectl exec -it deployment/vault -n aurum-dev -- vault status
```

## Support

### Contact Information

- **Platform Team**: platform-team@aurum-corp.com
- **Security Team**: security-team@aurum-corp.com
- **On-call**: +1-555-0123 (24/7)

### Escalation Path

1. **Level 1**: Platform team investigates
2. **Level 2**: Security team review
3. **Level 3**: Emergency response team
4. **Level 4**: Executive escalation

### Resources

- **Runbook**: [Vault Rotation Runbook](runbooks/vault-rotation.md)
- **Documentation**: [Vault Documentation](https://developer.hashicorp.com/vault/docs)
- **Monitoring**: [Grafana Dashboards](https://grafana.aurum-dev)

---

*This secure Vault rotation system ensures reliable, monitored, and compliant secret rotation across the Aurum platform.*
