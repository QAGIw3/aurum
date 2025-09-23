# Secrets Management Policy

## Overview

This policy defines the standards and procedures for managing secrets in the Aurum platform, including rotation, access controls, and audit requirements.

## Policy Scope

This policy applies to all secrets used in the Aurum platform, including:
- Database credentials
- API keys and tokens
- Encryption keys
- OAuth client secrets
- TLS certificates
- External service credentials

## Secret Classification

### Critical Secrets (7-day rotation)
- Database master passwords
- JWT signing keys
- OAuth client secrets
- Root CA private keys

### Important Secrets (30-day rotation)
- Database user passwords
- API keys for critical services
- Service account tokens
- Encryption keys for sensitive data

### Regular Secrets (90-day rotation)
- Cache configuration
- Rate limiting keys
- Non-sensitive API keys
- Test credentials

## Rotation Requirements

### Automated Rotation
- All secrets must be automatically rotated on schedule
- Rotation jobs run weekly (Monday 2 AM UTC)
- Failed rotations trigger immediate alerts
- Rotation history is maintained for audit purposes

### Manual Rotation
- Required for emergency situations
- Must be documented with justification
- Triggers security incident response if unauthorized

## Access Controls

### Principle of Least Privilege
- Users receive minimum required secret access
- Access is time-limited where possible
- All access is logged and monitored

### Multi-Party Approval
- Critical secret access requires dual approval
- Important secret access requires team lead approval
- Regular secret access is self-service with audit

## Audit and Monitoring

### Access Logging
- All secret access is logged with user, timestamp, and reason
- Access patterns are analyzed for anomalies
- Failed access attempts trigger alerts

### Rotation Tracking
- Each rotation is recorded with timestamp and operator
- Rotation failures are tracked and reported
- Rotation history is retained for 7 years

## Emergency Procedures

### Secret Compromise
1. Immediately revoke compromised secret
2. Generate emergency rotation
3. Notify security team
4. Perform incident response
5. Update affected systems

### Rotation Failure
1. Trigger immediate alert to on-call engineer
2. Attempt manual rotation
3. Investigate root cause
4. Update rotation procedures
5. Document incident

## Implementation

### Vault Configuration
- All secrets stored in HashiCorp Vault
- Transit backend for encryption keys
- Database backend for dynamic credentials
- Audit backend for all operations

### GitHub OIDC Integration
- OIDC providers configured for AWS, GCP, Azure
- Short-lived tokens for cloud access
- No long-term credentials in repositories
- Automated credential rotation

### Monitoring and Alerts
- Prometheus metrics for secret operations
- AlertManager integration for failures
- Grafana dashboards for visibility
- Automated daily summaries

## Compliance

### SOX Compliance
- All secret operations are audited
- Access controls meet SOX requirements
- Rotation schedules are documented
- Emergency procedures are tested

### PCI DSS
- Encryption keys meet PCI requirements
- Access controls follow PCI guidelines
- Audit logs maintained for 1 year
- Penetration testing performed annually

## Review and Updates

### Policy Review
- Review every 90 days
- Update for new threat vectors
- Incorporate lessons from incidents
- Align with industry best practices

### Exception Process
- Temporary exceptions must be documented
- Exceptions expire automatically
- Extension requires re-approval
- All exceptions tracked in security system

## Contact Information

- **Security Team**: security@aurum.com
- **Platform Team**: platform@aurum.com
- **Emergency**: +1 (555) 123-4567 (24/7 on-call)

## Related Documentation

- [Vault Operations Guide](vault-operations.md)
- [Incident Response Plan](incident-response.md)
- [Security Monitoring Guide](security-monitoring.md)
- [Compliance Requirements](compliance-requirements.md)
