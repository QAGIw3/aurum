# Paging Policy and On-Call Governance

## Scope
This policy governs paging and incident response for core services: API, Airflow, Kafka, Trino, workers, and supporting infrastructure. It defines when we page, who is paged, targets for acknowledgment and mitigation, and escalation paths.

## Severity Levels
- **P0 – Critical outage**: Broad customer impact, data loss risk, security incident, or control-plane failure.
  - Page 24/7. Ack ≤ 5 minutes. Mitigate ≤ 30 minutes. Updates every 15 minutes.
  - Escalate to platform lead at 10 minutes if unacked; to exec at 30 minutes if unmitigated.
- **P1 – Major degradation**: Single-tenant outage or severe performance/throughput issues; RPO/RTO risk.
  - Page 24/7. Ack ≤ 10 minutes. Mitigate ≤ 60 minutes. Updates every 30 minutes.
- **P2 – Minor impact**: Partial feature impairment, elevated errors without hard SLO breach.
  - Business-hours paging. Ack ≤ 2 hours. Next-business-day mitigation plan.
- **P3 – Informational**: No immediate customer impact; track in backlog. No page.

## What Warrants a Page (SLO-driven)
- SLO burn-rate alerts using multi-window policy:
  - 2h window burn-rate > 14x and 6h window > 6x → Page (P0/P1 depending on scope)
  - 1h window > 14x but 6h window < 6x → Page if corroborated by customer impact
- Golden signals sustained outside thresholds (5+ minutes):
  - Availability < 99.9%, 5xx rate > 1%, p95 latency > 1s (API), queue depth > 75% (Trino pool), consumer lag growing (Kafka), scheduler heartbeats missing (Airflow > 5m)
- Security signal: suspected credential compromise, data exfiltration, or auth bypass → P0

## What Does Not Page (Noise Filters)
- Single node CPU spikes without error/latency corroboration
- Transient pod restarts with auto-recovery and no SLO impact
- One-off DAG task flaps with successful retries and within SLA

## On-Call Structure
- **Primary**: Service rotation for each domain (API, Airflow, Kafka, Trino) weekly, handoff Fridays 17:00 local
- **Secondary**: Cross-domain platform engineer for backup and coordination
- **Shadow**: Optional trainee paired with Primary
- **Coverage**: 24/7 for P0/P1; P2 is business-hours only

### Handoff Checklist (Friday 17:00)
- Confirm pager schedule and contact numbers are up to date
- Review active incidents, known risks, feature flags, rate limits
- Verify dashboards and alerts green; run pager test page

## Targets & Expectations
- Ack times: P0 ≤ 5m, P1 ≤ 10m, P2 ≤ 2h
- Mitigation: P0 ≤ 30m, P1 ≤ 60m; communicate ETA if longer
- Status updates cadence: P0 every 15m, P1 every 30m
- Customer comms via status page for P0/P1; internal Slack for all pages

## Escalation Path
1. Primary On-call (service)
2. Secondary Platform On-call
3. Service Owner/Tech Lead
4. Platform Engineering Director
5. Executive Escalation (VP Eng)

## Paging Channels
- PagerDuty services: API, Airflow, Kafka, Trino, Platform
- Slack: #oncall-ops (war room), #engineering, #stakeholders for executive updates
- Email lists: oncall-platform@aurum.com, sre@aurum.com

## Incident Roles
- **Incident Commander (IC)**: Coordinates response, comms, and timeline
- **Operations (Ops)**: Executes technical remediation
- **Communications (Comms)**: Stakeholder updates and status page
- **Scribe**: Captures timeline and decisions for postmortem

## Communications Templates
- Initial page acknowledgment: “Investigating, next update in 15 minutes, IC @handle”
- Customer status page: impact, start time (UTC), scope, mitigation, next update

## Post-Incident
- Blameless postmortem required for P0/P1 or repeated P2 (≥2 in 7 days)
- Deadline: draft in 24h, review in 48h, actions tracked with owners and due dates
- Store in `docs/incidents/` and link from the runbook relevant to the incident

## Compliance & Drills
- Monthly chaos drills per `docs/runbooks/chaos-drills-runbook.md`
- Quarterly paging rehearsal (test pages for all rotations)
- Alert reviews monthly to prune noise and tune thresholds

## Runbook Index
- API On-call: `docs/runbooks/api-oncall-runbook.md`
- Airflow On-call: `docs/runbooks/airflow-oncall-runbook.md`
- Kafka On-call: `docs/runbooks/kafka-oncall-runbook.md`
- Trino On-call: `docs/runbooks/trino-oncall-runbook.md`
