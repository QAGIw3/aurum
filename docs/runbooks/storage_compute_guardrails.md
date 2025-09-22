# Storage & Compute Cost Guardrails

This runbook captures the guardrails we enforce on storage- and compute-heavy
workloads and outlines the weekly review process used by the data platform team.

## Guardrails

- **Iceberg / Trino**
  - Table compaction jobs target <128 MB file sizes; auto-compaction triggers
    when more than 500 small files are detected.
  - Warnings raised (Slack #data-infra) when daily storage growth exceeds 5% or
    when query scan bytes surpass 2x the rolling 30-day average.
  - Result-set caching TTLs tuned per query type (see `golden_query_cache`).

- **TimescaleDB**
  - Hypertable chunk compression enabled at 7 days; retention policies purge
    data older than 90/120 days depending on table.
  - Maintenance coordinator schedules compression/drops hourly and emits
    metrics for chunk churn (see `WarehouseMaintenanceCoordinator`).

- **ClickHouse**
  - Nightly `OPTIMIZE TABLE FINAL` runs for analytics tables and TTL enforced at
    45 days by default.
  - Alerts page operator if OPTIMIZE latency > 10 minutes or backlog > 5
    partitions.

- **Cache / Query Layer**
  - Golden query cache hit-rate alarms when <70% for three consecutive hours.
  - Trino pool wait timeout lowered to 5s; queue depth alert at 75% capacity.

## Weekly Review Process

1. **Every Monday 10:00** – Review metrics dashboard (`/ops/cost-guardrails`)
   covering storage growth, query scan bytes, cache hit rates, and maintenance
   job KPIs.
2. **Exception Report** – Inspect any guardrail breach tickets opened by the
   automated alerts (Jira label `COST-GUARDRAIL`).
3. **Plan Adjustments**
   - Update retention policies or cache TTLs as needed.
   - Capture action items in the runbook change log.
4. **Sign-off** – Platform lead acknowledges review in the #infra-weekly channel.

## Change Log

| Date       | Change | Owner |
| ---------- | ------ | ------ |
| 2024-09-23 | Initial guardrail doc and review cadence | Platform Team |
