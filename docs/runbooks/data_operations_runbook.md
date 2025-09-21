# Data Operations Runbook

This runbook provides comprehensive procedures for data operations including backfills, replays, and partial day reprocessing in the Aurum data ingestion platform.

## Overview

Data operations in Aurum are designed to handle:
- **Historical data gaps** through backfill operations
- **Data corrections** through replay operations
- **Partial failures** through targeted reprocessing
- **Schema changes** requiring data transformation
- **Quality issues** requiring data revalidation

## Quick Reference

### Emergency Contacts
- **Data Engineering Team**: data-eng@company.com
- **On-call Engineer**: Check PagerDuty rotation
- **Slack Channel**: #data-ingestion-ops

### Key Tools
- Backfill Driver: `python scripts/ops/backfill_driver.py`
- Quota Validator: `python scripts/config/validate_quotas.py`
- Cost Profiler: `python scripts/cost_profiler/cost_profiler_cli.py`
- SeaTunnel CLI: `seatunnel.sh`

### Monitoring Dashboards
- **Airflow**: http://airflow.company.com
- **Kafka**: http://kafka-manager.company.com
- **Cost Monitoring**: http://grafana.company.com/d/cost-analysis
- **System Health**: http://grafana.company.com/d/system-overview

## 1. Backfill Operations

### 1.1 Overview

Backfill operations fill historical data gaps by processing data for past dates that were not captured during normal operations.

### 1.2 When to Use Backfills

- Initial system setup with historical data requirements
- Recovery from extended outages (days/weeks)
- Adding new data sources with historical depth
- Compliance requirements for historical data retention
- Research and analysis requiring historical datasets

### 1.3 Pre-Backfill Checklist

#### 1.3.1 System Health Check

```bash
# Validate current quotas and configuration
python scripts/config/validate_quotas.py --format json

# Check system resources
kubectl top nodes
kubectl top pods -n aurum

# Verify data source availability
python scripts/ops/test_data_source_health.py
```

#### 1.3.2 Data Gap Analysis

```bash
# Analyze watermark gaps
python scripts/ops/analyze_watermark_gaps.py --source eia --days 90

# Generate backfill plan
python scripts/ops/backfill_planner.py --source eia --start-date 2024-01-01 --end-date 2024-12-31
```

#### 1.3.3 Resource Planning

```bash
# Estimate backfill requirements
python scripts/ops/estimate_backfill_resources.py \
  --source eia \
  --date-range 2024-01-01:2024-12-31 \
  --concurrency 2

# Check available capacity
python scripts/ops/check_cluster_capacity.py --required-memory 8Gi --required-cpu 4
```

### 1.4 Backfill Execution

#### 1.4.1 Using the Backfill Driver

```bash
# Basic backfill execution
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 2 \
  --batch-size 1000 \
  --dry-run

# Execute actual backfill
python scripts/ops/backfill_driver.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --concurrency 2 \
  --batch-size 1000 \
  --max-retries 3 \
  --progress-interval 300
```

#### 1.4.2 Monitoring Backfill Progress

```bash
# Monitor backfill job
python scripts/ops/monitor_backfill.py --job-id BACKFILL_20241231_EIA --follow

# View real-time metrics
python scripts/ops/backfill_metrics.py --source eia --active-only

# Check data quality during backfill
python scripts/ops/validate_backfill_quality.py --source eia --date 2024-12-31
```

#### 1.4.3 Backfill Parameters

| Parameter | Description | Default | Recommended |
|-----------|-------------|---------|-------------|
| `--source` | Data source (eia, fred, cpi, noaa, iso) | Required | - |
| `--start-date` | Start date (YYYY-MM-DD) | Required | - |
| `--end-date` | End date (YYYY-MM-DD) | Required | - |
| `--concurrency` | Max concurrent SeaTunnel jobs | 3 | 1-5 |
| `--batch-size` | Records per batch | 1000 | 500-5000 |
| `--max-retries` | Maximum retry attempts | 3 | 2-5 |
| `--rate-limit` | API calls per second | Auto | 50-80% of quota |
| `--dry-run` | Validate without executing | false | true for testing |

### 1.5 Backfill Troubleshooting

#### 1.5.1 Common Issues

**Rate Limiting:**
```bash
# Reduce concurrency and increase delays
python scripts/ops/backfill_driver.py \
  --source eia \
  --concurrency 1 \
  --rate-limit 5 \
  --retry-backoff 10
```

**Memory Issues:**
```bash
# Reduce batch size and parallelism
python scripts/ops/backfill_driver.py \
  --source eia \
  --batch-size 500 \
  --parallelism 1 \
  --max-memory 2g
```

**API Failures:**
```bash
# Enable detailed error logging
python scripts/ops/backfill_driver.py \
  --source eia \
  --debug \
  --log-level DEBUG \
  --error-details
```

#### 1.5.2 Pause/Resume Backfill

```bash
# Pause current backfill
python scripts/ops/backfill_driver.py --pause --job-id BACKFILL_20241231_EIA

# Resume from last successful point
python scripts/ops/backfill_driver.py --resume --job-id BACKFILL_20241231_EIA

# Check resume point
python scripts/ops/backfill_driver.py --status --job-id BACKFILL_20241231_EIA
```

#### 1.5.3 Cancel Backfill

```bash
# Cancel backfill (with confirmation)
python scripts/ops/backfill_driver.py --cancel --job-id BACKFILL_20241231_EIA --force

# Clean up partial results
python scripts/ops/cleanup_partial_backfill.py --job-id BACKFILL_20241231_EIA
```

### 1.6 Post-Backfill Validation

#### 1.6.1 Data Completeness Check

```bash
# Verify all dates are processed
python scripts/ops/validate_backfill_completeness.py \
  --source eia \
  --start-date 2024-01-01 \
  --end-date 2024-12-31

# Check record counts by date
python scripts/ops/analyze_backfill_coverage.py --source eia --monthly
```

#### 1.6.2 Quality Validation

```bash
# Run data quality checks
python scripts/ops/validate_backfill_quality.py \
  --source eia \
  --date-range 2024-01-01:2024-12-31 \
  --checks completeness,accuracy,consistency

# Compare with expected patterns
python scripts/ops/compare_backfill_patterns.py \
  --source eia \
  --reference-date 2024-12-01 \
  --comparison-date 2024-06-15
```

#### 1.6.3 Performance Analysis

```bash
# Analyze backfill performance
python scripts/ops/analyze_backfill_performance.py --job-id BACKFILL_20241231_EIA

# Generate performance report
python scripts/ops/generate_backfill_report.py \
  --job-id BACKFILL_20241231_EIA \
  --format markdown \
  --output backfill_report.md
```

## 2. Replay Operations

### 2.1 Overview

Replay operations reprocess recent data to correct issues, apply schema changes, or improve data quality.

### 2.2 When to Use Replays

- Data corruption discovered after ingestion
- Schema changes requiring reprocessing
- Data quality issues identified post-ingestion
- Algorithm improvements for data transformation
- Compliance requirement changes

### 2.3 Replay Planning

#### 2.3.1 Impact Assessment

```bash
# Assess replay scope
python scripts/ops/assess_replay_impact.py \
  --source eia \
  --date-range 2024-12-20:2024-12-31 \
  --data-volume-estimate

# Check downstream dependencies
python scripts/ops/check_downstream_impact.py \
  --source eia \
  --date-range 2024-12-20:2024-12-31
```

#### 2.3.2 Resource Estimation

```bash
# Estimate replay resources
python scripts/ops/estimate_replay_resources.py \
  --source eia \
  --date-range 2024-12-20:2024-12-31 \
  --concurrency 3

# Check capacity availability
python scripts/ops/check_replay_capacity.py \
  --estimated-hours 24 \
  --required-memory 4Gi
```

### 2.4 Replay Execution

#### 2.4.1 Basic Replay

```bash
# Replay recent data
python scripts/ops/replay_driver.py \
  --source eia \
  --start-date 2024-12-20 \
  --end-date 2024-12-31 \
  --reason "Schema correction for field validation" \
  --priority high \
  --concurrency 3
```

#### 2.4.2 Selective Replay

```bash
# Replay specific datasets only
python scripts/ops/replay_driver.py \
  --source eia \
  --datasets "ELEC.PRICE,ELEC.GEN" \
  --date-range 2024-12-20:2024-12-31 \
  --reason "Price and generation data correction"

# Replay with custom processing logic
python scripts/ops/replay_driver.py \
  --source eia \
  --custom-transform "scripts/transforms/schema_correction.py" \
  --validation-script "scripts/validation/quality_checks.py"
```

### 2.5 Replay Types

#### 2.5.1 Schema Correction Replay

```bash
# Replay with schema changes
python scripts/ops/replay_driver.py \
  --type schema_correction \
  --source eia \
  --schema-version v2.1 \
  --migration-script "scripts/migrations/v2.1_migration.py" \
  --date-range 2024-12-01:2024-12-31
```

#### 2.5.2 Quality Improvement Replay

```bash
# Replay with improved data quality checks
python scripts/ops/replay_driver.py \
  --type quality_improvement \
  --source eia \
  --quality-profile "enhanced_validation" \
  --validation-threshold 0.95 \
  --date-range 2024-12-15:2024-12-31
```

#### 2.5.3 Algorithm Update Replay

```bash
# Replay with updated processing algorithms
python scripts/ops/replay_driver.py \
  --type algorithm_update \
  --source eia \
  --algorithm-version "outlier_detection_v2" \
  --algorithm-params "sensitivity=0.8,window_size=24" \
  --date-range 2024-11-01:2024-12-31
```

### 2.6 Replay Monitoring

#### 2.6.1 Progress Tracking

```bash
# Monitor replay progress
python scripts/ops/monitor_replay.py --replay-id REPLAY_20241231_EIA --follow

# View detailed progress
python scripts/ops/replay_progress.py \
  --replay-id REPLAY_20241231_EIA \
  --format json
```

#### 2.6.2 Quality Monitoring

```bash
# Monitor data quality during replay
python scripts/ops/monitor_replay_quality.py \
  --replay-id REPLAY_20241231_EIA \
  --quality-metrics completeness,accuracy,timeliness

# Compare before/after quality
python scripts/ops/compare_replay_quality.py \
  --replay-id REPLAY_20241231_EIA \
  --comparison-metric data_quality_score
```

### 2.7 Replay Completion

#### 2.7.1 Validation Steps

```bash
# Validate replay results
python scripts/ops/validate_replay_completion.py \
  --replay-id REPLAY_20241231_EIA \
  --validation-checks all

# Verify data integrity
python scripts/ops/verify_replay_integrity.py \
  --source eia \
  --date-range 2024-12-20:2024-12-31 \
  --integrity-checks schema,completeness,relationships
```

#### 2.7.2 Cleanup and Archival

```bash
# Archive original data (if needed)
python scripts/ops/archive_replay_originals.py \
  --replay-id REPLAY_20241231_EIA \
  --archive-location /data/archive/replay_originals/

# Clean up temporary files
python scripts/ops/cleanup_replay_temp_files.py --replay-id REPLAY_20241231_EIA
```

## 3. Partial Day Reprocessing

### 3.1 Overview

Partial day reprocessing handles cases where only part of a day's data needs to be reprocessed, typically due to partial failures or data quality issues.

### 3.2 When to Use Partial Reprocessing

- Partial data ingestion failures
- Data corruption in specific time windows
- Quality issues with specific datasets
- Schema validation failures
- Downstream processing errors

### 3.3 Partial Reprocessing Types

#### 3.3.1 Time-Window Reprocessing

```bash
# Reprocess specific hours
python scripts/ops/partial_reprocess.py \
  --type time_window \
  --source eia \
  --date 2024-12-31 \
  --hours 14,15,16 \
  --reason "Partial failure during peak hours"

# Reprocess time range
python scripts/ops/partial_reprocess.py \
  --type time_range \
  --source eia \
  --start-time "2024-12-31 09:00:00" \
  --end-time "2024-12-31 17:00:00" \
  --reason "Business hours data correction"
```

#### 3.3.2 Dataset-Specific Reprocessing

```bash
# Reprocess specific datasets
python scripts/ops/partial_reprocess.py \
  --type datasets \
  --source eia \
  --datasets "ELEC.PRICE,ELEC.DEMAND" \
  --date 2024-12-31 \
  --reason "Price and demand data quality issues"

# Reprocess failed records only
python scripts/ops/partial_reprocess.py \
  --type failed_records \
  --source eia \
  --failure-log "logs/failed_records_20241231.json" \
  --max-attempts 3
```

#### 3.3.3 Incremental Reprocessing

```bash
# Reprocess with incremental updates
python scripts/ops/partial_reprocess.py \
  --type incremental \
  --source eia \
  --base-date 2024-12-31 \
  --incremental-field "last_updated" \
  --reason "Incremental data refresh"
```

### 3.4 Partial Reprocessing Execution

#### 3.4.1 Basic Execution

```bash
# Simple partial reprocessing
python scripts/ops/partial_reprocess.py \
  --source eia \
  --date 2024-12-31 \
  --hours 10-16 \
  --priority normal \
  --dry-run

# Execute partial reprocessing
python scripts/ops/partial_reprocess.py \
  --source eia \
  --date 2024-12-31 \
  --hours 10-16 \
  --max-concurrent 2 \
  --batch-size 500
```

#### 3.4.2 Advanced Configuration

```bash
# Partial reprocessing with custom settings
python scripts/ops/partial_reprocess.py \
  --source eia \
  --date 2024-12-31 \
  --config-file "config/partial_reprocess_eia.json" \
  --validation-script "scripts/validation/partial_validation.py" \
  --rollback-plan "Rollback to previous version if quality < 0.9"
```

### 3.5 Conflict Resolution

#### 3.5.1 Data Conflicts

```bash
# Detect conflicts
python scripts/ops/detect_partial_conflicts.py \
  --source eia \
  --date 2024-12-31 \
  --hours 10-16

# Resolve conflicts
python scripts/ops/resolve_partial_conflicts.py \
  --source eia \
  --conflict-file "conflicts_20241231.json" \
  --resolution-strategy "latest_wins"
```

#### 3.5.2 Version Conflicts

```bash
# Check for schema version conflicts
python scripts/ops/check_schema_conflicts.py \
  --source eia \
  --date 2024-12-31 \
  --expected-version "v2.1"

# Resolve version conflicts
python scripts/ops/resolve_version_conflicts.py \
  --source eia \
  --version-conflicts "version_conflicts.json" \
  --resolution "upgrade_to_latest"
```

### 3.6 Rollback Procedures

#### 3.6.1 Emergency Rollback

```bash
# Emergency rollback for partial reprocessing
python scripts/ops/rollback_partial_reprocess.py \
  --operation-id PARTIAL_20241231_EIA_10-16 \
  --rollback-type emergency \
  --confirm-rollback
```

#### 3.6.2 Controlled Rollback

```bash
# Controlled rollback with validation
python scripts/ops/rollback_partial_reprocess.py \
  --operation-id PARTIAL_20241231_EIA_10-16 \
  --rollback-type controlled \
  --pre-rollback-validation \
  --post-rollback-validation \
  --backup-original-data
```

## 4. Emergency Procedures

### 4.1 System-Wide Emergency

#### 4.1.1 Immediate Actions

```bash
# 1. Stop all non-critical operations
python scripts/ops/emergency_stop.py --all --reason "Data corruption detected"

# 2. Isolate affected systems
python scripts/ops/isolate_system.py --source eia --isolate-kafka

# 3. Activate emergency monitoring
python scripts/ops/activate_emergency_monitoring.py --level critical
```

#### 4.1.2 Assessment

```bash
# Assess damage
python scripts/ops/assess_emergency_damage.py --source eia --full-scan

# Generate recovery plan
python scripts/ops/generate_emergency_recovery_plan.py \
  --source eia \
  --damage-assessment "damage_report.json" \
  --output recovery_plan.json
```

#### 4.1.3 Recovery

```bash
# Execute emergency recovery
python scripts/ops/emergency_recovery.py \
  --recovery-plan "recovery_plan.json" \
  --parallel-recovery \
  --validate-each-step
```

### 4.2 Data Corruption Emergency

#### 4.2.1 Detection

```bash
# Detect data corruption
python scripts/ops/detect_data_corruption.py \
  --source eia \
  --corruption-types schema,completeness,accuracy \
  --scan-depth full
```

#### 4.2.2 Isolation

```bash
# Isolate corrupted data
python scripts/ops/isolate_corrupted_data.py \
  --source eia \
  --corruption-report "corruption_report.json" \
  --isolation-mode strict
```

#### 4.2.3 Recovery

```bash
# Recover from corruption
python scripts/ops/recover_from_corruption.py \
  --source eia \
  --recovery-method selective_replay \
  --data-range 2024-12-20:2024-12-31
```

## 5. Monitoring and Alerting

### 5.1 Real-time Monitoring

#### 5.1.1 Dashboard Access

```bash
# Open monitoring dashboard
open "http://grafana.company.com/d/data-operations"

# View active operations
python scripts/ops/monitor_active_operations.py --format table

# Check system health
python scripts/ops/check_system_health.py --detailed
```

#### 5.1.2 Alert Configuration

```bash
# Configure alerts for operations
python scripts/ops/configure_operation_alerts.py \
  --alert-thresholds "error_rate:0.05,duration:3600,throughput:100" \
  --notification-channels "slack,pagerduty,email" \
  --escalation-policy "immediate_escalation"
```

### 5.2 Performance Monitoring

#### 5.2.1 Metrics Collection

```bash
# Collect operation metrics
python scripts/ops/collect_operation_metrics.py \
  --operation-type backfill \
  --metrics "duration,throughput,error_rate,memory_usage" \
  --output-format prometheus

# Analyze performance trends
python scripts/ops/analyze_performance_trends.py \
  --time-range "7d" \
  --metrics "avg_duration,p95_duration,error_rate"
```

### 5.3 Cost Monitoring

#### 5.3.1 Cost Tracking

```bash
# Track operation costs
python scripts/ops/track_operation_costs.py \
  --operation-id BACKFILL_20241231_EIA \
  --cost-metrics "api_calls,data_transfer,compute_time"

# Generate cost report
python scripts/ops/generate_cost_report.py \
  --time-range "2024-12-01:2024-12-31" \
  --group-by operation_type \
  --format detailed
```

## 6. Tools and Scripts Reference

### 6.1 Core Operation Tools

| Tool | Purpose | Location |
|------|---------|----------|
| `backfill_driver.py` | Execute backfill operations | `scripts/ops/` |
| `replay_driver.py` | Execute replay operations | `scripts/ops/` |
| `partial_reprocess.py` | Partial day reprocessing | `scripts/ops/` |
| `monitor_operations.py` | Monitor active operations | `scripts/ops/` |
| `validate_operations.py` | Validate operation results | `scripts/ops/` |

### 6.2 Planning and Assessment Tools

| Tool | Purpose | Location |
|------|---------|----------|
| `analyze_watermark_gaps.py` | Identify data gaps | `scripts/ops/` |
| `estimate_resources.py` | Estimate resource requirements | `scripts/ops/` |
| `check_capacity.py` | Check system capacity | `scripts/ops/` |
| `assess_impact.py` | Assess operation impact | `scripts/ops/` |

### 6.3 Validation and Quality Tools

| Tool | Purpose | Location |
|------|---------|----------|
| `validate_completeness.py` | Validate data completeness | `scripts/ops/` |
| `validate_quality.py` | Validate data quality | `scripts/ops/` |
| `verify_integrity.py` | Verify data integrity | `scripts/ops/` |
| `compare_results.py` | Compare before/after results | `scripts/ops/` |

### 6.4 Emergency Tools

| Tool | Purpose | Location |
|------|---------|----------|
| `emergency_stop.py` | Stop all operations | `scripts/ops/` |
| `emergency_recovery.py` | Execute emergency recovery | `scripts/ops/` |
| `detect_corruption.py` | Detect data corruption | `scripts/ops/` |
| `rollback_operations.py` | Rollback failed operations | `scripts/ops/` |

## 7. Best Practices

### 7.1 Planning

1. **Always run dry-run first** for new operations
2. **Validate quotas** before starting large operations
3. **Plan resource allocation** based on estimates
4. **Have rollback plans** ready before starting
5. **Schedule during low-traffic periods** when possible

### 7.2 Execution

1. **Monitor continuously** during execution
2. **Log all actions** for audit trails
3. **Validate incrementally** during long operations
4. **Pause if issues arise** rather than pushing through
5. **Document deviations** from planned execution

### 7.3 Post-Operation

1. **Validate completeness** before declaring success
2. **Check data quality** against expectations
3. **Update documentation** with lessons learned
4. **Archive logs and metrics** for future reference
5. **Communicate results** to stakeholders

### 7.4 Emergency Response

1. **Stay calm and follow procedures**
2. **Document all actions** in real-time
3. **Escalate early** if issues compound
4. **Preserve evidence** for post-incident analysis
5. **Conduct thorough post-mortem** after resolution

## 8. Troubleshooting

### 8.1 Common Issues

#### Rate Limiting During Operations
```bash
# Solution: Reduce concurrency and add delays
python scripts/ops/adjust_operation_params.py \
  --operation-id BACKFILL_20241231_EIA \
  --new-concurrency 1 \
  --new-rate-limit 5
```

#### Memory Issues
```bash
# Solution: Reduce batch sizes and parallelism
python scripts/ops/adjust_operation_params.py \
  --operation-id BACKFILL_20241231_EIA \
  --new-batch-size 500 \
  --new-parallelism 2
```

#### Data Quality Issues
```bash
# Solution: Enable additional validation
python scripts/ops/enable_enhanced_validation.py \
  --operation-id BACKFILL_20241231_EIA \
  --validation-checks "completeness,accuracy,consistency"
```

### 8.2 Getting Help

1. **Check this runbook first**
2. **Review operation logs** for error details
3. **Consult monitoring dashboards** for system state
4. **Contact on-call engineer** for urgent issues
5. **Escalate to data engineering team** for complex issues

## 9. Appendices

### 9.1 Configuration Files

- **Quotas Configuration**: `config/data_source_quotas.json`
- **Environment Template**: `config/environment_template.env`
- **Operation Defaults**: `config/operation_defaults.json`

### 9.2 Log Locations

- **Airflow Logs**: `/opt/airflow/logs/`
- **SeaTunnel Logs**: `/opt/seatunnel/logs/`
- **Application Logs**: `/var/log/aurum/`
- **Operation Logs**: `/var/log/aurum/operations/`

### 9.3 Contact Information

- **Data Engineering**: data-eng@company.com
- **Infrastructure**: infra@company.com
- **Security**: security@company.com
- **Emergency**: emergency@company.com

### 9.4 Additional Resources

- **API Documentation**: [Internal Wiki](https://wiki.company.com/data-ingestion)
- **Architecture Docs**: `docs/architecture/`
- **Monitoring Guide**: `docs/monitoring/`
- **Troubleshooting Guide**: `docs/troubleshooting/`

---

**Last Updated**: 2025-01-21
**Version**: 1.0.0
**Owner**: Data Engineering Team
