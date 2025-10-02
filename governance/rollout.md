# Governance Platform Rollout Guide

The governance platform combines OpenLineage/Marquez, OpenMetadata, and the Aurum
API to provide end-to-end lineage, quality, and catalog capabilities. This guide
covers rollout phases, training, and validation steps.

## 1. Prerequisites
- Docker runtime with access to the Aurum repository.
- Prometheus and Grafana instances for metrics and dashboards.
- Credentials for OpenMetadata (JWT or personal access token).
- Airflow environment equipped with the `openlineage-airflow` provider.

## 2. Bootstrap Infrastructure
1. Start governance services:
   ```bash
   docker compose -f compose/docker-compose.dev.yml -f compose/docker-compose.governance.yml up -d marquez openmetadata-server
   ```
2. Seed metadata:
   ```bash
   ./scripts/governance/run_openmetadata_ingestion.sh
   ```
3. Configure Airflow environment variables:
   - `OPENLINEAGE_URL`
   - `OPENLINEAGE_NAMESPACE`
   - `MARQUEZ_URL`
   - `OPENMETADATA_SERVER`
   - `OPENMETADATA_TOKEN`

## 3. Governance Service Wiring
- The GraphQL API now exposes `governanceLineage`, `governanceQuality`,
  `governanceClassifications`, `governancePrivacy`, `governanceDatasetHealth`,
  and `governanceLineageGaps` queries.
- Ensure the Aurum API service exports the following environment variables:
  - `OPENLINEAGE_URL` (e.g., `http://marquez:5000`)
  - `MARQUEZ_URL`
  - `OPENMETADATA_SERVER` (e.g., `http://openmetadata-server:8585/api`)
  - `OPENMETADATA_TOKEN` (optional when no-auth mode disabled)

## 4. Data Steward Workflow
1. Review lineage via GraphQL or the Marquez UI.
2. Monitor data quality scores:
   - `Governance` dashboard in Grafana (`uid: aurum-governance`).
   - Alert rules located at `monitoring_alerting/governance_alerts.yml`.
3. Handle schema changes:
   - Use `governanceDatasetHealth` before deployments.
   - Run `schema_tracker.SchemaEvolutionTracker` to record diffs and notify downstream owners.

## 5. Training Checklist
- **Data Producers**
  - Instrument DAGs with the new `AirflowOpenLineageAdapter` callbacks.
  - Define dataset tags and glossary terms in OpenMetadata.
  - Configure data quality suites using `quality_engine.TestSuiteConfig`.
- **Data Stewards**
  - Use `ColumnClassifier` results to validate automated tagging.
  - Maintain privacy policies via the `PrivacyPolicyManager`.
  - Review impact analysis outputs before approving schema deployments.
- **Consumers & Analysts**
  - Query `governanceLineage` to understand upstream dependencies.
  - Monitor freshness/completeness with the governance dashboards.

## 6. Rollout Plan
1. **Pilot**
   - Select two critical pipelines, enable OpenLineage emission, and ingest metadata.
   - Validate lineage completeness and quality scores.
2. **Expand**
   - Onboard additional pipelines, enforce automated classification, and set SLA targets.
   - Establish alert routing to PagerDuty/Slack for governance rules.
3. **Standardise**
   - Integrate governance checks into CI/CD gates via `quality_engine`.
   - Publish steward and producer playbooks.

## 7. Validation Steps
- Run the Great Expectations suites and ensure scores persist to OpenMetadata.
- Execute `governanceDatasetHealth` GraphQL queries for pilot datasets.
- Confirm alerts fire when forcing simulated failures (see Prometheus alert rules).

## 8. Support & Ownership
- **Primary Team:** Data Platform (governance squad).
- **Fallback:** Real-time ingestion SREs for infrastructure escalations.
- Update this guide as processes and tooling evolve.
