# Advanced Workflow Orchestration

This guide captures the rollout plan, operational runbooks, and training notes for the
advanced Airflow orchestration stack.

## Rollout Checklist

1. **Baseline**
   - Ensure Airflow >= 2.6 with dataset support and deferrable workers enabled.
   - Configure remote logging and metrics (StatsD/Prometheus) before enabling dynamic DAGs.
   - Point the scheduler to `airflow/dags/dynamic/loader.py` (already referenced in repo).

2. **Config Store**
   - Author workflow specs under `config/workflows/` (JSON or YAML).
   - Use the new pydantic schema for validation; required fields: `dag_id`, `schedule`, `start_date`.
   - Optional sections:
     - `feature_flags.performance` (`max_active_tasks`, `default_retries`, `retry_delay_minutes`...)
     - `feature_flags.self_healing` (`default_retries`, `retry_delay_minutes`, `execution_timeout_minutes`)
     - `tasks[].wait_for_datasets` or dataset strings inside `depends_on`
     - `tasks[].on_failure.trigger_dag` for recovery DAGs.

3. **Validation CLI** (`python -m aurum.cli.workflow`)
   - `validate [paths]` – schema + import checks.
   - `dry-run [paths]` – builds DAG objects, reports materialised tasks.
   - `inspect --config path` – prints task topology, upstream/downstream edges, version tags.
   - `render --config path --output airflow/dags/generated` – emits static DAG modules (optional for canary schedulers).

4. **Versioning & Promotion**
   - Registry file defaults to `config/workflows/registry.json` (override `AURUM_WORKFLOW_REGISTRY`).
   - `promote --dag-id ... --version ... --config ... [--render]` – validates, records audit event, optionally renders DAG.
   - `rollback --dag-id ... --version ...` – switches active version, preserves history.
   - `versions [--dag-id]` – dumps registry state.
   - Every promotion stores user, git SHA (if supplied), previous version, and optional notes/metadata.

5. **Observability**
   - DAGs publish structured events via `aurum.workflow.observability` logger (`task_success`, `task_failure`, `dag_success`, `dag_failure`, etc.).
   - Dataset-driven dependencies auto-create deferrable `DatasetSensor` tasks (`wait_for__*`).
   - `integration_event` tasks bridge to `integration_manager.trigger_integration_event` (webhooks, APIs, marketplaces).
   - CLI: `integrations [--marketplace|--installed]` for marketplace telemetry.

6. **Performance & Self-Healing**
   - `feature_flags.performance.max_active_tasks` tunes DAG-level concurrency.
   - `feature_flags.performance.default_retries` / `retry_delay_minutes` set DAG defaults.
   - `feature_flags.self_healing.default_retries` / `retry_delay_minutes` / `execution_timeout_minutes` apply to tasks lacking explicit overrides.
   - `tasks[].on_failure.fallback` supports secondary bash remediation; `tasks[].on_failure.trigger_dag` raises repair workflows.

7. **Integration Patterns**
   - `integration_event` tasks trigger marketplace integrations or downstream orchestrators.
   - `external_sensor` and `trigger_dag` templates continue to support cross-DAG dependencies.
   - New CLI inspect output helps map dependencies before enabling external triggers.

## Training Session Outline

1. **Introduction (10 min)** – Why dynamic configs, design principles, registry concepts.
2. **Hands-on CLI (25 min)** – Validate, inspect, promote, rollback (use `examples` pipeline).
3. **Templates Deep Dive (15 min)** – Mapping, dataset sensors, integration events, short circuit.
4. **Self-Healing & Performance (10 min)** – Feature-flag defaults, failure triggers, fallback paths.
5. **Operations Runbook (10 min)** – Promotion ladder (stage/canary/prod), rollback, observability dashboards.
6. **Q&A (10 min)** – Collect feedback for incremental improvements.

## Suggested Automation

- Add CI job to run `python -m aurum.cli.workflow validate config/workflows` on every PR touching workflow configs.
- Optional: commit hook invoking `workflow inspect --config` to snapshot task graph changes.
- Observability dashboards should scrape logs with event types emitted by the new callbacks.

## Next Steps

- Populate registry with initial versions via `workflow promote` once DAGs are validated.
- Coordinate with SRE to route observability logger to log aggregation / SIEM.
- Extend `integration_manager` with real vendor credentials before enabling production triggers.
