from __future__ import annotations

from datetime import datetime
from pathlib import Path

import json

import pytest

from aurum.workflow.config_schema import WorkflowConfig
from aurum.workflow.dynamic_dags import build_dag_from_config
from aurum.cli import workflow as workflow_cli


@pytest.fixture()
def sample_config() -> dict:
    return {
        "dag_id": "test_dynamic_pipeline",
        "description": "Test pipeline",
        "schedule": "@daily",
        "start_date": "2024-01-01",
        "catchup": False,
        "tags": ["aurum", "dynamic", "test"],
        "feature_flags": {
            "performance": {"max_active_tasks": 3, "default_retries": 1, "retry_delay_minutes": 5},
            "self_healing": {"default_retries": 2, "retry_delay_minutes": 1},
        },
        "tasks": [
            {"id": "start", "type": "empty"},
            {
                "id": "mapped",
                "type": "bash",
                "params": {
                    "command": "echo {{ params.value }}",
                    "map": {"param": "params", "items": [{"value": 1}, {"value": 2}]},
                },
                "depends_on": ["start", "dataset://aurum/tests/external_gate"],
                "wait_for_datasets": ["dataset://aurum/tests/example_ready"],
            },
            {
                "id": "fire_event",
                "type": "integration_event",
                "params": {"event_type": "unit_test_event", "payload": {"marker": "ok"}},
                "depends_on": ["mapped"],
                "on_failure": {"trigger_dag": {"dag_id": "repair_pipeline"}},
            },
        ],
    }


def test_workflow_config_validates(sample_config: dict) -> None:
    cfg = WorkflowConfig.model_validate(sample_config)
    assert cfg.dag_id == "test_dynamic_pipeline"
    assert isinstance(cfg.start_date, datetime)
    assert cfg.tasks[1].mapping is not None
    # Mapping removed from params for internal storage
    assert "map" not in cfg.tasks[1].params


def test_build_dag_from_config_creates_dag(sample_config: dict) -> None:
    cfg = WorkflowConfig.model_validate(sample_config)
    dag = build_dag_from_config(cfg)
    assert dag.dag_id == "test_dynamic_pipeline"
    assert {t.task_id for t in dag.tasks} >= {"start", "mapped", "fire_event", "fire_event__on_failure"}
    assert getattr(dag, "max_active_tasks", None) == 3
    sensors = {"wait_for__aurum_tests_example_ready", "wait_for__aurum_tests_external_gate"}
    dag_task_ids = {t.task_id for t in dag.tasks}
    assert sensors.issubset(dag_task_ids)
    mapped = dag.get_task("mapped")
    upstream_ids = {t.task_id for t in mapped.upstream_list}
    assert sensors <= upstream_ids
    assert callable(mapped.on_success_callback)
    assert callable(mapped.on_retry_callback)
    assert mapped.retries == 2
    assert mapped.retry_delay.total_seconds() == 60
    fire_event = dag.get_task("fire_event")
    assert callable(fire_event.on_success_callback)
    failure_task = dag.get_task("fire_event__on_failure")
    assert str(failure_task.trigger_rule).lower().endswith("one_failed")


def test_cli_validate_and_dry_run(tmp_path: Path, sample_config: dict) -> None:
    config_path = tmp_path / "sample.json"
    config_path.write_text(json.dumps(sample_config), encoding="utf-8")

    exit_code = workflow_cli.main(["validate", str(config_path)])
    assert exit_code == 0

    exit_code = workflow_cli.main(["dry-run", str(config_path)])
    assert exit_code == 0

    render_dir = tmp_path / "rendered"
    exit_code = workflow_cli.main([
        "render",
        "--config",
        str(config_path),
        "--output",
        str(render_dir),
    ])
    assert exit_code == 0
    rendered_file = render_dir / "test_dynamic_pipeline.py"
    assert rendered_file.exists()

    exit_code = workflow_cli.main([
        "inspect",
        "--config",
        str(config_path),
    ])
    assert exit_code == 0

    exit_code = workflow_cli.main(["integrations"])
    assert exit_code == 0
