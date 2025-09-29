from __future__ import annotations

import json
from pathlib import Path

import pytest

from aurum.cli import workflow as workflow_cli
from aurum.workflow.versioning import WorkflowVersionRegistry


@pytest.fixture()
def registry_path(tmp_path: Path) -> Path:
    return tmp_path / "registry.json"


@pytest.fixture()
def simple_config() -> dict:
    return {
        "dag_id": "versioned_pipeline",
        "description": "Versioned test pipeline",
        "schedule": "@hourly",
        "start_date": "2024-01-01",
        "tasks": [
            {"id": "start", "type": "empty"},
        ],
    }


def test_registry_promote_and_rollback(registry_path: Path) -> None:
    registry = WorkflowVersionRegistry(registry_path)
    event = registry.promote("example_dag", "1.0.0", user="tester")
    assert event.version == "1.0.0"
    assert registry.get_active_version("example_dag") == "1.0.0"

    rollback_event = registry.rollback("example_dag", "1.0.0", user="tester")
    assert rollback_event.version == "1.0.0"
    assert registry.get_active_version("example_dag") == "1.0.0"


def test_cli_promote_versions_and_rollback(tmp_path: Path, registry_path: Path, simple_config: dict) -> None:
    config_path = tmp_path / "workflow.json"
    config_path.write_text(json.dumps(simple_config), encoding="utf-8")

    exit_code = workflow_cli.main(
        [
            "promote",
            "--dag-id",
            simple_config["dag_id"],
            "--version",
            "2024.05.1",
            "--config",
            str(config_path),
            "--registry",
            str(registry_path),
        ]
    )
    assert exit_code == 0

    data = json.loads(registry_path.read_text())
    assert data["workflows"][simple_config["dag_id"]]["active_version"] == "2024.05.1"

    exit_code = workflow_cli.main(
        [
            "versions",
            "--dag-id",
            simple_config["dag_id"],
            "--registry",
            str(registry_path),
        ]
    )
    assert exit_code == 0

    exit_code = workflow_cli.main(
        [
            "rollback",
            "--dag-id",
            simple_config["dag_id"],
            "--version",
            "2024.05.1",
            "--registry",
            str(registry_path),
        ]
    )
    assert exit_code == 0
*** End Patch
