from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_iceberg_maintenance_dag_structure(monkeypatch):
    pytest.importorskip("airflow", reason="airflow not installed")
    try:
        from airflow.models import DagBag  # type: ignore
    except ModuleNotFoundError:
        pytest.skip("airflow.models not available")

    monkeypatch.setenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "true")

    dagbag = DagBag(dag_folder=str(REPO_ROOT / "airflow" / "dags"), include_examples=False)
    assert dagbag.import_errors == {}

    dag = dagbag.get_dag("iceberg_maintenance")
    assert dag is not None

    expected_tasks = {
        "expire_snapshots_iceberg_market_curve_observation",
        "rewrite_manifests_iceberg_market_curve_observation",
        "compact_files_iceberg_market_curve_observation",
        "purge_orphans_iceberg_market_curve_observation",
        "check_sla",
        "publish_maintenance_summary",
    }
    assert expected_tasks.issubset(set(dag.task_dict.keys()))

    rewrite_manifests = dag.get_task("rewrite_manifests_iceberg_market_curve_observation")
    assert rewrite_manifests.op_kwargs["dry_run"] is True

    summary_task = dag.get_task("publish_maintenance_summary")
    assert summary_task.trigger_rule == "all_done"
    assert dag.on_failure_callback is not None
