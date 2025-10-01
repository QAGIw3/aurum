from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_noaa_dataset_specific_dag_has_timescale_task():
    pytest.importorskip("airflow", reason="airflow not installed")
    try:
        from airflow.models import DagBag  # type: ignore
    except ModuleNotFoundError:
        pytest.skip("airflow.models not available")

    dagbag = DagBag(dag_folder=str(REPO_ROOT / "airflow" / "dags"), include_examples=False)
    assert dagbag.import_errors == {}

    dag = dagbag.get_dag("noaa_ghcnd_daily_ingest")
    assert dag is not None

    task_ids = set(dag.task_dict.keys())
    # Ensure the refactored Timescale task id exists for the daily dataset
    assert "noaa_ghcnd_daily_to_timescale" in task_ids

