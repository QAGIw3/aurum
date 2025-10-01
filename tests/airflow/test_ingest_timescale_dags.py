from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dagbag():
    pytest.importorskip("airflow", reason="airflow not installed")
    try:
        from airflow.models import DagBag  # type: ignore
    except ModuleNotFoundError:
        pytest.skip("airflow.models not available")
    return DagBag(dag_folder=str(REPO_ROOT / "airflow" / "dags"), include_examples=False)


def test_ingest_iso_aux_timescale_dag_structure():
    dagbag = _load_dagbag()
    assert dagbag.import_errors == {}

    dag = dagbag.get_dag("ingest_iso_aux_timescale")
    assert dag is not None

    task_ids = set(dag.task_dict.keys())
    assert "iso_asm_kafka_to_timescale" in task_ids
    assert "iso_pnode_kafka_to_timescale" in task_ids


def test_ingest_eia_series_timescale_dag_structure():
    dagbag = _load_dagbag()
    assert dagbag.import_errors == {}

    dag = dagbag.get_dag("ingest_eia_series_timescale")
    assert dag is not None

    task_ids = set(dag.task_dict.keys())
    assert "eia_series_kafka_to_timescale" in task_ids

