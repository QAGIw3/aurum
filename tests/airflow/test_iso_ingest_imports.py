from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _dagbag():
    pytest.importorskip("airflow", reason="airflow not installed")
    try:
        from airflow.models import DagBag  # type: ignore
    except ModuleNotFoundError:
        pytest.skip("airflow.models not available")
    return DagBag(dag_folder=str(REPO_ROOT / "airflow" / "dags"), include_examples=False)


def test_ingest_iso_metrics_pjm_import_and_tasks():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag("ingest_iso_metrics_pjm")
    assert dag is not None
    tasks = set(dag.task_dict.keys())
    # Check standard chain task ids for both load and genmix
    assert {"pjm_load_render", "pjm_load_execute", "pjm_load_watermark"}.issubset(tasks)
    assert {"pjm_genmix_render", "pjm_genmix_execute", "pjm_genmix_watermark"}.issubset(tasks)


def test_ingest_iso_metrics_miso_both_dags_import():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}
    dag1 = dagbag.get_dag("ingest_iso_metrics_miso")
    dag2 = dagbag.get_dag("ingest_iso_metrics_miso_load_genmix")
    assert dag1 is not None
    assert dag2 is not None
    tasks2 = set(dag2.task_dict.keys())
    assert {"miso_load_render", "miso_load_execute", "miso_load_watermark"}.issubset(tasks2)
    assert {"miso_genmix_render", "miso_genmix_execute", "miso_genmix_watermark"}.issubset(tasks2)


def test_ingest_iso_prices_ercot_import_and_tasks():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag("ingest_iso_prices_ercot")
    assert dag is not None
    tasks = set(dag.task_dict.keys())
    assert {"ercot_lmp_render", "ercot_lmp_execute", "ercot_lmp_watermark"}.issubset(tasks)


def test_ingest_isone_comprehensive_import_and_some_tasks():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag("ingest_isone_comprehensive")
    assert dag is not None
    tasks = set(dag.task_dict.keys())
    # Spot-check that at least the RTM LMP chain exists
    assert "seatunnel_isone_lmp_rtm_render" in tasks

