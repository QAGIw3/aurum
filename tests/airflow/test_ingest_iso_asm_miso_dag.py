from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_ingest_iso_asm_miso_dag_structure():
    pytest.importorskip("airflow", reason="airflow not installed")
    try:
        from airflow.models import DagBag  # type: ignore
    except ModuleNotFoundError:
        pytest.skip("airflow.models not available")

    dagbag = DagBag(dag_folder=str(REPO_ROOT / "airflow" / "dags"), include_examples=False)
    assert dagbag.import_errors == {}

    dag = dagbag.get_dag("ingest_iso_asm_miso")
    assert dag is not None

    task_ids = set(dag.task_dict.keys())
    expected_tasks = {
        "start",
        "preflight",
        "register_sources",
        "miso_asm_exante_render",
        "miso_asm_exante_execute",
        "miso_asm_exante_watermark",
        "miso_asm_expost_render",
        "miso_asm_expost_execute",
        "miso_asm_expost_watermark",
        "end",
    }
    assert expected_tasks.issubset(task_ids)

    exante_render = dag.get_task("miso_asm_exante_render")
    expost_render = dag.get_task("miso_asm_expost_render")

    assert "MISO_ASM_MARKET='DAY_AHEAD_EXANTE'" in exante_render.bash_command
    assert "MISO_ASM_MARKET='DAY_AHEAD_EXPOST'" in expost_render.bash_command

    register = dag.get_task("register_sources")
    assert "miso_asm_exante_render" in register.downstream_task_ids
    assert "miso_asm_expost_render" in register.downstream_task_ids

    assert dag.on_failure_callback is not None
