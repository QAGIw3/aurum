"""Helpers for constructing Timescale loading tasks in Airflow DAGs.

This module centralizes creation of BashOperator tasks that execute
SeaTunnel jobs to load Kafka topics into TimescaleDB, including
optional Vault secret mapping injection. Airflow imports are local to
functions to remain safe when imported outside Airflow contexts.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Iterable, List, Optional


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def build_timescale_task(
    task_id: str,
    *,
    job_name: str,
    env_entries: Iterable[str],
    mappings: Optional[Iterable[str]] = None,
    execution_timeout_minutes: int = 45,
    pool: Optional[str] = None,
    queue: Optional[str] = None,
    debug_describe: bool = False,
):
    """Return a BashOperator that runs a SeaTunnel job for Timescale loads.

    Args:
        task_id: Airflow task id.
        job_name: SeaTunnel job name passed to run_job.sh.
        env_entries: Iterable of KEY='value' or KEY=value strings exported before run.
        mappings: Optional Vault mappings (e.g., ["secret/...:user=TIMESCALE_USER"]).
        execution_timeout_minutes: Operator execution timeout.
        pool: Optional Airflow pool name.
        queue: Optional Airflow queue name.
    """

    # Lazy import to avoid hard Airflow dependency at module import time.
    from airflow.operators.bash import BashOperator

    vault_addr = _env("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
    vault_token = _env("AURUM_VAULT_TOKEN", "aurum-dev-token")
    venv_python = _env("AURUM_VENV_PYTHON", ".venv/bin/python")
    bin_path = _env("AURUM_BIN_PATH", ".venv/bin:$PATH")
    pythonpath_entry = _env("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

    mapping_flags = ""
    if mappings:
        mapping_flags = " ".join(f"--mapping {m}" for m in mappings)

    pull_cmd = ""
    if mapping_flags:
        pull_cmd = (
            f"eval \"$(VAULT_ADDR={vault_addr} VAULT_TOKEN={vault_token} "
            f"PYTHONPATH=${{PYTHONPATH:-}}:{pythonpath_entry} "
            f"{venv_python} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true\n"
        )

    # Always ensure a reasonable PATH/PYTHONPATH for scripts
    env_line = " ".join(list(env_entries))

    describe_line = ""
    if debug_describe:
        describe_line = (
            f"if [ \"${{AURUM_DEBUG:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi\n"
        )

    operator_kwargs = {
        "task_id": task_id,
        "bash_command": (
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            f"{pull_cmd}"
            f"export PATH=\"{bin_path}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{pythonpath_entry}\"\n"
            f"{describe_line}"
            f"{env_line} scripts/seatunnel/run_job.sh {job_name}"
        ),
        "execution_timeout": timedelta(minutes=execution_timeout_minutes),
    }

    if pool:
        operator_kwargs["pool"] = pool
    if queue:
        operator_kwargs["queue"] = queue

    return BashOperator(**operator_kwargs)


__all__ = ["build_timescale_task"]
