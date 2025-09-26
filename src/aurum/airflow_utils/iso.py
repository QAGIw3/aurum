"""Helpers for ISO-related Airflow DAGs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence

import os
import sys

from . import metrics


@dataclass(frozen=True)
class IngestSource:
    """Metadata describing an ingest source to register."""

    name: str
    description: str
    schedule: str | None = None
    target: str | None = None


def _ensure_src_path() -> None:
    src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
    if src_path and src_path not in sys.path:
        sys.path.insert(0, src_path)


def register_sources(sources: Sequence[IngestSource]) -> None:
    """Register one or more ingest sources, logging but not raising on failure."""

    if not sources:
        return
    try:
        _ensure_src_path()
        from aurum.db import register_ingest_source  # type: ignore
    except Exception as exc:  # pragma: no cover - import guard in DAG parse env
        print(f"Failed during register_sources setup: {exc}")
        return

    for source in sources:
        try:
            register_ingest_source(
                source.name,
                description=source.description,
                schedule=source.schedule,
                target=source.target,
            )
        except Exception as exc:  # pragma: no cover - registration best effort
            print(f"Failed to register ingest source {source.name}: {exc}")


def update_watermark(
    source_name: str,
    watermark: datetime,
    *,
    key: str = "logical_date",
    policy: str = "exact"
) -> None:
    """Persist the watermark for ``source_name`` and emit metrics with policy-based rounding."""

    try:
        _ensure_src_path()
        from aurum.db import update_ingest_watermark  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(f"Failed to setup watermark update for {source_name}: {exc}")
        return

    ts = watermark.astimezone(timezone.utc)
    try:
        update_ingest_watermark(source_name, key, ts, policy=policy)
        metrics.record_watermark_success(source_name, ts)
    except Exception as exc:  # pragma: no cover - persistence best effort
        print(f"Failed to update watermark for {source_name}: {exc}")


def make_watermark_callable(
    source_name: str,
    *,
    key: str = "logical_date",
    policy: str = "exact",
) -> Callable[..., None]:
    """Return an Airflow-compatible callable that updates ``source_name`` watermark with policy."""

    def _callable(**context: Any) -> None:
        value = context.get(key)
        if not isinstance(value, datetime):
            raise RuntimeError(f"Context missing datetime '{key}' for {source_name}")
        update_watermark(source_name, value, key=key, policy=policy)

    return _callable


def build_render_command(
    job_name: str,
    env_assignments: str,
    *,
    bin_path: str,
    pythonpath_entry: str,
    working_dir: str = "/opt/airflow",
    describe: bool = True,
    render_only: bool = True,
    debug_var: str = "AURUM_DEBUG",
    debug_dump_env: bool = False,
    pre_lines: Iterable[str] | None = None,
    extra_lines: Iterable[str] | None = None,
) -> str:
    """Compose a bash command that renders a SeaTunnel job template."""

    lines: list[str] = [
        "set -euo pipefail",
        f"if [ \"${{{debug_var}:-0}}\" != \"0\" ]; then set -x; fi",
    ]
    if working_dir:
        lines.append(f"cd {working_dir}")
    if pre_lines:
        lines.extend(pre_lines)
    lines.append(f"export PATH=\"{bin_path}\"")
    lines.append(f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{pythonpath_entry}\"")
    if describe:
        lines.append(
            f"if [ \"${{{debug_var}:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi"
        )
    if debug_dump_env:
        lines.append(
            f"if [ \"${{{debug_var}:-0}}\" != \"0\" ]; then env | grep -E 'DLQ_TOPIC|DLQ_SUBJECT' || true; fi"
        )
    if extra_lines:
        lines.extend(extra_lines)
    command = f"{env_assignments} scripts/seatunnel/run_job.sh {job_name}"
    if render_only:
        command += " --render-only"
    lines.append(command)
    return "\n".join(lines)


def build_k8s_command(
    job_name: str,
    *,
    bin_path: str,
    pythonpath_entry: str,
    working_dir: str = "/opt/airflow",
    timeout: int = 600,
    debug_var: str = "AURUM_DEBUG",
    namespace: str | None = None,
    cleanup: bool = False,
    extra_lines: Iterable[str] | None = None,
) -> str:
    """Compose a bash command that executes a SeaTunnel job via Kubernetes."""

    args = [
        "set -euo pipefail",
        f"if [ \"${{{debug_var}:-0}}\" != \"0\" ]; then set -x; fi",
    ]
    if working_dir:
        args.append(f"cd {working_dir}")
    args.append(f"export PATH=\"{bin_path}\"")
    args.append(f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{pythonpath_entry}\"")
    if extra_lines:
        args.extend(extra_lines)
    command = [
        "python",
        "scripts/k8s/run_seatunnel_job.py",
        "--job-name",
        job_name,
        "--wait",
        "--timeout",
        str(timeout),
    ]
    if namespace:
        command.extend(["--namespace", namespace])
    if cleanup:
        command.append("--cleanup")
    args.append(" ".join(command))
    return "\n".join(args)


__all__ = [
    "IngestSource",
    "register_sources",
    "update_watermark",
    "make_watermark_callable",
    "build_render_command",
    "build_k8s_command",
]


def create_seatunnel_ingest_chain(
    task_prefix: str,
    *,
    job_name: str,
    source_name: str,
    env_entries: Iterable[str],
    pool: str | None = None,
    queue: str | None = None,
    pre_lines: Iterable[str] | None = None,
    extra_lines: Iterable[str] | None = None,
    render_timeout_minutes: int = 10,
    execute_timeout_minutes: int = 20,
    k8s_timeout_seconds: int = 600,
    watermark_policy: str = "exact",
):
    """Create a standard render/execute/watermark task chain for a SeaTunnel job.

    Returns a tuple of (render_task, execute_task, watermark_task).
    """
    # Import Airflow operators lazily to avoid import issues in non-Airflow contexts
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    bin_path = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
    pythonpath_entry = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_line = " ".join(
        list(env_entries)
        + [
            f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}'",
            f"SCHEMA_REGISTRY_URL='{schema_registry}'",
        ]
    )

    # If CeleryExecutor is used, routing to a Celery queue can be helpful.
    # Default to using the pool name as the queue if queue is not explicitly set.
    task_queue = queue or pool

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=build_render_command(
            job_name,
            env_assignments=f"AURUM_EXECUTE_SEATUNNEL=0 {env_line}",
            bin_path=bin_path,
            pythonpath_entry=pythonpath_entry,
            debug_dump_env=True,
            pre_lines=pre_lines,
            extra_lines=extra_lines,
        ),
        execution_timeout=timedelta(minutes=render_timeout_minutes),
        **({"pool": pool} if pool else {}),
        **({"queue": task_queue} if task_queue else {}),
    )

    execute = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=build_k8s_command(
            job_name,
            bin_path=bin_path,
            pythonpath_entry=pythonpath_entry,
            timeout=k8s_timeout_seconds,
            extra_lines=extra_lines,
        ),
        execution_timeout=timedelta(minutes=execute_timeout_minutes),
        **({"pool": pool} if pool else {}),
        **({"queue": task_queue} if task_queue else {}),
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=make_watermark_callable(source_name, policy=watermark_policy),
    )

    return render, execute, watermark
