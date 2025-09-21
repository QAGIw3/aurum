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


def update_watermark(source_name: str, watermark: datetime, *, key: str = "logical_date") -> None:
    """Persist the watermark for ``source_name`` and emit metrics."""

    try:
        _ensure_src_path()
        from aurum.db import update_ingest_watermark  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(f"Failed to setup watermark update for {source_name}: {exc}")
        return

    ts = watermark.astimezone(timezone.utc)
    try:
        update_ingest_watermark(source_name, key, ts)
        metrics.record_watermark_success(source_name, ts)
    except Exception as exc:  # pragma: no cover - persistence best effort
        print(f"Failed to update watermark for {source_name}: {exc}")


def make_watermark_callable(
    source_name: str,
    *,
    key: str = "logical_date",
) -> Callable[..., None]:
    """Return an Airflow-compatible callable that updates ``source_name`` watermark."""

    def _callable(**context: Any) -> None:
        value = context.get(key)
        if not isinstance(value, datetime):
            raise RuntimeError(f"Context missing datetime '{key}' for {source_name}")
        update_watermark(source_name, value, key=key)

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
