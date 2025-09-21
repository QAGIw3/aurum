"""Factories and helpers for building Aurum Airflow DAGs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, MutableMapping, Sequence

from airflow.decorators import dag

__all__ = [
    "DagSchedule",
    "DagConfig",
    "build_default_args",
    "dag_factory",
]


@dataclass(frozen=True)
class DagSchedule:
    """Represents the scheduling metadata for an Airflow DAG."""

    cron: str
    start_date: datetime
    catchup: bool = False
    max_active_runs: int = 1


@dataclass(frozen=True)
class DagConfig:
    """Typed configuration describing a DAG to be generated."""

    dag_id: str
    description: str
    schedule: DagSchedule
    tags: Sequence[str] = field(default_factory=tuple)
    default_args: Mapping[str, Any] | None = None


_DEFAULT_ARGS_TEMPLATE: Mapping[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ("aurum-ops@example.com",),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}


def build_default_args(**overrides: Any) -> MutableMapping[str, Any]:
    """Return a mutable copy of the shared default arguments with overrides."""

    args: MutableMapping[str, Any] = dict(_DEFAULT_ARGS_TEMPLATE)
    if "email" in overrides and isinstance(overrides["email"], Sequence):
        overrides["email"] = tuple(overrides["email"])
    args.update(overrides)
    return args


def dag_factory(config: DagConfig) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Return a decorator that converts a function into an Airflow DAG using ``config``."""

    default_args = build_default_args(**(dict(config.default_args) if config.default_args else {}))

    def _decorate(factory: Callable[..., Any]) -> Callable[..., Any]:
        return dag(
            dag_id=config.dag_id,
            description=config.description,
            default_args=default_args,
            schedule=config.schedule.cron,
            start_date=config.schedule.start_date,
            catchup=config.schedule.catchup,
            max_active_runs=config.schedule.max_active_runs,
            tags=list(config.tags),
        )(factory)

    return _decorate
