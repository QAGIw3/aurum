"""Dynamic Airflow DAG generation from JSON/YAML configuration.

Features:
- Configuration-driven DAG creation (JSON; YAML if PyYAML available)
- Reusable task templates and common operators
- Complex dependencies and trigger rules
- Conditional execution via branch tasks
- Versioning via config and optional rollback override
- Monitoring hooks via standardized failure callbacks
- External integrations (trigger other DAGs, external sensors)
- Self-healing fallbacks per-task
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from .templates import TaskSpec, build_default_args_with_monitoring, build_task_from_spec

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional
    yaml = None  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class DynamicDAGConfig:
    dag_id: str
    description: str
    schedule: str
    start_date: str
    catchup: bool = False
    max_active_runs: int = 1
    tags: List[str] = field(default_factory=lambda: ["aurum", "dynamic"]) 
    default_args: Dict[str, Any] = field(default_factory=dict)
    default_task_kwargs: Dict[str, Any] = field(default_factory=dict)
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    groups: List[Dict[str, Any]] = field(default_factory=list)
    feature_flags: Dict[str, Any] = field(default_factory=dict)  # versioning/monitoring/self-healing
    user_defined_macros: Dict[str, Any] = field(default_factory=dict)
    doc_md: Optional[str] = None
    datasets: List[Any] = field(default_factory=list)

    def start_datetime(self) -> datetime:
        # Accept ISO date/date-time; fallback to YYYY-MM-DD
        try:
            return datetime.fromisoformat(self.start_date)
        except Exception:
            return datetime.strptime(self.start_date, "%Y-%m-%d")


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    return s not in {"", "0", "false", "none"}


def _load_config_file(path: str) -> Optional[DynamicDAGConfig]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            if path.endswith('.yaml') or path.endswith('.yml'):
                if yaml is None:
                    logger.warning("Skipping YAML config without PyYAML: %s", path)
                    return None
                raw = yaml.safe_load(f)
            else:
                raw = json.load(f)
        # Basic validation
        for key in ("dag_id", "description", "schedule", "start_date"):
            if key not in raw:
                raise ValueError(f"Missing required field '{key}' in {path}")
        return DynamicDAGConfig(
            dag_id=raw["dag_id"],
            description=raw.get("description", raw["dag_id"]),
            schedule=raw["schedule"],
            start_date=raw["start_date"],
            catchup=bool(raw.get("catchup", False)),
            max_active_runs=int(raw.get("max_active_runs", 1)),
            tags=list(raw.get("tags", ["aurum", "dynamic"])),
            default_args=dict(raw.get("default_args", {})),
            default_task_kwargs=dict(raw.get("default_task_kwargs", {})),
            tasks=list(raw.get("tasks", [])),
            groups=list(raw.get("groups", [])),
            feature_flags=dict(raw.get("feature_flags", {})),
            user_defined_macros=dict(raw.get("user_defined_macros", {})),
            doc_md=raw.get("doc_md"),
            datasets=list(raw.get("datasets", [])),
        )
    except Exception as exc:
        logger.error("Failed to load DAG config %s: %s", path, exc)
        return None


def _iter_config_files(config_dir: str) -> Iterable[str]:
    if not os.path.isdir(config_dir):
        return []
    for name in sorted(os.listdir(config_dir)):
        if name.startswith("."):
            continue
        if name.endswith(".json") or name.endswith(".yaml") or name.endswith(".yml"):
            yield os.path.join(config_dir, name)


def _compute_effective_dag_id(base_id: str, cfg: DynamicDAGConfig) -> str:
    flags = cfg.feature_flags or {}
    version = (flags.get("dag_version") or "").strip()
    rollback_to = (flags.get("rollback_to") or os.getenv(f"AURUM_ROLLBACK_{base_id}") or "").strip()
    use_suffix = _coerce_bool(flags.get("enable_version_suffix", False))
    if rollback_to:
        version = rollback_to
        use_suffix = True
    if use_suffix and version:
        return f"{base_id}__v{version}"
    return base_id


def _maybe_minutes(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _make_task_spec(raw: Mapping[str, Any]) -> TaskSpec:
    return TaskSpec(
        task_id=raw.get("id") or raw.get("task_id"),
        type=raw.get("type", "empty"),
        params=dict(raw.get("params", {})),
        depends_on=list(raw.get("depends_on", [])),
        trigger_rule=raw.get("trigger_rule"),
        condition=raw.get("condition"),
        pool=raw.get("pool"),
        retries=raw.get("retries"),
        retry_delay_minutes=raw.get("retry_delay_minutes"),
        task_concurrency=raw.get("task_concurrency"),
        execution_timeout_minutes=_maybe_minutes(raw.get("execution_timeout_minutes")),
        sla_minutes=_maybe_minutes(raw.get("sla_minutes")),
        doc_md=raw.get("doc_md"),
        inlets=list(raw.get("inlets", [])) if raw.get("inlets") is not None else None,
        outlets=list(raw.get("outlets", [])) if raw.get("outlets") is not None else None,
        on_failure=raw.get("on_failure"),
    )


def _attach_dependencies(tasks_index: Dict[str, Any], edges: List[Tuple[Any, Any]]) -> None:
    # Wire dependencies once all tasks are created
    for upstream, downstream in edges:
        try:
            tasks_index[upstream] >> tasks_index[downstream]
        except Exception as exc:
            logger.error("Failed wiring dependency %s -> %s: %s", upstream, downstream, exc)


def _import_dotted(path: str) -> Any:
    mod_name, _, attr = path.partition(":")
    if not _:
        mod_name, _, attr = path.rpartition(".")
    if not mod_name or not attr:
        raise ValueError(f"Invalid dotted path: {path}")
    module = __import__(mod_name, fromlist=[attr])
    return getattr(module, attr)


def build_dag_from_config(cfg: DynamicDAGConfig) -> DAG:
    dag_id = _compute_effective_dag_id(cfg.dag_id, cfg)

    default_args = cfg.default_args or {}
    if _coerce_bool(cfg.feature_flags.get("enable_monitoring", True)):
        default_args = build_default_args_with_monitoring(dag_id, default_args)

    # Build schedule: cron or datasets
    schedule_value: Any
    if cfg.datasets:
        try:
            from airflow.datasets import Dataset  # type: ignore

            schedule_value = [Dataset(d["uri"]) if isinstance(d, dict) else Dataset(str(d)) for d in cfg.datasets]
        except Exception:
            # If Dataset API unavailable, fall back to cron schedule
            schedule_value = cfg.schedule
    else:
        schedule_value = cfg.schedule

    dag = DAG(
        dag_id=dag_id,
        description=cfg.description,
        default_args=default_args,
        schedule=schedule_value,
        start_date=cfg.start_datetime(),
        catchup=cfg.catchup,
        max_active_runs=cfg.max_active_runs,
        tags=cfg.tags,
        user_defined_macros=cfg.user_defined_macros or None,
    )

    with dag:
        if cfg.doc_md:
            try:
                dag.doc_md = cfg.doc_md
            except Exception:
                pass

        # Build tasks
        created: Dict[str, Any] = {}
        published_id: Dict[str, str] = {}  # logical id -> actual downstream id to depend on
        pending_edges: List[Tuple[str, str]] = []

        from airflow.operators.empty import EmptyOperator  # local import to avoid global DAG context

        # Helper to create a task within an optional TaskGroup
        def _create_from_raw(raw: Mapping[str, Any], group: Optional[TaskGroup] = None) -> None:
            spec = _make_task_spec(raw)
            # Merge DAG-level defaults, group-level defaults, and task-level
            task_kwargs = dict(cfg.default_task_kwargs)
            if group:
                task_kwargs.update(getattr(group, "_aurum_default_task_kwargs", {}))
                task_kwargs["task_group"] = group
            task_kwargs.update(raw.get("task_kwargs", {}))
            context = {"task_kwargs": task_kwargs}
            op = build_task_from_spec(spec, context)
            if isinstance(op, tuple):
                primary, fallback = op
                created[spec.task_id] = primary
                created[fallback.task_id] = fallback
                # Create healed join marker that represents successful completion via either path
                healed_id = f"{spec.task_id}__healed"
                healed = EmptyOperator(task_id=healed_id)
                created[healed_id] = healed
                published_id[spec.task_id] = healed_id
                # Wire primary success and fallback success into healed marker
                pending_edges.append((spec.task_id, healed_id))
                pending_edges.append((fallback.task_id, healed_id))
                # Also wire primary -> fallback (fallback has one_failed rule)
                pending_edges.append((spec.task_id, fallback.task_id))
            else:
                created[spec.task_id] = op
                published_id[spec.task_id] = spec.task_id

            # Register declared dependencies pointing to published ids
            for upstream in spec.depends_on:
                pending_edges.append((published_id.get(upstream, upstream), published_id.get(spec.task_id, spec.task_id)))

        # Top-level tasks
        for raw in cfg.tasks:
            _create_from_raw(raw, None)

        # Task groups
        for g in cfg.groups:
            group_id = g.get("id") or g.get("group_id")
            tooltip = g.get("tooltip", "")
            g_defaults = dict(g.get("default_task_kwargs", {}))
            with TaskGroup(group_id=group_id, tooltip=tooltip, dag=dag) as tg:
                # Attach defaults for later merge
                setattr(tg, "_aurum_default_task_kwargs", g_defaults)
                for raw in g.get("tasks", []):
                    _create_from_raw(raw, tg)

        _attach_dependencies(created, pending_edges)

    # Optional callbacks via dotted paths
    try:
        on_fail = cfg.feature_flags.get("dag_on_failure_callback")
        if on_fail:
            dag.on_failure_callback = _import_dotted(on_fail)
    except Exception as exc:
        logger.warning("Unable to import on_failure_callback: %s", exc)
    try:
        on_success = cfg.feature_flags.get("dag_on_success_callback")
        if on_success:
            dag.on_success_callback = _import_dotted(on_success)
    except Exception as exc:
        logger.warning("Unable to import on_success_callback: %s", exc)

    return dag


def build_dags_from_dir(config_dir: str) -> Dict[str, DAG]:
    """Load all configs from a directory and build DAGs.

    Respects env var AURUM_DYNAMIC_DAGS_DISABLED.
    """
    if _coerce_bool(os.getenv("AURUM_DYNAMIC_DAGS_DISABLED"), False):
        logger.info("Dynamic DAGs disabled via environment")
        return {}

    dags: Dict[str, DAG] = {}
    for path in _iter_config_files(config_dir):
        cfg = _load_config_file(path)
        if not cfg:
            continue
        try:
            dag = build_dag_from_config(cfg)
            dags[dag.dag_id] = dag
            logger.info("Built dynamic DAG %s from %s", dag.dag_id, path)
        except Exception as exc:
            logger.error("Failed to build DAG from %s: %s", path, exc)
    return dags
