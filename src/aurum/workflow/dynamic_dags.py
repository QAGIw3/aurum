"""Dynamic Airflow DAG generation from JSON/YAML configuration.

Enhancements add strong schema validation, richer template support, and
version-aware DAG materialisation for the advanced orchestration track.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from . import observability
from .config_schema import WorkflowConfig, TaskConfig, GroupConfig, load_workflow_config
from .templates import (
    TaskSpec,
    build_default_args_with_monitoring,
    build_task_from_spec,
    template_trigger_dag,
)
from .versioning import WorkflowVersionRegistry, load_registry

try:  # Optional dependency for YAML configs
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore

logger = logging.getLogger(__name__)

_VERSION_REGISTRY: Optional[WorkflowVersionRegistry] = None
_VERSION_REGISTRY_FAILED = False


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    return s not in {"", "0", "false", "none"}


def _load_config_file(path: str) -> Optional[WorkflowConfig]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            if path.endswith((".yaml", ".yml")):
                if yaml is None:
                    logger.warning("Skipping YAML config without PyYAML: %s", path)
                    return None
                raw = yaml.safe_load(handle)
            else:
                raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError("workflow config must be a JSON/YAML object")
        return load_workflow_config(raw)
    except Exception as exc:
        logger.error("Failed to load DAG config %s: %s", path, exc)
        return None


def _iter_config_files(config_dir: str) -> Iterable[str]:
    if not os.path.isdir(config_dir):
        return []
    for name in sorted(os.listdir(config_dir)):
        if name.startswith("."):
            continue
        if name.endswith((".json", ".yaml", ".yml")):
            yield os.path.join(config_dir, name)


def _get_version_registry() -> Optional[WorkflowVersionRegistry]:
    global _VERSION_REGISTRY, _VERSION_REGISTRY_FAILED
    if _VERSION_REGISTRY_FAILED:
        return None
    if _VERSION_REGISTRY is not None:
        return _VERSION_REGISTRY
    try:
        _VERSION_REGISTRY = load_registry()
        return _VERSION_REGISTRY
    except Exception as exc:  # pragma: no cover - optional dependency failure
        logger.debug("Workflow version registry unavailable: %s", exc)
        _VERSION_REGISTRY_FAILED = True
        return None


def _resolve_dag_identity(base_id: str, cfg: WorkflowConfig) -> Tuple[str, Optional[str]]:
    flags = cfg.feature_flags or {}
    version = (flags.get("dag_version") or "").strip()
    use_suffix = _coerce_bool(flags.get("enable_version_suffix", False))

    registry = _get_version_registry()
    registry_version = registry.get_active_version(base_id) if registry else None
    if registry_version:
        version = registry_version
        use_suffix = True

    rollback_to = (flags.get("rollback_to") or os.getenv(f"AURUM_ROLLBACK_{base_id}") or "").strip()
    if rollback_to:
        version = rollback_to
        use_suffix = True

    if use_suffix and version:
        dag_id = f"{base_id}__v{version}"
    else:
        dag_id = base_id
    return dag_id, (version or None)


@dataclass
class DatasetDependencyInfo:
    datasets: List[str]
    remaining: List[str]


def _is_dataset_reference(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("dataset://")


def _normalise_dataset_uri(uri: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", uri.replace("dataset://", ""))
    return slug.strip("_") or "dataset"


def _dataset_sensor_id(dataset_uri: str) -> str:
    return f"wait_for__{_normalise_dataset_uri(dataset_uri)}"


def _extract_dataset_dependencies(task_cfg: TaskConfig) -> DatasetDependencyInfo:
    datasets: List[str] = []
    remaining: List[str] = []
    for dep in task_cfg.depends_on:
        if _is_dataset_reference(dep):
            if dep not in datasets:
                datasets.append(dep)
        else:
            remaining.append(dep)
    for uri in task_cfg.wait_for_datasets:
        if uri not in datasets:
            datasets.append(uri)
    return DatasetDependencyInfo(datasets=datasets, remaining=remaining)


def _ensure_dataset_sensor(
    dataset_uri: str,
    created: Dict[str, Any],
    published_id: Dict[str, str],
) -> str:
    sensor_id = _dataset_sensor_id(dataset_uri)
    if sensor_id in created:
        return sensor_id
    try:
        from airflow.datasets import Dataset  # type: ignore
        from airflow.sensors.dataset import DatasetSensor  # type: ignore

        dataset = Dataset(dataset_uri)
        try:
            sensor = DatasetSensor(task_id=sensor_id, datasets=[dataset])
        except TypeError:
            sensor = DatasetSensor(task_id=sensor_id, dataset=dataset)
        if hasattr(sensor, "mode"):
            try:
                sensor.mode = "reschedule"
            except Exception:
                pass
    except Exception:
        from airflow.operators.empty import EmptyOperator

        sensor = EmptyOperator(task_id=sensor_id)
    created[sensor_id] = sensor
    published_id[sensor_id] = sensor_id
    return sensor_id


def _make_task_spec(task_cfg: TaskConfig, *, depends_on: Optional[List[str]] = None) -> TaskSpec:
    params = dict(task_cfg.params or {})
    if task_cfg.mapping and "map" not in params:
        params["map"] = task_cfg.mapping.model_dump(exclude_none=True)
    upstream = list(depends_on) if depends_on is not None else list(task_cfg.depends_on)
    return TaskSpec(
        task_id=task_cfg.task_id,
        type=task_cfg.type,
        params=params,
        depends_on=upstream,
        trigger_rule=task_cfg.trigger_rule,
        condition=task_cfg.condition,
        pool=task_cfg.pool,
        retries=task_cfg.retries,
        retry_delay_minutes=task_cfg.retry_delay_minutes,
        task_concurrency=task_cfg.task_concurrency,
        execution_timeout_minutes=task_cfg.execution_timeout_minutes,
        sla_minutes=task_cfg.sla_minutes,
        doc_md=task_cfg.doc_md,
        inlets=list(task_cfg.inlets) if task_cfg.inlets is not None else None,
        outlets=list(task_cfg.outlets) if task_cfg.outlets is not None else None,
        on_failure=task_cfg.on_failure,
    )


def _attach_dependencies(tasks_index: Dict[str, Any], edges: List[Tuple[str, str]]) -> None:
    for upstream, downstream in edges:
        try:
            tasks_index[upstream] >> tasks_index[downstream]
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed wiring dependency %s -> %s: %s", upstream, downstream, exc)


def _import_dotted(path: str) -> Any:
    mod_name, _, attr = path.partition(":")
    if not _:
        mod_name, _, attr = path.rpartition(".")
    if not mod_name or not attr:
        raise ValueError(f"Invalid dotted path: {path}")
    module = __import__(mod_name, fromlist=[attr])
    return getattr(module, attr)


def _create_task(
    cfg: WorkflowConfig,
    task_cfg: TaskConfig,
    created: Dict[str, Any],
    published_id: Dict[str, str],
    pending_edges: List[Tuple[str, str]],
    *,
    active_version: Optional[str],
    group: Optional[TaskGroup] = None,
) -> None:
    dataset_dependencies = _extract_dataset_dependencies(task_cfg)
    spec = _make_task_spec(task_cfg, depends_on=dataset_dependencies.remaining)
    task_kwargs = dict(cfg.default_task_kwargs)
    if group is not None:
        task_kwargs.update(getattr(group, "_aurum_default_task_kwargs", {}))
        task_kwargs["task_group"] = group
    task_kwargs.update(task_cfg.task_kwargs or {})

    healing_flags = cfg.feature_flags.get("self_healing") if isinstance(cfg.feature_flags.get("self_healing"), dict) else {}
    if isinstance(healing_flags, dict):
        if "default_retries" in healing_flags and "retries" not in task_kwargs:
            try:
                task_kwargs["retries"] = int(healing_flags["default_retries"])
            except Exception:
                pass
        if "retry_delay_minutes" in healing_flags and "retry_delay" not in task_kwargs:
            try:
                task_kwargs["retry_delay"] = timedelta(minutes=int(healing_flags["retry_delay_minutes"]))
            except Exception:
                pass
        if "execution_timeout_minutes" in healing_flags and "execution_timeout" not in task_kwargs:
            try:
                task_kwargs["execution_timeout"] = timedelta(minutes=int(healing_flags["execution_timeout_minutes"]))
            except Exception:
                pass

    context = {"task_kwargs": task_kwargs}
    op = build_task_from_spec(spec, context)

    def _instrument(task_obj: Any, task_type: str) -> None:
        try:
            observability.instrument_task(
                task_obj,
                dag_id=cfg.dag_id,
                task_type=task_type,
                version=active_version,
            )
        except Exception:  # pragma: no cover - observability best effort
            logger.debug(
                "Unable to attach observability to %s", getattr(task_obj, "task_id", task_type)
            )

    if isinstance(op, tuple):
        primary, fallback = op
        created[spec.task_id] = primary
        created[fallback.task_id] = fallback
        healed_id = f"{spec.task_id}__healed"
        from airflow.operators.empty import EmptyOperator  # local import to avoid DAG context bleed

        healed = EmptyOperator(task_id=healed_id, task_group=group)
        created[healed_id] = healed
        published_id[spec.task_id] = healed_id
        pending_edges.append((spec.task_id, healed_id))
        pending_edges.append((fallback.task_id, healed_id))
        pending_edges.append((spec.task_id, fallback.task_id))
        _instrument(primary, task_cfg.type)
        _instrument(fallback, f"{task_cfg.type}_fallback")
    else:
        created[spec.task_id] = op
        published_id[spec.task_id] = spec.task_id
        _instrument(op, task_cfg.type)

    failure_conf = (task_cfg.on_failure or {}).get("trigger_dag") or (task_cfg.on_failure or {}).get("trigger")
    if failure_conf:
        failure_task_id = failure_conf.get("task_id") or f"{spec.task_id}__on_failure"
        try:
            trigger_rule = TriggerRule(failure_conf.get("trigger_rule", "one_failed"))
        except Exception:
            trigger_rule = TriggerRule.ONE_FAILED
        trigger_kwargs = dict(cfg.default_task_kwargs)
        if group is not None:
            trigger_kwargs.update(getattr(group, "_aurum_default_task_kwargs", {}))
            trigger_kwargs["task_group"] = group
        trigger_kwargs.update(failure_conf.get("task_kwargs", {}))
        trigger_kwargs["trigger_rule"] = trigger_rule
        failure_task = template_trigger_dag(
            failure_task_id,
            failure_conf.get("dag_id"),
            failure_conf.get("conf"),
            **trigger_kwargs,
        )
        created[failure_task_id] = failure_task
        _instrument(failure_task, f"{task_cfg.type}_failure_trigger")
        pending_edges.append((spec.task_id, failure_task_id))
        if isinstance(op, tuple):
            fallback = op[1]
            pending_edges.append((fallback.task_id, failure_task_id))

    dataset_sensor_ids: List[str] = []
    for dataset_uri in dataset_dependencies.datasets:
        sensor_id = _ensure_dataset_sensor(dataset_uri, created, published_id)
        dataset_sensor_ids.append(sensor_id)

    downstream_target = published_id.get(spec.task_id, spec.task_id)
    for sensor_id in dataset_sensor_ids:
        pending_edges.append((sensor_id, downstream_target))

    for upstream in spec.depends_on:
        upstream_id = published_id.get(upstream, upstream)
        downstream_id = published_id.get(spec.task_id, spec.task_id)
        pending_edges.append((upstream_id, downstream_id))


def _materialise_group(
    cfg: WorkflowConfig,
    group_cfg: GroupConfig,
    created: Dict[str, Any],
    published_id: Dict[str, str],
    pending_edges: List[Tuple[str, str]],
    dag: DAG,
    active_version: Optional[str],
) -> None:
    with TaskGroup(group_id=group_cfg.group_id, tooltip=group_cfg.tooltip, dag=dag) as task_group:
        setattr(task_group, "_aurum_default_task_kwargs", dict(group_cfg.default_task_kwargs))
        for task in group_cfg.tasks:
            _create_task(
                cfg,
                task,
                created,
                published_id,
                pending_edges,
                active_version=active_version,
                group=task_group,
            )


def build_dag_from_config(cfg: WorkflowConfig) -> DAG:
    dag_id, active_version = _resolve_dag_identity(cfg.dag_id, cfg)

    default_args = dict(cfg.default_args)
    if _coerce_bool(cfg.feature_flags.get("enable_monitoring", True)):
        default_args = build_default_args_with_monitoring(dag_id, default_args)

    perf_flags: Dict[str, Any] = {}
    perf_cfg = cfg.feature_flags.get("performance") if isinstance(cfg.feature_flags, dict) else None
    if isinstance(perf_cfg, dict):
        perf_flags = perf_cfg
        if "default_retries" in perf_cfg and "retries" not in default_args:
            try:
                default_args["retries"] = int(perf_cfg["default_retries"])
            except Exception:
                pass
        if "retry_delay_minutes" in perf_cfg and "retry_delay" not in default_args:
            try:
                default_args["retry_delay"] = timedelta(minutes=int(perf_cfg["retry_delay_minutes"]))
            except Exception:
                pass

    schedule_value: Any
    if cfg.datasets:
        try:
            from airflow.datasets import Dataset  # type: ignore

            schedule_value = [Dataset(d["uri"]) if isinstance(d, dict) else Dataset(str(d)) for d in cfg.datasets]
        except Exception:
            schedule_value = cfg.schedule
    else:
        schedule_value = cfg.schedule

    dag = DAG(
        dag_id=dag_id,
        description=cfg.description or cfg.dag_id,
        default_args=default_args,
        schedule=schedule_value,
        start_date=cfg.start_date,
        catchup=cfg.catchup,
        max_active_runs=cfg.max_active_runs,
        tags=cfg.tags,
        user_defined_macros=cfg.user_defined_macros or None,
    )

    if active_version:
        version_tag = f"version:{active_version}"
        if version_tag not in dag.tags:
            dag.tags = list(dict.fromkeys(list(dag.tags) + [version_tag]))
        params = dict(getattr(dag, "params", {}) or {})
        params["active_version"] = active_version
        dag.params = params

    try:
        observability.instrument_dag(dag, version=active_version)
    except Exception:  # pragma: no cover - observability best effort
        logger.debug("Unable to attach DAG observability for %s", dag.dag_id)

    if perf_flags.get("max_active_tasks"):
        try:
            dag.max_active_tasks = int(perf_flags["max_active_tasks"])
        except Exception:
            pass
    if perf_flags.get("dag_concurrency"):
        try:
            dag.concurrency = int(perf_flags["dag_concurrency"])
        except Exception:
            pass

    with dag:
        if cfg.doc_md:
            try:
                dag.doc_md = cfg.doc_md
            except Exception:
                pass

        if active_version:
            version_note = f"**Active Version:** {active_version}"
            try:
                current_doc = getattr(dag, "doc_md", "") or ""
                if version_note not in current_doc:
                    combined = f"{version_note}\n\n{current_doc}".strip()
                    dag.doc_md = combined
            except Exception:
                pass

        created: Dict[str, Any] = {}
        published_id: Dict[str, str] = {}
        pending_edges: List[Tuple[str, str]] = []

        for task_cfg in cfg.tasks:
            _create_task(
                cfg,
                task_cfg,
                created,
                published_id,
                pending_edges,
                active_version=active_version,
            )

        for group_cfg in cfg.groups:
            _materialise_group(
                cfg,
                group_cfg,
                created,
                published_id,
                pending_edges,
                dag,
                active_version,
            )

        _attach_dependencies(created, pending_edges)

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
            active_version = getattr(dag, "params", {}).get("active_version")
            if active_version:
                logger.info("Built dynamic DAG %s (version=%s) from %s", dag.dag_id, active_version, path)
            else:
                logger.info("Built dynamic DAG %s from %s", dag.dag_id, path)
        except Exception as exc:
            logger.error("Failed to build DAG from %s: %s", path, exc)
    return dags


__all__ = ["build_dag_from_config", "build_dags_from_dir", "_load_config_file"]
