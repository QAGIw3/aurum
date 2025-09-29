"""Reusable Airflow workflow templates and task builders.

This module provides a simple template registry and helpers to create common
task patterns with consistent monitoring, retries, pools and resource
configuration. It is used by the dynamic DAG generator to materialize tasks
from configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import asyncio
from typing import Any, Callable, Dict, List, Optional

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.branch import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import BaseOperator

try:  # Dataset support (Airflow >=2.4)
    from airflow.datasets import Dataset  # type: ignore
except Exception:  # pragma: no cover - optional
    Dataset = None  # type: ignore

try:
    # Monitoring utilities (optional during import)
    from aurum.airflow_utils import build_failure_callback
except Exception:  # pragma: no cover - DAG parse env safety
    build_failure_callback = None  # type: ignore


@dataclass
class TaskSpec:
    """Specification for a task created from configuration."""

    task_id: str
    type: str
    params: Dict[str, Any]
    depends_on: List[str]
    trigger_rule: Optional[str] = None
    condition: Optional[str] = None
    pool: Optional[str] = None
    retries: Optional[int] = None
    retry_delay_minutes: Optional[int] = None
    task_concurrency: Optional[int] = None
    execution_timeout_minutes: Optional[int] = None
    sla_minutes: Optional[int] = None
    doc_md: Optional[str] = None
    inlets: Optional[List[str]] = None
    outlets: Optional[List[str]] = None
    on_failure: Optional[Dict[str, Any]] = None


class TemplateRegistry:
    """Registry for reusable workflow templates."""

    def __init__(self) -> None:
        self._templates: Dict[str, Callable[..., Any]] = {}

    def register(self, name: str, factory: Callable[..., Any]) -> None:
        self._templates[name] = factory

    def get(self, name: str) -> Callable[..., Any]:
        if name not in self._templates:
            raise KeyError(f"Unknown template: {name}")
        return self._templates[name]

    def names(self) -> List[str]:
        return sorted(self._templates.keys())


registry = TemplateRegistry()


def _apply_common_task_kwargs(task: Any, spec: TaskSpec) -> Any:
    if spec.pool:
        setattr(task, "pool", spec.pool)
    if spec.retries is not None:
        setattr(task, "retries", spec.retries)
    if spec.retry_delay_minutes is not None:
        setattr(task, "retry_delay", timedelta(minutes=spec.retry_delay_minutes))
    if spec.task_concurrency is not None:
        setattr(task, "task_concurrency", spec.task_concurrency)
    if spec.trigger_rule:
        setattr(task, "trigger_rule", TriggerRule(spec.trigger_rule))
    if spec.execution_timeout_minutes is not None:
        setattr(task, "execution_timeout", timedelta(minutes=spec.execution_timeout_minutes))
    if spec.sla_minutes is not None:
        setattr(task, "sla", timedelta(minutes=spec.sla_minutes))
    if spec.doc_md:
        try:
            setattr(task, "doc_md", spec.doc_md)
        except Exception:
            pass
    if Dataset is not None:
        try:
            if spec.inlets:
                setattr(task, "inlets", [Dataset(uri) for uri in spec.inlets])
            if spec.outlets:
                setattr(task, "outlets", [Dataset(uri) for uri in spec.outlets])
        except Exception:
            pass
    return task


def template_empty(task_id: str, **kwargs: Any) -> EmptyOperator:
    return EmptyOperator(task_id=task_id, **kwargs)


def template_bash(task_id: str, command: str, **kwargs: Any) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=command, **kwargs)


def template_python(task_id: str, python_path: str, op_kwargs: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PythonOperator:
    """Create a PythonOperator from a dotted-path callable.

    python_path: e.g. "aurum.some.module:function_name"
    """
    module_name, _, func_name = python_path.partition(":")
    if not _:
        module_name, _, func_name = python_path.rpartition(".")
    if not module_name or not func_name:
        raise ValueError(f"Invalid python callable path: {python_path}")
    module = __import__(module_name, fromlist=[func_name])
    func = getattr(module, func_name)
    # Support factory pattern: if 'factory' truthy in kwargs, call to obtain callable
    params = kwargs.pop("params", {}) or {}
    factory_mode = False
    if isinstance(op_kwargs, dict) and op_kwargs.pop("factory", None):
        factory_mode = True
    if isinstance(params, dict) and params.get("factory"):
        factory_mode = True
    if factory_mode:
        # Build the actual callable and clear op_kwargs for operator
        func = func(**(op_kwargs or {}))
        op_kwargs = {}
    # Re-attach params if present
    if params:
        kwargs["params"] = params
    return PythonOperator(task_id=task_id, python_callable=func, op_kwargs=op_kwargs or {}, **kwargs)


def template_branch(task_id: str, condition: str, branch_map: Dict[str, str], **kwargs: Any) -> BranchPythonOperator:
    """Branch based on a simple expression evaluated in Airflow context.

    branch_map: mapping from result key ("true"/"false" or any custom value) to task_id to follow.
    """

    def _branch(**context: Any) -> str:
        # Extremely limited evaluator: treat non-empty/non-"false" string as True
        value = str(context.get("params", {}).get("condition_result", "")).strip().lower()
        if not value:  # fallback to direct condition string for simple use
            value = str(condition).strip().lower()
        key = "true" if value not in {"", "0", "false", "none"} else "false"
        return branch_map.get(key) or next(iter(branch_map.values()))

    return BranchPythonOperator(task_id=task_id, python_callable=_branch, params={"condition_result": condition}, **kwargs)


def template_trigger_dag(task_id: str, dag_id: str, conf: Optional[Dict[str, Any]] = None, **kwargs: Any) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(task_id=task_id, trigger_dag_id=dag_id, conf=conf or {}, **kwargs)


def template_external_sensor(task_id: str, external_dag_id: str, external_task_id: Optional[str] = None, mode: str = "reschedule", **kwargs: Any) -> ExternalTaskSensor:
    return ExternalTaskSensor(task_id=task_id, external_dag_id=external_dag_id, external_task_id=external_task_id, mode=mode, **kwargs)


def template_short_circuit(task_id: str, condition: Any, **kwargs: Any) -> ShortCircuitOperator:
    """Short-circuit downstream tasks when the evaluated condition is falsy."""

    def _evaluate(**context: Any) -> bool:
        value = context.get("params", {}).get("condition", condition)
        if isinstance(value, bool):
            return value
        stringified = str(value).strip().lower()
        return stringified not in {"", "0", "false", "none"}

    params = dict(kwargs.get("params", {}))
    params.setdefault("condition", condition)
    kwargs["params"] = params
    return ShortCircuitOperator(task_id=task_id, python_callable=_evaluate, **kwargs)


def template_integration_event(task_id: str, event_type: str, payload: Optional[Dict[str, Any]] = None, **kwargs: Any) -> PythonOperator:
    """Trigger an integration event using the integration manager."""

    from aurum.workflow.integrations import integration_manager

    def _fire(**context: Any) -> None:
        event_payload = dict(payload or {})
        rendered = context.get("params", {}).get("payload")
        if isinstance(rendered, dict):
            event_payload.update(rendered)
        asyncio.run(integration_manager.trigger_integration_event(event_type, event_payload))

    params = dict(kwargs.get("params", {}))
    params.setdefault("payload", payload or {})
    kwargs["params"] = params
    return PythonOperator(task_id=task_id, python_callable=_fire, **kwargs)


def template_sql(
    task_id: str,
    sql: Any,
    conn_id: str = "postgres_default",
    *,
    mapping: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> BaseOperator:
    """Execute SQL using the common SQL provider when available."""

    try:
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # type: ignore

        op_kwargs = dict(kwargs)
        op_kwargs.update({"task_id": task_id, "sql": sql, "conn_id": conn_id})
        if mapping:
            partial = SQLExecuteQueryOperator.partial(**op_kwargs)
            if "kwargs" in mapping:
                return partial.expand_kwargs(kwargs=mapping["kwargs"])
            if "expand" in mapping:
                return partial.expand(**mapping["expand"])
            if mapping.get("param") and mapping.get("items") is not None:
                return partial.expand(**{mapping["param"]: mapping["items"]})
        return SQLExecuteQueryOperator(**op_kwargs)
    except Exception:
        # Fallback to bash echo so DAG still parses in environments without the provider installed
        command = f"echo 'SQL provider unavailable; skipped execution of {task_id}'"
        return template_bash(task_id, command, **kwargs)


def _common_operator_kwargs(spec: TaskSpec, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Translate TaskSpec common fields into BaseOperator constructor kwargs."""
    kwargs: Dict[str, Any] = dict(base_kwargs)
    if spec.pool:
        kwargs["pool"] = spec.pool
    if spec.retries is not None:
        kwargs["retries"] = spec.retries
    if spec.retry_delay_minutes is not None:
        kwargs["retry_delay"] = timedelta(minutes=spec.retry_delay_minutes)
    if spec.task_concurrency is not None:
        kwargs["task_concurrency"] = spec.task_concurrency
    if spec.trigger_rule:
        try:
            kwargs["trigger_rule"] = TriggerRule(spec.trigger_rule)
        except Exception:
            kwargs["trigger_rule"] = spec.trigger_rule
    if spec.execution_timeout_minutes is not None:
        kwargs["execution_timeout"] = timedelta(minutes=spec.execution_timeout_minutes)
    if spec.sla_minutes is not None:
        kwargs["sla"] = timedelta(minutes=spec.sla_minutes)
    if spec.doc_md:
        kwargs["doc_md"] = spec.doc_md
    if Dataset is not None:
        try:
            if spec.inlets:
                kwargs["inlets"] = [Dataset(uri) for uri in spec.inlets]
            if spec.outlets:
                kwargs["outlets"] = [Dataset(uri) for uri in spec.outlets]
        except Exception:
            pass
    return kwargs


def build_task_from_spec(spec: TaskSpec, context: Dict[str, Any]) -> Any:
    """Create an Airflow task operator from a TaskSpec.

    The returned operator is not yet wired with dependencies.
    """
    ttype = spec.type.lower()
    base_kwargs = context.get("task_kwargs", {})

    mapping = spec.params.get("map") if isinstance(spec.params, dict) else None

    if ttype == "empty":
        if mapping:
            # empty operator does not support mapping; ignore mapping
            task = template_empty(spec.task_id, **base_kwargs)
        else:
            task = template_empty(spec.task_id, **base_kwargs)
    elif ttype == "bash":
        from airflow.operators.bash import BashOperator as _Op

        op_kwargs = _common_operator_kwargs(spec, dict(base_kwargs))
        op_kwargs.update({"task_id": spec.task_id, "bash_command": spec.params.get("command", "echo 'no-op'")})
        if mapping:
            partial = _Op.partial(**op_kwargs)
            if "kwargs" in mapping:
                task = partial.expand_kwargs(kwargs=mapping["kwargs"])  # list[dict]
            elif "expand" in mapping:
                task = partial.expand(**mapping["expand"])  # dict of sequences
            elif mapping.get("param") and mapping.get("items") is not None:
                task = partial.expand(**{mapping["param"]: mapping["items"]})
            else:
                task = _Op(**op_kwargs)
        else:
            task = _Op(**op_kwargs)
    elif ttype == "python":
        from airflow.operators.python import PythonOperator as _Op

        op_kwargs = _common_operator_kwargs(spec, dict(base_kwargs))
        # Resolve callable and (optional) factory
        module_name, _, func_name = (spec.params.get("callable") or "").partition(":")
        if not _:
            module_name, _, func_name = (spec.params.get("callable") or "").rpartition(".")
        if not module_name or not func_name:
            raise ValueError(f"Invalid python callable path: {spec.params.get('callable')}")
        module = __import__(module_name, fromlist=[func_name])
        func = getattr(module, func_name)
        op_kwargs_value = spec.params.get("op_kwargs") or {}
        if op_kwargs_value.pop("factory", None):
            func = func(**op_kwargs_value)
            op_kwargs_value = {}
        op_kwargs.update({"task_id": spec.task_id, "python_callable": func, "op_kwargs": op_kwargs_value})
        if mapping:
            partial = _Op.partial(**op_kwargs)
            if "kwargs" in mapping:
                task = partial.expand_kwargs(kwargs=mapping["kwargs"])  # list[dict]
            elif "expand" in mapping:
                task = partial.expand(**mapping["expand"])  # e.g., op_kwargs=[{...}, {...}]
            elif mapping.get("param") and mapping.get("items") is not None:
                task = partial.expand(**{mapping["param"]: mapping["items"]})
            else:
                task = _Op(**op_kwargs)
        else:
            task = _Op(**op_kwargs)
    elif ttype == "branch":
        task = template_branch(spec.task_id, spec.condition or spec.params.get("condition", ""), spec.params.get("branch_map", {}), **base_kwargs)
    elif ttype == "trigger_dag":
        task = template_trigger_dag(spec.task_id, spec.params.get("dag_id"), spec.params.get("conf"), **base_kwargs)
    elif ttype == "external_sensor":
        task = template_external_sensor(spec.task_id, spec.params.get("external_dag_id"), spec.params.get("external_task_id"), spec.params.get("mode", "reschedule"), **base_kwargs)
    elif ttype == "short_circuit":
        condition_value = spec.condition or spec.params.get("condition")
        task = template_short_circuit(spec.task_id, condition_value, **base_kwargs)
    elif ttype == "integration_event":
        event_type = spec.params.get("event_type") or spec.params.get("event")
        if not event_type:
            raise ValueError("integration_event tasks require 'event_type'")
        event_payload = spec.params.get("payload") or {}
        task = template_integration_event(spec.task_id, event_type, event_payload, **base_kwargs)
    elif ttype == "sql":
        op_kwargs = _common_operator_kwargs(spec, dict(base_kwargs))
        sql_statement = spec.params.get("sql") or spec.params.get("query") or "SELECT 1"
        conn_id = spec.params.get("conn_id", "postgres_default")
        task = template_sql(spec.task_id, sql_statement, conn_id, mapping=mapping, **op_kwargs)
    elif ttype == "http":
        # Optional provider: airflow.providers.http
        try:
            from airflow.providers.http.operators.http import SimpleHttpOperator  # type: ignore

            op_kwargs = _common_operator_kwargs(spec, dict(base_kwargs))
            op_kwargs.update(
                {
                    "task_id": spec.task_id,
                    "http_conn_id": spec.params.get("http_conn_id", "http_default"),
                    "endpoint": spec.params.get("endpoint", "/"),
                    "method": (spec.params.get("method") or "GET").upper(),
                    "headers": spec.params.get("headers"),
                    "data": spec.params.get("data"),
                    "response_check": spec.params.get("response_check"),
                    "log_response": bool(spec.params.get("log_response", True)),
                }
            )
            if mapping:
                partial = SimpleHttpOperator.partial(**op_kwargs)
                if "kwargs" in mapping:
                    task = partial.expand_kwargs(kwargs=mapping["kwargs"])  # list[dict]
                elif "expand" in mapping:
                    task = partial.expand(**mapping["expand"])  # dict of sequences
                elif mapping.get("param") and mapping.get("items") is not None:
                    task = partial.expand(**{mapping["param"]: mapping["items"]})
                else:
                    task = SimpleHttpOperator(**op_kwargs)
            else:
                task = SimpleHttpOperator(**op_kwargs)
        except Exception:
            # Fallback to bash so DAG still parses
            cmd = f"echo 'HTTP {spec.params.get('method', 'GET')} {spec.params.get('endpoint', '/')} (provider missing)'"
            task = template_bash(spec.task_id, cmd, **base_kwargs)
    elif ttype == "kubernetes_pod":
        # Optional provider: cncf.kubernetes
        try:
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator  # type: ignore

            op_kwargs = _common_operator_kwargs(spec, dict(base_kwargs))
            op_kwargs.update(
                {
                    "task_id": spec.task_id,
                    "name": spec.params.get("name", spec.task_id),
                    "namespace": spec.params.get("namespace", "default"),
                    "image": spec.params.get("image", "alpine:3.19"),
                    "cmds": spec.params.get("cmds"),
                    "arguments": spec.params.get("arguments"),
                    "env_vars": spec.params.get("env_vars"),
                    "secrets": spec.params.get("secrets"),
                    "get_logs": True,
                    "is_delete_operator_pod": bool(spec.params.get("delete_on_finish", True)),
                }
            )
            if mapping:
                partial = KubernetesPodOperator.partial(**op_kwargs)
                if "kwargs" in mapping:
                    task = partial.expand_kwargs(kwargs=mapping["kwargs"])  # list[dict]
                elif "expand" in mapping:
                    task = partial.expand(**mapping["expand"])  # dict of sequences
                elif mapping.get("param") and mapping.get("items") is not None:
                    task = partial.expand(**{mapping["param"]: mapping["items"]})
                else:
                    task = KubernetesPodOperator(**op_kwargs)
            else:
                task = KubernetesPodOperator(**op_kwargs)
        except Exception:
            task = template_bash(spec.task_id, "echo 'Kubernetes provider unavailable'", **base_kwargs)
    else:
        # Fallback to empty for unknown types to avoid parse errors
        task = template_empty(spec.task_id, **base_kwargs)

    task = _apply_common_task_kwargs(task, spec)

    # Self-healing fallback: if configured, create a fallback task and wire with ONE_FAILED
    if spec.on_failure and spec.on_failure.get("fallback"):
        fb = spec.on_failure["fallback"]
        fb_task = template_bash(f"{spec.task_id}__fallback", fb.get("command", "echo 'fallback'"), **base_kwargs)
        fb_rule = fb.get("trigger_rule", "one_failed").lower()
        setattr(fb_task, "trigger_rule", TriggerRule(fb_rule))
        # Return a tuple to allow caller to wire both
        return task, fb_task

    return task


def build_default_args_with_monitoring(dag_id: str, default_args: Dict[str, Any]) -> Dict[str, Any]:
    """Attach standardized failure callbacks and tighten defaults if monitoring requested."""
    args = dict(default_args)
    if build_failure_callback is not None and not args.get("on_failure_callback"):
        try:
            args["on_failure_callback"] = build_failure_callback(source=f"aurum.airflow.{dag_id}")
        except Exception:
            # Keep args intact if callback builder is unavailable
            pass
    return args


# Register a couple of named templates for convenience
registry.register("empty", template_empty)
registry.register("bash", template_bash)
registry.register("python", template_python)
registry.register("branch", template_branch)
registry.register("trigger_dag", template_trigger_dag)
registry.register("external_sensor", template_external_sensor)
registry.register("short_circuit", template_short_circuit)
registry.register("integration_event", template_integration_event)
registry.register("sql", template_sql)
