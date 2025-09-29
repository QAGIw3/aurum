from __future__ import annotations

"""Workflow orchestration CLI helpers.

These commands support validating configuration, rendering static DAG modules,
and performing dry-run imports for advanced orchestration workflows.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import ValidationError

from aurum.workflow.config_schema import WorkflowConfig
from aurum.workflow.dynamic_dags import build_dag_from_config
from aurum.workflow.versioning import load_registry, registry_path_from_env
from aurum.workflow.integrations import integration_manager

try:  # Optional dependency for YAML configs
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore

SUPPORTED_SUFFIXES = {".json", ".yaml", ".yml"}
DEFAULT_CONFIG_DIR = Path("config/workflows")
DEFAULT_RENDER_DIR = Path("airflow/dags/generated")


class WorkflowCLIError(RuntimeError):
    """Raised when the workflow CLI encounters an error."""


def _discover_paths(inputs: Iterable[str]) -> List[Path]:
    files: List[Path] = []
    seen: set[Path] = set()
    for item in inputs:
        path = Path(item)
        if not path.exists():
            continue
        if path.is_dir():
            for candidate in sorted(path.iterdir()):
                if candidate.suffix.lower() in SUPPORTED_SUFFIXES:
                    if candidate not in seen:
                        files.append(candidate)
                        seen.add(candidate)
        elif path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES:
            if path not in seen:
                files.append(path)
                seen.add(path)
    return files


def _load_workflow(path: Path) -> WorkflowConfig:
    with path.open("r", encoding="utf-8") as handle:
        if path.suffix.lower() in {".yaml", ".yml"}:
            if yaml is None:
                raise WorkflowCLIError("PyYAML is required to parse YAML configs")
            raw = yaml.safe_load(handle)
        else:
            raw = json.load(handle)
    if not isinstance(raw, dict):
        raise WorkflowCLIError(f"Configuration in {path} must be a JSON/YAML object")
    return WorkflowConfig.model_validate(raw)


def _print_validation_error(path: Path, error: ValidationError) -> None:
    print(f"Validation failed for {path}:", file=sys.stderr)
    for entry in error.errors():
        location = " -> ".join(str(part) for part in entry.get("loc", []))
        message = entry.get("msg", "")
        print(f"  [{location}] {message}", file=sys.stderr)


def _registry_path(value: str | None) -> Path:
    return Path(value) if value else registry_path_from_env()


def _parse_metadata(pairs: List[str] | None) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def cmd_validate(args: argparse.Namespace) -> int:
    targets = args.paths or [str(DEFAULT_CONFIG_DIR)]
    files = _discover_paths(targets)
    if not files:
        print("No workflow configuration files discovered", file=sys.stderr)
        return 1

    success = True
    for path in files:
        try:
            cfg = _load_workflow(path)
            print(f"✔ {cfg.dag_id} ({path}) -> {len(cfg.tasks)} tasks, {len(cfg.groups)} groups")
        except ValidationError as exc:
            success = False
            _print_validation_error(path, exc)
        except WorkflowCLIError as exc:
            success = False
            print(f"Error processing {path}: {exc}", file=sys.stderr)
    return 0 if success else 1


def _render_module(cfg: WorkflowConfig, source_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    config_payload = json.dumps(cfg.model_dump(mode="json"), indent=2, sort_keys=True)
    module = (
        "\"\"\"Auto-generated Airflow DAG module.\n"
        f"Source: {source_path}\n"
        "Do not edit manually.\n\"\"\"\n\n"
        "from aurum.workflow.config_schema import WorkflowConfig\n"
        "from aurum.workflow.dynamic_dags import build_dag_from_config\n\n"
        f"_CONFIG = WorkflowConfig.model_validate({config_payload})\n"
        "_DAG = build_dag_from_config(_CONFIG)\n"
        "globals()[_DAG.dag_id] = _DAG\n"
    )
    output_path.write_text(module, encoding="utf-8")


def cmd_render(args: argparse.Namespace) -> int:
    if not args.config:
        print("--config is required", file=sys.stderr)
        return 1
    config_path = Path(args.config)
    try:
        cfg = _load_workflow(config_path)
    except ValidationError as exc:
        _print_validation_error(config_path, exc)
        return 1
    except WorkflowCLIError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.stdout:
        print(json.dumps(cfg.model_dump(mode="json"), indent=2, sort_keys=True))
        return 0

    output_dir = Path(args.output or DEFAULT_RENDER_DIR)
    dag_id = cfg.dag_id
    output_path = output_dir / f"{dag_id}.py"
    _render_module(cfg, config_path, output_path)
    print(f"Rendered {dag_id} to {output_path}")
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    targets = args.paths or [str(DEFAULT_CONFIG_DIR)]
    files = _discover_paths(targets)
    if not files:
        print("No workflow configuration files discovered", file=sys.stderr)
        return 1

    success = True
    for path in files:
        try:
            cfg = _load_workflow(path)
            dag = build_dag_from_config(cfg)
            task_count = len(dag.tasks)
            print(f"✔ Imported {dag.dag_id} ({path}) with {task_count} materialised tasks")
        except ValidationError as exc:
            success = False
            _print_validation_error(path, exc)
        except Exception as exc:
            success = False
            print(f"Error importing {path}: {exc}", file=sys.stderr)
    return 0 if success else 1


def cmd_versions(args: argparse.Namespace) -> int:
    registry = load_registry(_registry_path(args.registry))
    data = registry.list_versions(args.dag_id)
    print(json.dumps(data, indent=2, sort_keys=True))
    return 0


def cmd_promote(args: argparse.Namespace) -> int:
    if not args.dag_id or not args.version:
        print("--dag-id and --version are required", file=sys.stderr)
        return 1

    cfg: Optional[WorkflowConfig] = None
    config_path: Path | None = Path(args.config) if args.config else None
    if config_path:
        try:
            cfg = _load_workflow(config_path)
        except ValidationError as exc:
            _print_validation_error(config_path, exc)
            return 1
        except WorkflowCLIError as exc:
            print(f"Error reading {config_path}: {exc}", file=sys.stderr)
            return 1

    registry = load_registry(_registry_path(args.registry))
    metadata = _parse_metadata(args.metadata)
    user = args.user or os.getenv("USER")
    event = registry.promote(
        args.dag_id,
        args.version,
        user=user,
        git_sha=args.git_sha,
        config_path=str(config_path) if config_path else None,
        notes=args.notes,
        metadata=metadata,
    )
    print(
        f"Promoted {args.dag_id} to version {event.version}"
        + (f" (previously {event.previous_version})" if event.previous_version else "")
    )

    if args.render:
        if not cfg or not config_path:
            print("Render requested but --config was not provided", file=sys.stderr)
            return 1
        output_dir = Path(args.render_output or DEFAULT_RENDER_DIR)
        output_path = output_dir / f"{cfg.dag_id}.py"
        _render_module(cfg, config_path, output_path)
        print(f"Rendered {cfg.dag_id} version {event.version} to {output_path}")
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    if not args.dag_id or not args.version:
        print("--dag-id and --version are required", file=sys.stderr)
        return 1

    registry = load_registry(_registry_path(args.registry))
    metadata = _parse_metadata(args.metadata)
    user = args.user or os.getenv("USER")
    try:
        event = registry.rollback(
            args.dag_id,
            args.version,
            user=user,
            notes=args.notes,
            metadata=metadata,
        )
    except ValueError as exc:
        print(f"Rollback failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Rolled back {args.dag_id} to version {event.version}"
        + (f" (previously {event.previous_version})" if event.previous_version else "")
    )
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    if not args.config:
        print("--config is required", file=sys.stderr)
        return 1

    config_path = Path(args.config)
    try:
        cfg = _load_workflow(config_path)
    except ValidationError as exc:
        _print_validation_error(config_path, exc)
        return 1
    except WorkflowCLIError as exc:
        print(f"Error reading {config_path}: {exc}", file=sys.stderr)
        return 1

    dag = build_dag_from_config(cfg)
    tasks_summary = []
    for task in sorted(dag.tasks, key=lambda t: t.task_id):
        tasks_summary.append(
            {
                "task_id": task.task_id,
                "trigger_rule": getattr(task, "trigger_rule", None),
                "upstream": sorted(t.task_id for t in task.upstream_list),
                "downstream": sorted(t.task_id for t in task.downstream_list),
            }
        )

    schedule = getattr(dag, "schedule_interval", None)
    if schedule is None:
        schedule = getattr(dag, "schedule", None)
    if isinstance(schedule, (list, tuple)):
        schedule_repr = [str(item) for item in schedule]
    else:
        schedule_repr = str(schedule)

    result = {
        "dag_id": dag.dag_id,
        "schedule": schedule_repr,
        "tags": sorted(dag.tags or []),
        "active_version": getattr(dag, "params", {}).get("active_version"),
        "tasks": tasks_summary,
    }
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_integrations(args: argparse.Namespace) -> int:
    if args.marketplace:
        entries = integration_manager.get_marketplace_entries(category=args.category)
        payload = [
            {
                "entry_id": entry.entry_id,
                "name": entry.name,
                "category": entry.category,
                "vendor": entry.vendor,
                "version": entry.version,
                "downloads": entry.downloads,
            }
            for entry in entries
        ]
    elif args.installed:
        installations = integration_manager.get_installed_integrations()
        payload = installations
    else:
        payload = integration_manager.get_integration_analytics()
    print(json.dumps(payload, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="aurum-workflow",
        description="Workflow orchestration helper commands",
    )
    sub = parser.add_subparsers(dest="command")

    validate_parser = sub.add_parser("validate", help="Validate workflow configuration files")
    validate_parser.add_argument("paths", nargs="*", help="Config files or directories (defaults to config/workflows)")
    validate_parser.set_defaults(func=cmd_validate)

    render_parser = sub.add_parser("render", help="Render a config into a static DAG module")
    render_parser.add_argument("--config", help="Path to a single workflow config file")
    render_parser.add_argument("--output", help="Directory to emit the rendered DAG (defaults to airflow/dags/generated)")
    render_parser.add_argument("--stdout", action="store_true", help="Print the validated config JSON to stdout instead of writing a module")
    render_parser.set_defaults(func=cmd_render)

    dry_run_parser = sub.add_parser("dry-run", help="Build DAG objects in-memory to ensure import succeeds")
    dry_run_parser.add_argument("paths", nargs="*", help="Config files or directories (defaults to config/workflows)")
    dry_run_parser.set_defaults(func=cmd_dry_run)

    versions_parser = sub.add_parser("versions", help="List workflow versions tracked in the registry")
    versions_parser.add_argument("--dag-id", help="Optional DAG identifier to filter results")
    versions_parser.add_argument("--registry", help="Path to the registry JSON file")
    versions_parser.set_defaults(func=cmd_versions)

    promote_parser = sub.add_parser("promote", help="Promote a workflow config version to active")
    promote_parser.add_argument("--dag-id", required=True, help="DAG identifier to promote")
    promote_parser.add_argument("--version", required=True, help="Version string (e.g. 2024.05.1)")
    promote_parser.add_argument("--config", help="Path to the workflow config file for validation/rendering")
    promote_parser.add_argument("--registry", help="Path to the registry JSON file")
    promote_parser.add_argument("--git-sha", help="Git SHA recorded with the promotion")
    promote_parser.add_argument("--user", help="User or service performing the promotion")
    promote_parser.add_argument("--notes", help="Free-form notes added to the audit trail")
    promote_parser.add_argument(
        "--metadata",
        action="append",
        help="Additional metadata entries in key=value form (repeatable)",
    )
    promote_parser.add_argument("--render", action="store_true", help="Render a static DAG module after promotion")
    promote_parser.add_argument("--render-output", help="Directory for rendered DAG modules")
    promote_parser.set_defaults(func=cmd_promote)

    rollback_parser = sub.add_parser("rollback", help="Rollback a workflow to a previous version")
    rollback_parser.add_argument("--dag-id", required=True, help="DAG identifier to rollback")
    rollback_parser.add_argument("--version", required=True, help="Target version to activate")
    rollback_parser.add_argument("--registry", help="Path to the registry JSON file")
    rollback_parser.add_argument("--user", help="User or service initiating the rollback")
    rollback_parser.add_argument("--notes", help="Free-form notes added to the audit trail")
    rollback_parser.add_argument(
        "--metadata",
        action="append",
        help="Additional metadata entries in key=value form (repeatable)",
    )
    rollback_parser.set_defaults(func=cmd_rollback)

    inspect_parser = sub.add_parser("inspect", help="Inspect a workflow config and output task topology")
    inspect_parser.add_argument("--config", required=True, help="Path to the workflow config file")
    inspect_parser.set_defaults(func=cmd_inspect)

    integrations_parser = sub.add_parser("integrations", help="Query integration marketplace and installations")
    integrations_parser.add_argument("--marketplace", action="store_true", help="List marketplace integrations")
    integrations_parser.add_argument("--installed", action="store_true", help="List installed integrations")
    integrations_parser.add_argument("--category", help="Filter marketplace entries by category")
    integrations_parser.set_defaults(func=cmd_integrations)

    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
