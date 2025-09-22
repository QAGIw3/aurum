#!/usr/bin/env python3
"""Generate Markdown documentation from dbt exposures with publish paths."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[2]
DBT_MODELS_DIR = ROOT / "dbt" / "models"
EXPOSURES_PATHS = [ROOT / "dbt" / "models" / "marts" / "exposures.yml"]


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _collect_model_metadata() -> Dict[str, dict]:
    metadata: Dict[str, dict] = {}
    for schema_path in DBT_MODELS_DIR.rglob("schema.yml"):
        payload = _load_yaml(schema_path)
        for model in payload.get("models", []) or []:
            metadata[model["name"]] = {
                "description": model.get("description"),
                "columns": model.get("columns", []),
                "tests": model.get("tests", []),
            }
    return metadata


def _collect_source_metadata() -> Dict[Tuple[str, str], dict]:
    metadata: Dict[Tuple[str, str], dict] = {}
    sources_path = ROOT / "dbt" / "models" / "sources" / "schema.yml"
    if not sources_path.exists():
        return metadata
    payload = _load_yaml(sources_path)
    for source in payload.get("sources", []) or []:
        source_name = source.get("name")
        for table in source.get("tables", []) or []:
            metadata[(source_name, table["name"])] = {
                "description": table.get("description"),
                "columns": table.get("columns", []),
            }
    return metadata


def _titleize(name: str) -> str:
    return name.replace("_", " ").title()


def _parse_dependency(expr: str) -> Tuple[str, Tuple[str, ...]]:
    if expr.startswith("ref("):
        match = re.findall(r"'([^']+)'", expr)
        if match:
            return "model", (match[0],)
    if expr.startswith("source("):
        match = re.findall(r"'([^']+)'", expr)
        if len(match) == 2:
            return "source", (match[0], match[1])
    return "other", (expr,)


def _render_columns(columns: List[dict]) -> List[str]:
    if not columns:
        return []
    lines = ["| Column | Description | Tests |", "| --- | --- | --- |"]
    for column in columns:
        name = f"`{column.get('name')}`"
        description = column.get("description") or ""
        tests = column.get("tests") or []
        if tests:
            rendered_tests = ", ".join(
                test if isinstance(test, str) else next(iter(test.keys()))
                for test in tests
            )
        else:
            rendered_tests = ""
        lines.append(f"| {name} | {description} | {rendered_tests} |")
    lines.append("")
    return lines


def _render_dependency_sections(deps: List[Tuple[str, Tuple[str, ...]]],
                                 model_meta: Dict[str, dict],
                                 source_meta: Dict[Tuple[str, str], dict]) -> List[str]:
    lines: List[str] = []
    for dep_type, parts in deps:
        if dep_type == "model":
            model_name = parts[0]
            meta = model_meta.get(model_name, {})
            lines.append(f"## {model_name}")
            if meta.get("description"):
                lines.append(meta["description"])
                lines.append("")
            lines.extend(_render_columns(meta.get("columns", [])))
        elif dep_type == "source":
            key = (parts[0], parts[1])
            meta = source_meta.get(key, {})
            lines.append(f"## Source: {parts[0]}.{parts[1]}")
            if meta.get("description"):
                lines.append(meta["description"])
                lines.append("")
            lines.extend(_render_columns(meta.get("columns", [])))
        else:
            lines.append(f"## {parts[0]}")
            lines.append("")
    return lines


def generate_docs() -> None:
    model_meta = _collect_model_metadata()
    source_meta = _collect_source_metadata()

    exposures_by_path: Dict[Path, List[dict]] = {}
    for exposures_file in EXPOSURES_PATHS:
        payload = _load_yaml(exposures_file)
        for exposure in payload.get("exposures", []) or []:
            publish_path = exposure.get("meta", {}).get("publish_path")
            if not publish_path:
                continue
            target_path = ROOT / publish_path
            exposures_by_path.setdefault(target_path, []).append(exposure)

    for path, exposures in exposures_by_path.items():
        lines: List[str] = []
        if len(exposures) == 1:
            exposure = exposures[0]
            title = _titleize(exposure["name"])
            lines.append(f"# {title}")
            if exposure.get("description"):
                lines.append(exposure["description"])
                lines.append("")
            owner = exposure.get("owner", {})
            if owner:
                owner_line = owner.get("name", "")
                if owner.get("email"):
                    owner_line += f" ({owner['email']})"
                if owner_line:
                    lines.append(f"**Owner:** {owner_line}")
                    lines.append("")
            deps_exprs = exposure.get("depends_on", [])
            deps = [_parse_dependency(expr) for expr in deps_exprs]
            lines.extend(_render_dependency_sections(deps, model_meta, source_meta))
        else:
            title = _titleize(path.stem)
            lines.append(f"# {title}")
            for exposure in exposures:
                lines.append("")
                lines.append(f"## {_titleize(exposure['name'])}")
                if exposure.get("description"):
                    lines.append(exposure["description"])
                    lines.append("")
                owner = exposure.get("owner", {})
                if owner:
                    owner_line = owner.get("name", "")
                    if owner.get("email"):
                        owner_line += f" ({owner['email']})"
                    if owner_line:
                        lines.append(f"**Owner:** {owner_line}")
                        lines.append("")
                deps_exprs = exposure.get("depends_on", [])
                deps = [_parse_dependency(expr) for expr in deps_exprs]
                lines.extend(_render_dependency_sections(deps, model_meta, source_meta))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        print(f"Wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--exposures",
        nargs="*",
        default=None,
        help="Optional list of exposure YAML files to include",
    )
    args = parser.parse_args()

    global EXPOSURES_PATHS
    if args.exposures:
        EXPOSURES_PATHS = [Path(p).resolve() for p in args.exposures]

    generate_docs()


if __name__ == "__main__":
    main()
