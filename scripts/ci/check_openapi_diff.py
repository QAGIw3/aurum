#!/usr/bin/env python3
"""Gate OpenAPI changes behind semantic versioning requirements.

This script compares the committed OpenAPI specification (typically generated
as ``aurum/docs/api/openapi-spec.yaml``) with the version on a base ref
(``origin/main`` by default). It enforces the following rules:

* Any change to the spec must increment ``info.version``.
* Breaking changes (removing paths/operations/responses/required params)
  require a **major** version bump.
* Additive changes (new paths/operations/responses) require at least a
  **minor** version bump.
* Other non-breaking tweaks (description/metadata/etc.) require at least a
  **patch** increment.

The script prints a summary of detected changes and exits with a non-zero code
if the semantic version bump does not meet the required threshold.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

try:
    import yaml
except Exception as exc:  # pragma: no cover - handled gracefully at runtime
    print(f"❌ Unable to import PyYAML: {exc}")
    sys.exit(1)

try:
    from packaging.version import Version
except Exception as exc:  # pragma: no cover - handled gracefully at runtime
    print(f"❌ Unable to import packaging.version: {exc}")
    sys.exit(1)


OPENAPI_PATH = Path("aurum/docs/api/openapi-spec.yaml")


class SpecLoadError(RuntimeError):
    """Raised when a specification cannot be loaded from git."""


@dataclass(frozen=True)
class ChangeSummary:
    """Summary of detected OpenAPI differences."""

    added_paths: List[str]
    removed_paths: List[str]
    added_operations: List[Tuple[str, str]]
    removed_operations: List[Tuple[str, str]]
    added_responses: List[Tuple[str, str, str]]
    removed_responses: List[Tuple[str, str, str]]
    added_required_params: List[Tuple[str, str, str]]
    removed_required_params: List[Tuple[str, str, str]]

    def is_empty(self) -> bool:
        return all(
            not getattr(self, field)
            for field in (
                "added_paths",
                "removed_paths",
                "added_operations",
                "removed_operations",
                "added_responses",
                "removed_responses",
                "added_required_params",
                "removed_required_params",
            )
        )

    def has_breaking_changes(self) -> bool:
        return any(
            getattr(self, field)
            for field in (
                "removed_paths",
                "removed_operations",
                "removed_responses",
                "added_required_params",
            )
        )

    def has_additive_changes(self) -> bool:
        return any(
            getattr(self, field)
            for field in (
                "added_paths",
                "added_operations",
                "added_responses",
            )
        )


def run_git_show(ref: str, path: Path) -> str:
    """Return the contents of ``path`` at ``ref`` using git show."""
    try:
        result = subprocess.run(
            ["git", "show", f"{ref}:{path.as_posix()}"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime error path
        raise SpecLoadError(
            f"Unable to load {path} from ref '{ref}': {exc.stderr.strip()}"
        ) from exc
    return result.stdout


def load_spec_from_ref(ref: str, path: Path) -> Dict[str, object]:
    """Load an OpenAPI spec from git at the given ref."""
    raw = run_git_show(ref, path)
    return yaml.safe_load(raw)


def load_current_spec(path: Path) -> Dict[str, object]:
    """Load the working-tree OpenAPI spec."""
    if not path.exists():
        raise SpecLoadError(f"Spec not found at {path}")
    return yaml.safe_load(path.read_text())


def normalize_http_methods(path_item: Mapping[str, object]) -> Dict[str, Mapping[str, object]]:
    """Return a mapping of lowercase HTTP methods to operation objects."""
    valid_methods = {"get", "put", "post", "delete", "patch", "options", "head"}
    return {
        method: operation
        for method, operation in path_item.items()
        if method.lower() in valid_methods and isinstance(operation, Mapping)
    }


def extract_required_parameters(operation: Mapping[str, object]) -> Dict[str, Mapping[str, object]]:
    """Return required parameters keyed by ``(in, name)``."""
    params = operation.get("parameters", [])
    result: Dict[str, Mapping[str, object]] = {}
    if not isinstance(params, list):
        return result

    for param in params:
        if not isinstance(param, Mapping):
            continue
        if not param.get("required"):
            continue
        name = str(param.get("name"))
        location = str(param.get("in", "query"))
        key = f"{location}:{name}"
        result[key] = param
    return result


def analyse_spec_changes(base: Dict[str, object], new: Dict[str, object]) -> ChangeSummary:
    """Compute a summary of changes between two OpenAPI specs."""
    base_paths = base.get("paths", {}) or {}
    new_paths = new.get("paths", {}) or {}

    base_path_set = set(base_paths.keys())
    new_path_set = set(new_paths.keys())

    added_paths = sorted(new_path_set - base_path_set)
    removed_paths = sorted(base_path_set - new_path_set)

    added_operations: List[Tuple[str, str]] = []
    removed_operations: List[Tuple[str, str]] = []
    added_responses: List[Tuple[str, str, str]] = []
    removed_responses: List[Tuple[str, str, str]] = []
    added_required_params: List[Tuple[str, str, str]] = []
    removed_required_params: List[Tuple[str, str, str]] = []

    for path in sorted(base_path_set & new_path_set):
        base_ops = normalize_http_methods(base_paths[path])
        new_ops = normalize_http_methods(new_paths[path])

        base_methods = set(base_ops.keys())
        new_methods = set(new_ops.keys())

        for method in sorted(new_methods - base_methods):
            added_operations.append((path, method))
        for method in sorted(base_methods - new_methods):
            removed_operations.append((path, method))

        for method in sorted(base_methods & new_methods):
            base_op = base_ops[method]
            new_op = new_ops[method]

            base_responses = set((base_op.get("responses") or {}).keys())
            new_responses_keys = set((new_op.get("responses") or {}).keys())

            for status in sorted(new_responses_keys - base_responses):
                added_responses.append((path, method, status))
            for status in sorted(base_responses - new_responses_keys):
                removed_responses.append((path, method, status))

            base_required = extract_required_parameters(base_op)
            new_required = extract_required_parameters(new_op)

            for key in sorted(new_required.keys() - base_required.keys()):
                location, name = key.split(":", 1)
                added_required_params.append((path, method, f"{location}:{name}"))
            for key in sorted(base_required.keys() - new_required.keys()):
                location, name = key.split(":", 1)
                removed_required_params.append((path, method, f"{location}:{name}"))

    return ChangeSummary(
        added_paths=added_paths,
        removed_paths=removed_paths,
        added_operations=added_operations,
        removed_operations=removed_operations,
        added_responses=added_responses,
        removed_responses=removed_responses,
        added_required_params=added_required_params,
        removed_required_params=removed_required_params,
    )


def determine_required_bump(summary: ChangeSummary, base: Dict[str, object], new: Dict[str, object]) -> Optional[str]:
    """Return the minimum required semantic version bump type."""
    if summary.is_empty():
        # Check for other spec differences such as descriptions/examples.
        # If the raw JSON differs, require at least a patch bump.
        if normalize_spec(base) != normalize_spec(new):
            return "patch"
        return None

    if summary.has_breaking_changes():
        return "major"

    if summary.has_additive_changes():
        return "minor"

    return "patch"


def normalize_spec(spec: Dict[str, object]) -> str:
    """Return a canonical JSON representation for broad equality checks."""
    return json.dumps(spec, sort_keys=True, separators=(",", ":"))


def describe_changes(summary: ChangeSummary) -> None:
    """Print a human-readable summary of detected changes."""
    if summary.is_empty():
        print("No structural path/operation changes detected.")
        return

    if summary.added_paths:
        print("➕ Added paths:")
        for path in summary.added_paths:
            print(f"  - {path}")

    if summary.removed_paths:
        print("➖ Removed paths:")
        for path in summary.removed_paths:
            print(f"  - {path}")

    if summary.added_operations:
        print("➕ Added operations:")
        for path, method in summary.added_operations:
            print(f"  - {method.upper()} {path}")

    if summary.removed_operations:
        print("➖ Removed operations:")
        for path, method in summary.removed_operations:
            print(f"  - {method.upper()} {path}")

    if summary.added_responses:
        print("➕ Added responses:")
        for path, method, status in summary.added_responses:
            print(f"  - {method.upper()} {path} → {status}")

    if summary.removed_responses:
        print("➖ Removed responses:")
        for path, method, status in summary.removed_responses:
            print(f"  - {method.upper()} {path} → {status}")

    if summary.added_required_params:
        print("➕ Added required parameters:")
        for path, method, param in summary.added_required_params:
            print(f"  - {method.upper()} {path} → {param}")

    if summary.removed_required_params:
        print("➖ Removed required parameters:")
        for path, method, param in summary.removed_required_params:
            print(f"  - {method.upper()} {path} → {param}")


def bump_type(base_version: Version, new_version: Version) -> Optional[str]:
    """Return the semantic bump type between two versions."""
    if new_version <= base_version:
        return None
    if new_version.major > base_version.major:
        return "major"
    if new_version.minor > base_version.minor:
        return "minor"
    if new_version.micro > base_version.micro:
        return "patch"
    # Allow pre-release/build metadata to count as patch-level bump
    if (new_version.micro == base_version.micro and
            (new_version.pre or new_version.post or new_version.dev or new_version.local) and
            not (base_version.pre or base_version.post or base_version.dev or base_version.local)):
        return "patch"
    return None


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enforce semantic versioning for OpenAPI changes")
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="Git ref to compare against (default: origin/main)",
    )
    parser.add_argument(
        "--spec-path",
        default=str(OPENAPI_PATH),
        help="Path to the generated OpenAPI spec (default: aurum/docs/api/openapi-spec.yaml)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    spec_path = Path(args.spec_path)

    try:
        base_spec = load_spec_from_ref(args.base_ref, spec_path)
    except SpecLoadError as exc:
        print(f"⚠️  {exc}")
        print("Skipping OpenAPI diff gate because base specification could not be loaded.")
        return 0

    try:
        new_spec = load_current_spec(spec_path)
    except SpecLoadError as exc:
        print(f"❌ {exc}")
        return 1

    base_version = Version(str(base_spec.get("info", {}).get("version", "0.0.0")))
    new_version = Version(str(new_spec.get("info", {}).get("version", "0.0.0")))

    summary = analyse_spec_changes(base_spec, new_spec)
    describe_changes(summary)

    required_bump = determine_required_bump(summary, base_spec, new_spec)

    if required_bump is None:
        print("✅ No semantic version change required.")
        if new_version != base_version:
            print(f"ℹ️  Version changed from {base_version} to {new_version} (no diff detected).")
        return 0

    actual_bump = bump_type(base_version, new_version)

    if actual_bump is None:
        print("❌ Specification changed but version was not incremented.")
        print(f"   base={base_version}, new={new_version}, required bump={required_bump}")
        return 2

    bump_order = {"patch": 0, "minor": 1, "major": 2}

    if bump_order[actual_bump] < bump_order[required_bump]:
        print("❌ Semantic version bump is insufficient for the type of change detected.")
        print(f"   Detected change requires >= {required_bump} bump but version only bumped {actual_bump}.")
        print(f"   base={base_version}, new={new_version}")
        return 3

    print(f"✅ OpenAPI changes comply with semantic versioning ({actual_bump} bump).")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
