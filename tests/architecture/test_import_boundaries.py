from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Iterable, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _iter_python_files(root: Path) -> Iterable[Path]:
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".py"):
                yield Path(dirpath) / filename


def _collect_imports(py_file: Path) -> List[Tuple[str, int, str]]:
    """Return list of (module, lineno, import_text)."""
    text = py_file.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        # Skip files with syntax errors in this scan
        return []

    imports: List[Tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod = alias.name
                imports.append((mod, node.lineno, f"import {alias.name}"))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            imports.append((mod, node.lineno, f"from {mod} import ..."))
    return imports


def _format_violations(violations: List[Tuple[Path, int, str, str]]) -> str:
    lines = ["\nImport boundary violations detected:"]
    for file_path, lineno, module, import_text in violations:
        rel = file_path.relative_to(PROJECT_ROOT)
        lines.append(f"- {rel}:{lineno} -> '{module}' via `{import_text}`")
    return "\n".join(lines)


def test_libs_packages_do_not_import_apps() -> None:
    libs_root = PROJECT_ROOT / "libs"
    violations: List[Tuple[Path, int, str, str]] = []

    for py_file in _iter_python_files(libs_root):
        for module, lineno, import_text in _collect_imports(py_file):
            if module == "apps" or module.startswith("apps."):
                violations.append((py_file, lineno, module, import_text))

    assert not violations, _format_violations(violations)


def test_storage_does_not_import_services() -> None:
    storage_root = PROJECT_ROOT / "libs" / "storage"
    violations: List[Tuple[Path, int, str, str]] = []

    for py_file in _iter_python_files(storage_root):
        for module, lineno, import_text in _collect_imports(py_file):
            if module == "libs.services" or module.startswith("libs.services."):
                violations.append((py_file, lineno, module, import_text))

    assert not violations, _format_violations(violations)


