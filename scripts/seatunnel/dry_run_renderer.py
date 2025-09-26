#!/usr/bin/env python3
"""Lightweight CLI for inspecting and dry-running SeaTunnel templates.

Features:
    - List available templates under one or more directories
    - Show the required environment variables/placeholders for a template
    - Render a template to stdout without touching the filesystem

By default missing environment variables are rendered as
``__MISSING_<VARIABLE>__`` so operators can see gaps. Pass
``--fail-on-missing`` to enforce completeness instead.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from aurum.seatunnel.renderer import _PLACEHOLDER_PATTERN, _collect_placeholders

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEMPLATE_DIRS = [REPO_ROOT / "seatunnel" / "jobs" / "templates"]
TEMPLATE_SUFFIX = ".conf.tmpl"


class TemplateNotFoundError(FileNotFoundError):
    """Raised when a requested template cannot be located."""


class MissingVariablesError(RuntimeError):
    """Raised when rendering fails due to missing environment variables."""


def discover_templates(template_dirs: Sequence[Path]) -> Dict[str, Path]:
    """Return a mapping of template names to their absolute paths."""

    discovered: Dict[str, Path] = {}
    for directory in template_dirs:
        if not directory.exists():
            raise FileNotFoundError(f"Template directory does not exist: {directory}")

        for path in directory.rglob(f"*{TEMPLATE_SUFFIX}"):
            relative_name = path.relative_to(directory).as_posix()
            template_name = relative_name[: -len(TEMPLATE_SUFFIX)]
            discovered.setdefault(template_name, path)

    return discovered


def load_template(path: Path) -> Tuple[str, Set[str], Set[str]]:
    """Load template content and collect placeholders/defaults."""

    text = path.read_text(encoding="utf-8")
    placeholders = _collect_placeholders(text)
    placeholders_with_defaults: Set[str] = set()
    for match in _PLACEHOLDER_PATTERN.finditer(text):
        name = match.group(1)
        if match.group(2) is not None:
            placeholders_with_defaults.add(name)
    return text, placeholders, placeholders_with_defaults


def render_template_text(
    template_text: str,
    env: Dict[str, str],
    *,
    fail_on_missing: bool,
) -> Tuple[str, List[str]]:
    """Render template text using provided environment values."""

    missing: List[str] = []

    def _substitute(match):
        name = match.group(1)
        default = match.group(2)
        value = env.get(name)
        if value:
            return value
        if default is not None:
            return default
        missing.append(name)
        return f"__MISSING_{name}__"

    rendered = _PLACEHOLDER_PATTERN.sub(_substitute, template_text)

    if fail_on_missing and missing:
        raise MissingVariablesError(
            "Missing environment variables: " + ", ".join(sorted(set(missing)))
        )

    return rendered, sorted(set(missing))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template-dirs",
        metavar="DIR",
        nargs="+",
        type=Path,
        default=DEFAULT_TEMPLATE_DIRS,
        help="One or more directories containing *.conf.tmpl templates.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list",
        action="store_true",
        help="List available templates and exit.",
    )
    group.add_argument(
        "--show-vars",
        metavar="TEMPLATE",
        help="Show required placeholders for a template.",
    )
    group.add_argument(
        "--render",
        metavar="TEMPLATE",
        help="Render the given template to stdout.",
    )
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit non-zero if required placeholders are missing.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        template_dirs = [d.expanduser().resolve() for d in args.template_dirs]
        templates = discover_templates(template_dirs)

        if not templates:
            print("No templates found.", file=sys.stderr)
            return 1

        if args.list:
            for name in sorted(templates):
                print(name)
            return 0

        target = args.show_vars or args.render
        assert target is not None  # for type-checkers

        template_path = templates.get(target)
        if template_path is None:
            raise TemplateNotFoundError(f"Unknown template '{target}'")

        template_text, placeholders, placeholders_with_defaults = load_template(template_path)

        if args.show_vars:
            required = sorted(placeholders - placeholders_with_defaults)
            for placeholder in required:
                print(placeholder)
            return 0

        # args.render path
        env = {key: value for key, value in os.environ.items() if value}  # ignore empty
        rendered, missing = render_template_text(
            template_text,
            env,
            fail_on_missing=args.fail_on_missing,
        )
        sys.stdout.write(rendered)
        if missing:
            sys.stderr.write(
                "Warning: substituted placeholder values for "
                + ", ".join(missing)
                + "\n"
            )
        return 0

    except TemplateNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except MissingVariablesError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 4


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
