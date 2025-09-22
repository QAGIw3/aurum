from __future__ import annotations

"""Render SeaTunnel job templates from environment variables with validation.

The renderer resolves placeholders like ``${VAR}`` from the process env (with
support for legacy aliases), validates key settings for correctness, and
writes a fully materialized configuration file. Designed for use by CI/CD,
Airflow tasks, and ad‑hoc operations.
"""

import argparse
import os
import re
import sys
from pathlib import Path
from string import Template
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple
from urllib.parse import urlparse

ENV_ALIAS_MAP: Dict[str, Tuple[str, ...]] = {
    "AURUM_KAFKA_BOOTSTRAP_SERVERS": ("KAFKA_BOOTSTRAP_SERVERS",),
    "AURUM_SCHEMA_REGISTRY_URL": ("SCHEMA_REGISTRY_URL",),
    "AURUM_TIMESCALE_JDBC_URL": ("TIMESCALE_JDBC_URL",),
    "AURUM_TIMESCALE_USER": ("TIMESCALE_USER",),
    "AURUM_TIMESCALE_PASSWORD": ("TIMESCALE_PASSWORD",),
}

_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


class RendererError(RuntimeError):
    """Raised when rendering fails due to missing or invalid configuration."""


def _resolve_env_value(name: str, env: Mapping[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """Return the resolved value and the environment variable that supplied it."""

    if name in env:
        return env[name], name

    for alias in ENV_ALIAS_MAP.get(name, ()):  # legacy fallbacks
        if alias in env:
            return env[alias], alias

    return None, None


def _validate_env_value(name: str, value: str) -> None:
    if not value:
        raise RendererError(f"Environment variable {name} is empty")

    if name == "AURUM_SCHEMA_REGISTRY_URL":
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise RendererError(
                "AURUM_SCHEMA_REGISTRY_URL must be an http(s) URL with a hostname"
            )
    elif name == "AURUM_KAFKA_BOOTSTRAP_SERVERS":
        entries = [segment.strip() for segment in value.split(",") if segment.strip()]
        if not entries:
            raise RendererError(
                "AURUM_KAFKA_BOOTSTRAP_SERVERS must contain at least one host:port"
            )
        for entry in entries:
            if ":" not in entry:
                raise RendererError(
                    "AURUM_KAFKA_BOOTSTRAP_SERVERS entries must include :port; "
                    f"'{entry}' is invalid"
                )
    elif name == "AURUM_TIMESCALE_JDBC_URL":
        if not value.startswith("jdbc:postgresql://"):
            raise RendererError(
                "AURUM_TIMESCALE_JDBC_URL must start with 'jdbc:postgresql://'"
            )
    elif name == "AURUM_TIMESCALE_USER":
        if " " in value:
            raise RendererError("AURUM_TIMESCALE_USER must not contain spaces")
    elif name == "AURUM_TIMESCALE_PASSWORD":
        if value.strip() == "":
            raise RendererError("AURUM_TIMESCALE_PASSWORD must not be blank")


def _collect_placeholders(template: str) -> Set[str]:
    return set(_PLACEHOLDER_PATTERN.findall(template))


def render_template(
    *,
    job: str,
    template_path: Path,
    output_path: Path,
    required_vars: Iterable[str],
    env: Optional[Mapping[str, str]] = None,
) -> None:
    env = dict(env or os.environ)

    raw_template = template_path.read_text(encoding="utf-8")
    placeholders = _collect_placeholders(raw_template)

    required_set = set(required_vars)
    missing_required: List[str] = []
    missing_optional: List[str] = []
    context: Dict[str, str] = {}

    for placeholder in sorted(placeholders):
        value, source_name = _resolve_env_value(placeholder, env)
        if value is None or value == "":
            if placeholder in required_set:
                missing_required.append(placeholder)
            else:
                missing_optional.append(placeholder)
            continue

        if placeholder in ENV_ALIAS_MAP:
            _validate_env_value(placeholder, value)

        context[placeholder] = value
        for alias in ENV_ALIAS_MAP.get(placeholder, ()):  # keep for compatibility
            context.setdefault(alias, value)

        if source_name and source_name != placeholder:
            sys.stderr.write(
                f"[seatunnel] Using legacy environment variable {source_name} "
                f"for {placeholder}\n"
            )

    if missing_required:
        raise RendererError(
            "Missing required environment variables for job "
            f"'{job}': {', '.join(missing_required)}"
        )

    if missing_optional:
        raise RendererError(
            f"Template for job '{job}' references variables that were not provided: "
            f"{', '.join(missing_optional)}. Export them or provide defaults."
        )

    try:
        rendered = Template(raw_template).substitute(context)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise RendererError(
            f"Failed to render job '{job}'; missing value for {missing_key}"
        ) from None

    # Double-check no unresolved placeholders remain (defensive safeguard).
    unresolved = _collect_placeholders(rendered)
    if unresolved:
        raise RendererError(
            f"Rendered config for job '{job}' still contains placeholders: "
            f"{', '.join(sorted(unresolved))}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render SeaTunnel job templates.")
    parser.add_argument("--job", required=True, help="Job name (used for error messaging)")
    parser.add_argument("--template", required=True, type=Path, help="Template path")
    parser.add_argument("--output", required=True, type=Path, help="Output path")
    parser.add_argument(
        "--required",
        action="append",
        default=[],
        help="Environment variables that must be present",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    try:
        render_template(
            job=args.job,
            template_path=args.template,
            output_path=args.output,
            required_vars=args.required,
        )
    except RendererError as exc:
        sys.stderr.write(f"[seatunnel] {exc}\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
