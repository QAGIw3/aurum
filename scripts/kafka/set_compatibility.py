#!/usr/bin/env python
"""Set Schema Registry compatibility for a batch of subjects."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable

from aurum.compat import requests


DEFAULT_SUBJECT_FILE = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "subjects.json"


def load_subjects(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Subject file must map subjects to schema paths")
    return list(data.keys())


def set_compatibility(
    registry_url: str,
    subject: str,
    level: str,
    *,
    requests_module = requests,
) -> None:
    url = f"{registry_url.rstrip('/')}/config/{subject}"
    response = requests_module.put(url, json={"compatibility": level}, timeout=10)
    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Failed to set compatibility for {subject}: {response.status_code} {response.text}"
        )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Set compatibility for Schema Registry subjects")
    parser.add_argument(
        "--schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Schema Registry base URL",
    )
    parser.add_argument(
        "--subjects-file",
        type=Path,
        default=DEFAULT_SUBJECT_FILE,
        help="Path to JSON file listing subjects",
    )
    parser.add_argument(
        "--level",
        default="BACKWARD",
        help="Compatibility level to apply (default BACKWARD)",
    )
    parser.add_argument(
        "--subject",
        action="append",
        help="Specific subject(s) to update; defaults to all in the file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without contacting the Schema Registry",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    subjects = load_subjects(args.subjects_file)
    if args.subject:
        missing = sorted(set(args.subject) - set(subjects))
        if missing:
            raise RuntimeError(f"Subjects not found in mapping: {', '.join(missing)}")
        subjects = args.subject

    if args.dry_run:
        for subject in subjects:
            print(f"[dry-run] would set compatibility {args.level} for {subject}")
        return 0

    for subject in subjects:
        set_compatibility(args.schema_registry_url, subject, args.level)
        print(f"Set compatibility for {subject} -> {args.level}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=os.sys.stderr)
        raise SystemExit(1)
