#!/usr/bin/env python3
"""Run the unknown-units CLI and optionally open a GitHub issue with suggestions."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

try:
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    requests = None


def _run_cli(paths: Iterable[str]) -> list[dict[str, Any]]:
    cmd = [sys.executable or "python", "-m", "aurum.parsers.unknown_units_cli", *paths, "--json"]
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode not in {0, 1}:
        sys.stdout.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"Failed to execute {' '.join(cmd)} (exit code {proc.returncode})")
    try:
        payload = proc.stdout.strip() or "[]"
        return json.loads(payload)
    except json.JSONDecodeError as exc:  # pragma: no cover - unexpected CLI output
        raise RuntimeError(f"Unable to parse aurum-unknown-units output: {exc}") from exc


def _markdown_report(entries: list[dict[str, Any]]) -> str:
    lines = ["# Unknown unit mappings detected", "", "The following source rows do not have unit mappings:", ""]
    lines.append("| units_raw | rows | sample_iso | sample_market | sample_location |")
    lines.append("| --- | --- | --- | --- | --- |")
    for entry in entries:
        units_raw = entry.get("units_raw") or "<blank>"
        rows = entry.get("rows", 0)
        sample_iso = ", ".join(entry.get("sample_iso") or []) or "-"
        sample_market = ", ".join(entry.get("sample_market") or []) or "-"
        sample_location = ", ".join(entry.get("sample_location") or []) or "-"
        lines.append(f"| {units_raw} | {rows} | {sample_iso} | {sample_market} | {sample_location} |")
    lines.append("")
    lines.append("Consider updating `config/units_overrides.yaml` or the vendor parsers with the mappings above.")
    return "\n".join(lines)


def _post_github_issue(report: str) -> None:
    if requests is None:
        print("requests not installed; skipping GitHub issue creation")
        return

    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        print("GITHUB_TOKEN or GITHUB_REPOSITORY not set; skipping GitHub issue creation")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    issues_url = f"https://api.github.com/repos/{repo}/issues"
    params = {"state": "open", "labels": "unknown-units"}
    try:
        response = requests.get(issues_url, headers=headers, params=params, timeout=5)
        response.raise_for_status()
        open_issues = response.json()
    except Exception as exc:  # pragma: no cover - network errors
        print(f"Failed to query existing issues: {exc}")
        open_issues = []

    title = "Unknown unit mappings require attention"
    if open_issues:
        issue = open_issues[0]
        issue_url = issue.get("url")
        if issue_url:
            try:
                print(f"Updating existing issue #{issue.get('number')}")
                requests.patch(issue_url, headers=headers, json={"body": report}, timeout=5).raise_for_status()
                return
            except Exception as exc:  # pragma: no cover - update failure
                print(f"Failed to update existing issue: {exc}")

    payload = {"title": title, "body": report, "labels": ["unknown-units"]}
    try:
        response = requests.post(issues_url, headers=headers, json=payload, timeout=5)
        response.raise_for_status()
        created = response.json()
        print(f"Created GitHub issue #{created.get('number')}")
    except Exception as exc:  # pragma: no cover - creation failure
        print(f"Failed to create GitHub issue: {exc}")


def main(argv: list[str]) -> int:
    paths = argv or ["files"]
    entries = _run_cli(paths)
    if not entries:
        print("No unknown units detected")
        return 0

    report = _markdown_report(entries)
    output_path = Path(os.getenv("UNKNOWN_UNITS_REPORT", "artifacts/unknown_units_report.md"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"Unknown unit report written to {output_path}")

    if os.getenv("UNKNOWN_UNITS_AUTOREPORT", "1") != "0":
        _post_github_issue(report)

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
