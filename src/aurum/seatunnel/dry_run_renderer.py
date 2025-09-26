"""Dry-run renderer for SeaTunnel job templates.

The production implementation shell-executed SeaTunnel with environment
configuration.  For tests we provide a deterministic renderer that expands
``${VAR}`` style placeholders using the current environment, reports missing
variables, and produces a human readable report.
"""

from __future__ import annotations

import json
import os
import re
import textwrap
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from string import Template
from typing import Dict, Iterable, List, Sequence, Set

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
HASH_FILE = Path.home() / ".aurum" / "seatunnel_template_hashes.json"

_PLACEHOLDER_PATTERN = re.compile(r"\$\{(?P<name>[A-Z0-9_]+)\}")


class DryRunError(RuntimeError):
    """Raised when dry-run rendering cannot proceed."""


def _collect_placeholders(template_text: str) -> Set[str]:
    """Return the placeholder identifiers found in ``template_text``."""

    return {match.group("name") for match in _PLACEHOLDER_PATTERN.finditer(template_text)}


@dataclass
class DryRunResult:
    template: str
    rendered: bool
    placeholders: Sequence[str]
    content: str | None = None
    content_length: int | None = None
    line_count: int | None = None
    missing_variables: Sequence[str] | None = None
    error: str | None = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "template": self.template,
            "rendered": self.rendered,
            "placeholders": list(self.placeholders),
        }
        if self.content is not None:
            payload["content"] = self.content
        if self.content_length is not None:
            payload["content_length"] = self.content_length
        if self.line_count is not None:
            payload["line_count"] = self.line_count
        if self.missing_variables:
            payload["missing_variables"] = list(self.missing_variables)
        if self.error:
            payload["error"] = self.error
        return payload


class DryRunRenderer:
    """Render SeaTunnel templates without executing the engine."""

    _SAMPLE_ENV: Dict[str, Dict[str, str]] = {
        "eia_series_to_kafka": {
            "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "EIA_API_KEY": "demo-eia-key",
            "EIA_SERIES_PATH": "series/DOE/PET/MCRFPUS1/M",
            "OUTPUT_TOPIC": "aurum.external.eia",
        },
        "fred_series_to_kafka": {
            "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "FRED_API_KEY": "demo-fred-key",
            "FRED_SERIES_ID": "GDP",
            "OUTPUT_TOPIC": "aurum.external.fred",
        },
        "cpi_series_to_kafka": {
            "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "CPI_SERIES_ID": "CUSR0000SA0",
            "OUTPUT_TOPIC": "aurum.external.cpi",
        },
    }

    def __init__(self) -> None:
        self.template_hashes: Dict[str, str] = {}
        self._load_hashes()

    # ------------------------------------------------------------------ hashes
    def _hash_file(self, path: Path) -> str:
        digest = sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _get_template_hash(self, path: Path) -> str:
        digest = self._hash_file(path)
        self.template_hashes[path.stem] = digest
        self._save_hashes()
        return digest

    def _load_hashes(self) -> None:
        if not HASH_FILE.exists():
            self.template_hashes = {}
            return
        try:
            with HASH_FILE.open(encoding="utf-8") as handle:
                self.template_hashes = json.load(handle)
        except Exception:
            self.template_hashes = {}

    def _save_hashes(self) -> None:
        HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
        with HASH_FILE.open("w", encoding="utf-8") as handle:
            json.dump(self.template_hashes, handle, indent=2, sort_keys=True)

    # -------------------------------------------------------------- env helpers
    def _get_sample_env(self, template_name: str) -> Dict[str, str]:
        return self._SAMPLE_ENV.get(template_name, {}).copy()

    def _get_required_vars(self, template_name: str) -> List[str]:
        template_path = TEMPLATE_DIR / f"{template_name}.conf.tmpl"
        if not template_path.exists():
            return []
        placeholders = _collect_placeholders(template_path.read_text(encoding="utf-8"))
        return sorted(placeholders)

    # -------------------------------------------------------------- rendering
    def render_template_dry_run(self, template_path: Path) -> Dict[str, object]:
        if not template_path.exists():
            raise DryRunError(f"Template not found: {template_path}")

        template_text = template_path.read_text(encoding="utf-8")
        placeholders = sorted(_collect_placeholders(template_text))
        missing: List[str] = []
        env_values: Dict[str, str] = {}
        for name in placeholders:
            value = os.getenv(name)
            if value is None:
                missing.append(name)
            else:
                env_values[name] = value

        result = DryRunResult(
            template=template_path.stem,
            rendered=not missing,
            placeholders=placeholders,
            missing_variables=missing,
        )

        if not missing:
            rendered_content = Template(template_text).safe_substitute(env_values)
            result.content = rendered_content
            result.content_length = len(rendered_content)
            result.line_count = rendered_content.count("\n") + 1 if rendered_content else 0
            self._get_template_hash(template_path)
        else:
            result.error = "Missing required variables: " + ", ".join(sorted(missing))

        return result.to_dict()

    def render_all_templates(self) -> List[Dict[str, object]]:
        if not TEMPLATE_DIR.exists():
            raise DryRunError("Template directory not found")

        results: List[Dict[str, object]] = []
        for path in sorted(TEMPLATE_DIR.glob("*.conf.tmpl")):
            results.append(self.render_template_dry_run(path))
        return results

    # -------------------------------------------------------------- reporting
    def generate_report(self, results: Iterable[Dict[str, object]]) -> str:
        results_list = list(results)
        total = len(results_list)
        succeeded = sum(1 for item in results_list if item.get("rendered"))
        failed = total - succeeded

        lines: List[str] = ["# SeaTunnel Template Dry-Run Report", ""]
        lines.extend(
            [
                "## Summary",
                f"- Total templates: {total}",
                f"- Rendered successfully: {succeeded}",
                f"- Failed: {failed}",
                "",
            ]
        )

        failed_items = [item for item in results_list if not item.get("rendered")]
        lines.append("## Failed Templates")
        if failed_items:
            for item in failed_items:
                reason = item.get("error", "unknown error")
                lines.append(f"- **{item['template']}**: {reason}")
        else:
            lines.append("- None 🎉")
        lines.append("")

        lines.append("## Template Details")
        for item in results_list:
            status_icon = "✅" if item.get("rendered") else "❌"
            lines.append(f"### {item['template']} {status_icon}")
            placeholders = ", ".join(item.get("placeholders", [])) or "(none)"
            lines.append(f"- Placeholders: {placeholders}")
            if item.get("rendered"):
                lines.append(f"- Lines: {item.get('line_count', 0)}")
                lines.append(f"- Content length: {item.get('content_length', 0)} characters")
            else:
                lines.append(f"- Error: {item.get('error', 'unknown error')}")
            lines.append("")

        return "\n".join(lines).strip() + "\n"


def render_template(template: Path | str) -> Dict[str, object]:
    """Convenience helper that renders a template using the dry-run renderer."""

    renderer = DryRunRenderer()
    return renderer.render_template_dry_run(Path(template))


__all__ = [
    "DryRunError",
    "DryRunRenderer",
    "_collect_placeholders",
    "render_template",
]
