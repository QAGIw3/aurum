"""Seatunnel template linter ensuring env placeholders are well formed/documented."""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set

PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
DOC_VAR_PATTERN = re.compile(r"^([A-Z][A-Z0-9_]+)\b")
JOB_CASE_PATTERN = re.compile(r"^([a-z0-9_]+)\)$")
EXPORT_PATTERN = re.compile(r"^export\s+([A-Z][A-Z0-9_]+)")


@dataclass(frozen=True)
class JobEnvMetadata:
    required: Set[str]
    defaults: Set[str]


@dataclass(frozen=True)
class LintIssue:
    template: Path
    level: str
    message: str


@dataclass(frozen=True)
class TemplateDocumentation:
    required: Set[str]
    optional: Set[str]


def _collect_placeholders(text: str) -> Set[str]:
    return set(PLACEHOLDER_PATTERN.findall(text))


def _parse_template_documentation(path: Path) -> TemplateDocumentation:
    required: Set[str] = set()
    optional: Set[str] = set()
    section: Optional[str] = None

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("#"):
            break
        text = line[1:].strip()
        lower = text.lower()
        if "required environment" in lower:
            section = "required"
            continue
        if "optional environment" in lower:
            section = "optional"
            continue
        match = DOC_VAR_PATTERN.match(text)
        if match:
            var = match.group(1)
            if section == "required":
                required.add(var)
            elif section == "optional":
                optional.add(var)

    return TemplateDocumentation(required=required, optional=optional)


def _strip_comment(line: str) -> str:
    if "#" in line:
        return line.split("#", 1)[0].strip()
    return line.strip()


def _parse_array_inline(text: str) -> Optional[Set[str]]:
    # Handle inline definition e.g. REQUIRED_VARS=(A B)
    if "(" not in text or ")" not in text:
        return None
    start = text.index("(") + 1
    end = text.index(")", start)
    token_str = text[start:end]
    tokens = [tok for tok in token_str.split() if tok]
    return set(tokens)


def parse_run_job_metadata(path: Path) -> tuple[Dict[str, JobEnvMetadata], Set[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    global_defaults: Set[str] = set()
    job_metadata: Dict[str, JobEnvMetadata] = {}

    current_job: Optional[str] = None
    required: Set[str] = set()
    defaults: Set[str] = set()

    i = 0
    total = len(lines)
    while i < total:
        line = lines[i]
        stripped = line.strip()

        if not stripped or stripped.startswith('#'):
            i += 1
            continue

        case_match = JOB_CASE_PATTERN.match(stripped)
        if case_match and not stripped.startswith('esac'):
            if current_job:
                existing = job_metadata.get(current_job)
                combined_required = set(required)
                combined_defaults = set(defaults)
                if existing:
                    combined_required |= existing.required
                    combined_defaults |= existing.defaults
                job_metadata[current_job] = JobEnvMetadata(combined_required, combined_defaults)
            current_job = case_match.group(1)
            required = set()
            defaults = set()
            i += 1
            continue

        if stripped == ';;':
            if current_job:
                existing = job_metadata.get(current_job)
                combined_required = set(required)
                combined_defaults = set(defaults)
                if existing:
                    combined_required |= existing.required
                    combined_defaults |= existing.defaults
                job_metadata[current_job] = JobEnvMetadata(combined_required, combined_defaults)
            current_job = None
            required = set()
            defaults = set()
            i += 1
            continue

        if current_job is None:
            export_match = EXPORT_PATTERN.match(stripped)
            if export_match:
                global_defaults.add(export_match.group(1))
            i += 1
            continue

        if stripped.startswith('REQUIRED_VARS='):
            inline = _parse_array_inline(stripped)
            if inline is not None:
                required.update(inline)
                i += 1
                continue
            # multi-line array
            i += 1
            while i < total:
                entry = _strip_comment(lines[i])
                if not entry:
                    i += 1
                    continue
                if entry.startswith(')'):
                    break
                token = entry.rstrip(')')
                token = token.strip()
                if token:
                    required.add(token)
                if entry.endswith(')'):
                    break
                i += 1
            # ensure loop moves beyond closing line
            i += 1
            continue

        export_match = EXPORT_PATTERN.match(stripped)
        if export_match:
            defaults.add(export_match.group(1))

        i += 1

    if current_job:
        existing = job_metadata.get(current_job)
        combined_required = set(required)
        combined_defaults = set(defaults)
        if existing:
            combined_required |= existing.required
            combined_defaults |= existing.defaults
        job_metadata[current_job] = JobEnvMetadata(combined_required, combined_defaults)

    return job_metadata, global_defaults


def lint_template(
    template_path: Path,
    job_metadata: Mapping[str, JobEnvMetadata],
    global_defaults: Set[str],
) -> List[LintIssue]:
    issues: List[LintIssue] = []
    job_name = template_path.name.replace('.conf.tmpl', '')

    if job_name not in job_metadata:
        issues.append(
            LintIssue(
                template=template_path,
                level='warning',
                message=f"Job '{job_name}' not found in run_job.sh metadata",
            )
        )
        meta = JobEnvMetadata(required=set(), defaults=set())
    else:
        meta = job_metadata[job_name]
    job_required = set(meta.required)
    job_defaults = set(meta.defaults)

    template_text = template_path.read_text(encoding='utf-8')
    placeholders = _collect_placeholders(template_text)
    documentation = _parse_template_documentation(template_path)

    # Validate placeholder naming
    for placeholder in sorted(placeholders):
        if not re.fullmatch(r'[A-Z][A-Z0-9_]*', placeholder):
            issues.append(
                LintIssue(
                    template=template_path,
                    level='error',
                    message=f"Placeholder '{placeholder}' must be upper snake case",
                )
            )

    documented = documentation.required | documentation.optional
    for placeholder in sorted(placeholders):
        if placeholder not in documented:
            issues.append(
                LintIssue(
                    template=template_path,
                    level='warning',
                    message=f"Placeholder '{placeholder}' is not documented as required/optional",
                )
            )

    for documented_var in sorted(documented - placeholders):
        issues.append(
            LintIssue(
                template=template_path,
                level='suggestion',
                message=f"Documented variable '{documented_var}' is not used in template",
            )
        )

    # Combine allowed environment variables from metadata
    allowed_sources = job_required | job_defaults | global_defaults
    for placeholder in sorted(placeholders):
        if placeholder in allowed_sources:
            continue
        if placeholder in documented:
            continue
        issues.append(
            LintIssue(
                template=template_path,
                level='error',
                message=(
                    f"Placeholder '{placeholder}' is not provided by run_job.sh "
                    "(neither required nor defaulted)"
                ),
            )
        )

    # Documentation accuracy checks
    for var in sorted(documentation.required - job_required):
        if var in placeholders:
            issues.append(
                LintIssue(
                    template=template_path,
                    level='warning',
                    message=(
                        f"Variable '{var}' documented as required but run_job.sh does not mark it as required"
                    ),
                )
            )

    for var in sorted(job_required - documentation.required):
        if var in placeholders:
            issues.append(
                LintIssue(
                    template=template_path,
                    level='warning',
                    message=(
                        f"Variable '{var}' is required in run_job.sh but not documented as required"
                    ),
                )
            )

    return issues


def lint_all_templates(template_dir: Path, metadata: Mapping[str, JobEnvMetadata], global_defaults: Set[str]) -> List[LintIssue]:
    issues: List[LintIssue] = []
    for template in sorted(template_dir.glob('*.conf.tmpl')):
        issues.extend(lint_template(template, metadata, global_defaults))
    return issues


def _format_issue(issue: LintIssue) -> str:
    return f"[{issue.level.upper()}] {issue.template}: {issue.message}"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Lints SeaTunnel job templates for placeholder hygiene")
    parser.add_argument(
        '--templates',
        type=Path,
        default=Path('seatunnel/jobs/templates'),
        help='Directory containing SeaTunnel templates',
    )
    parser.add_argument(
        '--run-job-script',
        type=Path,
        default=Path('scripts/seatunnel/run_job.sh'),
        help='Path to run_job.sh script for metadata extraction',
    )
    parser.add_argument('--fail-level', default='error', choices=['error', 'warning', 'suggestion'])
    args = parser.parse_args(list(argv) if argv is not None else None)

    metadata, global_defaults = parse_run_job_metadata(args.run_job_script)
    issues = lint_all_templates(args.templates, metadata, global_defaults)

    if not issues:
        return 0

    for issue in issues:
        print(_format_issue(issue))

    level_order = {'error': 3, 'warning': 2, 'suggestion': 1}
    threshold = level_order[args.fail_level]
    highest = max(level_order[issue.level] for issue in issues)
    return 1 if highest >= threshold else 0


if __name__ == '__main__':  # pragma: no cover - CLI entry point
    raise SystemExit(main())
