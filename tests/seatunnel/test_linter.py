from __future__ import annotations

from pathlib import Path

from aurum.seatunnel import linter


def test_parse_run_job_metadata_merges_duplicate_cases(tmp_path: Path) -> None:
    script = tmp_path / "run_job.sh"
    script.write_text(
        """
case "${JOB_NAME}" in
  example_job)
    REQUIRED_VARS=(
      FOO
    )
    export BAR="${BAR:-default}"
    ;;
  other_job)
    ;;
  example_job)
    echo "describe helper"
    ;;
esac
""",
        encoding="utf-8",
    )

    jobs, globals_set = linter.parse_run_job_metadata(script)
    assert globals_set == set()
    assert "example_job" in jobs
    meta = jobs["example_job"]
    assert meta.required == {"FOO"}
    assert "BAR" in meta.defaults


def test_lint_template_detects_missing_doc_and_missing_metadata(tmp_path: Path) -> None:
    template = tmp_path / "example.conf.tmpl"
    template.write_text(
        """# Example template\n\nsource {\n  Http {\n    url = \"${FOO_ENDPOINT}\"\n  }\n}\n""",
        encoding="utf-8",
    )

    # No metadata: expect warning about missing job metadata and documentation warning
    metadata = {}
    issues = linter.lint_template(
        template_path=template,
        job_metadata=metadata,
        global_defaults=set(),
    )
    assert any(issue.level == "error" for issue in issues)

    # Provide metadata and documentation to ensure no errors
    template.write_text(
        """# Example template\n# Required environment variables:\n#   FOO_ENDPOINT - API endpoint\n\nsource {\n  Http {\n    url = \"${FOO_ENDPOINT}\"\n  }\n}\n""",
        encoding="utf-8",
    )
    metadata = {"example": linter.JobEnvMetadata(required={"FOO_ENDPOINT"}, defaults=set())}
    issues = linter.lint_template(template, metadata, global_defaults=set())
    assert not any(issue.level == "error" for issue in issues)
