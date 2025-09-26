"""Smoke tests for the seatunnel dry-run CLI."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.seatunnel import dry_run_renderer


@pytest.fixture
def template_dir(tmp_path: Path) -> Path:
    template = tmp_path / "simple.conf.tmpl"
    template.write_text(
        """
env {
  host = ${HOST}
  port = ${PORT:-9000}
}
""".strip()
    )
    return tmp_path


def test_list_templates(capsys, template_dir):
    exit_code = dry_run_renderer.main(["--list", "--template-dirs", str(template_dir)])
    assert exit_code == 0
    output = capsys.readouterr().out.strip().splitlines()
    assert output == ["simple"]


def test_show_vars_identifies_required_placeholders(capsys, template_dir):
    exit_code = dry_run_renderer.main(
        ["--show-vars", "simple", "--template-dirs", str(template_dir)]
    )
    assert exit_code == 0
    output = capsys.readouterr().out.strip().splitlines()
    assert output == ["HOST"]  # PORT has a default and is optional


def test_render_inserts_defaults_and_reports_missing(capsys, template_dir, monkeypatch):
    monkeypatch.setenv("HOST", "localhost")
    monkeypatch.delenv("PORT", raising=False)

    exit_code = dry_run_renderer.main(
        ["--render", "simple", "--template-dirs", str(template_dir)]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "localhost" in captured.out
    assert "9000" in captured.out  # default applied
    assert captured.err == ""


def test_render_fail_on_missing_aborts(tmp_path, template_dir, monkeypatch, capsys):
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.delenv("PORT", raising=False)

    exit_code = dry_run_renderer.main(
        ["--render", "simple", "--template-dirs", str(template_dir), "--fail-on-missing"]
    )
    assert exit_code == 3
    err = capsys.readouterr().err
    assert "Missing" in err
