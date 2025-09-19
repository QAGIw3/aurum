from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.kafka.set_compatibility import load_subjects, main


def test_load_subjects(tmp_path: Path) -> None:
    subject_file = tmp_path / "subjects.json"
    subject_file.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")
    subjects = load_subjects(subject_file)
    assert subjects == ["foo"]


def test_main_sets_compatibility(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict]] = []

    def fake_put(url: str, json: dict, timeout: int):  # type: ignore[override]
        calls.append((url, json))

        class _Resp:
            status_code = 200
            text = "OK"

        return _Resp()

    monkeypatch.setattr("scripts.kafka.set_compatibility.requests.put", fake_put)

    subject_file = tmp_path / "subjects.json"
    subject_file.write_text(json.dumps({"foo": "schema.avsc"}), encoding="utf-8")

    assert main([
        "--schema-registry-url",
        "http://registry:8081",
        "--subjects-file",
        str(subject_file),
        "--level",
        "BACKWARD",
    ]) == 0

    assert calls == [("http://registry:8081/config/foo", {"compatibility": "BACKWARD"})]


def test_main_dry_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)

    subject_file = tmp_path / "subjects.json"
    subject_file.write_text(json.dumps({"foo": "schema"}), encoding="utf-8")

    exit_code = main([
        "--subjects-file",
        str(subject_file),
        "--level",
        "FORWARD",
        "--dry-run",
    ])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[dry-run] would set compatibility FORWARD for foo" in captured.out


def test_main_missing_subject(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    subject_file = tmp_path / "subjects.json"
    subject_file.write_text(json.dumps({"foo": "schema"}), encoding="utf-8")

    with pytest.raises(RuntimeError):
        main([
            "--subjects-file",
            str(subject_file),
            "--subject",
            "bar",
        ])
