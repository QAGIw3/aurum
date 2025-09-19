import subprocess
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from aurum.parsers.runner import main as runner_main, parse_files, write_output

TEST_ASOF = date(2025, 9, 12)
FILES_DIR = Path("files")


@pytest.mark.parametrize("filename", ["EOD_PW_20250912_1430.xlsx", "EOD_EUGP_20250912_1430.xlsx"])
def test_parse_files_single(filename: str):
    df = parse_files([FILES_DIR / filename], as_of=TEST_ASOF)
    assert not df.empty
    assert (df["asof_date"] == TEST_ASOF).all()


def test_parse_files_combined(tmp_path):
    df = parse_files(
        [
            FILES_DIR / "EOD_PW_20250912_1430.xlsx",
            FILES_DIR / "EOD_EUGP_20250912_1430.xlsx",
        ],
        as_of=TEST_ASOF,
    )
    assert len(df) > 100
    output = write_output(df, tmp_path, as_of=TEST_ASOF, fmt="csv")
    assert output.exists()
    reloaded = pd.read_csv(output, low_memory=False)
    assert len(reloaded) == len(df)


def test_runner_lakefs_commit(monkeypatch, tmp_path):
    from aurum.parsers import runner

    called = {}

    def fake_commit(df, as_of, args):
        called['ref'] = 'abc123'
        return 'abc123'

    monkeypatch.setattr(runner, '_commit_lakefs', fake_commit)
    monkeypatch.setenv('AURUM_LAKEFS_COMMIT', '1')
    monkeypatch.setenv('AURUM_LAKEFS_REPO', 'demo')

    args = [
        '--as-of', '2025-09-12',
        '--format', 'csv',
        '--output-dir', str(tmp_path),
        str(FILES_DIR / 'EOD_PW_20250912_1430.xlsx'),
    ]
    runner.main(args)
    assert called['ref'] == 'abc123'

    monkeypatch.delenv('AURUM_LAKEFS_COMMIT', raising=False)
    monkeypatch.delenv('AURUM_LAKEFS_REPO', raising=False)


def test_runner_cli(tmp_path, monkeypatch):
    output_dir = tmp_path / "out"
    args = [
        "--as-of",
        TEST_ASOF.isoformat(),
        "--format",
        "csv",
        "--output-dir",
        str(output_dir),
        str(FILES_DIR / "EOD_PW_20250912_1430.xlsx"),
    ]
    rc = runner_main(args)
    assert rc == 0
    artifacts = list(output_dir.glob("*.csv"))
    assert artifacts, "Expected CLI to write output"
    df = pd.read_csv(artifacts[0], low_memory=False)
    assert not df.empty
