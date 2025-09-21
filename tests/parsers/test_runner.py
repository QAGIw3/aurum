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
    monkeypatch.setenv('AURUM_LAKEFS_REPO', 'demo')

    args = [
        '--as-of', '2025-09-12',
        '--format', 'csv',
        '--output-dir', str(tmp_path),
        '--lakefs-commit',
        str(FILES_DIR / 'EOD_PW_20250912_1430.xlsx'),
    ]
    runner.main(args)
    assert called['ref'] == 'abc123'

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
    if df.empty:
        quarantine_dir = output_dir / "quarantine"
        quarantine_files = list(quarantine_dir.glob("*.parquet"))
        assert quarantine_files, "Expected quarantined rows when canonical output is empty"
    else:
        assert not df.empty


def test_parse_files_respects_worker_count(monkeypatch, tmp_path):
    from aurum.parsers import runner

    calls: dict = {}

    def fake_detect_vendor(path: Path) -> str:  # type: ignore[override]
        return "pw"

    def fake_detect_asof(_path: Path) -> date:
        return TEST_ASOF

    def fake_parse(_vendor: str, path: str, detected_asof: date) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "curve_key": [path],
                "asof_date": [detected_asof],
            }
        )

    class DummyExecutor:
        def __init__(self, max_workers: int) -> None:
            calls["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, func, iterable):
            return [func(item) for item in iterable]

    monkeypatch.setattr(runner, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(runner, "detect_vendor", fake_detect_vendor)
    monkeypatch.setattr(runner, "detect_asof_from_workbook", fake_detect_asof)
    monkeypatch.setattr(runner.vendor_curves, "parse", fake_parse)

    paths = [tmp_path / "foo.xlsx", tmp_path / "bar.xlsx"]
    for path in paths:
        path.touch()

    df = runner.parse_files(paths, as_of=TEST_ASOF, max_workers=8)

    assert len(df) == 2
    assert calls["max_workers"] == 2  # bounded by input size


def test_parse_files_handles_large_results(monkeypatch, tmp_path):
    from aurum.parsers import runner

    def fake_detect_vendor(path: Path) -> str:  # type: ignore[override]
        return "pw"

    def fake_detect_asof(_path: Path) -> date:
        return TEST_ASOF

    def fake_parse(_vendor: str, path: str, detected_asof: date) -> pd.DataFrame:
        rows = 5000
        return pd.DataFrame(
            {
                "curve_key": [f"{path}-{i}" for i in range(rows)],
                "asof_date": [detected_asof] * rows,
                "contract_month": ["2025-01-01"] * rows,
            }
        )

    monkeypatch.setattr(runner, "detect_vendor", fake_detect_vendor)
    monkeypatch.setattr(runner, "detect_asof_from_workbook", fake_detect_asof)
    monkeypatch.setattr(runner.vendor_curves, "parse", fake_parse)

    paths = [tmp_path / "alpha.xlsx", tmp_path / "beta.xlsx"]
    for path in paths:
        path.touch()

    df = runner.parse_files(paths, as_of=TEST_ASOF, max_workers=2)

    assert len(df) == 10000
    assert (df["contract_month"].iloc[0]) == date(2025, 1, 1)
