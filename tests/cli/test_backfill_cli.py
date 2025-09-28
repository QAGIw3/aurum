from __future__ import annotations

from unittest import mock

import pytest

from scripts.airflow import backfill_cli


def test_parse_args_defaults():
    ns = backfill_cli._parse_args(
        ["eia", "2024-01-01", "2024-01-02"]
    )
    assert ns.source == "eia"
    assert ns.concurrency == 3


def test_build_config_validates_dates():
    ns = backfill_cli._parse_args(
        ["eia", "2024-01-03", "2024-01-05"]
    )
    cfg = backfill_cli._build_config(ns)
    assert cfg.start_date.isoformat() == "2024-01-03"


def test_build_config_invalid_dates():
    ns = backfill_cli._parse_args(
        ["eia", "2024-01-05", "2024-01-01"]
    )
    with pytest.raises(ValueError):
        backfill_cli._build_config(ns)


@mock.patch("scripts.airflow.backfill_cli._run_driver")
def test_main_success(mock_run_driver):
    mock_run_driver.return_value = {"failed_jobs": 0}
    exit_code = backfill_cli.main(["eia", "2024-01-01", "2024-01-02", "--dry-run"])
    assert exit_code == 0


@mock.patch("scripts.airflow.backfill_cli._run_driver")
def test_main_failure(mock_run_driver):
    mock_run_driver.return_value = {"failed_jobs": 1}
    exit_code = backfill_cli.main(["eia", "2024-01-01", "2024-01-02"])
    assert exit_code == 1

