from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "seatunnel" / "run_job.sh"
ISO_LMP_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "iso.lmp.v1.avsc"
NOAA_GHCND_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "noaa.weather.v1.avsc"
EIA_SERIES_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "eia.series.v1.avsc"
FRED_SERIES_SCHEMA_PATH = REPO_ROOT / "kafka" / "schemas" / "fred.series.v1.avsc"


def extract_value_schema(config_path: Path) -> dict:
    contents = config_path.read_text(encoding="utf-8")
    marker = 'value.schema = """'
    start = contents.index(marker) + len(marker)
    end = contents.index('"""', start)
    schema_str = contents[start:end]
    return json.loads(schema_str)


@pytest.mark.parametrize(
    "job_name",
    [
        (
            "noaa_ghcnd_to_kafka",
            {
                "NOAA_GHCND_TOKEN": "token",
                "NOAA_GHCND_START_DATE": "2024-01-01",
                "NOAA_GHCND_END_DATE": "2024-01-02",
                "NOAA_GHCND_TOPIC": "aurum.ref.noaa.weather.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "NOAA_GHCND_TOKEN",
        ),
        (
            "eia_series_to_kafka",
            {
                "EIA_API_KEY": "key",
                "EIA_SERIES_PATH": "electricity/wholesale/prices/data",
                "EIA_SERIES_ID": "EBA.ALL.D.H",
                "EIA_FREQUENCY": "HOURLY",
                "EIA_TOPIC": "aurum.ref.eia.series.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "EIA_API_KEY",
        ),
        (
            "fred_series_to_kafka",
            {
                "FRED_API_KEY": "fredkey",
                "FRED_SERIES_ID": "DGS10",
                "FRED_FREQUENCY": "DAILY",
                "FRED_SEASONAL_ADJ": "NSA",
                "FRED_TOPIC": "aurum.ref.fred.series.v1",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "FRED_API_KEY",
        ),
        (
            "pjm_lmp_to_kafka",
            {
                "PJM_API_KEY": "pjmtoken",
                "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
                "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
                "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            },
            "PJM_API_KEY",
        ),
        (
            "iso_lmp_kafka_to_timescale",
            {
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
                "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
                "TIMESCALE_USER": "ts",
                "TIMESCALE_PASSWORD": "ts",
            },
            "KAFKA_BOOTSTRAP_SERVERS",
        ),
        (
            "nyiso_lmp_to_kafka",
            {
                "NYISO_URL": "https://example.com/nyiso.csv",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
                "SEATUNNEL_OUTPUT_DIR": str(Path("/tmp")),
            },
            "NYISO_URL",
        ),
    ],
)
def test_run_job_requires_env_vars(job_name: tuple[str, dict[str, str], str], tmp_path: Path) -> None:
    name, base_env, missing_var = job_name
    env = os.environ.copy()
    env.update(base_env)
    env["SEATUNNEL_OUTPUT_DIR"] = str(tmp_path)
    env.pop(missing_var, None)

    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), name, "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert f"Missing required env var {missing_var}" in result.stderr


def test_run_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "NOAA_GHCND_TOKEN": "token",
            "NOAA_GHCND_START_DATE": "2024-01-01",
            "NOAA_GHCND_END_DATE": "2024-01-02",
            "NOAA_GHCND_TOPIC": "aurum.ref.noaa.weather.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
            "NOAA_GHCND_UNIT_CODE": "degC",
        }
    )

    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "noaa_ghcnd_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "noaa_ghcnd_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'format = "avro"' in rendered
    assert 'value.schema.subject' in rendered
    assert '${NOAA_GHCND_DATASET}' not in rendered
    assert 'degC' in rendered
    assert 'noaa_stations' in rendered
    assert "aurum.ref.noaa.weather.v1" in result.stdout

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(NOAA_GHCND_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_eia_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "key",
            "EIA_SERIES_PATH": "electricity/wholesale/prices/data",
            "EIA_SERIES_ID": "EBA.ALL.D.H",
            "EIA_FREQUENCY": "HOURLY",
            "EIA_TOPIC": "aurum.ref.eia.series.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
            "EIA_UNITS": "USD/MWh",
            "EIA_SEASONAL_ADJUSTMENT": "UNKNOWN",
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "eia_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "eia_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.ref.eia' in rendered
    assert '${EIA_SERIES_ID}' not in rendered
    assert 'USD/MWh' in rendered
    assert "COALESCE(source, 'EIA')" in rendered

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(EIA_SERIES_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_fred_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "FRED_API_KEY": "fredkey",
            "FRED_SERIES_ID": "DGS10",
            "FRED_FREQUENCY": "DAILY",
            "FRED_SEASONAL_ADJ": "NSA",
            "FRED_TOPIC": "aurum.ref.fred.series.v1",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "FRED_UNITS": "Percent",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "fred_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "fred_series_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.ref.fred' in rendered
    assert '${FRED_SERIES_ID}' not in rendered
    assert 'Percent' in rendered

    rendered_schema = extract_value_schema(rendered_path)
    expected_schema = json.loads(FRED_SERIES_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert rendered_schema == expected_schema


def test_pjm_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "PJM_API_KEY": "pjmtoken",
            "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
            "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
            "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "pjm_lmp_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "pjm_lmp_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'aurum.iso' in rendered
    assert '${PJM_TOPIC}' not in rendered


def test_iso_lmp_timescale_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "TIMESCALE_JDBC_URL": "jdbc:postgresql://timescale:5432/timeseries",
            "TIMESCALE_USER": "ts",
            "TIMESCALE_PASSWORD": "ts",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "iso_lmp_kafka_to_timescale", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "iso_lmp_kafka_to_timescale.conf"
    rendered = rendered_path.read_text(encoding="utf-8")

    assert 'lmp_enriched' in rendered
    assert 'jdbc:postgresql://timescale:5432/timeseries' in rendered


def test_nyiso_job_renders_config(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "NYISO_URL": "https://example.com/nyiso.csv",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )

    subprocess.run(
        ["bash", str(SCRIPT_PATH), "nyiso_lmp_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    rendered_path = tmp_path / "nyiso_lmp_to_kafka.conf"
    rendered = rendered_path.read_text(encoding="utf-8")
    assert 'aurum.iso.nyiso.lmp.v1' in rendered


def test_iso_jobs_render_canonical_schema(tmp_path: Path) -> None:
    iso_schema = json.loads(ISO_LMP_SCHEMA_PATH.read_text(encoding="utf-8"))

    base_env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
    }

    jobs: list[tuple[str, dict[str, str]]] = [
        (
            "pjm_lmp_to_kafka",
            {
                "PJM_API_KEY": "pjmtoken",
                "PJM_TOPIC": "aurum.iso.pjm.lmp.v1",
                "PJM_INTERVAL_START": "2024-01-01T00:00:00-05:00",
                "PJM_INTERVAL_END": "2024-01-01T01:00:00-05:00",
            },
        ),
        (
            "nyiso_lmp_to_kafka",
            {
                "NYISO_URL": "https://example.com/nyiso.csv",
            },
        ),
    ]

    for job_name, extra_env in jobs:
        env = os.environ.copy()
        env.update(base_env)
        env.update(extra_env)

        subprocess.run(
            ["bash", str(SCRIPT_PATH), job_name, "--render-only"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

        config_path = tmp_path / f"{job_name}.conf"
        rendered_schema = extract_value_schema(config_path)
        assert rendered_schema == iso_schema
